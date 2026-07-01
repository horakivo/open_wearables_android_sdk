package com.openwearables.health.sdk

import android.content.Context
import android.content.SharedPreferences
import androidx.work.*
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withContext
import kotlinx.serialization.Serializable
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.contentOrNull
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import okhttp3.MediaType.Companion.toMediaType
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody
import okhttp3.RequestBody.Companion.toRequestBody
import okio.Buffer
import okio.ForwardingSink
import okio.GzipSink
import okio.Sink
import okio.buffer
import java.io.File
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

@Serializable
data class TypeSyncProgress(
    val typeIdentifier: String,
    var sentCount: Int = 0,
    var isComplete: Boolean = false,
    var pendingAnchorTimestamp: Long? = null,
    var pendingOlderThan: Long? = null
)

@Serializable
data class SyncState(
    val userKey: String,
    val fullExport: Boolean,
    val createdAt: Long,
    var typeProgress: MutableMap<String, TypeSyncProgress> = mutableMapOf(),
    var totalSentCount: Int = 0,
    var completedTypes: MutableSet<String> = mutableSetOf(),
    var currentTypeIndex: Int = 0
) {
    val hasProgress: Boolean
        get() = totalSentCount > 0 || completedTypes.isNotEmpty()
}

/**
 * Manages health data synchronization.
 *
 * Works exclusively through the [HealthDataProvider] interface — all
 * provider-specific reading and unified-format conversion happens inside
 * the provider. The SyncManager just orchestrates timing, chunking,
 * auth retry, and payload delivery.
 */
class SyncManager(
    private val context: Context,
    private val secureStorage: SecureStorage,
    private val healthProvider: HealthDataProvider,
    private val dispatchers: DispatcherProvider,
    private val logger: (String) -> Unit,
    private val onAuthError: ((Int, String) -> Unit)? = null
) {
    companion object {
        // Health Connect's ReadRecordsRequest.pageSize is capped at 5000.
        const val MAX_PAGE_SIZE = 5000

        // Process-wide: HealthSyncWorker builds its own SyncManager, so a
        // per-instance flag would let a worker sync and a foreground syncNow()
        // run concurrently against the same persisted sync state.
        private val isSyncing = AtomicBoolean(false)

        val sharedHttpClient: OkHttpClient by lazy {
            OkHttpClient.Builder()
                .connectTimeout(30, TimeUnit.SECONDS)
                .readTimeout(120, TimeUnit.SECONDS)
                .writeTimeout(120, TimeUnit.SECONDS)
                .build()
        }
    }

    private val syncPrefs: SharedPreferences by lazy {
        context.getSharedPreferences(StorageKeys.SYNC_PREFS_NAME, Context.MODE_PRIVATE)
    }

    private val json = Json { ignoreUnknownKeys = true }

    private val httpClient = sharedHttpClient

    private val dateFormatter: java.time.format.DateTimeFormatter =
        java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
            .withZone(java.time.ZoneOffset.UTC)

    private val tokenRefreshLock = ReentrantLock()
    private var isRefreshingToken = false

    private val stateMutex = Mutex()
    private var inMemoryState: SyncState? = null

    private var fullSyncStartTime: Long? = null
    private var currentLogsEndpoint: String? = null

    var syncIntervalMinutes: Long = SyncDefaults.SYNC_INTERVAL_MINUTES
        set(value) {
            field = maxOf(value, SyncDefaults.MIN_SYNC_INTERVAL_MINUTES)
        }

    // MARK: - Sync Start Timestamp

    /**
     * Computes the earliest epoch-ms timestamp to sync from, based on persisted `syncDaysBack`.
     * Returns the start of the day (midnight local time) that many days ago,
     * or `null` if full sync (no limit) is configured.
     */
    private fun syncStartTimestamp(): Long? {
        val daysBack = secureStorage.getSyncDaysBack()
        if (daysBack <= 0) return null
        val cal = java.util.Calendar.getInstance()
        cal.set(java.util.Calendar.HOUR_OF_DAY, 0)
        cal.set(java.util.Calendar.MINUTE, 0)
        cal.set(java.util.Calendar.SECOND, 0)
        cal.set(java.util.Calendar.MILLISECOND, 0)
        cal.add(java.util.Calendar.DAY_OF_YEAR, -daysBack)
        return cal.timeInMillis
    }

    // MARK: - User Key

    private fun userKey(): String {
        val userId = secureStorage.getUserId()
        return if (userId.isNullOrEmpty()) "user.none" else "user.$userId"
    }

    // MARK: - Auth

    private fun bearerValue(token: String): String =
        if (token.startsWith("Bearer ")) token else "Bearer $token"

    private fun applyAuth(requestBuilder: Request.Builder) {
        val accessToken = secureStorage.getAccessToken()
        val apiKey = secureStorage.getApiKey()
        if (accessToken != null) {
            requestBuilder.header("Authorization", bearerValue(accessToken))
        } else if (apiKey != null) {
            requestBuilder.header("X-Open-Wearables-API-Key", apiKey)
        }
    }

    private fun applyAuth(requestBuilder: Request.Builder, credential: String) {
        if (secureStorage.isApiKeyAuth) {
            requestBuilder.header("X-Open-Wearables-API-Key", credential)
        } else {
            requestBuilder.header("Authorization", bearerValue(credential))
        }
    }

    private fun emitAuthError(statusCode: Int) {
        logger("Auth error: HTTP $statusCode - token invalid")
        onAuthError?.invoke(statusCode, "Unauthorized - please re-authenticate")
    }

    fun retryOutboxIfPossible() { /* reserved for future use */ }

    // MARK: - Background Sync

    suspend fun startBackgroundSync(host: String, customSyncUrl: String?): Boolean {
        schedulePeriodicSync(host, customSyncUrl)
        return true
    }

    private fun schedulePeriodicSync(host: String, customSyncUrl: String?) {
        val constraints = Constraints.Builder()
            .setRequiredNetworkType(NetworkType.CONNECTED)
            .build()

        val work = PeriodicWorkRequestBuilder<HealthSyncWorker>(
            syncIntervalMinutes, TimeUnit.MINUTES
        )
            .setConstraints(constraints)
            .setInputData(workDataOf(
                HealthSyncWorker.KEY_HOST to host,
                HealthSyncWorker.KEY_CUSTOM_SYNC_URL to customSyncUrl
            ))
            .build()

        WorkManager.getInstance(context).enqueueUniquePeriodicWork(
            SyncDefaults.WORK_NAME_PERIODIC,
            ExistingPeriodicWorkPolicy.UPDATE,
            work
        )
        logger("Scheduled periodic sync every $syncIntervalMinutes minute(s)")
    }

    fun scheduleExpeditedSync(host: String, customSyncUrl: String?) {
        val constraints = Constraints.Builder()
            .setRequiredNetworkType(NetworkType.CONNECTED)
            .build()

        val expeditedWork = OneTimeWorkRequestBuilder<HealthSyncWorker>()
            .setConstraints(constraints)
            .setExpedited(OutOfQuotaPolicy.RUN_AS_NON_EXPEDITED_WORK_REQUEST)
            .setInputData(workDataOf(
                HealthSyncWorker.KEY_HOST to host,
                HealthSyncWorker.KEY_CUSTOM_SYNC_URL to customSyncUrl
            ))
            .build()

        // KEEP: onBackground() schedules this on every backgrounding; REPLACE
        // would cancel a sync that is already running instead of ignoring it.
        WorkManager.getInstance(context).enqueueUniqueWork(
            SyncDefaults.WORK_NAME_EXPEDITED, ExistingWorkPolicy.KEEP, expeditedWork
        )
        logger("Scheduled expedited sync")
    }

    suspend fun stopBackgroundSync() {
        WorkManager.getInstance(context).cancelUniqueWork(SyncDefaults.WORK_NAME_PERIODIC)
        logger("Cancelled periodic sync")
    }

    // MARK: - Sync Now

    suspend fun syncNow(host: String, customSyncUrl: String?, fullExport: Boolean) {
        if (!isSyncing.compareAndSet(false, true)) {
            logger("Sync already in progress")
            return
        }

        try {
            val userId = secureStorage.getUserId()
            if (userId == null || !secureStorage.hasAuth) {
                logger("No credentials for sync")
                return
            }

            val endpoint = buildSyncEndpoint(host, customSyncUrl, userId)

            val trackedTypes = healthProvider.getTrackedTypes().toList()
            if (trackedTypes.isEmpty()) {
                logger("No tracked types configured")
                return
            }

            val existingState = stateMutex.withLock { loadSyncStateFromDisk() }
            val isResuming = existingState != null && existingState.hasProgress

            val floor = syncStartTimestamp()
            val floorLabel = if (floor != null) "since ${java.time.Instant.ofEpochMilli(floor)}" else "full history"

            val effectiveFullExport: Boolean
            if (isResuming) {
                effectiveFullExport = existingState!!.fullExport
                logger("Sync: resuming (${existingState.totalSentCount} sent, ${existingState.completedTypes.size}/${trackedTypes.size} types done, $floorLabel)")
                stateMutex.withLock { inMemoryState = existingState }
            } else {
                effectiveFullExport = fullExport || !hasCompletedInitialSync()
                val mode = if (effectiveFullExport) "full export" else "incremental"
                logger("Sync: starting ($mode, ${trackedTypes.size} types, ${healthProvider.providerName}, $floorLabel)")
                stateMutex.withLock {
                    inMemoryState = SyncState(
                        userKey = userKey(), fullExport = effectiveFullExport,
                        createdAt = System.currentTimeMillis()
                    )
                    persistStateToDisk()
                }
            }

            val syncStartTime = System.currentTimeMillis()
            val logsEndpoint = buildLogsEndpoint(host, userId)
            currentLogsEndpoint = logsEndpoint

            if (effectiveFullExport) {
                fullSyncStartTime = syncStartTime
                // TEMP: disabled to measure its impact on sync start latency.
                // countRecordsForTypes does a full read+convert pass over the
                // entire history before the first upload round.
                // try {
                //     logger("Counting records for sync start log...")
                //     val typeCounts = countRecordsForTypes(trackedTypes, floor)
                //     logger("Sending sync start log to $logsEndpoint")
                //     sendSyncStartLog(logsEndpoint, trackedTypes, typeCounts, floor)
                // } catch (e: Exception) {
                //     logger("Sync start log failed: ${e.message}")
                // }
            }

            val result = processTypesRoundRobin(trackedTypes, effectiveFullExport, endpoint)

            if (effectiveFullExport && !result.completed) {
                val durationMs = (System.currentTimeMillis() - syncStartTime).toInt()
                for (typeResult in result.typeResults) {
                    if (!typeResult.success && typeResult.recordCount > 0) {
                        sendTypeEndLog(logsEndpoint, typeResult.type, false, typeResult.recordCount, durationMs)
                    }
                }
            }

            fullSyncStartTime = null
            currentLogsEndpoint = null
        } finally {
            isSyncing.set(false)
        }
    }

    // MARK: - Round-Robin Sync Orchestration (combined payloads)

    private data class TypeResult(val type: String, val success: Boolean, val recordCount: Int)
    private data class RoundRobinResult(val completed: Boolean, val totalRecords: Int, val typeResults: List<TypeResult>)

    private data class FetchResult(
        val type: String,
        val data: UnifiedHealthData = UnifiedHealthData(),
        val count: Int = 0,
        // Parent records returned by this page (before expansion); drives the
        // observed-expansion estimate used to size the next page.
        val recordCount: Int = 0,
        val nextCursor: Long? = null,
        val anchorTimestamp: Long? = null,
        val isDone: Boolean = false
    )

    // Initial guess for how many child records one parent expands into on convert.
    // Used only for the FIRST round of a type, before we've measured a real ratio;
    // after that the page limit is sized from observed expansion (see the round loop).
    // These constants are unreliable across devices (a HeartRateRecord may hold ~1 or
    // ~40 samples), which is exactly why they're only a seed.
    private fun seedExpansion(type: String): Double = when (type) {
        "sleep" -> SyncDefaults.SLEEP_STAGES_PER_SESSION_ESTIMATE.toDouble()
        "heartRate" -> SyncDefaults.HEART_RATE_SAMPLES_PER_RECORD_ESTIMATE.toDouble()
        else -> 1.0
    }


    private suspend fun processTypesRoundRobin(
        types: List<String>,
        fullExport: Boolean,
        endpoint: String
    ): RoundRobinResult {
        val olderThanCursors = mutableMapOf<String, Long?>()
        val anchorCursors = mutableMapOf<String, Long?>()
        val completedTypes = mutableSetOf<String>()

        stateMutex.withLock {
            val state = inMemoryState
            if (state != null) {
                completedTypes.addAll(state.completedTypes)
                for ((id, progress) in state.typeProgress) {
                    if (!progress.isComplete) {
                        progress.pendingOlderThan?.let { olderThanCursors[id] = it }
                        progress.pendingAnchorTimestamp?.let { anchorCursors[id] = it }
                    }
                }
            }
        }

        if (!fullExport) {
            val anchors = loadAnchors()
            val floor = syncStartTimestamp()
            for (type in types) {
                if (!completedTypes.contains(type) && !anchorCursors.containsKey(type)) {
                    val storedAnchor = anchors[type]
                    val anchor = when {
                        storedAnchor != null && floor != null -> maxOf(storedAnchor, floor)
                        storedAnchor != null -> storedAnchor
                        else -> floor
                    }
                    anchorCursors[type] = anchor
                }
            }
        }

        // Observed samples-per-parent for each type, learned from each full page and
        // used to size the next page. Replaces the fixed expansion constants, which are
        // wrong in both directions depending on the device. In-memory for this sync
        // session; reseeds (and reconverges within a round) on resume.
        val observedExpansion = mutableMapOf<String, Double>()

        while (true) {
            val incompleteTypes = types.filter { !completedTypes.contains(it) }
            if (incompleteTypes.isEmpty()) break

            // Phase 1: Fetch one chunk from each type (no network yet)
            val roundResults = mutableListOf<FetchResult>()
            val fetchMsByType = mutableMapOf<String, Long>()

            for (type in incompleteTypes) {
                // `pageLimit` is a Health Connect pageSize counting PARENT records. Size it so
                // the EXPANDED payload (parents × observed samples-per-parent) targets
                // READ_TARGET_EXPANDED_ITEMS, then cap at the Health Connect page limit. This
                // adapts to how each provider writes a series: Garmin stores ~1 sample per
                // HeartRateRecord (expansion≈1 → read up to MAX_PAGE_SIZE parents), while
                // Wear-style providers pack ~25-40 samples per record (expansion high → ~hundreds
                // of parents). The read is decoupled from the upload, which is re-chunked to
                // CHUNK_SIZE by sub-batching, so a large expanded page is fine.
                val expansion = observedExpansion[type] ?: seedExpansion(type)
                val pageLimit = (SyncDefaults.READ_TARGET_EXPANDED_ITEMS / expansion).toInt().coerceIn(1, MAX_PAGE_SIZE)

                val fetchStart = System.nanoTime()
                val result = if (fullExport) {
                    fetchOneChunkNewestFirst(type, olderThanCursors[type], pageLimit)
                } else {
                    fetchOneChunkIncremental(type, anchorCursors[type], pageLimit)
                }
                fetchMsByType[type] = (System.nanoTime() - fetchStart) / 1_000_000

                roundResults.add(result)

                if (result.isDone) {
                    completedTypes.add(type)
                } else {
                    if (fullExport) olderThanCursors[type] = result.nextCursor
                    else anchorCursors[type] = result.nextCursor
                }

                // Learn the real expansion from a FULL page and use it directly: a provider
                // writes a series consistently, so the measured samples-per-parent is stable
                // and the next page can jump straight to the right size (converges in one
                // round, vs several with smoothing). Only full pages update it — the tail
                // page is partial and its ratio would skew the estimate.
                if (result.recordCount >= pageLimit && result.recordCount > 0) {
                    observedExpansion[type] = result.count.toDouble() / result.recordCount
                }
            }

            // Phase 2: Merge all fetched data into one combined payload
            val mergedData = UnifiedHealthData(
                records = roundResults.flatMap { it.data.records },
                workouts = roundResults.flatMap { it.data.workouts },
                sleep = roundResults.flatMap { it.data.sleep }
            )

            var uploadMs = 0L
            if (!mergedData.isEmpty) {
                // A dense type's read page (heartRate) can expand well past CHUNK_SIZE,
                // so split the merged round into bounded sub-batches and POST each. This
                // keeps payload size and per-request upload time bounded regardless of how
                // large the read pages were. Resume relies on the server upserting by
                // record id: a mid-round failure re-reads and re-sends the whole round
                // (Phase 3 progress is committed only after all batches succeed), so
                // already-sent batches arrive again — at-least-once, matching the outbox.
                val batches = chunkUnifiedData(mergedData, SyncDefaults.CHUNK_SIZE)
                for ((i, batch) in batches.withIndex()) {
                    logPayloadSummary(batch)
                    val uploadStart = System.nanoTime()
                    val sendResult = sendPayload(endpoint, batch)
                    uploadMs += (System.nanoTime() - uploadStart) / 1_000_000

                    if (!sendResult.success) {
                        val reason = sendResult.statusCode?.let { "HTTP $it" } ?: "network error"
                        logger("Combined round failed on batch ${i + 1}/${batches.size} ($reason)")
                        val (totalSent, typeResults) = stateMutex.withLock {
                            persistStateToDisk()
                            val state = inMemoryState
                            val sent = state?.totalSentCount ?: 0
                            val results = types.map { type ->
                                TypeResult(type, state?.completedTypes?.contains(type) == true, state?.typeProgress?.get(type)?.sentCount ?: 0)
                            }
                            Pair(sent, results)
                        }
                        return RoundRobinResult(false, totalSent, typeResults)
                    }

                    logger("Round sent batch ${i + 1}/${batches.size}: ${batch.totalCount} items (${sendResult.payloadSizeKb} KB) -> ${sendResult.statusCode}")
                }

                logger("Round sent: ${mergedData.totalCount} items in ${batches.size} batch(es)")
            }

            // Phase 3: Update progress for all types in this round
            val newlyCompletedTypes = mutableListOf<Pair<String, Int>>()
            val persistStart = System.nanoTime()
            stateMutex.withLock {
                for (result in roundResults) {
                    updateInMemoryProgress(result.type, result.count, isComplete = result.isDone, anchorTimestamp = result.anchorTimestamp)
                    if (fullExport && !result.isDone) {
                        inMemoryState?.typeProgress?.get(result.type)?.pendingOlderThan = result.nextCursor
                    }
                    if (result.isDone) {
                        newlyCompletedTypes.add(result.type to (inMemoryState?.typeProgress?.get(result.type)?.sentCount ?: 0))
                    }
                }
                persistStateToDisk()
            }
            val persistMs = (System.nanoTime() - persistStart) / 1_000_000

            // Per-round timing breakdown: read (Health Connect IPC + convert, summed
            // across types since Phase 1 is sequential) vs upload (network round-trip)
            // vs persist (state mutex + disk write). Read+upload don't overlap.
            val totalFetchMs = fetchMsByType.values.sum()
            val slowest = fetchMsByType.entries.sortedByDescending { it.value }.take(3)
                .joinToString(", ") { "${it.key}=${it.value}ms" }
            logger("Round timing: read ${totalFetchMs}ms (top: $slowest), upload ${uploadMs}ms, persist ${persistMs}ms")

            val startTime = fullSyncStartTime
            val logEndpoint = currentLogsEndpoint
            if (fullExport && startTime != null && logEndpoint != null && newlyCompletedTypes.isNotEmpty()) {
                for ((type, count) in newlyCompletedTypes) {
                    if (count > 0) {
                        val durationMs = (System.currentTimeMillis() - startTime).toInt()
                        logger("Sending sync end log: ${payloadTypeName(type)} ($count records, ${durationMs}ms)")
                        try {
                            sendTypeEndLog(logEndpoint, type, true, count, durationMs)
                        } catch (e: Exception) {
                            logger("Type end log failed for $type: ${e.message}")
                        }
                    }
                }
            }
        }

        val (totalSent, typeResults) = stateMutex.withLock {
            val state = inMemoryState
            val sent = state?.totalSentCount ?: 0
            val results = types.map { type ->
                TypeResult(type, state?.completedTypes?.contains(type) == true, state?.typeProgress?.get(type)?.sentCount ?: 0)
            }
            if (state?.fullExport == true) markFullExportDone()
            if (state != null) logger("Sync: complete ($sent items, ${state.completedTypes.size} types)")
            clearSyncSessionInternal()
            Pair(sent, results)
        }
        return RoundRobinResult(true, totalSent, typeResults)
    }

    // MARK: - Fetch-Only Chunk Processors (no network)

    private suspend fun fetchOneChunkNewestFirst(
        type: String,
        olderThan: Long?,
        limit: Int
    ): FetchResult {
        val floor = syncStartTimestamp()
        val floorIso = floor?.let { UnifiedTimestamp.fromEpochMs(it) }

        logger("  $type: querying (newest first, limit=$limit${olderThan?.let { ", olderThan=${java.time.Instant.ofEpochMilli(it)}" } ?: ""})...")

        val result = healthProvider.readDataDescending(type, olderThan, limit)

        if (result.data.isEmpty) {
            logger("  $type: all data sent (newest first)")
            return FetchResult(type = type, isDone = true)
        }

        val reachedFloor = floor != null && result.minTimestamp != null && result.minTimestamp <= floor
        // Compare the parent record count (what pageSize bounds) — not the expanded
        // sample/stage count — so this is unit-correct for series/session types (sleep).
        val isLastChunk = result.recordCount < limit || reachedFloor

        logger("  $type: termination check recordCount=${result.recordCount} < limit=$limit? ${result.recordCount < limit}; reachedFloor=$reachedFloor -> isDone=$isLastChunk; minTs=${result.minTimestamp?.let { java.time.Instant.ofEpochMilli(it) }}, nextOlderThan=${if (isLastChunk) "null" else result.minTimestamp?.let { java.time.Instant.ofEpochMilli(it) }.toString()}")

        val data = if (reachedFloor && floorIso != null) result.data.filterSince(floorIso) else result.data

        if (data.isEmpty) {
            logger("  $type: all data within range sent")
            return FetchResult(type = type, isDone = true)
        }

        val anchorTs = if (olderThan == null) result.maxTimestamp else null
        val nextOlderThan = if (isLastChunk) null else result.minTimestamp

        logger("  $type: ${data.totalCount} samples (newest first)")

        return FetchResult(
            type = type, data = data, count = data.totalCount, recordCount = result.recordCount,
            nextCursor = nextOlderThan, anchorTimestamp = anchorTs, isDone = isLastChunk
        )
    }

    private suspend fun fetchOneChunkIncremental(
        type: String,
        anchor: Long?,
        limit: Int
    ): FetchResult {
        logger("  $type: querying (limit=$limit)...")

        val result = healthProvider.readData(type, anchor, limit)

        if (result.data.isEmpty) {
            logger("  $type: no new data")
            return FetchResult(type = type, isDone = true)
        }

        val count = result.data.totalCount
        // Parent record count, not expanded count, for a unit-correct last-page check.
        val isLastChunk = result.recordCount < limit

        logger("  $type: $count samples; termination recordCount=${result.recordCount} < limit=$limit? $isLastChunk -> isDone=$isLastChunk, nextAnchor=${result.maxTimestamp?.let { java.time.Instant.ofEpochMilli(it) }}")

        return FetchResult(
            type = type, data = result.data, count = count, recordCount = result.recordCount,
            nextCursor = result.maxTimestamp, anchorTimestamp = result.maxTimestamp,
            isDone = isLastChunk
        )
    }

    private fun updateInMemoryProgress(typeIdentifier: String, sentInChunk: Int, isComplete: Boolean, anchorTimestamp: Long?) {
        val state = inMemoryState ?: return
        val progress = state.typeProgress.getOrPut(typeIdentifier) { TypeSyncProgress(typeIdentifier) }
        progress.sentCount += sentInChunk
        progress.isComplete = isComplete
        if (anchorTimestamp != null) progress.pendingAnchorTimestamp = anchorTimestamp
        state.totalSentCount += sentInChunk
        if (isComplete) {
            state.completedTypes.add(typeIdentifier)
            progress.pendingAnchorTimestamp?.let { saveAnchor(typeIdentifier, it) }
        }
    }

    // MARK: - Payload (unified)

    // Split a merged round into payloads of at most [maxItems] expanded items
    // (records + workouts + sleep), so a large dense-series read page is uploaded as
    // several bounded POSTs instead of one oversized request. Items keep their original
    // order; each batch fills records, then workouts, then sleep up to the budget.
    private fun chunkUnifiedData(data: UnifiedHealthData, maxItems: Int): List<UnifiedHealthData> {
        if (data.totalCount <= maxItems) return listOf(data)

        val batches = mutableListOf<UnifiedHealthData>()
        var records = data.records
        var workouts = data.workouts
        var sleep = data.sleep
        while (records.isNotEmpty() || workouts.isNotEmpty() || sleep.isNotEmpty()) {
            var budget = maxItems
            val rTake = minOf(budget, records.size); budget -= rTake
            val wTake = minOf(budget, workouts.size); budget -= wTake
            val sTake = minOf(budget, sleep.size)
            batches.add(UnifiedHealthData(records.take(rTake), workouts.take(wTake), sleep.take(sTake)))
            records = records.drop(rTake)
            workouts = workouts.drop(wTake)
            sleep = sleep.drop(sTake)
        }
        return batches
    }

    // MARK: - Payload Summary Logging

    private fun logPayloadSummary(data: UnifiedHealthData) {
        val typeCounts = mutableMapOf<String, Int>()

        for (r in data.records) {
            typeCounts[r.type] = (typeCounts[r.type] ?: 0) + 1
        }
        if (data.sleep.isNotEmpty()) {
            typeCounts["sleep"] = data.sleep.size
        }
        if (data.workouts.isNotEmpty()) {
            typeCounts["workouts"] = data.workouts.size
        }

        val totalCount = typeCounts.values.sum()
        val breakdown = typeCounts.entries
            .sortedByDescending { it.value }
            .joinToString(", ") { "${it.key}: ${it.value}" }

        logger("Sending $totalCount items ($breakdown)")
    }

    // MARK: - Token Refresh

    private enum class TokenRefreshResult {
        SUCCESS, AUTH_FAILURE, NETWORK_ERROR
    }

    private suspend fun attemptTokenRefresh(): TokenRefreshResult = withContext(dispatchers.io) {
        tokenRefreshLock.withLock { isRefreshingToken = true }
        try {
            val refreshToken = secureStorage.getRefreshToken()
            val apiBaseUrl = secureStorage.apiBaseUrl
            if (refreshToken == null || apiBaseUrl == null) {
                logger("Token refresh: missing credentials")
                return@withContext TokenRefreshResult.AUTH_FAILURE
            }

            val url = "$apiBaseUrl/token/refresh"
            val bodyMap = mapOf("refresh_token" to refreshToken)
            val body = json.encodeToString(bodyMap)
            val request = Request.Builder()
                .url(url)
                .post(body.toRequestBody("application/json".toMediaType()))
                .header("Content-Type", "application/json")
                .build()

            val response = httpClient.newCall(request).execute()
            val responseBody = response.body?.string()

            if (response.code in 401..403) {
                logger("Token refresh rejected: HTTP ${response.code}")
                return@withContext TokenRefreshResult.AUTH_FAILURE
            }

            if (response.isSuccessful && responseBody != null) {
                val jsonObj = json.parseToJsonElement(responseBody).jsonObject
                val newAccessToken = jsonObj["access_token"]?.jsonPrimitive?.contentOrNull
                val newRefreshToken = jsonObj["refresh_token"]?.jsonPrimitive?.contentOrNull
                if (newAccessToken != null) {
                    secureStorage.updateTokens(newAccessToken, newRefreshToken)
                    logger("Token refresh: HTTP ${response.code}")
                    return@withContext TokenRefreshResult.SUCCESS
                } else {
                    logger("Token refresh failed: HTTP ${response.code} (no access_token in response)")
                }
            } else {
                logger("Token refresh failed: HTTP ${response.code}")
            }
            TokenRefreshResult.NETWORK_ERROR
        } catch (e: Exception) {
            logger("Token refresh failed: ${e.javaClass.simpleName}: ${e.message}")
            TokenRefreshResult.NETWORK_ERROR
        } finally {
            tokenRefreshLock.withLock { isRefreshingToken = false }
        }
    }

    // MARK: - Send with Auth Retry

    private data class SendResult(val success: Boolean, val statusCode: Int?, val payloadSizeKb: Int)

    private suspend fun sendPayload(endpoint: String, data: UnifiedHealthData): SendResult {
        val body = streamingUnifiedBody(data)
        return sendWithBody(endpoint, body)
    }

    private fun gzipBody(body: RequestBody): RequestBody = object : RequestBody() {
        override fun contentType() = body.contentType()
        override fun contentLength() = -1L
        override fun writeTo(sink: okio.BufferedSink) {
            // Counters straddle the GzipSink: `compressed` sees the bytes that hit the
            // socket (gzip output), `raw` sees the JSON the inner body produces (gzip
            // input). Logs the on-the-wire savings and confirms compression is active.
            val compressed = CountingSink(sink)
            val gzip = GzipSink(compressed)
            val raw = CountingSink(gzip)
            val buffered = raw.buffer()
            body.writeTo(buffered)
            buffered.close()
            val rawKb = raw.bytesWritten / 1024
            val compKb = compressed.bytesWritten / 1024
            val pct = if (raw.bytesWritten > 0) 100 * compressed.bytesWritten / raw.bytesWritten else 0
            logger("Payload gzip: $rawKb KB -> $compKb KB ($pct% of original)")
        }
    }

    /** Forwarding sink that tallies the bytes passing through it. */
    private class CountingSink(delegate: Sink) : ForwardingSink(delegate) {
        var bytesWritten = 0L
            private set

        override fun write(source: Buffer, byteCount: Long) {
            super.write(source, byteCount)
            bytesWritten += byteCount
        }
    }

    /**
     * OkHttp RequestBody that streams the sync payload straight from the typed
     * [UnifiedHealthData] to the network via [writeUnifiedPayload] — no per-record `Map` is
     * materialized (see UnifiedPayload.kt). Memory stays O(depth). The syncTimestamp is
     * captured once here (not inside writeTo), so it's stable if the body is re-sent on retry.
     */
    private fun streamingUnifiedBody(data: UnifiedHealthData): RequestBody {
        val provider = healthProvider.providerId
        val syncTimestamp = UnifiedTimestamp.fromEpochMs(System.currentTimeMillis())
        return object : okhttp3.RequestBody() {
            override fun contentType() = "application/json".toMediaType()
            override fun writeTo(sink: okio.BufferedSink) {
                val writer = android.util.JsonWriter(
                    java.io.OutputStreamWriter(sink.outputStream(), Charsets.UTF_8)
                )
                writeUnifiedPayload(AndroidJsonWriterSink(writer), provider, SyncDefaults.SDK_VERSION, syncTimestamp, data)
                writer.flush()
            }
        }
    }

    /** Adapts [JsonSink] onto android.util.JsonWriter (thin 1:1 delegation). */
    private class AndroidJsonWriterSink(private val w: android.util.JsonWriter) : JsonSink {
        override fun beginObject() { w.beginObject() }
        override fun endObject() { w.endObject() }
        override fun beginArray() { w.beginArray() }
        override fun endArray() { w.endArray() }
        override fun name(name: String) { w.name(name) }
        override fun value(value: String?) { w.value(value) }
        override fun value(value: Double) { w.value(value) }
        override fun value(value: Long) { w.value(value) }
        override fun value(value: Boolean) { w.value(value) }
        override fun nullValue() { w.nullValue() }
    }

    /** Streams a small map (e.g. a sync-end log) to JSON. For the health payload use
     *  [streamingUnifiedBody], which avoids per-record Map materialization. */
    private fun streamingJsonBody(payload: Map<String, Any>): RequestBody {
        return object : okhttp3.RequestBody() {
            override fun contentType() = "application/json".toMediaType()
            override fun writeTo(sink: okio.BufferedSink) {
                val writer = android.util.JsonWriter(
                    java.io.OutputStreamWriter(sink.outputStream(), Charsets.UTF_8)
                )
                writeJsonValue(AndroidJsonWriterSink(writer), payload)
                writer.flush()
            }
        }
    }

    private suspend fun sendWithBody(endpoint: String, body: okhttp3.RequestBody): SendResult =
        withContext(dispatchers.io) {
            try {
                val requestBuilder = Request.Builder()
                    .url(endpoint)
                    .post(gzipBody(body))
                    .header("Content-Type", "application/json")
                    .header("Content-Encoding", "gzip")
                applyAuth(requestBuilder)

                val response = httpClient.newCall(requestBuilder.build()).execute()
                val sizeKb = (response.header("Content-Length")?.toLongOrNull() ?: 0L) / 1024
                val code = response.code
                response.body?.close()

                if (response.isSuccessful) return@withContext SendResult(true, code, sizeKb.toInt())
                if (code == 401) {
                    logger("Got 401, refreshing token...")
                    val retryOk = handle401(endpoint, body)
                    return@withContext SendResult(retryOk, if (retryOk) 200 else 401, sizeKb.toInt())
                }

                SendResult(false, code, sizeKb.toInt())
            } catch (e: Exception) {
                logger("Upload error: ${e.javaClass.simpleName}: ${e.message}")
                SendResult(false, null, 0)
            }
        }

    private suspend fun handle401(endpoint: String, body: okhttp3.RequestBody): Boolean {
        if (secureStorage.isApiKeyAuth) {
            emitAuthError(401)
            return false
        }

        when (attemptTokenRefresh()) {
            TokenRefreshResult.SUCCESS -> {
                val newCredential = secureStorage.authCredential
                if (newCredential != null) {
                    logger("Token refreshed, retrying...")
                    return try {
                        val retryBuilder = Request.Builder()
                            .url(endpoint)
                            .post(gzipBody(body))
                            .header("Content-Type", "application/json")
                            .header("Content-Encoding", "gzip")
                        applyAuth(retryBuilder, newCredential)

                        val retryResponse = httpClient.newCall(retryBuilder.build()).execute()
                        val retryCode = retryResponse.code
                        retryResponse.body?.close()
                        if (retryResponse.isSuccessful) {
                            logger("Retry: HTTP $retryCode")
                            true
                        } else {
                            logger("Retry failed: HTTP $retryCode")
                            if (retryCode in 401..403) emitAuthError(401)
                            false
                        }
                    } catch (e: Exception) {
                        logger("Retry failed: ${e.message}")
                        false
                    }
                }
                return false
            }
            TokenRefreshResult.AUTH_FAILURE -> {
                emitAuthError(401)
                return false
            }
            TokenRefreshResult.NETWORK_ERROR -> {
                logger("Token refresh failed (network) - will retry later")
                return false
            }
        }
    }

    // MARK: - Sync Logging

    private fun payloadTypeName(trackedTypeId: String): String = when (trackedTypeId) {
        "steps" -> "STEP_COUNT"
        "heartRate" -> "HEART_RATE"
        "restingHeartRate" -> "RESTING_HEART_RATE"
        "heartRateVariabilitySDNN" -> "HEART_RATE_VARIABILITY"
        "oxygenSaturation" -> "OXYGEN_SATURATION"
        "bloodPressure", "bloodPressureSystolic" -> "BLOOD_PRESSURE_SYSTOLIC"
        "bloodPressureDiastolic" -> "BLOOD_PRESSURE_DIASTOLIC"
        "bloodGlucose" -> "BLOOD_GLUCOSE"
        "activeEnergy" -> "ACTIVE_CALORIES_BURNED"
        "basalEnergy" -> "BASAL_METABOLIC_RATE"
        "bodyTemperature" -> "BODY_TEMPERATURE"
        "bodyMass" -> "WEIGHT"
        "height" -> "HEIGHT"
        "bodyFatPercentage" -> "BODY_FAT"
        "leanBodyMass" -> "LEAN_BODY_MASS"
        "flightsClimbed" -> "FLOORS_CLIMBED"
        "distanceWalkingRunning", "distanceCycling" -> "DISTANCE"
        "water", "dietaryWater" -> "HYDRATION"
        "vo2Max" -> "VO2_MAX"
        "respiratoryRate" -> "RESPIRATORY_RATE"
        "workout" -> "WORKOUT"
        "sleep" -> "SLEEP"
        else -> trackedTypeId.uppercase()
    }

    private fun buildLogsEndpoint(host: String, userId: String): String {
        val h = if (host.endsWith("/")) host.dropLast(1) else host
        return "$h/api/v1/sdk/users/$userId/logs"
    }

    private fun collectDeviceState(): Map<String, Any> {
        val batteryManager = context.getSystemService(Context.BATTERY_SERVICE) as? android.os.BatteryManager
        val powerManager = context.getSystemService(Context.POWER_SERVICE) as? android.os.PowerManager
        val activityManager = context.getSystemService(Context.ACTIVITY_SERVICE) as? android.app.ActivityManager

        val batteryLevel = batteryManager
            ?.getIntProperty(android.os.BatteryManager.BATTERY_PROPERTY_CAPACITY)
            ?.let { it / 100f } ?: -1f

        val batteryIntent = try {
            context.registerReceiver(null, android.content.IntentFilter(android.content.Intent.ACTION_BATTERY_CHANGED))
        } catch (_: Exception) { null }
        val status = batteryIntent?.getIntExtra(android.os.BatteryManager.EXTRA_STATUS, -1) ?: -1
        val batteryState = when (status) {
            android.os.BatteryManager.BATTERY_STATUS_CHARGING -> "CHARGING"
            android.os.BatteryManager.BATTERY_STATUS_FULL -> "FULL"
            android.os.BatteryManager.BATTERY_STATUS_DISCHARGING -> "DISCHARGING"
            android.os.BatteryManager.BATTERY_STATUS_NOT_CHARGING -> "NOT_CHARGING"
            else -> "UNKNOWN"
        }

        val isLowPower = powerManager?.isPowerSaveMode ?: false

        val thermalState = if (android.os.Build.VERSION.SDK_INT >= android.os.Build.VERSION_CODES.Q) {
            when (powerManager?.currentThermalStatus) {
                android.os.PowerManager.THERMAL_STATUS_NONE -> "NONE"
                android.os.PowerManager.THERMAL_STATUS_LIGHT -> "LIGHT"
                android.os.PowerManager.THERMAL_STATUS_MODERATE -> "MODERATE"
                android.os.PowerManager.THERMAL_STATUS_SEVERE -> "SEVERE"
                android.os.PowerManager.THERMAL_STATUS_CRITICAL -> "CRITICAL"
                android.os.PowerManager.THERMAL_STATUS_EMERGENCY -> "EMERGENCY"
                android.os.PowerManager.THERMAL_STATUS_SHUTDOWN -> "SHUTDOWN"
                else -> "UNKNOWN"
            }
        } else "UNSUPPORTED"

        val memInfo = android.app.ActivityManager.MemoryInfo()
        activityManager?.getMemoryInfo(memInfo)

        return mapOf(
            "eventType" to "device_state",
            "timestamp" to dateFormatter.format(java.time.Instant.now()),
            "batteryLevel" to batteryLevel,
            "batteryState" to batteryState,
            "isLowPowerMode" to isLowPower,
            "thermalState" to thermalState,
            "taskType" to "background",
            "availableRamBytes" to memInfo.availMem,
            "totalRamBytes" to memInfo.totalMem
        )
    }

    private suspend fun countRecordsForTypes(types: List<String>, sinceTimestamp: Long?): Map<String, Int> {
        val counts = mutableMapOf<String, Int>()
        for (type in types) {
            try {
                var count = 0
                var cursor = sinceTimestamp
                while (true) {
                    val result = healthProvider.readData(type, cursor, MAX_PAGE_SIZE)
                    count += result.data.totalCount
                    if (result.data.totalCount < MAX_PAGE_SIZE || result.maxTimestamp == null) break
                    cursor = result.maxTimestamp
                }
                counts[type] = count
            } catch (e: Exception) {
                logger("Count failed for $type: ${e.message}")
                counts[type] = 0
            }
        }
        logger("Record counts: ${counts.entries.filter { it.value > 0 }.joinToString { "${it.key}=${it.value}" }}")
        return counts
    }


    private suspend fun sendSyncStartLog(logsEndpoint: String, types: List<String>, typeCounts: Map<String, Int>, startTimestamp: Long?) {
        val dataTypeCounts = types.map { mapOf("type" to payloadTypeName(it), "count" to (typeCounts[it] ?: 0)) }

        val timeRange = mutableMapOf<String, String>(
            "endDate" to dateFormatter.format(java.time.Instant.now())
        )
        startTimestamp?.let {
            timeRange["startDate"] = dateFormatter.format(java.time.Instant.ofEpochMilli(it))
        }

        val startEvent: Map<String, Any> = mapOf(
            "eventType" to "historical_data_sync_start",
            "timestamp" to dateFormatter.format(java.time.Instant.now()),
            "dataTypeCounts" to dataTypeCounts,
            "timeRange" to timeRange
        )

        val body: Map<String, Any> = mapOf(
            "sdkVersion" to SyncDefaults.SDK_VERSION,
            "provider" to healthProvider.providerId,
            "events" to listOf(startEvent, collectDeviceState())
        )

        sendSyncLog(logsEndpoint, body)
    }

    private suspend fun sendTypeEndLog(logsEndpoint: String, type: String, success: Boolean, recordCount: Int, durationMs: Int) {
        val endEvent: Map<String, Any> = mapOf(
            "eventType" to "historical_data_type_sync_end",
            "timestamp" to dateFormatter.format(java.time.Instant.now()),
            "dataType" to payloadTypeName(type),
            "success" to success,
            "recordCount" to recordCount,
            "durationMs" to durationMs
        )

        val body: Map<String, Any> = mapOf(
            "sdkVersion" to SyncDefaults.SDK_VERSION,
            "provider" to healthProvider.providerId,
            "events" to listOf(endEvent, collectDeviceState())
        )

        sendSyncLog(logsEndpoint, body)
    }

    private suspend fun sendSyncLog(endpoint: String, body: Map<String, Any>) = withContext(dispatchers.io) {
        try {
            val requestBody = streamingJsonBody(body)
            val requestBuilder = Request.Builder()
                .url(endpoint)
                .post(requestBody)
                .header("Content-Type", "application/json")
            applyAuth(requestBuilder)

            val response = httpClient.newCall(requestBuilder.build()).execute()
            response.body?.close()
            logger("Sync log: HTTP ${response.code}")
        } catch (e: Exception) {
            logger("Sync log error: ${e.message}")
        }
    }

    // MARK: - Sync Endpoint

    private fun buildSyncEndpoint(host: String, customSyncUrl: String?, userId: String): String {
        if (customSyncUrl != null) {
            if (customSyncUrl.contains("{user_id}") || customSyncUrl.contains("{userId}")) {
                return customSyncUrl
                    .replace("{userId}", userId)
                    .replace("{user_id}", userId)
            }
            val normalizedBase = customSyncUrl.trimEnd('/')
            return "$normalizedBase/sdk/users/$userId/sync"
        }
        val h = if (host.endsWith("/")) host.dropLast(1) else host
        return "$h/api/v1/sdk/users/$userId/sync"
    }

    // MARK: - Anchors

    private fun loadAnchors(): Map<String, Long> {
        val jsonStr = syncPrefs.getString(StorageKeys.KEY_ANCHORS, null) ?: return emptyMap()
        return try {
            val map = json.decodeFromString<Map<String, Double>>(jsonStr)
            map.mapValues { it.value.toLong() }
        } catch (_: Exception) { emptyMap() }
    }

    private fun saveAnchor(type: String, timestamp: Long) {
        val current = loadAnchors().toMutableMap()
        current[type] = timestamp
        syncPrefs.edit().putString(
            StorageKeys.KEY_ANCHORS,
            json.encodeToString(current.mapValues { it.value.toDouble() })
        ).apply()
    }

    fun resetAnchors() {
        syncPrefs.edit()
            .remove(StorageKeys.KEY_ANCHORS)
            .putBoolean(fullDoneKey(), false)
            .apply()
        clearSyncSession()
        logger("Anchors reset - will perform full sync on next sync")
    }

    private fun fullDoneKey(): String = "fullDone.${userKey()}"
    private fun hasCompletedInitialSync(): Boolean = syncPrefs.getBoolean(fullDoneKey(), false)
    private fun markFullExportDone() { syncPrefs.edit().putBoolean(fullDoneKey(), true).apply() }

    // MARK: - Sync State (Mutex-protected disk I/O)

    private fun syncStateDir(): File = File(context.filesDir, StorageKeys.SYNC_STATE_DIR).also { if (!it.exists()) it.mkdirs() }
    private fun syncStateFile(): File = File(syncStateDir(), StorageKeys.SYNC_STATE_FILE)

    private fun persistStateToDisk() {
        val state = inMemoryState ?: return
        try {
            val jsonStr = json.encodeToString(state)
            if (jsonStr.isNotBlank() && jsonStr.startsWith("{")) {
                val file = syncStateFile()
                val tempFile = File(file.parent, "${file.name}.tmp")
                tempFile.writeText(jsonStr)
                tempFile.renameTo(file)
            }
        } catch (e: Exception) {
            logger("Failed to save sync state: ${e.message}")
        }
    }

    private fun loadSyncStateFromDisk(): SyncState? {
        return try {
            val file = syncStateFile()
            if (!file.exists()) return null
            val jsonStr = file.readText()
            if (jsonStr.isBlank()) { file.delete(); return null }
            val state = json.decodeFromString<SyncState>(jsonStr)
            if (state.userKey != userKey()) { clearSyncSessionInternal(); return null }
            state
        } catch (e: Exception) {
            logger("Corrupted sync state, clearing: ${e.message}")
            try { syncStateFile().delete() } catch (_: Exception) {}
            null
        }
    }

    private fun clearSyncSessionInternal() {
        inMemoryState = null
        try { syncStateFile().delete() } catch (_: Exception) {}
    }

    fun getSyncStatus(): Map<String, Any?> {
        val state = inMemoryState ?: loadSyncStateFromDisk()
        return if (state != null) {
            mapOf(
                "hasResumableSession" to state.hasProgress,
                "sentCount" to state.totalSentCount,
                "completedTypes" to state.completedTypes.size,
                "isFullExport" to state.fullExport,
                "createdAt" to dateFormatter.format(java.time.Instant.ofEpochMilli(state.createdAt))
            )
        } else {
            mapOf(
                "hasResumableSession" to false,
                "sentCount" to 0,
                "completedTypes" to 0,
                "isFullExport" to false,
                "createdAt" to null
            )
        }
    }

    fun hasResumableSyncSession(): Boolean {
        return (inMemoryState ?: loadSyncStateFromDisk())?.hasProgress == true
    }

    fun clearSyncSession() {
        clearSyncSessionInternal()
        logger("Cleared sync state")
    }
}
