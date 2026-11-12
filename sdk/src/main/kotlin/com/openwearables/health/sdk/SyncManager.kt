package com.openwearables.health.sdk

import android.content.Context
import android.content.SharedPreferences
import androidx.lifecycle.Lifecycle
import androidx.lifecycle.ProcessLifecycleOwner
import androidx.work.*
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.sync.withPermit
import kotlinx.coroutines.withContext
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.withTimeoutOrNull
import kotlinx.serialization.Serializable
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.contentOrNull
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.longOrNull
import okhttp3.MediaType.Companion.toMediaType
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody
import okhttp3.RequestBody.Companion.toRequestBody
import okio.Buffer
import okio.GzipSink
import okio.buffer
import java.io.File
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

@Serializable
data class TypeSyncProgress(
    val typeIdentifier: String,
    var sentCount: Int = 0,
    var isComplete: Boolean = false,
    var pendingAnchorTimestamp: Long? = null,
    var pendingOlderThan: Long? = null,
    var pendingChangeToken: String? = null
)

/**
 * One sync run's resumable progress.
 *
 * [fullExport] is the initial export: every type is read newest-first. Otherwise only the
 * types in [resyncTypes] are (a per-type resync); the rest follow their change tokens.
 */
@Serializable
data class SyncState(
    val userKey: String,
    val fullExport: Boolean,
    val createdAt: Long,
    var typeProgress: MutableMap<String, TypeSyncProgress> = mutableMapOf(),
    var totalSentCount: Int = 0,
    var completedTypes: MutableSet<String> = mutableSetOf(),
    var currentTypeIndex: Int = 0,
    var resyncTypes: MutableSet<String> = mutableSetOf(),
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
        // Optional mTLS configurator. Set by [MtlsConfigurator] when an mTLS
        // source is available (KeyChain alias or bundled .p12). Reassigning
        // this and calling [rebuildSharedHttpClient] swaps the shared client
        // at runtime so the user can pick a new cert without restarting.
        @Volatile
        internal var mtlsConfigurator: ((OkHttpClient.Builder) -> Unit)? = null

        @Volatile
        private var _sharedHttpClient: OkHttpClient? = null

        @get:Synchronized
        val sharedHttpClient: OkHttpClient
            get() = _sharedHttpClient ?: rebuildSharedHttpClient()

        @Synchronized
        internal fun rebuildSharedHttpClient(): OkHttpClient {
            val client = OkHttpClient.Builder()
                .connectTimeout(30, TimeUnit.SECONDS)
                .readTimeout(120, TimeUnit.SECONDS)
                .writeTimeout(120, TimeUnit.SECONDS)
                .also { builder -> mtlsConfigurator?.invoke(builder) }
                .build()
            _sharedHttpClient = client
            return client
        }

        private const val CATCHUP_TOKEN_PREFIX = "catchup:"

        // Health Connect's ReadRecordsRequest.pageSize is capped at 5000.
        const val MAX_PAGE_SIZE = 5000

        /** One sync per process. Workers construct their own [SyncManager]. */
        internal val processSyncLock = AtomicBoolean(false)

        /**
         * Bumped when anchors are cleared. A sync that started earlier must not
         * write its cursors back on top of the reset.
         */
        internal val syncEpoch = AtomicInteger(0)

        /**
         * Per [HealthDataProvider.storageKey], bumped when that type is paused or a resync is
         * requested for it. A run that started earlier drops the type and must not write its
         * cursor back. Kept apart from [syncEpoch] so the rest of the run keeps its progress.
         */
        private val typeEpochs = java.util.concurrent.ConcurrentHashMap<String, Int>()

        /**
         * Guards every read-modify-write of [StorageKeys.KEY_RESYNC_TYPES] together with the
         * epoch check/bump that decides it: the app thread adds requests while a sync thread
         * clears finished ones.
         */
        private val resyncLock = Any()

        /**
         * Set when a run stops because Health Connect rejected a read.
         * The worker that owns the run consumes this and delays the follow-up.
         */
        internal val quotaBackoffPending = AtomicBoolean(false)
    }

    private val syncPrefs: SharedPreferences by lazy {
        context.getSharedPreferences(StorageKeys.SYNC_PREFS_NAME, Context.MODE_PRIVATE)
    }

    private val json = Json { ignoreUnknownKeys = true }

    // Read live each call so cert changes via MtlsConfigurator.reload() take
    // effect on subsequent requests without recreating the SyncManager.
    private val httpClient get() = sharedHttpClient

    private val dateFormatter: java.time.format.DateTimeFormatter =
        java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
            .withZone(java.time.ZoneOffset.UTC)

    private val isSyncing = AtomicBoolean(false)

    /**
     * False when the last [syncNow] stopped before every type finished (upload failure,
     * rate limit, token mint failure). The worker then retries after a delay instead of
     * re-enqueueing itself immediately.
     */
    @Volatile
    var lastRunCompleted: Boolean = true
        private set

    /** Test seam: replaces the HTTP upload of one sync batch. */
    internal var uploadOverride: (suspend (endpoint: String, data: UnifiedHealthData) -> SendResult)? = null
    private var runEpoch = 0
    private var runTypeEpochs: Map<String, Int> = emptyMap()

    private fun writesAllowed(): Boolean = runEpoch == syncEpoch.get()

    private fun epochKey(type: String): String = healthProvider.storageKey(type) ?: type

    private fun snapshotTypeEpochs() {
        runTypeEpochs = HashMap(typeEpochs)
    }

    /** False once [type] was paused or rescheduled for resync after this run started. */
    private fun typeEpochCurrent(type: String): Boolean {
        val key = epochKey(type)
        return (typeEpochs[key] ?: 0) == (runTypeEpochs[key] ?: 0)
    }

    private fun typeWritesAllowed(type: String): Boolean = writesAllowed() && typeEpochCurrent(type)

    /** Makes a running sync drop [typeIds] (wrapper or tracked ids) and keep its hands off their cursors. */
    fun invalidateTypes(typeIds: Collection<String>) {
        for (key in typeIds.mapTo(HashSet()) { epochKey(it) }) typeEpochs.merge(key, 1, Int::plus)
    }
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

    suspend fun startBackgroundSync(): Boolean {
        schedulePeriodicSync(replace = false)
        return true
    }

    /**
     * @param replace `true` when the interval changed. Default `KEEP` so
     * `configure` / app restart does not reset the 15-minute timer (#20).
     */
    private fun schedulePeriodicSync(replace: Boolean = false) {
        val constraints = Constraints.Builder()
            .setRequiredNetworkType(NetworkType.CONNECTED)
            .build()

        val work = PeriodicWorkRequestBuilder<HealthSyncWorker>(
            syncIntervalMinutes, TimeUnit.MINUTES
        )
            .setConstraints(constraints)
            .build()

        WorkManager.getInstance(context).enqueueUniquePeriodicWork(
            SyncDefaults.WORK_NAME_PERIODIC,
            if (replace) ExistingPeriodicWorkPolicy.UPDATE else ExistingPeriodicWorkPolicy.KEEP,
            work
        )
        logger("Scheduled periodic sync every $syncIntervalMinutes minute(s)")
    }

    fun reschedulePeriodicSync() {
        schedulePeriodicSync(replace = true)
    }

    /**
     * @param keepExisting keep an expedited sync that is already enqueued or running
     *   instead of replacing (cancelling) it.
     */
    fun scheduleExpeditedSync(initialDelayMs: Long = 0L, keepExisting: Boolean = false) {
        val constraints = Constraints.Builder()
            .setRequiredNetworkType(NetworkType.CONNECTED)
            .build()

        val builder = OneTimeWorkRequestBuilder<HealthSyncWorker>()
            .setConstraints(constraints)
        if (initialDelayMs > 0L) {
            builder.setInitialDelay(initialDelayMs, TimeUnit.MILLISECONDS)
        } else {
            builder.setExpedited(OutOfQuotaPolicy.RUN_AS_NON_EXPEDITED_WORK_REQUEST)
        }
        val expeditedWork = builder.build()

        WorkManager.getInstance(context).enqueueUniqueWork(
            SyncDefaults.WORK_NAME_EXPEDITED,
            if (keepExisting) ExistingWorkPolicy.KEEP else ExistingWorkPolicy.REPLACE,
            expeditedWork
        )
        if (initialDelayMs > 0L) {
            logger("Scheduled sync in ${initialDelayMs / 1000}s")
        } else {
            logger("Scheduled expedited sync")
        }
    }

    suspend fun stopBackgroundSync() {
        val wm = WorkManager.getInstance(context)
        wm.cancelUniqueWork(SyncDefaults.WORK_NAME_PERIODIC)
        wm.cancelUniqueWork(SyncDefaults.WORK_NAME_EXPEDITED)
        logger("Cancelled periodic and expedited sync")
    }

    // MARK: - Sync Now

    suspend fun syncNow(
        fullExport: Boolean,
        background: Boolean = false,
    ) {
        if (!processSyncLock.compareAndSet(false, true)) {
            logger("Sync already in progress")
            return
        }
        lastRunCompleted = true
        runEpoch = syncEpoch.get()
        // Before the paused list is read: a pause landing in between then drops the type
        // from this run instead of letting it sync to the end.
        snapshotTypeEpochs()
        isSyncing.set(true)
        quotaBackoffPending.set(false)

        try {
            val userId = secureStorage.getUserId()
            if (userId == null || !secureStorage.hasAuth) {
                logger("No credentials for sync")
                return
            }

            val endpoints = secureStorage.getEndpoints()
            if (endpoints == null) {
                logger("No endpoints for sync - call configure")
                return
            }
            val endpoint = SyncEndpoints.sync(endpoints, userId)

            val allTrackedTypes = healthProvider.getTrackedTypes().toList()
            if (allTrackedTypes.isEmpty()) {
                logger("No tracked types configured")
                return
            }
            val pausedTypes = pausedTrackedTypes(allTrackedTypes)
            val trackedTypes = allTrackedTypes.filter { it !in pausedTypes }
            if (pausedTypes.isNotEmpty()) {
                logger("Sync: skipping paused type(s): ${pausedTypes.joinToString()}")
            }
            if (trackedTypes.isEmpty()) {
                // Nothing to read. An unfinished initial export counts as done without the
                // paused types (they resync on resume); otherwise the worker would keep
                // re-enqueueing itself for an export that can never progress.
                logger("Sync: every tracked type is paused")
                stateMutex.withLock {
                    if (!hasCompletedInitialSync()) markFullExportDone()
                    clearSyncSessionInternal()
                }
                return
            }

            val existingState = stateMutex.withLock { loadSyncStateFromDisk() }
            val isResuming = existingState != null && existingState.hasProgress
            var pendingResync = pendingResyncTrackedTypes(trackedTypes)
            // A resync of a type whose read permission was revoked can never finish (no
            // token, no data); drop it instead of retrying it forever.
            val unreadable = pendingResync.filter { !healthProvider.hasReadPermission(it) }
            if (unreadable.isNotEmpty()) {
                logger("Sync: dropping resync of ${unreadable.joinToString()} (read permission not granted)")
                unreadable.forEach { completePendingResync(it, "resync dropped") }
                pendingResync = pendingResync - unreadable.toSet()
            }

            val floor = syncStartTimestamp()
            val floorLabel = if (floor != null) "since ${java.time.Instant.ofEpochMilli(floor)}" else "full history"

            val effectiveFullExport: Boolean
            val fullExportTypes: Set<String>
            if (isResuming) {
                effectiveFullExport = existingState!!.fullExport
                logger("Sync: resuming (${existingState.totalSentCount} sent, ${existingState.completedTypes.size}/${trackedTypes.size} types done, $floorLabel)")
                fullExportTypes = stateMutex.withLock {
                    inMemoryState = existingState
                    startPendingResyncs(existingState, pendingResync)
                    PerTypeSync.fullExportScope(existingState, trackedTypes)
                }
            } else {
                effectiveFullExport = fullExport || !hasCompletedInitialSync()
                val mode = if (effectiveFullExport) "full export" else "incremental"
                logger("Sync: starting ($mode, ${trackedTypes.size} types, ${healthProvider.providerName}, $floorLabel)")
                fullExportTypes = stateMutex.withLock {
                    val state = SyncState(
                        userKey = userKey(), fullExport = effectiveFullExport,
                        createdAt = System.currentTimeMillis()
                    )
                    inMemoryState = state
                    startPendingResyncs(state, pendingResync)
                    persistStateToDisk()
                    PerTypeSync.fullExportScope(state, trackedTypes)
                }
            }
            if (!effectiveFullExport && fullExportTypes.isNotEmpty()) {
                logger("Sync: resyncing ${fullExportTypes.joinToString()} (full history of these types)")
            }

            val syncStartTime = System.currentTimeMillis()
            val logsEndpoint = SyncEndpoints.logs(endpoints, userId)

            val result = coroutineScope {
            if (effectiveFullExport) {
                fullSyncStartTime = syncStartTime
                currentLogsEndpoint = logsEndpoint
                // Fire-and-forget: the round loop does not wait for telemetry (the request
                // can take up to the client's read timeout) and the backend does not need
                // the start event before the first batch.
                launch {
                try {
                    // Already-sent totals only. Paging the whole store here
                    // burns the Health Connect request quota before any payload
                    // is uploaded. A fresh session reports 0.
                    val typeCounts = if (isResuming && existingState != null) {
                        trackedTypes.associateWith { existingState.typeProgress[it]?.sentCount ?: 0 }
                    } else {
                        trackedTypes.associateWith { 0 }
                    }
                    logger("Sending sync start log to $logsEndpoint")
                    sendSyncStartLog(logsEndpoint, trackedTypes, typeCounts, floor)
                } catch (e: Exception) {
                    logger("Sync start log failed: ${e.message}")
                }
                }
            }

            var roundRobin = processTypesRoundRobin(
                trackedTypes, fullExportTypes, endpoint, background,
            )

            // Server-side data reset detected mid-run: cursors are already cleared
            // (applyServerSyncGeneration ran resetAnchors), so re-drive once as a
            // full export. A second reset within the same call is left for the next
            // scheduled sync rather than looping here.
            if (roundRobin.generationReset) {
                // resetAnchors bumped the epoch; this restart is a new run, not a stale one.
                runEpoch = syncEpoch.get()
                snapshotTypeEpochs()
                // Re-read the paused list (after the snapshot): a type paused during the run
                // must not be re-exported. The app's "delete and pause" pauses first, and the
                // server delete is what bumps the generation.
                val restartAll = healthProvider.getTrackedTypes().toList()
                val restartPaused = pausedTrackedTypes(restartAll)
                val restartTypes = restartAll.filter { it !in restartPaused }
                if (restartTypes.isEmpty()) {
                    logger("Sync: server data reset; every tracked type is paused, nothing to re-export")
                    stateMutex.withLock {
                        markFullExportDone()
                        clearSyncSessionInternal()
                    }
                    roundRobin = RoundRobinResult(true, 0, emptyList())
                } else {
                    logger("Sync: restarting as full export (server data reset)")
                    stateMutex.withLock {
                        inMemoryState = SyncState(
                            userKey = userKey(), fullExport = true,
                            createdAt = System.currentTimeMillis()
                        )
                        persistStateToDisk()
                    }
                    fullSyncStartTime = System.currentTimeMillis()
                    roundRobin = processTypesRoundRobin(restartTypes, restartTypes.toSet(), endpoint, background)
                }
            }
            roundRobin
            }

            if (effectiveFullExport && !result.completed) {
                val durationMs = (System.currentTimeMillis() - syncStartTime).toInt()
                for (typeResult in result.typeResults) {
                    if (!typeResult.success && typeResult.recordCount > 0) {
                        sendTypeEndLog(logsEndpoint, typeResult.type, false, typeResult.recordCount, durationMs)
                    }
                }
            }

            lastRunCompleted = result.completed
            fullSyncStartTime = null
            currentLogsEndpoint = null
        } finally {
            isSyncing.set(false)
            processSyncLock.set(false)
        }
    }

    // MARK: - Round-Robin Sync Orchestration (combined payloads)

    private data class TypeResult(val type: String, val success: Boolean, val recordCount: Int)
    private data class RoundRobinResult(
        val completed: Boolean,
        val totalRecords: Int,
        val typeResults: List<TypeResult>,
        // Set when the server's sync generation changed mid-run: local cursors were
        // reset and the caller must restart the sync as a full export.
        val generationReset: Boolean = false
    )

    private data class FetchResult(
        val type: String,
        val data: UnifiedHealthData = UnifiedHealthData(),
        val count: Int = 0,
        // Parent records returned by this page (before expansion) and the page limit it was
        // read with; a full page drives the observed-expansion estimate for the next one.
        val recordCount: Int = 0,
        val pageLimit: Int = 0,
        val nextCursor: Long? = null,
        val anchorTimestamp: Long? = null,
        val nextChangeToken: String? = null,
        val isDone: Boolean = false,
        val quotaExceeded: Boolean = false,
    )

    // One round's fetched pages plus how long the read took (wall clock).
    private data class RoundFetch(val results: List<FetchResult>, val fetchMs: Long)

    // Initial guess for how many child records one parent expands into on convert. Used
    // only for a type's FIRST foreground page; after that the page limit is sized from the
    // observed expansion (see the round loop), because a HeartRateRecord may hold ~1 or ~40
    // samples depending on the device.
    private fun seedExpansion(type: String): Double = when (type) {
        "sleep" -> SyncDefaults.SLEEP_STAGES_PER_SESSION_ESTIMATE.toDouble()
        "heartRate", "speed", "cyclingSpeed", "runningSpeed", "power", "cyclingPower", "runningPower",
        "stepsCadence", "cyclingPedalingCadence", "cyclingCadence" ->
            SyncDefaults.HEART_RATE_SAMPLES_PER_RECORD_ESTIMATE.toDouble()
        else -> 1.0
    }

    /**
     * @param fullExportTypes types read newest-first over their whole history: every type
     *   in the initial export, only the resyncing ones otherwise. The rest read incrementally.
     */
    private suspend fun processTypesRoundRobin(
        types: List<String>,
        fullExportTypes: Set<String>,
        endpoint: String,
        background: Boolean,
    ): RoundRobinResult {
        fun isFullExport(type: String) = type in fullExportTypes
        val olderThanCursors = mutableMapOf<String, Long?>()
        val anchorCursors = mutableMapOf<String, Long?>()
        val changeTokens = mutableMapOf<String, String?>()
        val completedTypes = mutableSetOf<String>()

        stateMutex.withLock {
            val state = inMemoryState
            if (state != null) {
                completedTypes.addAll(state.completedTypes)
                for ((id, progress) in state.typeProgress) {
                    if (!progress.isComplete) {
                        progress.pendingOlderThan?.let { olderThanCursors[id] = it }
                        progress.pendingAnchorTimestamp?.let { anchorCursors[id] = it }
                        progress.pendingChangeToken?.let { changeTokens[id] = it }
                    }
                }
            }
        }

        if (fullExportTypes.isNotEmpty() && healthProvider.supportsChangeTracking()) {
            // Capture each type's change token BEFORE its first export page, so the first
            // incremental sync replays everything written or deleted while the (possibly
            // multi-hour) export ran. A token minted at type completion would skip those.
            // A failed capture is retried by Phase 4 when the type completes.
            var captured = false
            for (type in types) {
                if (!isFullExport(type)) continue
                if (completedTypes.contains(type) || changeTokens[type] != null) continue
                if (!healthProvider.canTrackChanges(type)) continue
                val token = healthProvider.mintChangeToken(type) ?: continue
                changeTokens[type] = token
                captured = true
                stateMutex.withLock {
                    inMemoryState?.let { state ->
                        state.typeProgress.getOrPut(type) { TypeSyncProgress(type) }.pendingChangeToken = token
                    }
                }
            }
            if (captured) stateMutex.withLock { persistStateToDisk() }
        }

        if (types.any { !isFullExport(it) }) {
            val anchors = loadAnchors()
            val storedTokens = loadChangeTokens()
            val floor = syncStartTimestamp()
            for (type in types) {
                // A resyncing type must not pick up its old token: completing the resync
                // would store it instead of the one minted before the re-read.
                if (isFullExport(type) || completedTypes.contains(type)) continue
                if (!changeTokens.containsKey(type)) {
                    storedTokens[type]?.let { changeTokens[type] = it }
                }
                if (!anchorCursors.containsKey(type)) {
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

        // Observed samples-per-parent per type, learned from each full page and used to size
        // the next foreground page. In-memory for this sync; reseeds on resume.
        val observedExpansion = mutableMapOf<String, Double>()

        // Page limit (in PARENT records) for each type. Background pages stay small and split
        // evenly; foreground pages target READ_TARGET_EXPANDED_ITEMS expanded items per type,
        // capped at the Health Connect page limit. Computed eagerly so a prefetch never reads
        // observedExpansion while the loop updates it.
        fun pageLimits(types: List<String>, smallPages: Boolean): Map<String, Int> {
            if (smallPages) {
                val perType = maxOf(1, SyncDefaults.BACKGROUND_CHUNK_SIZE / types.size)
                return types.associateWith { perType }
            }
            return types.associateWith { type ->
                val expansion = observedExpansion[type] ?: seedExpansion(type)
                (SyncDefaults.READ_TARGET_EXPANDED_ITEMS / expansion).toInt().coerceIn(1, MAX_PAGE_SIZE)
            }
        }

        suspend fun timedFetch(fetchTypes: List<String>, limits: Map<String, Int>): RoundFetch {
            val started = System.nanoTime()
            val results = fetchTypesForRound(
                fetchTypes, fullExportTypes, olderThanCursors, anchorCursors, changeTokens, limits,
            )
            return RoundFetch(results, (System.nanoTime() - started) / 1_000_000)
        }

        // Types still part of this run. A type paused, or rescheduled for resync, while the
        // run is in flight leaves it: its cursors are dropped (its stored token stays as it
        // was) and anything already read for it is not committed.
        val activeTypes = types.toMutableList()
        suspend fun dropInvalidatedTypes() {
            val stale = activeTypes.filter { !typeEpochCurrent(it) }
            if (stale.isEmpty()) return
            activeTypes.removeAll(stale)
            for (type in stale) {
                completedTypes.remove(type)
                olderThanCursors.remove(type)
                anchorCursors.remove(type)
                changeTokens.remove(type)
            }
            logger("Sync: dropping ${stale.joinToString()} from this run (paused or resync requested)")
            stateMutex.withLock {
                inMemoryState?.let { state ->
                    for (type in stale) {
                        state.typeProgress.remove(type)
                        state.completedTypes.remove(type)
                        state.resyncTypes.remove(type)
                    }
                }
                persistStateToDisk()
            }
        }

        var loggedSmallPages: Boolean? = null
        return coroutineScope syncRound@{
        // Next page is read while the current one uploads. A failed upload
        // cancels it; cursors are persisted only after a 2xx.
        var prefetched: Deferred<RoundFetch>? = null
        while (true) {
            dropInvalidatedTypes()
            val incompleteTypes = activeTypes.filter { !completedTypes.contains(it) }
            if (incompleteTypes.isEmpty()) break

            // The worker always asks for a background run. While the app is open,
            // use the large page: Health Connect charges quota per request, so a
            // 100-record page split across every type burns the limit on a handful
            // of samples.
            val smallPages = background && !isAppInForeground()
            if (smallPages != loggedSmallPages) {
                loggedSmallPages = smallPages
                logger(
                    if (smallPages) "Sync page size: ${SyncDefaults.BACKGROUND_CHUNK_SIZE} (app in background)"
                    else "Sync page size: adaptive, ~${SyncDefaults.READ_TARGET_EXPANDED_ITEMS} items per type"
                )
            }

            // Phase 1: Fetch one chunk from each type (no network yet).
            // Health Connect reads are cross-process; do not wait on them one by one.
            val round = prefetched?.await() ?: timedFetch(incompleteTypes, pageLimits(incompleteTypes, smallPages))
            prefetched = null
            // A prefetched page may include a type dropped since it was requested.
            val roundResults = round.results.filter { it.type in activeTypes && typeEpochCurrent(it.type) }

            // Learn the real expansion from a FULL page: a provider writes a series
            // consistently, so the next page can jump straight to the right size. The tail
            // page is partial and its ratio would skew the estimate.
            for (result in roundResults) {
                if (result.pageLimit > 0 && result.recordCount >= result.pageLimit) {
                    observedExpansion[result.type] = result.count.toDouble() / result.recordCount
                }
            }

            for (result in roundResults) {
                if (result.quotaExceeded) {
                    continue
                }
                if (result.isDone) {
                    completedTypes.add(result.type)
                } else {
                    if (isFullExport(result.type)) olderThanCursors[result.type] = result.nextCursor
                    else {
                        result.nextCursor?.let { anchorCursors[result.type] = it }
                        result.nextChangeToken?.let { changeTokens[result.type] = it }
                    }
                }
            }

            val hitQuota = roundResults.any { it.quotaExceeded }

            // Read the following page during the upload. Cursors above are already
            // the next page; they are written to disk only after a 2xx.
            val stillIncomplete = activeTypes.filter { !completedTypes.contains(it) }
            if (!hitQuota && stillIncomplete.isNotEmpty()) {
                val nextLimits = pageLimits(stillIncomplete, smallPages)
                prefetched = async { timedFetch(stillIncomplete, nextLimits) }
            }

            // Phase 2: Merge all fetched data into one combined payload
            val mergedData = UnifiedHealthData(
                records = roundResults.flatMap { it.data.records },
                workouts = roundResults.flatMap { it.data.workouts },
                sleep = roundResults.flatMap { it.data.sleep },
                deleted = roundResults.flatMap { it.data.deleted }
            )

            var uploadMs = 0L
            if (!mergedData.isEmpty) {
                // A dense type's read page (heart rate) can expand well past CHUNK_SIZE, so
                // the round is split into bounded sub-batches. Progress is committed only
                // after every batch succeeds; a mid-round failure re-sends the whole round on
                // resume, which the server absorbs by upserting on record id.
                val batches = chunkUnifiedData(mergedData, SyncDefaults.CHUNK_SIZE)
                var failed: SendResult? = null
                var failedBatch = 0
                for ((i, batch) in batches.withIndex()) {
                    logPayloadSummary(batch)
                    val uploadStarted = System.currentTimeMillis()
                    val sendResult = sendHealthData(endpoint, batch)
                    uploadMs += System.currentTimeMillis() - uploadStarted
                    if (!sendResult.success) {
                        failed = sendResult
                        failedBatch = i + 1
                        break
                    }
                    logger("Round sent batch ${i + 1}/${batches.size}: ${batch.totalCount} items (${sendResult.payloadSizeKb} KB) -> ${sendResult.statusCode}")

                    if (applyServerSyncGeneration(sendResult.syncGeneration)) {
                        prefetched?.cancel()
                        prefetched = null
                        return@syncRound RoundRobinResult(false, 0, emptyList(), generationReset = true)
                    }
                }

                if (failed != null) {
                    prefetched?.cancel()
                    prefetched = null
                    val reason = failed.statusCode?.let { "HTTP $it" } ?: "network error"
                    logger("Combined round failed on batch $failedBatch/${batches.size} ($reason)")
                    val (totalSent, typeResults) = stateMutex.withLock {
                        persistStateToDisk()
                        val state = inMemoryState
                        val sent = state?.totalSentCount ?: 0
                        val results = types.map { type ->
                            TypeResult(type, state?.completedTypes?.contains(type) == true, state?.typeProgress?.get(type)?.sentCount ?: 0)
                        }
                        Pair(sent, results)
                    }
                    return@syncRound RoundRobinResult(false, totalSent, typeResults)
                }

                logger("Round sent: ${mergedData.totalCount} items in ${batches.size} batch(es), ${uploadMs}ms")
            }

            // Phase 3: Update progress for all types in this round.
            // Full-export + change tracking: do not persist isComplete until
            // Phase 4 mints a token. A failed mint leaves the type incomplete
            // so the next incremental run cannot start from nil and re-crawl history.
            val newlyCompletedTypes = mutableListOf<Pair<String, Int>>()
            fun deferCompleteForToken(type: String) = isFullExport(type) && healthProvider.supportsChangeTracking()
            val persistStart = System.nanoTime()
            stateMutex.withLock {
                for (result in roundResults) {
                    if (!typeEpochCurrent(result.type)) continue
                    val persistComplete = result.isDone && !deferCompleteForToken(result.type)
                    updateInMemoryProgress(
                        result.type, result.count, isComplete = persistComplete,
                        anchorTimestamp = result.anchorTimestamp,
                        changeToken = result.nextChangeToken,
                    )
                    if (isFullExport(result.type) && !result.isDone && !result.quotaExceeded) {
                        inMemoryState?.typeProgress?.get(result.type)?.pendingOlderThan = result.nextCursor
                    }
                    if (!isFullExport(result.type) && !result.isDone) {
                        result.nextChangeToken?.let {
                            inMemoryState?.typeProgress?.get(result.type)?.pendingChangeToken = it
                        }
                    }
                    if (persistComplete) {
                        newlyCompletedTypes.add(result.type to (inMemoryState?.typeProgress?.get(result.type)?.sentCount ?: 0))
                    }
                }
                persistStateToDisk()
            }
            val persistMs = (System.nanoTime() - persistStart) / 1_000_000

            // Per-round timing: read (Health Connect IPC + convert; from round 2 on it ran
            // during the previous round's upload) vs upload vs persist.
            logger("Round timing: read ${round.fetchMs}ms, upload ${uploadMs}ms, persist ${persistMs}ms (${mergedData.totalCount} items)")

            if (hitQuota) {
                prefetched?.cancel()
                prefetched = null
                logger("Health Connect rate limit — pausing sync, cursors unchanged for the rejected read")
                quotaBackoffPending.set(true)
                val (totalSent, typeResults) = stateMutex.withLock {
                    val state = inMemoryState
                    val sent = state?.totalSentCount ?: 0
                    val results = types.map { type ->
                        TypeResult(type, state?.completedTypes?.contains(type) == true, state?.typeProgress?.get(type)?.sentCount ?: 0)
                    }
                    Pair(sent, results)
                }
                return@syncRound RoundRobinResult(false, totalSent, typeResults)
            }

            // Phase 4: mint HC change tokens after a successful upload.
            // A failed mint pauses the sync instead of pretending the type is done.
            val doneThisRound = roundResults.filter { it.isDone && deferCompleteForToken(it.type) }
            for (done in doneThisRound) {
                if (!typeEpochCurrent(done.type)) continue
                if (!healthProvider.canTrackChanges(done.type)) {
                    logger("  ${done.type}: no change stream, marking complete")
                    stateMutex.withLock {
                        updateInMemoryProgress(
                            done.type, sentInChunk = 0, isComplete = true,
                            anchorTimestamp = null, changeToken = null,
                        )
                        persistStateToDisk()
                        newlyCompletedTypes.add(
                            done.type to (inMemoryState?.typeProgress?.get(done.type)?.sentCount ?: 0)
                        )
                    }
                    continue
                }
                val minted = changeTokens[done.type] ?: healthProvider.mintChangeToken(done.type)
                val perTypeResync = stateMutex.withLock {
                    inMemoryState?.let { !it.fullExport && done.type in it.resyncTypes } == true
                }
                if (minted == null && perTypeResync && !quotaBackoffPending.get()) {
                    // Not a rate limit: most likely the read permission was revoked. Retrying
                    // cannot succeed, and leaving the resync open would re-run it forever
                    // while every other type waits. The stored token (if any) stays.
                    logger("  ${done.type}: change token mint failed — finishing resync without a new token")
                    stateMutex.withLock {
                        updateInMemoryProgress(
                            done.type, sentInChunk = 0, isComplete = true,
                            anchorTimestamp = null, changeToken = null,
                        )
                        persistStateToDisk()
                        newlyCompletedTypes.add(done.type to (inMemoryState?.typeProgress?.get(done.type)?.sentCount ?: 0))
                    }
                    continue
                }
                if (minted == null) {
                    prefetched?.cancel()
                    prefetched = null
                    logger("  ${done.type}: change token mint failed — leaving type incomplete, pausing sync")
                    completedTypes.remove(done.type)
                    stateMutex.withLock {
                        inMemoryState?.completedTypes?.remove(done.type)
                        inMemoryState?.typeProgress?.get(done.type)?.isComplete = false
                        persistStateToDisk()
                    }
                    val (totalSent, typeResults) = stateMutex.withLock {
                        val state = inMemoryState
                        val sent = state?.totalSentCount ?: 0
                        val results = types.map { type ->
                            TypeResult(type, state?.completedTypes?.contains(type) == true, state?.typeProgress?.get(type)?.sentCount ?: 0)
                        }
                        Pair(sent, results)
                    }
                    return@syncRound RoundRobinResult(false, totalSent, typeResults)
                }
                logger("  ${done.type}: captured change token after full export")
                stateMutex.withLock {
                    updateInMemoryProgress(
                        done.type, sentInChunk = 0, isComplete = true,
                        anchorTimestamp = null, changeToken = minted,
                    )
                    persistStateToDisk()
                    newlyCompletedTypes.add(done.type to (inMemoryState?.typeProgress?.get(done.type)?.sentCount ?: 0))
                }
            }

            val startTime = fullSyncStartTime
            val logEndpoint = currentLogsEndpoint
            if (startTime != null && logEndpoint != null && newlyCompletedTypes.isNotEmpty()) {
                for ((type, count) in newlyCompletedTypes) {
                    if (count > 0 && isFullExport(type)) {
                        val durationMs = (System.currentTimeMillis() - startTime).toInt()
                        logger("Sending sync end log: ${payloadTypeName(type)} ($count records, ${durationMs}ms)")
                        launch {
                            try {
                                sendTypeEndLog(logEndpoint, type, true, count, durationMs)
                            } catch (e: Exception) {
                                logger("Type end log failed for $type: ${e.message}")
                            }
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
            if (state?.fullExport == true) {
                markFullExportDone()
                resyncTypesExportMissed()
            }
            if (state != null) {
                logger("Sync: complete ($sent items, ${state.completedTypes.size} types)")
                if (state.fullExport && sent == 0) {
                    logger("Full export found 0 records from ${healthProvider.providerName}. If the wearable writes to Health Connect, set provider to google.")
                }
            }
            clearSyncSessionInternal()
            Pair(sent, results)
        }
        RoundRobinResult(true, totalSent, typeResults)
        }
    }

    /** True when the process has a started activity. Workers still pass background=true. */
    private fun isAppInForeground(): Boolean = try {
        ProcessLifecycleOwner.get().lifecycle.currentState.isAtLeast(Lifecycle.State.STARTED)
    } catch (_: Exception) {
        false
    }

    // MARK: - Fetch-Only Chunk Processors (no network)

    private suspend fun fetchTypesForRound(
        types: List<String>,
        fullExportTypes: Set<String>,
        olderThanCursors: Map<String, Long?>,
        anchorCursors: Map<String, Long?>,
        changeTokens: Map<String, String?>,
        limits: Map<String, Int>,
    ): List<FetchResult> {
        suspend fun fetch(type: String): FetchResult = if (type in fullExportTypes) {
            fetchOneChunkNewestFirst(type, olderThanCursors[type], limits.getValue(type))
        } else {
            fetchOneChunkIncremental(type, anchorCursors[type], changeTokens[type], limits.getValue(type))
        }

        if (!healthProvider.supportsParallelReads() || types.size <= 1) {
            val results = ArrayList<FetchResult>(types.size)
            for (type in types) {
                val result = fetch(type)
                results.add(result)
                if (result.quotaExceeded) break
            }
            return results
        }

        val gate = Semaphore(4)
        return coroutineScope {
            types.map { type ->
                async {
                    gate.withPermit { fetch(type) }
                }
            }.awaitAll()
        }
    }

    private suspend fun fetchOneChunkNewestFirst(
        type: String,
        olderThan: Long?,
        limit: Int
    ): FetchResult {
        val floor = syncStartTimestamp()
        val floorIso = floor?.let { UnifiedTimestamp.fromEpochMs(it) }

        logger("  $type: querying (newest first, limit=$limit${olderThan?.let { ", olderThan=${java.time.Instant.ofEpochMilli(it)}" } ?: ""})...")

        val result = healthProvider.readDataDescending(type, olderThan, limit)

        if (result.quotaExceeded) {
            logger("  $type: rate limited — leaving cursor unchanged")
            return FetchResult(type = type, quotaExceeded = true)
        }

        if (result.data.isEmpty) {
            logger("  $type: all data sent (newest first)")
            return FetchResult(type = type, isDone = true)
        }

        val reachedFloor = floor != null && result.minTimestamp != null && result.minTimestamp <= floor
        val isLastChunk = result.exhaustive || pageCount(result) < limit || reachedFloor

        val data = if (reachedFloor && floorIso != null) result.data.filterSince(floorIso) else result.data

        if (data.isEmpty) {
            logger("  $type: all data within range sent")
            return FetchResult(type = type, isDone = true)
        }

        val anchorTs = if (olderThan == null) result.maxTimestamp else null
        val nextOlderThan = if (isLastChunk) null else result.minTimestamp

        logger("  $type: ${data.totalCount} samples (newest first)")

        return FetchResult(
            type = type, data = data, count = data.totalCount,
            recordCount = result.recordCount, pageLimit = limit,
            nextCursor = nextOlderThan, anchorTimestamp = anchorTs, isDone = isLastChunk
        )
    }

    private suspend fun fetchOneChunkIncremental(
        type: String,
        anchor: Long?,
        changeToken: String?,
        limit: Int
    ): FetchResult {
        if (healthProvider.supportsChangeTracking()) {
            return fetchOneChunkFromChanges(type, changeToken, limit, catchupSince = anchor)
        }
        return fetchOneChunkFromTimestamp(type, anchor, limit)
    }

    /**
     * Health Connect incremental: write-order [HealthDataProvider.readChanges].
     * A missing or expired token is minted at "now" and
     * we do a one-time [SyncDefaults.CHANGE_CATCHUP_MS] time-range catch-up so upgrades
     * and token expiry do not skip recent backfills.
     *
     * [catchupSince] is the in-memory page cursor for a multi-page catch-up.
     * The first mint always looks back [SyncDefaults.CHANGE_CATCHUP_MS] — never
     * the old timestamp anchor, which is the #19 bug.
     */
    private suspend fun fetchOneChunkFromChanges(
        type: String,
        changeToken: String?,
        limit: Int,
        catchupSince: Long? = null,
    ): FetchResult {
        val token = changeToken
        if (token == null || token.startsWith(CATCHUP_TOKEN_PREFIX)) {
            val realToken = token?.removePrefix(CATCHUP_TOKEN_PREFIX)
                ?: healthProvider.mintChangeToken(type)
            if (realToken == null) {
                logger("  $type: could not mint change token, falling back to timestamp")
                return fetchOneChunkFromTimestamp(type, catchupSince ?: loadAnchors()[type], limit)
            }
            val since = if (token != null && token.startsWith(CATCHUP_TOKEN_PREFIX)) {
                catchupSince ?: (System.currentTimeMillis() - SyncDefaults.CHANGE_CATCHUP_MS)
            } else {
                System.currentTimeMillis() - SyncDefaults.CHANGE_CATCHUP_MS
            }
            logger("  $type: change-token catch-up since ${java.time.Instant.ofEpochMilli(since)}")
            val result = healthProvider.readData(type, since, limit)
            if (result.quotaExceeded) {
                logger("  $type: rate limited during catch-up — leaving cursor unchanged")
                return FetchResult(type = type, quotaExceeded = true)
            }
            val count = result.data.totalCount
            val isLastChunk = pageCount(result) < limit
            if (result.data.isEmpty) {
                logger("  $type: catch-up empty")
                return FetchResult(type = type, nextChangeToken = realToken, isDone = true)
            }
            logger("  $type: catch-up $count samples")
            return FetchResult(
                type = type, data = result.data, count = count,
                recordCount = result.recordCount, pageLimit = limit,
                nextCursor = result.maxTimestamp,
                anchorTimestamp = result.maxTimestamp,
                nextChangeToken = if (isLastChunk) realToken else CATCHUP_TOKEN_PREFIX + realToken,
                isDone = isLastChunk
            )
        }

        logger("  $type: querying changes...")
        val changes = healthProvider.readChanges(type, token)
        if (changes.tokenExpired) {
            logger("  $type: change token expired — reminting + 30-day catch-up")
            return fetchOneChunkFromChanges(type, changeToken = null, limit = limit, catchupSince = null)
        }

        val deletedNote = if (changes.deletedCount > 0) ", ${changes.deletedCount} deleted" else ""
        val pageData = if (changes.deletedIds.isEmpty()) changes.data else changes.data.copy(
            deleted = changes.deletedIds.map { UnifiedDeleted(it, payloadTypeName(type)) }
        )
        if (pageData.isEmpty) {
            logger("  $type: no new data$deletedNote")
            return FetchResult(type = type, nextChangeToken = changes.nextToken ?: token, isDone = true)
        }

        val count = pageData.totalCount
        val isLastChunk = !changes.hasMore
        logger("  $type: $count samples$deletedNote")
        return FetchResult(
            type = type, data = pageData, count = count,
            nextChangeToken = changes.nextToken ?: token,
            isDone = isLastChunk
        )
    }

    private suspend fun fetchOneChunkFromTimestamp(
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
        val isLastChunk = result.exhaustive || pageCount(result) < limit

        logger("  $type: $count samples")

        return FetchResult(
            type = type, data = result.data, count = count,
            recordCount = result.recordCount, pageLimit = limit,
            nextCursor = result.maxTimestamp, anchorTimestamp = result.maxTimestamp,
            isDone = isLastChunk
        )
    }

    // Records that count against the read's pageSize: parent records when the provider
    // reports them (a heart-rate series or sleep session expands into many samples/stages),
    // otherwise the converted count.
    private fun pageCount(result: ProviderReadResult): Int =
        if (result.recordCount > 0) result.recordCount else result.data.totalCount

    private fun updateInMemoryProgress(
        typeIdentifier: String,
        sentInChunk: Int,
        isComplete: Boolean,
        anchorTimestamp: Long?,
        changeToken: String? = null,
    ) {
        val state = inMemoryState ?: return
        if (!typeEpochCurrent(typeIdentifier)) return
        val progress = state.typeProgress.getOrPut(typeIdentifier) { TypeSyncProgress(typeIdentifier) }
        progress.sentCount += sentInChunk
        progress.isComplete = isComplete
        if (anchorTimestamp != null) progress.pendingAnchorTimestamp = anchorTimestamp
        if (changeToken != null) progress.pendingChangeToken = changeToken
        state.totalSentCount += sentInChunk
        if (isComplete) {
            state.completedTypes.add(typeIdentifier)
            progress.pendingAnchorTimestamp?.let { saveAnchor(typeIdentifier, it) }
            progress.pendingChangeToken?.let { saveChangeToken(typeIdentifier, it) }
            // A full read of the type (initial export or resync) satisfies a pending resync.
            if (state.fullExport || typeIdentifier in state.resyncTypes) {
                completePendingResync(typeIdentifier)
            }
        }
    }

    // MARK: - Payload (unified)

    /**
     * Encodes one round straight to bytes. Building [UnifiedHealthData.toDataMap]
     * first copies every record into a Map tree (several MB per page) and the
     * collector pauses the upload. Workouts and sleep stay on their maps —
     * a page has few of them. The byte array sets Content-Length and can be
     * replayed after a 401.
     */
    private fun encodeHealthPayload(data: UnifiedHealthData): ByteArray {
        val out = java.io.ByteArrayOutputStream(64 * 1024)
        // BufferedWriter is load-bearing: JsonWriter emits ~50 tiny writes per record and an
        // unbuffered OutputStreamWriter allocates per call (GC pressure during upload).
        val writer = android.util.JsonWriter(
            java.io.BufferedWriter(java.io.OutputStreamWriter(out, Charsets.UTF_8), 32 * 1024)
        )
        writer.beginObject()
        writer.name("provider").value(healthProvider.providerId)
        writer.name("sdkVersion").value(SyncDefaults.SDK_VERSION)
        writer.name("syncTimestamp").value(UnifiedTimestamp.fromEpochMs(System.currentTimeMillis()))
        writer.name("data")
        writer.beginObject()
        writer.name("records")
        writer.beginArray()
        for (record in data.records) writeRecord(writer, record)
        writer.endArray()
        writer.name("workouts")
        writeValue(writer, data.workouts.map { it.toMap() })
        writer.name("sleep")
        writeValue(writer, data.sleep.map { it.toMap() })
        writer.name("deleted")
        writer.beginArray()
        for (d in data.deleted) {
            writer.beginObject()
            writer.name("id").value(d.id)
            writer.name("type").value(d.type)
            writer.endObject()
        }
        writer.endArray()
        writer.endObject()
        writer.endObject()
        writer.flush()
        return out.toByteArray()
    }

    private fun writeRecord(writer: android.util.JsonWriter, record: UnifiedRecord) {
        writer.beginObject()
        writer.name("id").value(record.id)
        writer.name("type").value(record.type)
        writer.name("startDate").value(record.startDate)
        writer.name("endDate").value(record.endDate)
        writer.name("zoneOffset")
        writeNullableString(writer, record.zoneOffset)
        writer.name("source")
        writeSource(writer, record.source)
        writer.name("value").value(record.value)
        writer.name("unit").value(record.unit)
        writer.name("parentId")
        writeNullableString(writer, record.parentId)
        writer.name("metadata")
        writeValue(writer, record.metadata)
        writer.endObject()
    }

    private fun writeSource(writer: android.util.JsonWriter, source: UnifiedSource) {
        writer.beginObject()
        writer.name("appId"); writeNullableString(writer, source.appId)
        writer.name("deviceId"); writeNullableString(writer, source.deviceId)
        writer.name("deviceName"); writeNullableString(writer, source.deviceName)
        writer.name("deviceManufacturer"); writeNullableString(writer, source.deviceManufacturer)
        writer.name("deviceModel"); writeNullableString(writer, source.deviceModel)
        writer.name("deviceType"); writeNullableString(writer, source.deviceType)
        writer.name("recordingMethod"); writeNullableString(writer, source.recordingMethod)
        writer.endObject()
    }

    private fun writeNullableString(writer: android.util.JsonWriter, value: String?) {
        if (value == null) writer.nullValue() else writer.value(value)
    }

    private suspend fun sendHealthData(endpoint: String, data: UnifiedHealthData): SendResult {
        uploadOverride?.let { return it(endpoint, data) }
        val bytes = withContext(dispatchers.io) {
            val raw = encodeHealthPayload(data)
            val gzipped = gzip(raw)
            val pct = if (raw.isNotEmpty()) 100L * gzipped.size / raw.size else 0
            logger("Payload gzip: ${raw.size / 1024} KB -> ${gzipped.size / 1024} KB ($pct% of original)")
            gzipped
        }
        val body = bytes.toRequestBody("application/json".toMediaType())
        return sendWithBody(endpoint, body, AtomicLong(bytes.size.toLong()), contentEncoding = "gzip")
    }

    private fun gzip(bytes: ByteArray): ByteArray {
        val out = Buffer()
        GzipSink(out).buffer().use { it.write(bytes) }
        return out.readByteArray()
    }

    // Split a merged round into payloads of at most [maxItems] expanded items
    // (records + workouts + sleep + tombstones), so a large dense-series read page is uploaded as
    // several bounded POSTs. Items keep their order; each batch fills records, then
    // workouts, then sleep up to the budget.
    private fun chunkUnifiedData(data: UnifiedHealthData, maxItems: Int): List<UnifiedHealthData> {
        if (data.totalCount <= maxItems) return listOf(data)

        val batches = mutableListOf<UnifiedHealthData>()
        var records = data.records
        var workouts = data.workouts
        var sleep = data.sleep
        var deleted = data.deleted
        while (records.isNotEmpty() || workouts.isNotEmpty() || sleep.isNotEmpty() || deleted.isNotEmpty()) {
            var budget = maxItems
            val rTake = minOf(budget, records.size); budget -= rTake
            val wTake = minOf(budget, workouts.size); budget -= wTake
            val sTake = minOf(budget, sleep.size); budget -= sTake
            val dTake = minOf(budget, deleted.size)
            batches.add(UnifiedHealthData(records.take(rTake), workouts.take(wTake), sleep.take(sTake), deleted.take(dTake)))
            records = records.drop(rTake)
            workouts = workouts.drop(wTake)
            sleep = sleep.drop(sTake)
            deleted = deleted.drop(dTake)
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
        if (data.deleted.isNotEmpty()) {
            typeCounts["deleted"] = data.deleted.size
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
            val url = secureStorage.getEndpoints()?.tokenRefresh
            if (refreshToken == null || url == null) {
                logger("Token refresh: missing credentials")
                return@withContext TokenRefreshResult.AUTH_FAILURE
            }

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

    internal data class SendResult(
        val success: Boolean,
        val statusCode: Int?,
        val payloadSizeKb: Int,
        // Server's per-user sync generation echoed on successful uploads; null when
        // the backend predates it or the body couldn't be parsed.
        val syncGeneration: Long? = null
    )

    private suspend fun sendPayload(endpoint: String, payload: Map<String, Any>): SendResult {
        val requestBytes = AtomicLong(0)
        val body = streamingJsonBody(payload, requestBytes)
        return sendWithBody(endpoint, body, requestBytes)
    }

    /**
     * Creates an OkHttp RequestBody that streams JSON directly from the Map
     * to the network via android.util.JsonWriter. No intermediate JsonElement
     * tree or full String is allocated — only the writer's small internal buffer
     * is held in heap, making memory usage O(depth) instead of O(n).
     */
    private fun streamingJsonBody(payload: Map<String, Any>, requestBytes: AtomicLong? = null): RequestBody {
        return object : okhttp3.RequestBody() {
            override fun contentType() = "application/json".toMediaType()
            override fun writeTo(sink: okio.BufferedSink) {
                val out = if (requestBytes != null) {
                    object : java.io.OutputStream() {
                        private val delegate = sink.outputStream()
                        override fun write(b: Int) { delegate.write(b); requestBytes.incrementAndGet() }
                        override fun write(b: ByteArray, off: Int, len: Int) { delegate.write(b, off, len); requestBytes.addAndGet(len.toLong()) }
                        override fun flush() = delegate.flush()
                    }
                } else sink.outputStream()
                val writer = android.util.JsonWriter(java.io.OutputStreamWriter(out, Charsets.UTF_8))
                writeValue(writer, payload)
                writer.flush()
            }
        }
    }

    private fun writeValue(writer: android.util.JsonWriter, value: Any?) {
        when (value) {
            null -> writer.nullValue()
            is Boolean -> writer.value(value)
            is Int -> writer.value(value.toLong())
            is Long -> writer.value(value)
            is Float -> writer.value(value.toDouble())
            is Double -> writer.value(value)
            is Number -> writer.value(value.toDouble())
            is String -> writer.value(value)
            is Map<*, *> -> {
                writer.beginObject()
                for ((k, v) in value) {
                    writer.name(k.toString())
                    writeValue(writer, v)
                }
                writer.endObject()
            }
            is List<*> -> {
                writer.beginArray()
                for (item in value) {
                    writeValue(writer, item)
                }
                writer.endArray()
            }
            else -> writer.value(value.toString())
        }
    }

    private suspend fun sendWithBody(
        endpoint: String,
        body: okhttp3.RequestBody,
        requestBytes: AtomicLong? = null,
        contentEncoding: String? = null,
    ): SendResult =
        withContext(dispatchers.io) {
            try {
                val requestBuilder = Request.Builder()
                    .url(endpoint)
                    .post(body)
                    .header("Content-Type", "application/json")
                contentEncoding?.let { requestBuilder.header("Content-Encoding", it) }
                applyAuth(requestBuilder)

                val response = httpClient.newCall(requestBuilder.build()).execute()
                val sizeKb = ((requestBytes?.get() ?: 0L) / 1024).toInt()
                val code = response.code
                if (response.isSuccessful) {
                    return@withContext SendResult(true, code, sizeKb, readSyncGeneration(response))
                }
                response.body?.close()

                if (code == 401) {
                    logger("Got 401, refreshing token...")
                    val retryOk = handle401(endpoint, body, contentEncoding)
                    return@withContext SendResult(retryOk, if (retryOk) 200 else 401, sizeKb)
                }

                SendResult(false, code, sizeKb)
            } catch (e: Exception) {
                logger("Upload error: ${e.javaClass.simpleName}: ${e.message}")
                SendResult(false, null, 0)
            }
        }

    /** Reads the server's per-user sync generation from a successful sync response.
     *  Absent on older backends; null means "no signal", never "reset". Consumes
     *  (and thereby closes) the response body. */
    private fun readSyncGeneration(response: okhttp3.Response): Long? = try {
        val text = response.body?.string()
        if (text.isNullOrBlank()) null
        else json.parseToJsonElement(text).jsonObject["sync_generation"]?.jsonPrimitive?.longOrNull
    } catch (_: Exception) {
        null
    }

    private suspend fun handle401(endpoint: String, body: okhttp3.RequestBody, contentEncoding: String? = null): Boolean {
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
                            .post(body)
                            .header("Content-Type", "application/json")
                        contentEncoding?.let { retryBuilder.header("Content-Encoding", it) }
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
        "elevationGained" -> "ELEVATION_GAINED"
        "totalCaloriesBurned", "totalEnergy" -> "TOTAL_CALORIES_BURNED"
        "speed", "cyclingSpeed", "runningSpeed" -> "SPEED"
        "power", "cyclingPower", "runningPower" -> "POWER"
        "stepsCadence" -> "STEPS_CADENCE"
        "cyclingPedalingCadence", "cyclingCadence" -> "CYCLING_PEDALING_CADENCE"
        "water", "dietaryWater" -> "HYDRATION"
        "vo2Max" -> "VO2_MAX"
        "respiratoryRate" -> "RESPIRATORY_RATE"
        "skinTemperature" -> "SKIN_TEMPERATURE"
        "workout" -> "WORKOUT"
        "sleep" -> "SLEEP"
        else -> trackedTypeId.uppercase()
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

    // MARK: - Anchors

    private fun loadAnchors(): Map<String, Long> {
        val jsonStr = syncPrefs.getString(StorageKeys.KEY_ANCHORS, null) ?: return emptyMap()
        return try {
            val map = json.decodeFromString<Map<String, Double>>(jsonStr)
            map.mapValues { it.value.toLong() }
        } catch (_: Exception) { emptyMap() }
    }

    private fun saveAnchor(type: String, timestamp: Long) {
        if (!typeWritesAllowed(type)) return
        val current = loadAnchors().toMutableMap()
        current[type] = timestamp
        syncPrefs.edit().putString(
            StorageKeys.KEY_ANCHORS,
            json.encodeToString(current.mapValues { it.value.toDouble() })
        ).apply()
    }

    private fun loadChangeTokens(): Map<String, String> {
        val jsonStr = syncPrefs.getString(StorageKeys.KEY_CHANGE_TOKENS, null) ?: return emptyMap()
        return try {
            json.decodeFromString<Map<String, String>>(jsonStr)
        } catch (_: Exception) { emptyMap() }
    }

    private fun saveChangeToken(type: String, token: String) {
        if (!typeWritesAllowed(type)) return
        if (token.startsWith(CATCHUP_TOKEN_PREFIX)) return
        val current = loadChangeTokens().toMutableMap()
        current[type] = token
        syncPrefs.edit().putString(StorageKeys.KEY_CHANGE_TOKENS, json.encodeToString(current)).apply()
    }

    fun resetAnchors() {
        runBlocking {
            stateMutex.withLock {
                syncEpoch.incrementAndGet()
                synchronized(resyncLock) {
                    syncPrefs.edit()
                        .remove(StorageKeys.KEY_ANCHORS)
                        .remove(StorageKeys.KEY_CHANGE_TOKENS)
                        // The full export that follows re-reads every type; pending resyncs are moot.
                        .remove(StorageKeys.KEY_RESYNC_TYPES)
                        .remove(syncGenerationKey())
                        .putBoolean(fullDoneKey(), false)
                        .commit()
                }
                clearSyncSessionInternal()
            }
        }
        logger("Anchors reset - will perform full sync on next sync")
    }

    // MARK: - Per-type pause / resync

    // Paused and pending-resync ids are the app's wrapper ids; the sync loop works on tracked
    // ids (one per alias group). See [PerTypeSync] for the folding rule.

    /** Tracked ids: the provider's, or the stored list when it has not been set yet (worker). */
    private fun knownTrackedTypes(): Collection<String> =
        healthProvider.getTrackedTypes().ifEmpty { secureStorage.getTrackedTypes().toSet() }

    private fun pausedTrackedTypes(tracked: Collection<String>): Set<String> =
        PerTypeSync.pausedTrackedTypes(
            tracked, secureStorage.getRequestedTypes(), secureStorage.getPausedTypes().keys,
            healthProvider::storageKey,
        )

    private fun loadPendingResyncs(): List<String> =
        PerTypeSync.decodeIds(syncPrefs.getString(StorageKeys.KEY_RESYNC_TYPES, null))

    private fun savePendingResyncs(ids: Collection<String>) {
        syncPrefs.edit().putString(StorageKeys.KEY_RESYNC_TYPES, PerTypeSync.encodeIds(ids.distinct())).commit()
    }

    /** Tracked ids, among [activeTypes], with a pending resync. */
    private fun pendingResyncTrackedTypes(activeTypes: Collection<String>): Set<String> =
        PerTypeSync.trackedFor(loadPendingResyncs(), activeTypes, healthProvider::storageKey)

    /** Pending resync wrapper ids that fold into a tracked, unpaused type. */
    private fun actionablePendingResyncs(): List<String> {
        val pending = loadPendingResyncs()
        if (pending.isEmpty()) return emptyList()
        return PerTypeSync.actionableResyncs(
            pending, knownTrackedTypes(), secureStorage.getRequestedTypes(),
            secureStorage.getPausedTypes().keys, healthProvider::storageKey,
        )
    }

    private fun startPendingResyncs(state: SyncState, pending: Set<String>) {
        if (pending.isEmpty()) return
        val started = PerTypeSync.applyPendingResyncs(state, pending)
        if (started.isNotEmpty()) {
            logger("Sync: starting resync of ${started.joinToString()}")
            persistStateToDisk()
        }
    }

    /** Clears the pending resync ids that fold into [trackedId] once it was read in full. */
    private fun completePendingResync(trackedId: String, outcome: String = "resync complete") {
        synchronized(resyncLock) {
            // Checked under the lock: a request made after this run read the type bumped the
            // epoch first, so it is kept.
            if (!typeWritesAllowed(trackedId)) return
            val pending = loadPendingResyncs()
            if (pending.isEmpty()) return
            val key = epochKey(trackedId)
            val remaining = pending.filter { it != trackedId && healthProvider.storageKey(it) != key }
            if (remaining.size != pending.size) {
                savePendingResyncs(remaining)
                logger("  $trackedId: $outcome")
            }
        }
    }

    /**
     * Records a resync for [typeIds] (wrapper ids, already filtered by the caller) and makes
     * a running sync drop them, so it cannot store their old cursors afterwards.
     */
    fun scheduleResync(typeIds: Collection<String>) {
        if (typeIds.isEmpty()) return
        synchronized(resyncLock) {
            // Epoch first: a run finishing the same type then cannot clear this request.
            invalidateTypes(typeIds)
            savePendingResyncs(loadPendingResyncs() + typeIds)
        }
    }

    /**
     * Of [typeIds] (wrapper ids, not paused), those a resync can apply to: they fold into a
     * tracked type, and — while the initial export is unfinished — that export has already
     * completed the type (the export reads the others anyway).
     */
    suspend fun resyncCandidates(typeIds: Collection<String>): List<String> {
        val tracked = knownTrackedTypes()
        val mapped = typeIds.filter {
            PerTypeSync.trackedFor(listOf(it), tracked, healthProvider::storageKey).isNotEmpty()
        }
        if (hasCompletedInitialSync()) return mapped
        val exported = stateMutex.withLock { (inMemoryState ?: loadSyncStateFromDisk())?.completedTypes?.toSet() }
            ?: return emptyList()
        return mapped.filter {
            PerTypeSync.trackedFor(listOf(it), exported, healthProvider::storageKey).isNotEmpty()
        }
    }

    /**
     * Wrapper ids among [resumed] whose sync must restart from scratch: their alias group was
     * fully paused, and the type either has no cursor or was paused for 30+ days.
     * [pausedAt] is the paused list before the resume. Empty before the initial export
     * finished: that export reads every unpaused type in full anyway.
     */
    fun resyncNeededOnResume(resumed: Collection<String>, pausedAt: Map<String, Long>): List<String> {
        if (!hasCompletedInitialSync()) return emptyList()
        val tracked = knownTrackedTypes()
        val requested = secureStorage.getRequestedTypes()
        val key = healthProvider::storageKey
        val before = PerTypeSync.pausedTrackedTypes(tracked, requested, pausedAt.keys, key)
        val after = PerTypeSync.pausedTrackedTypes(tracked, requested, pausedAt.keys - resumed.toSet(), key)
        val tokens = loadChangeTokens()
        val anchors = loadAnchors()
        val now = System.currentTimeMillis()
        val needed = (before - after).filter { type ->
            val hasCursor = if (healthProvider.canTrackChanges(type)) tokens[type] != null else anchors[type] != null
            PerTypeSync.needsResyncOnResume(PerTypeSync.pausedSince(type, requested, pausedAt, key), now, hasCursor)
        }
        return resumed.filter { PerTypeSync.trackedFor(listOf(it), needed, key).isNotEmpty() }
    }

    /**
     * Invalidates the tracked types that pausing [typeIds] stopped, so a running sync drops
     * them. Not the ids alone: pausing one alias of a record another alias still needs must
     * leave that record in the run.
     */
    fun onTypesPaused(typeIds: Collection<String>) {
        val tracked = knownTrackedTypes()
        val stopped = pausedTrackedTypes(tracked).filter {
            PerTypeSync.trackedFor(typeIds, listOf(it), healthProvider::storageKey).isNotEmpty()
        }
        invalidateTypes(stopped)
    }

    /**
     * After the initial export: schedules a resync for every unpaused type it left without a
     * change token (paused or dropped mid-export, resumed before the export ended).
     */
    private fun resyncTypesExportMissed() {
        if (!writesAllowed() || !healthProvider.supportsChangeTracking()) return
        val tracked = knownTrackedTypes()
        val paused = pausedTrackedTypes(tracked)
        val missed = PerTypeSync.typesMissingToken(
            tracked.filter { it !in paused }, loadChangeTokens(), healthProvider::canTrackChanges,
        )
        if (missed.isEmpty()) return
        logger("Sync: ${missed.joinToString()} missed the initial export, scheduling resync")
        scheduleResync(missed)
    }

    /** Unfinished work the worker / foreground hook should run: an export, a resumable run or a resync. */
    fun hasUnfinishedWork(): Boolean =
        hasResumableSyncSession() || !hasCompletedInitialSync() || actionablePendingResyncs().isNotEmpty()

    // MARK: - Sync Generation

    // The server's per-user data generation, echoed on every sync response. It is
    // bumped server-side when the user's synced data is deleted; a change means
    // every local cursor points into a dataset that no longer exists, so the sync
    // resets anchors and re-exports in full. Stored with the same lifecycle as the
    // anchors it validates (cleared together in resetAnchors).
    private fun syncGenerationKey(): String = "syncGeneration.${userKey()}"

    private fun loadSyncGeneration(): Long? =
        if (syncPrefs.contains(syncGenerationKey())) syncPrefs.getLong(syncGenerationKey(), 0L) else null

    private fun saveSyncGeneration(value: Long) {
        syncPrefs.edit().putLong(syncGenerationKey(), value).apply()
    }

    /** Applies a generation observed on an upload response. Returns true when the
     *  generation changed (server data was reset): anchors are already cleared and
     *  the caller must restart as a full export. A first-seen value is adopted
     *  silently — "nothing stored" must never count as a mismatch, or every fresh
     *  sign-in would trigger a second, redundant full export. */
    private fun applyServerSyncGeneration(serverGeneration: Long?): Boolean {
        if (serverGeneration == null) return false
        val known = loadSyncGeneration()
        if (known == null) {
            saveSyncGeneration(serverGeneration)
            return false
        }
        if (known == serverGeneration) return false
        logger("Sync generation changed ($known -> $serverGeneration): server-side data was reset")
        resetAnchors()
        saveSyncGeneration(serverGeneration)
        return true
    }

    private fun fullDoneKey(): String = "fullDone.${userKey()}"
    fun hasCompletedInitialExport(): Boolean = hasCompletedInitialSync()

    private fun hasCompletedInitialSync(): Boolean = syncPrefs.getBoolean(fullDoneKey(), false)
    private fun markFullExportDone() {
        if (!writesAllowed()) return
        syncPrefs.edit().putBoolean(fullDoneKey(), true).apply()
    }

    // MARK: - Sync State (Mutex-protected disk I/O)

    private fun syncStateDir(): File = File(context.filesDir, StorageKeys.SYNC_STATE_DIR).also { if (!it.exists()) it.mkdirs() }
    private fun syncStateFile(): File = File(syncStateDir(), StorageKeys.SYNC_STATE_FILE)

    private fun persistStateToDisk() {
        if (!writesAllowed()) return
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

    // `isSyncing` reports the process-wide lock: the background worker runs the sync on its
    // own SyncManager instance, so this instance's flag would read false during it.
    fun getSyncStatus(): Map<String, Any?> {
        val state = inMemoryState ?: loadSyncStateFromDisk()
        // Whether the initial full export (whole history) has ever completed for
        // this user. While false, apps can show a "keep the app open" hint.
        val initialExportDone = hasCompletedInitialSync()
        val pausedTypes = secureStorage.getPausedTypes().keys.toList()
        val resyncingTypes = actionablePendingResyncs()
        val syncState = if (processSyncLock.get()) "running" else expeditedSyncState()
        return if (state != null) {
            mapOf(
                "hasResumableSession" to state.hasProgress,
                "sentCount" to state.totalSentCount,
                "completedTypes" to state.completedTypes.size,
                "isFullExport" to state.fullExport,
                "initialExportDone" to initialExportDone,
                "isSyncing" to processSyncLock.get(),
                "syncState" to syncState,
                "createdAt" to dateFormatter.format(java.time.Instant.ofEpochMilli(state.createdAt)),
                "pausedTypes" to pausedTypes,
                "resyncingTypes" to resyncingTypes,
            )
        } else {
            mapOf(
                "hasResumableSession" to false,
                "sentCount" to 0,
                "completedTypes" to 0,
                "isFullExport" to false,
                "initialExportDone" to initialExportDone,
                "isSyncing" to processSyncLock.get(),
                "syncState" to syncState,
                "createdAt" to null,
                "pausedTypes" to pausedTypes,
                "resyncingTypes" to resyncingTypes,
            )
        }
    }

    /**
     * The one-off sync job (the one that runs the initial export): "scheduled" while WorkManager holds or runs it,
     * "idle" when it is done or missing, "unknown" when WorkManager doesn't answer in time. The periodic job is
     * always queued, so it is not counted.
     */
    private fun expeditedSyncState(): String = try {
        runBlocking {
            withTimeoutOrNull(SyncDefaults.WORK_INFO_TIMEOUT_MS) {
                WorkManager.getInstance(context)
                    .getWorkInfosForUniqueWorkFlow(SyncDefaults.WORK_NAME_EXPEDITED)
                    .first()
                    .map { it.state }
            }
        }?.let { if (isSyncWorkPending(it)) "scheduled" else "idle" } ?: "unknown"
    } catch (e: Exception) {
        "unknown"
    }

    fun hasResumableSyncSession(): Boolean {
        return (inMemoryState ?: loadSyncStateFromDisk())?.hasProgress == true
    }

    fun clearSyncSession() {
        clearSyncSessionInternal()
        logger("Cleared sync state")
    }
}
