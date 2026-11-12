package com.openwearables.health.sdk

import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json

/**
 * Per-type pause / resume / resync decisions, kept free of Android and provider types.
 *
 * The app sends its own type ids ("wrapper ids", e.g. `distanceCycling`). A provider reads
 * several of them from one store record (Health Connect: `distanceWalkingRunning` and
 * `distanceCycling` both read `DistanceRecord`) and tracks only one of them, the first one
 * requested (the "tracked id"). [storageKey] names the record an id reads, so ids with the
 * same key are aliases of one tracked id; `null` means the provider cannot read the id.
 *
 * A tracked id is paused only when **every** requested id that folds into it is paused:
 * one Health Connect read serves all of them, so pausing `distanceCycling` alone cannot
 * stop the record that `distanceWalkingRunning` still needs. Without a stored requested
 * list (an install that has not called `requestAuthorization` since upgrading), the
 * group is just the tracked id itself.
 */
internal object PerTypeSync {

    private val json = Json { ignoreUnknownKeys = true }

    /** Requested ids (plus the tracked id itself) that fold into [trackedId]. */
    fun aliasGroup(
        trackedId: String,
        requested: Collection<String>,
        storageKey: (String) -> String?,
    ): Set<String> {
        val key = storageKey(trackedId) ?: return setOf(trackedId)
        return requested.filterTo(linkedSetOf()) { storageKey(it) == key } + trackedId
    }

    /** Tracked ids whose whole alias group is paused; the sync loop skips these. */
    fun pausedTrackedTypes(
        tracked: Collection<String>,
        requested: Collection<String>,
        paused: Set<String>,
        storageKey: (String) -> String?,
    ): Set<String> {
        if (paused.isEmpty()) return emptySet()
        return tracked.filterTo(linkedSetOf()) { id ->
            aliasGroup(id, requested, storageKey).all { it in paused }
        }
    }

    /** Tracked ids that [ids] fold into. Ids the provider cannot read, or that fold into no tracked id, are dropped. */
    fun trackedFor(
        ids: Collection<String>,
        tracked: Collection<String>,
        storageKey: (String) -> String?,
    ): Set<String> {
        val byKey = HashMap<String, String>()
        for (t in tracked) byKey[storageKey(t) ?: t] = t
        return ids.mapNotNullTo(linkedSetOf()) { id -> storageKey(id)?.let { byKey[it] } }
    }

    /**
     * Pending resync ids that can run: they fold into a tracked type that is not paused.
     * Others (paused, never authorized, unreadable) stay stored but must not keep the
     * worker re-enqueueing itself for work no run will do.
     */
    fun actionableResyncs(
        pending: List<String>,
        tracked: Collection<String>,
        requested: Collection<String>,
        paused: Set<String>,
        storageKey: (String) -> String?,
    ): List<String> {
        if (pending.isEmpty()) return emptyList()
        val pausedTracked = pausedTrackedTypes(tracked, requested, paused, storageKey)
        val active = tracked.filter { it !in pausedTracked }
        return pending.filter { trackedFor(listOf(it), active, storageKey).isNotEmpty() }
    }

    /**
     * Active types that a finished initial export left without a change token: paused or
     * dropped during the export and resumed before it ended. Without a resync their next
     * incremental read would mint a token and catch up only 30 days.
     */
    fun typesMissingToken(
        activeTypes: Collection<String>,
        tokens: Map<String, String>,
        canTrackChanges: (String) -> Boolean,
    ): List<String> = activeTypes.filter { canTrackChanges(it) && tokens[it] == null }

    /**
     * When [trackedId] stopped syncing: the latest pause among its alias group, because the
     * record kept syncing until the last alias was paused. `null` when no alias is paused.
     */
    fun pausedSince(
        trackedId: String,
        requested: Collection<String>,
        pausedAt: Map<String, Long>,
        storageKey: (String) -> String?,
    ): Long? = aliasGroup(trackedId, requested, storageKey).mapNotNull { pausedAt[it] }.maxOrNull()

    /**
     * Whether resuming a type must re-read its history instead of continuing from its cursor:
     * it has no cursor (paused before its first export finished), or it was paused for at
     * least [SyncDefaults.PAUSE_RESYNC_AFTER_MS] (Health Connect change tokens expire after
     * ~30 days; an expired token would only catch up 30 days and miss deletions).
     */
    fun needsResyncOnResume(pausedSince: Long?, now: Long, hasCursor: Boolean): Boolean {
        if (!hasCursor) return true
        return pausedSince != null && now - pausedSince >= SyncDefaults.PAUSE_RESYNC_AFTER_MS
    }

    /**
     * Starts the pending resyncs [pending] (tracked ids) in [state]. Returns the ids started.
     *
     * - Incremental state: a type not yet resyncing, or one whose resync already finished
     *   in this state, restarts from the newest record with fresh cursors. A type whose
     *   resync is in progress continues from its saved cursor.
     * - Full-export state (the initial export): a type the export has not finished yet is
     *   left to it (its completion clears the pending resync); a finished one is re-read.
     */
    fun applyPendingResyncs(state: SyncState, pending: Set<String>): Set<String> {
        val started = linkedSetOf<String>()
        for (type in pending) {
            val completed = type in state.completedTypes
            val restart = if (state.fullExport) completed else (type !in state.resyncTypes || completed)
            if (!restart) continue
            state.completedTypes.remove(type)
            val sent = state.typeProgress[type]?.sentCount ?: 0
            state.typeProgress[type] = TypeSyncProgress(type, sentCount = sent)
            if (!state.fullExport) state.resyncTypes.add(type)
            started.add(type)
        }
        return started
    }

    /** Types this run reads as a full export: every type of the initial export, else the resyncs. */
    fun fullExportScope(state: SyncState, activeTypes: Collection<String>): Set<String> =
        if (state.fullExport) activeTypes.toSet()
        else activeTypes.filterTo(linkedSetOf()) { it in state.resyncTypes }

    // MARK: - Storage codecs (insertion order kept: getPausedTypes returns ids in pause order)

    fun encodePaused(pausedAt: Map<String, Long>): String = json.encodeToString(pausedAt)

    fun decodePaused(raw: String?): LinkedHashMap<String, Long> {
        if (raw.isNullOrBlank()) return LinkedHashMap()
        return try {
            LinkedHashMap(json.decodeFromString<Map<String, Long>>(raw))
        } catch (_: Exception) {
            LinkedHashMap()
        }
    }

    fun encodeIds(ids: Collection<String>): String = json.encodeToString(ids.toList())

    fun decodeIds(raw: String?): List<String> {
        if (raw.isNullOrBlank()) return emptyList()
        return try {
            json.decodeFromString<List<String>>(raw)
        } catch (_: Exception) {
            emptyList()
        }
    }
}
