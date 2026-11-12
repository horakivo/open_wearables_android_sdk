package com.openwearables.health.sdk

import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class PerTypeSyncTest {

    // Health Connect-like folding: the distance ids read one record, as do the blood-pressure ids.
    private val storageKey: (String) -> String? = { id ->
        when (id) {
            "distanceWalkingRunning", "distanceCycling" -> "Distance"
            "bloodPressure", "bloodPressureSystolic", "bloodPressureDiastolic" -> "BloodPressure"
            "heartRate" -> "HeartRate"
            "steps" -> "Steps"
            else -> null // e.g. distanceSwimming: no Health Connect record
        }
    }

    private val requested = listOf(
        "steps", "heartRate", "distanceCycling", "distanceWalkingRunning", "distanceSwimming",
        "bloodPressureSystolic", "bloodPressureDiastolic",
    )
    // First requested id of each group is the one tracked.
    private val tracked = listOf("steps", "heartRate", "distanceCycling", "bloodPressureSystolic")

    private fun paused(vararg ids: String) =
        PerTypeSync.pausedTrackedTypes(tracked, requested, ids.toSet(), storageKey)

    @Test
    fun plainTypeIsPausedOnItsOwn() {
        assertEquals(setOf("heartRate"), paused("heartRate"))
    }

    @Test
    fun aliasGroupPausesOnlyWhenEveryRequestedAliasIsPaused() {
        assertEquals(emptySet(), paused("distanceCycling"))
        assertEquals(emptySet(), paused("distanceWalkingRunning"))
        assertEquals(setOf("distanceCycling"), paused("distanceCycling", "distanceWalkingRunning"))
    }

    @Test
    fun aliasWithoutRecordDoesNotBlockTheGroup() {
        // distanceSwimming folds into nothing on Health Connect.
        assertEquals(setOf("distanceCycling"), paused("distanceWalkingRunning", "distanceCycling"))
        assertEquals(emptySet(), paused("distanceSwimming"))
    }

    @Test
    fun bloodPressureIdsFoldIntoOneTrackedType() {
        assertEquals(emptySet(), paused("bloodPressureDiastolic"))
        assertEquals(setOf("bloodPressureSystolic"), paused("bloodPressureSystolic", "bloodPressureDiastolic"))
    }

    @Test
    fun withoutRequestedListTheGroupIsTheTrackedId() {
        val result = PerTypeSync.pausedTrackedTypes(tracked, emptyList(), setOf("distanceCycling"), storageKey)
        assertEquals(setOf("distanceCycling"), result)
        val viaAlias = PerTypeSync.pausedTrackedTypes(tracked, emptyList(), setOf("distanceWalkingRunning"), storageKey)
        assertEquals(emptySet(), viaAlias)
    }

    @Test
    fun wrapperIdsMapToTheTrackedRepresentative() {
        assertEquals(
            setOf("distanceCycling", "bloodPressureSystolic"),
            PerTypeSync.trackedFor(
                listOf("distanceWalkingRunning", "bloodPressureDiastolic", "distanceSwimming", "unknown"),
                tracked, storageKey,
            ),
        )
    }

    @Test
    fun onlyResyncsOfActiveTrackedTypesCountAsWork() {
        val pending = listOf("heartRate", "distanceWalkingRunning", "distanceSwimming", "sleep", "steps")
        val result = PerTypeSync.actionableResyncs(
            pending, tracked, requested, setOf("steps", "distanceCycling"), storageKey,
        )
        // steps is paused; distanceSwimming and sleep fold into no tracked type; the distance
        // group still has distanceWalkingRunning unpaused.
        assertEquals(listOf("heartRate", "distanceWalkingRunning"), result)
    }

    @Test
    fun exportSweepPicksActiveTypesWithoutToken() {
        val missed = PerTypeSync.typesMissingToken(
            activeTypes = listOf("steps", "heartRate", "unsupported"),
            tokens = mapOf("steps" to "t1"),
            canTrackChanges = { it != "unsupported" },
        )
        // steps has a token; unsupported has no change stream; paused types are not passed in.
        assertEquals(listOf("heartRate"), missed)
    }

    @Test
    fun groupPausedSinceItsLastAliasWasPaused() {
        val pausedAt = mapOf("distanceCycling" to 100L, "distanceWalkingRunning" to 300L, "heartRate" to 50L)
        assertEquals(300L, PerTypeSync.pausedSince("distanceCycling", requested, pausedAt, storageKey))
        assertEquals(50L, PerTypeSync.pausedSince("heartRate", requested, pausedAt, storageKey))
        assertEquals(null, PerTypeSync.pausedSince("steps", requested, pausedAt, storageKey))
    }

    @Test
    fun resumeAfterThirtyDaysResyncs() {
        val day = 24L * 60 * 60 * 1000
        val now = 1_000L * day
        assertFalse(PerTypeSync.needsResyncOnResume(now - 29 * day, now, hasCursor = true))
        assertFalse(PerTypeSync.needsResyncOnResume(now - 30 * day + 1, now, hasCursor = true))
        assertTrue(PerTypeSync.needsResyncOnResume(now - 30 * day, now, hasCursor = true))
        assertTrue(PerTypeSync.needsResyncOnResume(now - 90 * day, now, hasCursor = true))
    }

    @Test
    fun resumeWithoutCursorResyncs() {
        assertTrue(PerTypeSync.needsResyncOnResume(pausedSince = 0L, now = 1L, hasCursor = false))
        assertTrue(PerTypeSync.needsResyncOnResume(pausedSince = null, now = 1L, hasCursor = false))
        assertFalse(PerTypeSync.needsResyncOnResume(pausedSince = null, now = 1L, hasCursor = true))
    }

    private val json = Json { ignoreUnknownKeys = true }

    @Test
    fun resyncScopeSurvivesPersistence() {
        val state = SyncState(userKey = "user.u1", fullExport = false, createdAt = 1L)
        state.typeProgress["heartRate"] = TypeSyncProgress("heartRate", sentCount = 5, pendingChangeToken = "old")
        state.completedTypes.add("steps")

        val started = PerTypeSync.applyPendingResyncs(state, setOf("heartRate"))
        assertEquals(setOf("heartRate"), started)
        // Fresh cursors: a new token is minted before the re-read.
        assertEquals(null, state.typeProgress.getValue("heartRate").pendingChangeToken)
        assertEquals(5, state.typeProgress.getValue("heartRate").sentCount)

        // Interrupted mid-resync: the resumed run is a resync of heartRate only.
        state.typeProgress.getValue("heartRate").pendingOlderThan = 42L
        state.totalSentCount = 10
        val restored = json.decodeFromString<SyncState>(json.encodeToString(state))
        assertEquals(setOf("heartRate"), restored.resyncTypes)
        assertEquals(setOf("heartRate"), PerTypeSync.fullExportScope(restored, listOf("steps", "heartRate")))

        // The same pending resync continues instead of restarting.
        assertEquals(emptySet(), PerTypeSync.applyPendingResyncs(restored, setOf("heartRate")))
        assertEquals(42L, restored.typeProgress.getValue("heartRate").pendingOlderThan)

        // A paused resync type leaves the scope.
        assertEquals(emptySet(), PerTypeSync.fullExportScope(restored, listOf("steps")))
    }

    @Test
    fun finishedResyncRestartsOnNewRequest() {
        val state = SyncState(userKey = "u", fullExport = false, createdAt = 1L)
        state.resyncTypes.add("heartRate")
        state.completedTypes.add("heartRate")
        assertEquals(setOf("heartRate"), PerTypeSync.applyPendingResyncs(state, setOf("heartRate")))
        assertFalse("heartRate" in state.completedTypes)
    }

    @Test
    fun initialExportOnlyReReadsTypesItAlreadyFinished() {
        val state = SyncState(userKey = "u", fullExport = true, createdAt = 1L)
        state.completedTypes.add("steps")
        val started = PerTypeSync.applyPendingResyncs(state, setOf("steps", "heartRate"))
        assertEquals(setOf("steps"), started)
        assertFalse("steps" in state.completedTypes)
        assertTrue(state.resyncTypes.isEmpty())
        assertEquals(setOf("steps", "heartRate"), PerTypeSync.fullExportScope(state, listOf("steps", "heartRate")))
    }

    @Test
    fun stateFromOlderSdkDecodesWithoutResyncScope() {
        val legacy = """{"userKey":"user.u1","fullExport":true,"createdAt":1,"completedTypes":["steps"]}"""
        val state = json.decodeFromString<SyncState>(legacy)
        assertTrue(state.resyncTypes.isEmpty())
        assertEquals(setOf("heartRate"), PerTypeSync.fullExportScope(state, listOf("heartRate")))
        val incremental = json.decodeFromString<SyncState>(legacy.replace("\"fullExport\":true", "\"fullExport\":false"))
        assertEquals(emptySet(), PerTypeSync.fullExportScope(incremental, listOf("heartRate")))
    }

    @Test
    fun pausedListKeepsOrderAndTimestamps() {
        val map = linkedMapOf("sleep" to 3L, "heartRate" to 1L, "distanceCycling" to 2L)
        val decoded = PerTypeSync.decodePaused(PerTypeSync.encodePaused(map))
        assertEquals(listOf("sleep", "heartRate", "distanceCycling"), decoded.keys.toList())
        assertEquals(map, decoded)
        assertTrue(PerTypeSync.decodePaused("not json").isEmpty())
        assertEquals(listOf("a", "b"), PerTypeSync.decodeIds(PerTypeSync.encodeIds(listOf("a", "b"))))
    }
}
