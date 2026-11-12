package com.openwearables.health.sdk

import android.app.Activity
import android.content.Context
import android.content.SharedPreferences
import io.mockk.every
import io.mockk.mockk
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.runBlocking
import java.io.File
import java.nio.file.Files
import kotlin.test.AfterTest
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/**
 * Drives [SyncManager.syncNow] against a fake provider, in-memory prefs and a stubbed upload,
 * for the per-type pause / resync paths.
 */
class SyncManagerPerTypeTest {

    private lateinit var filesDir: File
    private val prefsByName = HashMap<String, InMemoryPrefs>()
    private val paused = LinkedHashMap<String, Long>()
    private val provider = FakeProvider()
    private val uploads = mutableListOf<UnifiedHealthData>()
    private lateinit var sm: SyncManager

    private val syncPrefs get() = prefsByName.getOrPut(StorageKeys.SYNC_PREFS_NAME) { InMemoryPrefs() }

    @BeforeTest
    fun setUp() {
        filesDir = Files.createTempDirectory("ow-sync-test").toFile()
        val context = mockk<Context>(relaxed = true)
        every { context.filesDir } returns filesDir
        every { context.getSharedPreferences(any(), any()) } answers {
            prefsByName.getOrPut(firstArg()) { InMemoryPrefs() }
        }
        val storage = mockk<SecureStorage>(relaxed = true)
        every { storage.getUserId() } returns "u1"
        every { storage.hasAuth } returns true
        every { storage.getEndpoints() } returns Endpoints(
            sync = "http://127.0.0.1:1/users/{user_id}/sync",
            logs = "http://127.0.0.1:1/users/{user_id}/logs",
        )
        every { storage.getSyncDaysBack() } returns 0
        every { storage.getTrackedTypes() } answers { provider.tracked.toList() }
        every { storage.getRequestedTypes() } returns emptyList()
        every { storage.getPausedTypes() } answers { LinkedHashMap(paused) }

        provider.tracked = setOf("steps", "heartRate")
        sm = SyncManager(context, storage, provider, UnconfinedDispatchers, logger = { println(it) })
        sm.uploadOverride = { _, data -> uploads.add(data); SyncManager.SendResult(true, 200, 1) }

        // Initial export done; both types have change tokens.
        syncPrefs.edit()
            .putBoolean("fullDone.user.u1", true)
            .putString(StorageKeys.KEY_CHANGE_TOKENS, """{"steps":"s0","heartRate":"h0"}""")
            .commit()
    }

    @AfterTest
    fun tearDown() {
        filesDir.deleteRecursively()
    }

    private fun pendingResyncs() = PerTypeSync.decodeIds(syncPrefs.getString(StorageKeys.KEY_RESYNC_TYPES, null))
    private fun storedTokens() = syncPrefs.getString(StorageKeys.KEY_CHANGE_TOKENS, null).orEmpty()

    @Test
    fun resyncWhoseTokenMintFailsFinishesInsteadOfLooping() = runBlocking {
        // Read permission revoked: no token can be minted and the history reads empty.
        provider.mintFails += "heartRate"
        sm.scheduleResync(listOf("heartRate"))

        sm.syncNow(fullExport = false)

        assertTrue(sm.lastRunCompleted)
        assertEquals(emptyList(), pendingResyncs())
        assertFalse(sm.hasResumableSyncSession())
        assertFalse(sm.hasUnfinishedWork())
        assertTrue(storedTokens().contains("\"heartRate\":\"h0\""), storedTokens())
        assertTrue("steps" in provider.changeReads)
    }

    @Test
    fun resyncOfUnreadableTypeIsDropped() = runBlocking {
        provider.unreadable += "heartRate"
        sm.scheduleResync(listOf("heartRate"))

        sm.syncNow(fullExport = false)

        assertEquals(emptyList(), pendingResyncs())
        assertFalse("heartRate" in provider.descendingReads)
        assertFalse(sm.hasUnfinishedWork())
    }

    @Test
    fun typePausedMidRunIsNotReExportedAfterGenerationReset() = runBlocking {
        syncPrefs.edit().putLong("syncGeneration.user.u1", 1L).commit()
        provider.changes["steps"] = ArrayDeque(listOf(data("steps", "s-new")))
        provider.history["steps"] = data("steps", "s-old")
        provider.history["heartRate"] = data("heartRate", "h-old")
        var first = true
        sm.uploadOverride = { _, data ->
            uploads.add(data)
            if (first) {
                first = false
                // The app's "delete and pause": pause, then the server delete bumps the generation.
                paused["heartRate"] = System.currentTimeMillis()
                sm.onTypesPaused(listOf("heartRate"))
            }
            SyncManager.SendResult(true, 200, 1, syncGeneration = 2L)
        }

        sm.syncNow(fullExport = false)

        assertTrue("steps" in provider.descendingReads, "restart re-exports unpaused types")
        assertFalse("heartRate" in provider.descendingReads, "paused type must not be re-read")
        assertTrue(uploads.drop(1).flatMap { it.records }.none { it.type == "heartRate" })
        assertTrue(sm.lastRunCompleted)
    }

    @Test
    fun resyncRequestedWhileTheSameResyncCompletesIsKept() = runBlocking {
        provider.history["heartRate"] = data("heartRate", "h-old")
        sm.scheduleResync(listOf("heartRate"))
        var requested = false
        sm.uploadOverride = { _, data ->
            uploads.add(data)
            if (!requested && data.records.any { it.type == "heartRate" }) {
                requested = true
                // Lands after the run read the type's last page, before it is marked complete.
                sm.scheduleResync(listOf("heartRate"))
            }
            SyncManager.SendResult(true, 200, 1)
        }

        sm.syncNow(fullExport = false)

        assertTrue(requested)
        assertEquals(listOf("heartRate"), pendingResyncs())
        // The superseded resync did not store its token.
        assertTrue(storedTokens().contains("\"heartRate\":\"h0\""), storedTokens())
        assertTrue(sm.hasUnfinishedWork())

        // The next run performs the kept request and clears it.
        sm.uploadOverride = { _, data -> uploads.add(data); SyncManager.SendResult(true, 200, 1) }
        sm.syncNow(fullExport = false)
        assertEquals(emptyList(), pendingResyncs())
        assertFalse(storedTokens().contains("\"heartRate\":\"h0\""), storedTokens())
    }

    private fun data(type: String, id: String) = UnifiedHealthData(
        records = listOf(
            UnifiedRecord(
                id = id, type = type,
                startDate = "2026-01-01T00:00:00.000Z", endDate = "2026-01-01T00:00:00.000Z",
                zoneOffset = null,
                source = UnifiedSource(null, null, null, null, null, null, null),
                value = 1.0, unit = "count", parentId = null, metadata = null,
            )
        )
    )

    private object UnconfinedDispatchers : DispatcherProvider {
        override val main: CoroutineDispatcher = Dispatchers.Unconfined
        override val io: CoroutineDispatcher = Dispatchers.Unconfined
        override val default: CoroutineDispatcher = Dispatchers.Unconfined
    }

    /** Health Connect-like provider: change tokens, one-page history per type. */
    private class FakeProvider : HealthDataProvider {
        override val providerId = "google"
        override val providerName = "Fake"
        var tracked: Set<String> = emptySet()
        val mintFails = mutableSetOf<String>()
        val unreadable = mutableSetOf<String>()
        val history = mutableMapOf<String, UnifiedHealthData>()
        val changes = mutableMapOf<String, ArrayDeque<UnifiedHealthData>>()
        val descendingReads = mutableListOf<String>()
        val changeReads = mutableListOf<String>()
        private var tokenSeq = 0

        override fun isAvailable() = true
        override suspend fun connect() = true
        override fun disconnect() {}
        override fun setActivity(activity: Activity?) {}
        override fun setTrackedTypes(typeIds: List<String>) { tracked = typeIds.toSet() }
        override fun getTrackedTypes() = tracked
        override suspend fun requestAuthorization(typeIds: List<String>) = true
        override suspend fun readData(typeId: String, sinceTimestamp: Long?, limit: Int) =
            ProviderReadResult(UnifiedHealthData(), null)

        override suspend fun readDataDescending(typeId: String, olderThanTimestamp: Long?, limit: Int): ProviderReadResult {
            descendingReads += typeId
            val page = if (olderThanTimestamp == null && typeId !in unreadable) history[typeId] else null
            return ProviderReadResult(
                page ?: UnifiedHealthData(), maxTimestamp = 2_000L, minTimestamp = 1_000L,
                exhaustive = true, recordCount = page?.totalCount ?: 0,
            )
        }

        override fun supportsChangeTracking() = true
        override suspend fun hasReadPermission(typeId: String) = typeId !in unreadable
        override suspend fun mintChangeToken(typeId: String): String? =
            if (typeId in mintFails || typeId in unreadable) null else "tok-$typeId-${++tokenSeq}"

        override suspend fun readChanges(typeId: String, token: String): ChangeReadResult {
            changeReads += typeId
            val page = changes[typeId]?.removeFirstOrNull() ?: UnifiedHealthData()
            return ChangeReadResult(data = page, nextToken = "$token+", hasMore = false)
        }
    }

    private class InMemoryPrefs : SharedPreferences {
        private val values = HashMap<String, Any?>()

        override fun getAll(): MutableMap<String, *> = HashMap(values)
        override fun getString(key: String, defValue: String?) = values[key] as String? ?: defValue
        @Suppress("UNCHECKED_CAST")
        override fun getStringSet(key: String, defValues: MutableSet<String>?) =
            values[key] as MutableSet<String>? ?: defValues
        override fun getInt(key: String, defValue: Int) = values[key] as Int? ?: defValue
        override fun getLong(key: String, defValue: Long) = values[key] as Long? ?: defValue
        override fun getFloat(key: String, defValue: Float) = values[key] as Float? ?: defValue
        override fun getBoolean(key: String, defValue: Boolean) = values[key] as Boolean? ?: defValue
        override fun contains(key: String) = values.containsKey(key)
        override fun registerOnSharedPreferenceChangeListener(l: SharedPreferences.OnSharedPreferenceChangeListener?) {}
        override fun unregisterOnSharedPreferenceChangeListener(l: SharedPreferences.OnSharedPreferenceChangeListener?) {}

        override fun edit(): SharedPreferences.Editor = object : SharedPreferences.Editor {
            private val puts = HashMap<String, Any?>()
            private val removes = HashSet<String>()
            private var clear = false
            override fun putString(key: String, value: String?) = apply { puts[key] = value }
            override fun putStringSet(key: String, values: MutableSet<String>?) = apply { puts[key] = values }
            override fun putInt(key: String, value: Int) = apply { puts[key] = value }
            override fun putLong(key: String, value: Long) = apply { puts[key] = value }
            override fun putFloat(key: String, value: Float) = apply { puts[key] = value }
            override fun putBoolean(key: String, value: Boolean) = apply { puts[key] = value }
            override fun remove(key: String) = apply { removes.add(key) }
            override fun clear() = apply { clear = true }
            override fun commit(): Boolean {
                synchronized(values) {
                    if (clear) values.clear()
                    removes.forEach { values.remove(it) }
                    values.putAll(puts)
                }
                return true
            }
            override fun apply() { commit() }
        }
    }
}
