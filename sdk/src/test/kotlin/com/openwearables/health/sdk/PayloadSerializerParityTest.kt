package com.openwearables.health.sdk

import kotlin.test.Test
import kotlin.test.assertEquals

/**
 * Guards the streaming payload serializer ([writeUnifiedPayload]) against drift from the
 * `toMap()`/`toDataMap()` models. If a field is added to a data class but not to its
 * `writeX` function (or vice-versa), the two representations diverge and this fails.
 *
 * Both paths are serialized through the same [JsonSink] recorder, so number normalization
 * (Int→Long, Float→Double, …) is applied identically — the comparison is purely about
 * which keys/values each path emits, which is exactly the drift we want to catch.
 */
class PayloadSerializerParityTest {

    @Test
    fun `streaming serializer emits the same payload as the toMap path`() {
        val data = sampleData()
        val provider = "google"
        val sdkVersion = "9.9.9"
        val syncTimestamp = "2026-07-01T10:00:00.000Z"

        // Reference: the old envelope map (built from toMap/toDataMap) serialized through the
        // shared generic writer.
        val expected = RecordingJsonSink().also { sink ->
            writeJsonValue(
                sink,
                mapOf(
                    "provider" to provider,
                    "sdkVersion" to sdkVersion,
                    "syncTimestamp" to syncTimestamp,
                    "data" to data.toDataMap()
                )
            )
        }.result

        // Under test: fields streamed directly, no intermediate maps.
        val actual = RecordingJsonSink().also { sink ->
            writeUnifiedPayload(sink, provider, sdkVersion, syncTimestamp, data)
        }.result

        assertEquals(expected, actual, "streamed payload differs from toMap() payload")
        // Order-sensitive check (LinkedHashMap.toString) so a reordered field is caught too.
        assertEquals(expected.toString(), actual.toString(), "field order differs from toMap()")
    }

    private fun sampleData(): UnifiedHealthData {
        val source = UnifiedSource(
            appId = "com.garmin.android.apps.connectmobile",
            deviceId = null,
            deviceName = "Fenix",
            deviceManufacturer = "Garmin",
            deviceModel = null,
            deviceType = "watch",
            recordingMethod = "automatic"
        )
        // metadata exercising every branch of writeJsonValue: Int, Long, Double, Boolean,
        // String, null, nested map, and list.
        val richMetadata: Map<String, Any?> = mapOf(
            "intVal" to 7,
            "longVal" to 7_000_000_000L,
            "doubleVal" to 1.5,
            "boolVal" to true,
            "strVal" to "x",
            "nullVal" to null,
            "nested" to mapOf("list" to listOf(1, 2, 3), "flag" to false)
        )

        val records = listOf(
            UnifiedRecord(
                id = "hr-1", type = "HEART_RATE", startDate = "2026-06-18T01:00:00.000Z",
                endDate = "2026-06-18T01:00:00.000Z", zoneOffset = "+02:00", source = source,
                value = 72.0, unit = "bpm", parentId = "parent-1", metadata = richMetadata
            ),
            // nullable fields + null metadata
            UnifiedRecord(
                id = "steps-1", type = "STEP_COUNT", startDate = "2026-06-18T00:00:00.000Z",
                endDate = "2026-06-18T01:00:00.000Z", zoneOffset = null, source = source,
                value = 1234.0, unit = "count", parentId = null, metadata = null
            )
        )

        val workouts = listOf(
            UnifiedWorkout(
                id = "w-1", parentId = null, type = "running",
                startDate = "2026-06-17T08:00:00.000Z", endDate = "2026-06-17T09:00:00.000Z",
                zoneOffset = "+02:00", source = source, title = "Morning run", notes = null,
                values = listOf(mapOf("type" to "duration", "value" to 3600.0, "unit" to "s")),
                segments = listOf(mapOf<String, Any?>("startDate" to "a", "reps" to 10)),
                laps = null,
                route = listOf(mapOf<String, Any?>("lat" to 1.0, "lng" to 2.0, "alt" to null)),
                samples = null,
                metadata = null
            )
        )

        val sleep = listOf(
            UnifiedSleep(
                id = "sl-1", parentId = "sleep-parent", stage = "deep",
                startDate = "2026-06-17T23:00:00.000Z", endDate = "2026-06-18T05:00:00.000Z",
                zoneOffset = "+02:00", source = source,
                values = listOf(mapOf("k" to "v")), metadata = richMetadata
            )
        )

        val deleted = listOf(
            UnifiedDeleted(id = "gone-1", type = "HEART_RATE"),
            UnifiedDeleted(id = "gone-2", type = "SLEEP")
        )

        return UnifiedHealthData(records = records, workouts = workouts, sleep = sleep, deleted = deleted)
    }
}

/** In-memory [JsonSink] that reconstructs the emitted structure as maps/lists/scalars. */
private class RecordingJsonSink : JsonSink {
    var result: Any? = null
        private set

    private val stack = ArrayDeque<Any>()
    private var pendingName: String? = null

    private fun emit(value: Any?) {
        when (val top = stack.lastOrNull()) {
            null -> result = value
            is MutableList<*> -> {
                @Suppress("UNCHECKED_CAST")
                (top as MutableList<Any?>).add(value)
            }
            is MutableMap<*, *> -> {
                val key = requireNotNull(pendingName) { "value emitted in object without a name" }
                pendingName = null
                @Suppress("UNCHECKED_CAST")
                (top as MutableMap<String, Any?>)[key] = value
            }
            else -> error("unexpected container on stack")
        }
    }

    private fun open(container: Any) {
        emit(container)
        stack.addLast(container)
    }

    override fun beginObject() = open(LinkedHashMap<String, Any?>())
    override fun endObject() { stack.removeLast() }
    override fun beginArray() = open(ArrayList<Any?>())
    override fun endArray() { stack.removeLast() }
    override fun name(name: String) { pendingName = name }
    override fun value(value: String?) = emit(value)
    override fun value(value: Double) = emit(value)
    override fun value(value: Long) = emit(value)
    override fun value(value: Boolean) = emit(value)
    override fun nullValue() = emit(null)
}
