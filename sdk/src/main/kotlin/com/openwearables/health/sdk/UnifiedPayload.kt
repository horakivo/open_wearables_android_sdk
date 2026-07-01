package com.openwearables.health.sdk

import java.time.Instant
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter

/**
 * Unified health data models following the Unified Health Payload specification.
 * All providers (Apple Health, Samsung Health, Health Connect) convert their
 * native data into these structures before syncing.
 */

// ---------------------------------------------------------------------------
// Source
// ---------------------------------------------------------------------------

data class UnifiedSource(
    val appId: String?,
    val deviceId: String?,
    val deviceName: String?,
    val deviceManufacturer: String?,
    val deviceModel: String?,
    val deviceType: String?,
    val recordingMethod: String?
) {
    fun toMap(): Map<String, Any?> = mapOf(
        "appId" to appId,
        "deviceId" to deviceId,
        "deviceName" to deviceName,
        "deviceManufacturer" to deviceManufacturer,
        "deviceModel" to deviceModel,
        "deviceType" to deviceType,
        "recordingMethod" to recordingMethod
    )
}

// ---------------------------------------------------------------------------
// Record (10 keys)
// ---------------------------------------------------------------------------

data class UnifiedRecord(
    val id: String,
    val type: String,
    val startDate: String,
    val endDate: String,
    val zoneOffset: String?,
    val source: UnifiedSource,
    val value: Double,
    val unit: String,
    val parentId: String?,
    val metadata: Map<String, Any?>?
) {
    fun toMap(): Map<String, Any?> = mapOf(
        "id" to id,
        "type" to type,
        "startDate" to startDate,
        "endDate" to endDate,
        "zoneOffset" to zoneOffset,
        "source" to source.toMap(),
        "value" to value,
        "unit" to unit,
        "parentId" to parentId,
        "metadata" to metadata
    )
}

// ---------------------------------------------------------------------------
// Workout (15 keys)
// ---------------------------------------------------------------------------

data class UnifiedWorkout(
    val id: String,
    val parentId: String?,
    val type: String,
    val startDate: String,
    val endDate: String,
    val zoneOffset: String?,
    val source: UnifiedSource,
    val title: String?,
    val notes: String?,
    val values: List<Map<String, Any>>?,
    val segments: List<Map<String, Any?>>?,
    val laps: List<Map<String, Any?>>?,
    val route: List<Map<String, Any?>>?,
    val samples: List<Map<String, Any?>>?,
    val metadata: Map<String, Any?>?
) {
    fun toMap(): Map<String, Any?> = mapOf(
        "id" to id,
        "parentId" to parentId,
        "type" to type,
        "startDate" to startDate,
        "endDate" to endDate,
        "zoneOffset" to zoneOffset,
        "source" to source.toMap(),
        "title" to title,
        "notes" to notes,
        "values" to values,
        "segments" to segments,
        "laps" to laps,
        "route" to route,
        "samples" to samples,
        "metadata" to metadata
    )
}

// ---------------------------------------------------------------------------
// Sleep (9 keys)
// ---------------------------------------------------------------------------

data class UnifiedSleep(
    val id: String,
    val parentId: String?,
    val stage: String,
    val startDate: String,
    val endDate: String,
    val zoneOffset: String?,
    val source: UnifiedSource,
    val values: List<Map<String, Any>>?,
    val metadata: Map<String, Any?>?
) {
    fun toMap(): Map<String, Any?> = mapOf(
        "id" to id,
        "parentId" to parentId,
        "stage" to stage,
        "startDate" to startDate,
        "endDate" to endDate,
        "zoneOffset" to zoneOffset,
        "source" to source.toMap(),
        "values" to values,
        "metadata" to metadata
    )
}

// ---------------------------------------------------------------------------
// Aggregated read result
// ---------------------------------------------------------------------------

data class UnifiedHealthData(
    val records: List<UnifiedRecord> = emptyList(),
    val workouts: List<UnifiedWorkout> = emptyList(),
    val sleep: List<UnifiedSleep> = emptyList()
) {
    val isEmpty: Boolean
        get() = records.isEmpty() && workouts.isEmpty() && sleep.isEmpty()

    val totalCount: Int
        get() = records.size + workouts.size + sleep.size

    fun toDataMap(): Map<String, Any> = mapOf(
        "records" to records.map { it.toMap() },
        "workouts" to workouts.map { it.toMap() },
        "sleep" to sleep.map { it.toMap() }
    )

    /**
     * Returns a copy with only records/workouts/sleep whose startDate >= [floorIso].
     * The ISO string comparison works because dates are in ISO-8601 format (lexicographic order).
     */
    fun filterSince(floorIso: String): UnifiedHealthData = UnifiedHealthData(
        records = records.filter { it.startDate >= floorIso },
        workouts = workouts.filter { it.startDate >= floorIso },
        sleep = sleep.filter { it.startDate >= floorIso },
    )
}

data class ProviderReadResult(
    val data: UnifiedHealthData,
    val maxTimestamp: Long?,
    val minTimestamp: Long? = null,
    // Number of *parent* Health Connect records returned by this page (before any
    // expansion into samples/stages). This is what `pageSize` actually bounds, so it
    // is the correct unit for the "last page" check — comparing the expanded sample
    // count against the page size is a unit mismatch that breaks paging for series/
    // session types (e.g. sleep). 0 for empty/error reads, which reads as "last page".
    val recordCount: Int = 0
)

// ---------------------------------------------------------------------------
// Timestamp helpers
// ---------------------------------------------------------------------------

object UnifiedTimestamp {
    private val isoFormatter: DateTimeFormatter =
        DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'")
            .withZone(ZoneOffset.UTC)

    fun fromEpochMs(epochMs: Long): String =
        isoFormatter.format(Instant.ofEpochMilli(epochMs))

}

// ---------------------------------------------------------------------------
// Samsung device type → unified device type mapping
// ---------------------------------------------------------------------------

object DeviceTypeMapper {
    fun fromSamsungDeviceType(type: String?): String? = when (type?.uppercase()) {
        "MOBILE" -> "phone"
        "WATCH" -> "watch"
        "RING" -> "ring"
        "BAND" -> "fitness_band"
        "ACCESSORY" -> "unknown"
        else -> null
    }

    fun fromHealthConnectDeviceType(type: Int): String? = when (type) {
        0 -> "unknown"
        1 -> "watch"
        2 -> "phone"
        3 -> "scale"
        4 -> "ring"
        5 -> "head_mounted"
        6 -> "fitness_band"
        7 -> "chest_strap"
        8 -> "smart_display"
        else -> "unknown"
    }

    fun fromHealthConnectRecordingMethod(method: Int): String? = when (method) {
        1 -> "active"
        2 -> "automatic"
        3 -> "manual"
        else -> "unknown"
    }
}

// ---------------------------------------------------------------------------
// Streaming payload serializer
// ---------------------------------------------------------------------------

/**
 * Minimal streaming JSON sink. Lets the payload serializer target both
 * `android.util.JsonWriter` (production) and an in-memory recorder (tests) without
 * pulling Android onto the unit-test classpath.
 */
interface JsonSink {
    fun beginObject()
    fun endObject()
    fun beginArray()
    fun endArray()
    fun name(name: String)
    fun value(value: String?)
    fun value(value: Double)
    fun value(value: Long)
    fun value(value: Boolean)
    fun nullValue()
}

/**
 * Streams the full sync payload (envelope + data) straight to [sink], writing each record's
 * fields directly instead of materializing a `Map` per record. This avoids the ~40 short-lived
 * objects/record that `toDataMap()`/`toMap()` allocate (a major GC source during large syncs).
 *
 * Emits JSON semantically identical to the `toMap()`-based path — enforced by
 * PayloadSerializerParityTest. Keep the field lists here in lock-step with the `toMap()`
 * functions above.
 */
fun writeUnifiedPayload(
    sink: JsonSink,
    provider: String,
    sdkVersion: String,
    syncTimestamp: String,
    data: UnifiedHealthData
) {
    sink.beginObject()
    sink.name("provider"); sink.value(provider)
    sink.name("sdkVersion"); sink.value(sdkVersion)
    sink.name("syncTimestamp"); sink.value(syncTimestamp)
    sink.name("data")
    sink.beginObject()
    sink.name("records"); sink.beginArray(); for (r in data.records) writeRecord(sink, r); sink.endArray()
    sink.name("workouts"); sink.beginArray(); for (w in data.workouts) writeWorkout(sink, w); sink.endArray()
    sink.name("sleep"); sink.beginArray(); for (s in data.sleep) writeSleep(sink, s); sink.endArray()
    sink.endObject()
    sink.endObject()
}

private fun writeSource(sink: JsonSink, s: UnifiedSource) {
    sink.beginObject()
    sink.name("appId"); sink.value(s.appId)
    sink.name("deviceId"); sink.value(s.deviceId)
    sink.name("deviceName"); sink.value(s.deviceName)
    sink.name("deviceManufacturer"); sink.value(s.deviceManufacturer)
    sink.name("deviceModel"); sink.value(s.deviceModel)
    sink.name("deviceType"); sink.value(s.deviceType)
    sink.name("recordingMethod"); sink.value(s.recordingMethod)
    sink.endObject()
}

private fun writeRecord(sink: JsonSink, r: UnifiedRecord) {
    sink.beginObject()
    sink.name("id"); sink.value(r.id)
    sink.name("type"); sink.value(r.type)
    sink.name("startDate"); sink.value(r.startDate)
    sink.name("endDate"); sink.value(r.endDate)
    sink.name("zoneOffset"); sink.value(r.zoneOffset)
    sink.name("source"); writeSource(sink, r.source)
    sink.name("value"); sink.value(r.value)
    sink.name("unit"); sink.value(r.unit)
    sink.name("parentId"); sink.value(r.parentId)
    sink.name("metadata"); writeJsonValue(sink, r.metadata)
    sink.endObject()
}

private fun writeWorkout(sink: JsonSink, w: UnifiedWorkout) {
    sink.beginObject()
    sink.name("id"); sink.value(w.id)
    sink.name("parentId"); sink.value(w.parentId)
    sink.name("type"); sink.value(w.type)
    sink.name("startDate"); sink.value(w.startDate)
    sink.name("endDate"); sink.value(w.endDate)
    sink.name("zoneOffset"); sink.value(w.zoneOffset)
    sink.name("source"); writeSource(sink, w.source)
    sink.name("title"); sink.value(w.title)
    sink.name("notes"); sink.value(w.notes)
    sink.name("values"); writeJsonValue(sink, w.values)
    sink.name("segments"); writeJsonValue(sink, w.segments)
    sink.name("laps"); writeJsonValue(sink, w.laps)
    sink.name("route"); writeJsonValue(sink, w.route)
    sink.name("samples"); writeJsonValue(sink, w.samples)
    sink.name("metadata"); writeJsonValue(sink, w.metadata)
    sink.endObject()
}

private fun writeSleep(sink: JsonSink, s: UnifiedSleep) {
    sink.beginObject()
    sink.name("id"); sink.value(s.id)
    sink.name("parentId"); sink.value(s.parentId)
    sink.name("stage"); sink.value(s.stage)
    sink.name("startDate"); sink.value(s.startDate)
    sink.name("endDate"); sink.value(s.endDate)
    sink.name("zoneOffset"); sink.value(s.zoneOffset)
    sink.name("source"); writeSource(sink, s.source)
    sink.name("values"); writeJsonValue(sink, s.values)
    sink.name("metadata"); writeJsonValue(sink, s.metadata)
    sink.endObject()
}

/**
 * Generic writer for the already-built, small nested structures (metadata, values, segments,
 * laps, route, samples). Mirrors the number/typing rules the old map-walking serializer used
 * (Int→Long, Float→Double, unknown→toString) so output is byte-identical. `internal` so the
 * parity test can serialize the `toMap()`-based reference through the exact same rules.
 */
internal fun writeJsonValue(sink: JsonSink, value: Any?) {
    when (value) {
        null -> sink.nullValue()
        is Boolean -> sink.value(value)
        is Int -> sink.value(value.toLong())
        is Long -> sink.value(value)
        is Float -> sink.value(value.toDouble())
        is Double -> sink.value(value)
        is Number -> sink.value(value.toDouble())
        is String -> sink.value(value)
        is Map<*, *> -> {
            sink.beginObject()
            for ((k, v) in value) {
                sink.name(k.toString())
                writeJsonValue(sink, v)
            }
            sink.endObject()
        }
        is List<*> -> {
            sink.beginArray()
            for (item in value) writeJsonValue(sink, item)
            sink.endArray()
        }
        else -> sink.value(value.toString())
    }
}
