package com.openwearables.health.sdk

object ProviderIds {
    const val SAMSUNG = "samsung"
    const val GOOGLE = "google"
}

object ProviderDisplayNames {
    const val SAMSUNG_HEALTH = "Samsung Health"
    const val HEALTH_CONNECT = "Health Connect"
}

object SyncDefaults {
    const val SYNC_INTERVAL_MINUTES = 15L
    const val MIN_SYNC_INTERVAL_MINUTES = 15L
    // Sized so a typical round (READ_TARGET_EXPANDED_ITEMS) uploads as ONE POST —
    // each extra batch costs a full server round-trip. 8000 items ≈ 3.4 MB JSON,
    // ~300 KB gzipped on the wire.
    const val CHUNK_SIZE = 8000

    // Some record types expand into many child records when converted, so
    // their page limits are divided by these factors to keep the expanded
    // record count of a round near CHUNK_SIZE (counting parent records 1:1
    // would otherwise let a round balloon far past CHUNK_SIZE).
    //
    // A sleep session expands into its individual stage records (often 50+
    // per night).
    const val SLEEP_STAGES_PER_SESSION_ESTIMATE = 50

    // A Health Connect HeartRateRecord is a series that expands into one
    // record per sample (observed ~25-40 samples per series).
    const val HEART_RATE_SAMPLES_PER_RECORD_ESTIMATE = 40

    // Target number of EXPANDED items (parents × samples-per-parent) to read per type per
    // round. A type's page size in PARENT records is this divided by its observed expansion,
    // capped at the Health Connect page limit. This adapts the read to the provider: a
    // sparse provider (Garmin: ~1 sample per HeartRateRecord, expansion≈1) reads up to
    // MAX_PAGE_SIZE parents, while a dense one (~25-40 samples per record) reads ~hundreds —
    // both yielding a similar-sized expanded page, so the cursor advances efficiently in
    // either case. Decoupled from CHUNK_SIZE (the upload batch): the round's expanded data
    // is re-chunked to CHUNK_SIZE by sub-batched upload, so this can exceed it. ~8k items is
    // a few MB transient. Tune against the "Round timing" logs.
    const val READ_TARGET_EXPANDED_ITEMS = 8000
    const val WORK_NAME_PERIODIC = "health_sync_periodic"
    const val WORK_NAME_EXPEDITED = "health_sync_expedited"
    const val SDK_VERSION = "0.13.0"
}

object StorageKeys {
    const val SYNC_PREFS_NAME = "com.openwearables.healthsdk.sync"
    const val KEY_ANCHORS = "anchors"
    const val KEY_CHANGES_TOKENS = "changesTokens"
    const val SYNC_STATE_DIR = "health_sync_state"
    const val SYNC_STATE_FILE = "state.json"
}

object NotificationConfig {
    const val NOTIFICATION_ID = 9001
    const val CHANNEL_ID = "health_sync_channel"
    const val CHANNEL_NAME = "Health Sync"
    const val CHANNEL_DESCRIPTION = "Background health data synchronization"
    const val DEFAULT_TEXT = "Syncing health data..."
}
