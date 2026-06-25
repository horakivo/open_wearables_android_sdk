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
    const val CHUNK_SIZE = 4000

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

    // heartRate is read by a fixed PARENT-count page rather than by expanded-payload
    // budget. Each Health Connect heartRate read has a large fixed IPC cost that is
    // nearly independent of page size (observed: 9 parents ~2.1s, 35 parents ~0.7-2.3s),
    // so sizing the page to keep the expanded payload near CHUNK_SIZE shrinks it to ~30
    // parents — each ~2s read then advances the cursor only ~30 min and the round count
    // explodes over a long history. A moderate parent page amortizes that fixed cost
    // (advancing hours per read); the expanded result is uploaded in CHUNK_SIZE
    // sub-batches so payload size and upload duration stay bounded. ~300 parents expands
    // to ~8k samples (a few MB transient). Tune against the "Round timing" logs.
    const val HEART_RATE_READ_PAGE_PARENTS = 300
    const val WORK_NAME_PERIODIC = "health_sync_periodic"
    const val WORK_NAME_EXPEDITED = "health_sync_expedited"
    const val SDK_VERSION = "0.11.0"
}

object StorageKeys {
    const val SYNC_PREFS_NAME = "com.openwearables.healthsdk.sync"
    const val KEY_ANCHORS = "anchors"
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
