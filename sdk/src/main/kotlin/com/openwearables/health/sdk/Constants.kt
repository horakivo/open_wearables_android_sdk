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
    const val CHUNK_SIZE = 2000

    // A sleep session expands into its individual stage records (often 50+
    // per night), so sleep pages are divided by this factor to keep the
    // expanded record count of a round near CHUNK_SIZE.
    const val SLEEP_STAGES_PER_SESSION_ESTIMATE = 50
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
