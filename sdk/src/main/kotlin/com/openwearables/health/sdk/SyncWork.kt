package com.openwearables.health.sdk

import androidx.work.WorkInfo

/** A unique sync job counts as pending while WorkManager still holds it or is running it. */
internal fun isSyncWorkPending(states: List<WorkInfo.State>): Boolean =
    states.any { it == WorkInfo.State.ENQUEUED || it == WorkInfo.State.RUNNING || it == WorkInfo.State.BLOCKED }
