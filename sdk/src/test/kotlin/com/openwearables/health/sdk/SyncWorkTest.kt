package com.openwearables.health.sdk

import androidx.work.WorkInfo
import kotlin.test.Test
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class SyncWorkTest {
    @Test
    fun `a job WorkManager still holds or runs is pending`() {
        assertTrue(isSyncWorkPending(listOf(WorkInfo.State.ENQUEUED)))
        assertTrue(isSyncWorkPending(listOf(WorkInfo.State.RUNNING)))
    }

    @Test
    fun `a finished, failed, cancelled or missing job is not pending`() {
        assertFalse(isSyncWorkPending(listOf(WorkInfo.State.SUCCEEDED)))
        assertFalse(isSyncWorkPending(listOf(WorkInfo.State.FAILED)))
        assertFalse(isSyncWorkPending(listOf(WorkInfo.State.CANCELLED)))
        assertFalse(isSyncWorkPending(emptyList()))
    }
}
