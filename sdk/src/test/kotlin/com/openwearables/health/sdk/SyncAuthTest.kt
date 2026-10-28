package com.openwearables.health.sdk

import kotlin.test.Test
import kotlin.test.assertEquals

class SyncAuthTest {

    @Test
    fun backgroundChunkIsSmallerThanForeground() {
        assertEquals(100, SyncDefaults.BACKGROUND_CHUNK_SIZE)
    }
}
