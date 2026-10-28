package com.openwearables.health.sdk

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull

class SyncEndpointsTest {

    private val endpoints = Endpoints(
        sync = "https://api.example.com/sync/sdk/users/{user_id}/sync",
        logs = "https://api.example.com/sync/sdk/users/{user_id}/logs",
    )

    @Test
    fun configuredEndpointsAreUsedAsGiven() {
        assertEquals("https://api.example.com/sync/sdk/users/u1/sync", SyncEndpoints.sync(endpoints, "u1"))
        assertEquals("https://api.example.com/sync/sdk/users/u1/logs", SyncEndpoints.logs(endpoints, "u1"))
    }

    @Test
    fun omittedDisconnectStaysOff() {
        assertNull(SyncEndpoints.disconnect(endpoints, "u1", "google"))
    }

    @Test
    fun disconnectTemplateTakesUserAndProvider() {
        val withDisconnect = endpoints.copy(disconnect = "https://api.example.com/users/{user_id}/{provider}")
        assertEquals(
            "https://api.example.com/users/u1/samsung",
            SyncEndpoints.disconnect(withDisconnect, "u1", "samsung"),
        )
    }

    @Test
    fun relativeEndpointIsRejected() {
        assertNull(SyncEndpoints.firstInvalid(endpoints))
        assertEquals(
            "logs" to "/sync/sdk/users/{user_id}/logs",
            SyncEndpoints.firstInvalid(endpoints.copy(logs = "/sync/sdk/users/{user_id}/logs")),
        )
    }
}
