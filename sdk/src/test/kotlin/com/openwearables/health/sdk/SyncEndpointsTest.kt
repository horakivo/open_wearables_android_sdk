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
        assertEquals(
            "https://api.example.com/sync/sdk/users/u1/sync",
            SyncEndpoints.sync("", null, "u1", endpoints),
        )
        assertEquals(
            "https://api.example.com/sync/sdk/users/u1/logs",
            SyncEndpoints.logs("", "u1", endpoints),
        )
    }

    @Test
    fun omittedRefreshAndDisconnectStayOff() {
        assertNull(SyncEndpoints.tokenRefresh("https://api.example.com/api/v1", endpoints))
        assertNull(SyncEndpoints.disconnect("https://api.example.com/api/v1", "u1", "google", endpoints))
    }

    @Test
    fun disconnectTemplateTakesUserAndProvider() {
        val withDisconnect = endpoints.copy(disconnect = "https://api.example.com/users/{user_id}/{provider}")
        assertEquals(
            "https://api.example.com/users/u1/samsung",
            SyncEndpoints.disconnect(null, "u1", "samsung", withDisconnect),
        )
    }

    @Test
    fun withoutEndpointsUrlsDeriveFromTheHost() {
        assertEquals(
            "https://api.example.com/api/v1/sdk/users/u1/sync",
            SyncEndpoints.sync("https://api.example.com/", null, "u1", null),
        )
        assertEquals(
            "https://api.example.com/api/v1/sdk/users/u1/logs",
            SyncEndpoints.logs("https://api.example.com/", "u1", null),
        )
        assertEquals(
            "https://api.example.com/api/v1/token/refresh",
            SyncEndpoints.tokenRefresh("https://api.example.com/api/v1", null),
        )
    }

    @Test
    fun customSyncUrlStillAppliesInHostMode() {
        assertEquals(
            "https://ingest.example.com/sdk/users/u1/sync",
            SyncEndpoints.sync("https://api.example.com", "https://ingest.example.com/", "u1", null),
        )
        assertEquals(
            "https://ingest.example.com/u/u1",
            SyncEndpoints.sync("https://api.example.com", "https://ingest.example.com/u/{user_id}", "u1", null),
        )
    }
}
