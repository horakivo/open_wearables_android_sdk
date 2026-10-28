package com.openwearables.health.sdk

import okhttp3.HttpUrl.Companion.toHttpUrlOrNull

/**
 * The URL of each request: the configured [Endpoints] with `{user_id}` (and `{provider}`
 * in [Endpoints.disconnect]) filled in. Nothing is appended.
 */
internal object SyncEndpoints {
    fun sync(endpoints: Endpoints, userId: String): String = withUser(endpoints.sync, userId)

    fun logs(endpoints: Endpoints, userId: String): String = withUser(endpoints.logs, userId)

    fun disconnect(endpoints: Endpoints, userId: String, provider: String): String? =
        endpoints.disconnect?.let { withUser(it, userId).replace("{provider}", provider) }

    /** The first endpoint, as (name, value), that is not an absolute http(s) URL. */
    fun firstInvalid(endpoints: Endpoints): Pair<String, String>? = listOf(
        "sync" to endpoints.sync,
        "logs" to endpoints.logs,
        "tokenRefresh" to endpoints.tokenRefresh,
        "disconnect" to endpoints.disconnect,
    ).firstNotNullOfOrNull { (name, template) ->
        template?.takeIf {
            withUser(it, "user").replace("{provider}", "google").toHttpUrlOrNull() == null
        }?.let { name to it }
    }

    private fun withUser(template: String, userId: String): String = template.replace("{user_id}", userId)
}
