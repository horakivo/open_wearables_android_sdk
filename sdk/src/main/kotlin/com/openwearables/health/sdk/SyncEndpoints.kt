package com.openwearables.health.sdk

/**
 * The URL of each request. Configured [Endpoints] are used as given; without them the
 * URLs are derived from the host (`{host}/api/v1/...`) as upstream does.
 */
internal object SyncEndpoints {
    fun sync(host: String, customSyncUrl: String?, userId: String, endpoints: Endpoints?): String {
        if (endpoints != null) return withUser(endpoints.sync, userId)
        if (customSyncUrl != null) {
            if (customSyncUrl.contains("{user_id}") || customSyncUrl.contains("{userId}")) {
                return withUser(customSyncUrl, userId)
            }
            return "${customSyncUrl.trimEnd('/')}/sdk/users/$userId/sync"
        }
        return "${host.trimEnd('/')}/api/v1/sdk/users/$userId/sync"
    }

    fun logs(host: String, userId: String, endpoints: Endpoints?): String =
        endpoints?.let { withUser(it.logs, userId) }
            ?: "${host.trimEnd('/')}/api/v1/sdk/users/$userId/logs"

    fun tokenRefresh(apiBaseUrl: String?, endpoints: Endpoints?): String? =
        if (endpoints != null) endpoints.tokenRefresh else apiBaseUrl?.let { "$it/token/refresh" }

    fun disconnect(apiBaseUrl: String?, userId: String, provider: String, endpoints: Endpoints?): String? =
        if (endpoints != null) {
            endpoints.disconnect?.let { withUser(it, userId).replace("{provider}", provider) }
        } else {
            apiBaseUrl?.let { SyncAuth.disconnectUrl(it, userId, provider) }
        }

    private fun withUser(template: String, userId: String): String =
        template.replace("{userId}", userId).replace("{user_id}", userId)
}
