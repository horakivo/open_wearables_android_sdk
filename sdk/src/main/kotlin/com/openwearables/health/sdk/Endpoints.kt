package com.openwearables.health.sdk

/**
 * Where the SDK sends each request, as given to [OpenWearablesHealthSDK.configure].
 * Every URL is absolute and used as is, except that `{user_id}` is replaced with the
 * signed-in user's id (and `{provider}` in [disconnect] with `samsung` / `google`).
 * Without [tokenRefresh] the SDK does not refresh tokens (sign in with an API key);
 * without [disconnect] sign-out skips the server-side disconnect.
 */
data class Endpoints(
    val sync: String,
    val logs: String,
    val tokenRefresh: String? = null,
    val disconnect: String? = null,
)
