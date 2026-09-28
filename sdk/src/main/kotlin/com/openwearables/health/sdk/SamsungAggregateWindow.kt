package com.openwearables.health.sdk

import java.time.Instant
import java.time.LocalDateTime
import java.time.ZoneId

/**
 * Steps and active energy are hourly aggregates. Samsung merges phone and watch
 * counts into a bucket after that bucket was already read, so an anchor of
 * "last end + 1ms" drops the late data. Each sync re-reads a trailing window
 * aligned to the local hour. The same hour then overwrites the previous row.
 */
internal object SamsungAggregateWindow {
    const val LOOKBACK_MS: Long = 48L * 60 * 60 * 1000
    const val HISTORY_DAYS: Long = 30

    fun floorToHour(epochMs: Long, zone: ZoneId): Long {
        return Instant.ofEpochMilli(epochMs).atZone(zone)
            .withMinute(0).withSecond(0).withNano(0)
            .toInstant()
            .toEpochMilli()
    }

    /**
     * No anchor: the last [HISTORY_DAYS], floored to the hour.
     * With an anchor: 48h before that anchor, floored to the hour.
     */
    fun windowStart(anchorMs: Long?, now: LocalDateTime, zone: ZoneId): LocalDateTime {
        if (anchorMs == null) {
            return now.minusDays(HISTORY_DAYS).withMinute(0).withSecond(0).withNano(0)
        }
        val rewound = Instant.ofEpochMilli(anchorMs).minusMillis(LOOKBACK_MS)
        return LocalDateTime.ofInstant(rewound, zone)
            .withMinute(0).withSecond(0).withNano(0)
    }
}
