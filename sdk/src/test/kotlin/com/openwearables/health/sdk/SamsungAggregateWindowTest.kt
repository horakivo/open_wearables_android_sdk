package com.openwearables.health.sdk

import java.time.LocalDateTime
import java.time.ZoneOffset
import kotlin.test.Test
import kotlin.test.assertEquals

class SamsungAggregateWindowTest {

    private val zone = ZoneOffset.UTC

    @Test
    fun lookbackStartsAtTheHour48HoursBeforeTheAnchor() {
        val anchor = LocalDateTime.of(2026, 9, 28, 10, 37, 45)
            .toInstant(ZoneOffset.UTC).toEpochMilli()
        val start = SamsungAggregateWindow.windowStart(anchor, LocalDateTime.of(2026, 9, 28, 10, 37, 45), zone)
        assertEquals(LocalDateTime.of(2026, 9, 26, 10, 0, 0), start)
    }

    @Test
    fun missingAnchorReads30DaysFlooredToTheHour() {
        val now = LocalDateTime.of(2026, 9, 28, 10, 37, 45)
        val start = SamsungAggregateWindow.windowStart(null, now, zone)
        assertEquals(LocalDateTime.of(2026, 8, 29, 10, 0, 0), start)
    }

    @Test
    fun floorToHourDropsMinutes() {
        val mid = LocalDateTime.of(2026, 9, 28, 7, 14, 45)
            .toInstant(ZoneOffset.UTC).toEpochMilli()
        val floored = SamsungAggregateWindow.floorToHour(mid, zone)
        assertEquals(
            LocalDateTime.of(2026, 9, 28, 7, 0, 0).toInstant(ZoneOffset.UTC).toEpochMilli(),
            floored,
        )
    }
}
