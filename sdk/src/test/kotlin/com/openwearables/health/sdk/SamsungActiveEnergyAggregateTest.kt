package com.openwearables.health.sdk

import com.samsung.android.sdk.health.data.data.AggregateOperation
import com.samsung.android.sdk.health.data.request.DataTypes
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

/**
 * The same lookup [SamsungHealthManager] uses for `activeEnergy`.
 * `TOTAL_CALORIES` is not an ActivitySummary operation; resolving it used to
 * fall through to another metric and upload distance as kcal.
 */
class SamsungActiveEnergyAggregateTest {

    @Test
    fun activeEnergyFieldIsActiveCaloriesInKilocalories() {
        val dataType = DataTypes.ACTIVITY_SUMMARY
        val op = dataType.javaClass.getField("TOTAL_ACTIVE_CALORIES_BURNED").get(dataType) as AggregateOperation<*, *>
        assertEquals("activity_summary", op.typeName)
        assertEquals("TOTAL.active_calories_burned", op.operationName)
    }

    @Test
    fun totalCaloriesFieldDoesNotExist() {
        assertFailsWith<NoSuchFieldException> {
            DataTypes.ACTIVITY_SUMMARY.javaClass.getField("TOTAL_CALORIES")
        }
    }

    @Test
    fun stepsTotalFieldStillExists() {
        val op = DataTypes.STEPS.javaClass.getField("TOTAL").get(DataTypes.STEPS) as AggregateOperation<*, *>
        assertEquals("steps", op.typeName)
        assertEquals("TOTAL.steps", op.operationName)
    }
}
