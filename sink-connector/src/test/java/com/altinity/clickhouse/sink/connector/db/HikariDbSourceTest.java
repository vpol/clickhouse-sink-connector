package com.altinity.clickhouse.sink.connector.db;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class HikariDbSourceTest {

    @Test
    void effectiveMinimumIdleUsesConfiguredValueWithinPoolLimit() {
        assertEquals(10, HikariDbSource.effectiveMinimumIdle(10, 500));
    }

    @Test
    void effectiveMinimumIdleClampsToPoolLimit() {
        assertEquals(5, HikariDbSource.effectiveMinimumIdle(10, 5));
    }

    @Test
    void effectiveMinimumIdleDoesNotGoBelowZero() {
        assertEquals(0, HikariDbSource.effectiveMinimumIdle(-1, 500));
    }
}
