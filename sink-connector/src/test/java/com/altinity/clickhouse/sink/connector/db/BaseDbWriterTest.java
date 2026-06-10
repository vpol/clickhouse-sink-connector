package com.altinity.clickhouse.sink.connector.db;


import com.altinity.clickhouse.sink.connector.ClickHouseSinkConnectorConfig;
import com.altinity.clickhouse.sink.connector.ClickHouseSinkConnectorConfigVariables;
import org.junit.Assert;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class BaseDbWriterTest {
    @Test
    public void testSplitJdbcProperties() {
        String jdbcProperties = "max_buffer_size=1000000,socket_timeout=10000";
        Map<String, String> props = new HashMap<>();
        props.put(ClickHouseSinkConnectorConfigVariables.JDBC_PARAMETERS.toString(), jdbcProperties);
        ClickHouseSinkConnectorConfig config = new ClickHouseSinkConnectorConfig(props);

        Properties properties = new BaseDbWriter(
                "localhost",
                8123,
                "default",
                "default",
                "",
                config, null
        ).splitJdbcProperties(jdbcProperties);
        Assert.assertEquals(properties.getProperty("max_buffer_size"), "1000000");
        Assert.assertEquals(properties.getProperty("socket_timeout"), "10000");
    }

    @Test
    public void testSanitizeJdbcSettingsRemovesExperimentalObjectType() {
        String jdbcSettings = "input_format_null_as_default=0,allow_experimental_object_type=1,insert_allow_materialized_columns=1";

        String sanitizedSettings = BaseDbWriter.sanitizeJdbcSettings(jdbcSettings);

        Assert.assertEquals(
                "input_format_null_as_default=0,insert_allow_materialized_columns=1",
                sanitizedSettings);
    }

    @Test
    public void testSanitizeJdbcSettingsHandlesWhitespaceAndCase() {
        String jdbcSettings = " input_format_null_as_default=0 , Allow_Experimental_Object_Type = 1 , insert_allow_materialized_columns=1 ";

        String sanitizedSettings = BaseDbWriter.sanitizeJdbcSettings(jdbcSettings);

        Assert.assertEquals(
                "input_format_null_as_default=0,insert_allow_materialized_columns=1",
                sanitizedSettings);
    }
}
