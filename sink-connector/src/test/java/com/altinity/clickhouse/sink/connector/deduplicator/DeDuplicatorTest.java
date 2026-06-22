package com.altinity.clickhouse.sink.connector.deduplicator;

import com.altinity.clickhouse.sink.connector.ClickHouseSinkConnectorConfig;
import com.altinity.clickhouse.sink.connector.ClickHouseSinkConnectorConfigVariables;
import com.altinity.clickhouse.sink.connector.ClickHouseSinkTaskTest;

import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Assert;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

public class DeDuplicatorTest {

    @Test
    public void testIsNew() {
        Map<String, String> properties = new HashMap<String, String>();
        properties.put(ClickHouseSinkConnectorConfigVariables.DEDUPLICATION_POLICY.toString(), "new");

        ClickHouseSinkConnectorConfig config = new ClickHouseSinkConnectorConfig(properties);
        DeDuplicator dedupe = new DeDuplicator(config);

        String topic = "products";
        String keyField = "productId";
        String key = "11";
        String valueField = "amount";
        String value = "2000";
        Long timestamp1 = System.currentTimeMillis();


        // Same key
        SinkRecord recordOne = ClickHouseSinkTaskTest.spoofSinkRecord(topic, keyField, key, valueField, value,
        TimestampType.NO_TIMESTAMP_TYPE, timestamp1);

        boolean result1 = dedupe.isNew(topic, recordOne);
        Assert.assertTrue(result1 == true);

        long timestamp2 = System.currentTimeMillis();

        SinkRecord recordTwo = ClickHouseSinkTaskTest.spoofSinkRecord(topic, keyField, key, valueField, value, 
        TimestampType.NO_TIMESTAMP_TYPE, timestamp2);

        boolean result2 = dedupe.isNew(topic, recordTwo);
        Assert.assertTrue(result2 == false);
        // End: Send key

        //  Different key.
        SinkRecord recordDifferentKey = ClickHouseSinkTaskTest.spoofSinkRecord(topic, keyField, "22", valueField, value,
                TimestampType.NO_TIMESTAMP_TYPE, timestamp2);
        boolean resultDifferentKey = dedupe.isNew(topic, recordDifferentKey);
        Assert.assertTrue(resultDifferentKey == true);

        // Test Different Topic.
        boolean resultDifferentTopic = dedupe.isNew("employees", recordTwo);
        Assert.assertTrue(resultDifferentTopic == true);

    }

    @Test
    public void testDedupePoolEvictsOldKeys() {
        Map<String, String> properties = new HashMap<String, String>();
        properties.put(ClickHouseSinkConnectorConfigVariables.DEDUPLICATION_POLICY.toString(), "old");
        properties.put(ClickHouseSinkConnectorConfigVariables.BUFFER_COUNT.toString(), "2");

        ClickHouseSinkConnectorConfig config = new ClickHouseSinkConnectorConfig(properties);
        DeDuplicator dedupe = new DeDuplicator(config);

        String topic = "products";
        String keyField = "productId";
        String valueField = "amount";
        String value = "2000";

        SinkRecord recordOne = ClickHouseSinkTaskTest.spoofSinkRecord(topic, keyField, "11", valueField, value,
                TimestampType.NO_TIMESTAMP_TYPE, System.currentTimeMillis());
        SinkRecord recordTwo = ClickHouseSinkTaskTest.spoofSinkRecord(topic, keyField, "22", valueField, value,
                TimestampType.NO_TIMESTAMP_TYPE, System.currentTimeMillis());
        SinkRecord recordThree = ClickHouseSinkTaskTest.spoofSinkRecord(topic, keyField, "33", valueField, value,
                TimestampType.NO_TIMESTAMP_TYPE, System.currentTimeMillis());
        SinkRecord recordOneAgain = ClickHouseSinkTaskTest.spoofSinkRecord(topic, keyField, "11", valueField, value,
                TimestampType.NO_TIMESTAMP_TYPE, System.currentTimeMillis());

        Assert.assertTrue(dedupe.isNew(topic, recordOne));
        Assert.assertTrue(dedupe.isNew(topic, recordTwo));
        Assert.assertTrue(dedupe.isNew(topic, recordThree));
        Assert.assertTrue(dedupe.isNew(topic, recordOneAgain));
    }
}
