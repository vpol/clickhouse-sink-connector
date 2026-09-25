package com.altinity.clickhouse.sink.connector.db.batch;

import com.altinity.clickhouse.sink.connector.ClickHouseSinkConnectorConfig;
import com.altinity.clickhouse.sink.connector.converters.ClickHouseConverter.CDC_OPERATION;
import com.altinity.clickhouse.sink.connector.db.DBMetadata;
import com.altinity.clickhouse.sink.connector.model.BlockMetaData;
import com.altinity.clickhouse.sink.connector.model.ClickHouseStruct;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class PreparedStatementExecutorIgnoreDeleteTest {
    private static final String TOPIC = "sink.public.rows";
    private static final String UUID_VALUE = "00000000-0000-0000-0000-000000000123";
    private static final Schema ROW_SCHEMA = SchemaBuilder.struct()
            .field("id", Schema.STRING_SCHEMA)
            .field("payload_uuid", SchemaBuilder.string().name("io.debezium.data.Uuid").optional().build())
            .build();
    private static final Map<String, Integer> INDEXES = Map.of(
            "id", 1, "payload_uuid", 2, "_version", 3, "is_deleted", 4);
    private static final Map<String, String> TYPES = Map.of(
            "id", "String", "payload_uuid", "UUID", "_version", "UInt64", "is_deleted", "UInt8");

    @Test
    void ignoredKeyOnlyDeleteSucceedsWithoutBindingOrBatchingItsPayload() throws Exception {
        RecordingJdbc jdbc = new RecordingJdbc();
        ClickHouseStruct delete = record(CDC_OPERATION.DELETE, "deleted", null, 20);
        List<ClickHouseStruct> records = new ArrayList<>(List.of(delete));
        BlockMetaData metadata = new BlockMetaData();

        assertTrue(execute(records, true, false, jdbc, metadata));
        assertEquals(0, jdbc.bindings, "even binding NULL to the UUID is forbidden for an ignored delete");
        assertTrue(jdbc.rows.isEmpty());
        assertEquals(List.of(delete), records, "retain the event for the caller's normal offset commit");
        assertEquals(20L, metadata.getPartitionToOffsetMap().get(TOPIC).getRight());
    }

    @Test
    void ignoredDeleteDoesNotEnterReplicationHistorySql() throws Exception {
        RecordingJdbc jdbc = new RecordingJdbc();
        assertTrue(execute(List.of(record(CDC_OPERATION.DELETE, "deleted", null, 20)),
                true, true, jdbc, new BlockMetaData()));
        assertEquals(0, jdbc.bindings);
        assertTrue(jdbc.rows.isEmpty());
    }

    @Test
    void mixedBatchPreservesInsertsAndUpdatesAroundIgnoredDeletes() throws Exception {
        RecordingJdbc jdbc = new RecordingJdbc();
        List<ClickHouseStruct> records = new ArrayList<>(List.of(
                record(CDC_OPERATION.CREATE, "first", UUID_VALUE, 10),
                record(CDC_OPERATION.DELETE, "deleted", null, 20),
                record(CDC_OPERATION.UPDATE, "updated", UUID_VALUE, 30),
                record(CDC_OPERATION.CREATE, "later", UUID_VALUE, 40),
                record(CDC_OPERATION.DELETE, "last-delete", null, 50)));
        BlockMetaData metadata = new BlockMetaData();

        assertTrue(execute(records, true, false, jdbc, metadata));
        assertEquals(List.of("first", "updated", "later"),
                jdbc.rows.stream().map(row -> row.get(1)).toList());
        assertTrue(jdbc.rows.stream().allMatch(row -> Integer.valueOf(0).equals(row.get(4))));
        assertTrue(jdbc.rows.stream().allMatch(row -> row.get(2) != null));
        assertEquals(5, records.size(), "do not remove ignored events from the commit batch");
        assertEquals(50L, metadata.getPartitionToOffsetMap().get(TOPIC).getRight());
    }

    @Test
    void disabledIgnoreDeleteStillWritesATombstone() throws Exception {
        RecordingJdbc jdbc = new RecordingJdbc();
        assertTrue(execute(List.of(record(CDC_OPERATION.DELETE, "deleted", UUID_VALUE, 20)),
                false, false, jdbc, new BlockMetaData()));
        assertEquals(1, jdbc.rows.size());
        assertEquals("deleted", jdbc.rows.get(0).get(1));
        assertEquals(1, jdbc.rows.get(0).get(4));
    }

    @Test
    void ignoredDeleteDoesNotHideFailureToWriteAnotherRecord() {
        RecordingJdbc jdbc = new RecordingJdbc();
        jdbc.failWrites = true;
        RuntimeException error = assertThrows(RuntimeException.class, () -> execute(List.of(
                record(CDC_OPERATION.DELETE, "deleted", null, 20),
                record(CDC_OPERATION.CREATE, "later", UUID_VALUE, 30)),
                true, false, jdbc, new BlockMetaData()));
        assertEquals("Simulated insert failure", error.getCause().getMessage());
        assertEquals(List.of("later"), jdbc.rows.stream().map(row -> row.get(1)).toList());
    }

    private static ClickHouseStruct record(CDC_OPERATION operation, String id, String uuid, long offset) {
        Struct row = new Struct(ROW_SCHEMA).put("id", id).put("payload_uuid", uuid);
        ClickHouseStruct record = new ClickHouseStruct();
        record.setCdcOperation(operation);
        record.setPrimaryKey(new ArrayList<>(List.of("id")));
        if (operation != CDC_OPERATION.CREATE) {
            record.setBeforeStruct(row);
            record.setBeforeModifiedFields(ROW_SCHEMA.fields());
        }
        if (operation != CDC_OPERATION.DELETE) {
            record.setAfterStruct(row);
            record.setAfterModifiedFields(ROW_SCHEMA.fields());
        }
        record.setVersion(offset);
        record.setTopic(TOPIC);
        record.setKafkaPartition(0);
        record.setKafkaOffset(offset);
        return record;
    }

    private static boolean execute(List<ClickHouseStruct> records, boolean ignoreDelete, boolean history,
                                   RecordingJdbc jdbc, BlockMetaData metadata) throws Exception {
        Map<MutablePair<String, Map<String, Integer>>, List<ClickHouseStruct>> queries = new LinkedHashMap<>();
        queries.put(MutablePair.of("INSERT INTO sink.rows VALUES (?, ?, ?, ?)", INDEXES), records);
        ClickHouseSinkConnectorConfig config = new ClickHouseSinkConnectorConfig(Map.of(
                "connector.class", "io.debezium.connector.postgresql.PostgresConnector",
                "ignore_delete", Boolean.toString(ignoreDelete),
                "replication.history.enable", Boolean.toString(history),
                "buffer.max.records", "2"));
        return new PreparedStatementExecutor("is_deleted", true, null, "_version",
                "sink", ZoneId.of("UTC"), () -> List.of("id"))
                .addToPreparedStatementBatch(TOPIC, queries, metadata, config, jdbc.connection(),
                        "rows", TYPES, DBMetadata.TABLE_ENGINE.SHARED_REPLACING_MERGE_TREE);
    }

    private static class RecordingJdbc {
        private final List<Map<Integer, Object>> rows = new ArrayList<>();
        private int bindings;
        private boolean failWrites;

        Connection connection() {
            return (Connection) Proxy.newProxyInstance(Connection.class.getClassLoader(),
                    new Class<?>[]{Connection.class}, (proxy, method, args) -> {
                        if ("prepareStatement".equals(method.getName())) {
                            return statement();
                        }
                        throw new AssertionError("Unexpected JDBC call: " + method.getName());
                    });
        }

        PreparedStatement statement() {
            Map<Integer, Object> bound = new HashMap<>();
            return (PreparedStatement) Proxy.newProxyInstance(PreparedStatement.class.getClassLoader(),
                    new Class<?>[]{PreparedStatement.class}, (proxy, method, args) -> {
                        String name = method.getName();
                        if (name.startsWith("set") && args != null && args[0] instanceof Integer) {
                            bindings++;
                            if ("setNull".equals(name) && Integer.valueOf(2).equals(args[0])) {
                                throw new SQLException("NULL payload for non-nullable UUID");
                            }
                            bound.put((Integer) args[0], "setNull".equals(name) ? null : args[1]);
                        } else if ("addBatch".equals(name)) {
                            rows.add(new HashMap<>(bound));
                        } else if ("executeBatch".equals(name)) {
                            if (failWrites && !rows.isEmpty()) {
                                throw new SQLException("Simulated insert failure");
                            }
                            return new int[0];
                        } else if (!"close".equals(name)) {
                            throw new AssertionError("Unexpected JDBC call: " + name);
                        }
                        return null;
                    });
        }
    }
}
