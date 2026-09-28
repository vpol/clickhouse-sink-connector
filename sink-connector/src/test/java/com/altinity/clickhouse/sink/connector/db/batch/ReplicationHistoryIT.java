package com.altinity.clickhouse.sink.connector.db.batch;

import com.altinity.clickhouse.sink.connector.ClickHouseSinkConnectorConfig;
import com.altinity.clickhouse.sink.connector.converters.ClickHouseConverter.CDC_OPERATION;
import com.altinity.clickhouse.sink.connector.db.DBMetadata;
import com.altinity.clickhouse.sink.connector.model.BlockMetaData;
import com.altinity.clickhouse.sink.connector.model.ClickHouseStruct;
import com.clickhouse.jdbc.ClickHouseDataSource;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.containers.ClickHouseContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.sql.Connection;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

@Testcontainers
class ReplicationHistoryIT {
    @Container
    private static final ClickHouseContainer CLICKHOUSE = new ClickHouseContainer("clickhouse/clickhouse-server:24.8.8");

    @ParameterizedTest
    @ValueSource(strings = {"is_deleted", "_is_deleted", "history_deleted"})
    void updateAndDeleteLeaveSiblingAccountUntouched(String deleteColumn) throws Exception {
        boolean businessFlag = !deleteColumn.equals("is_deleted");
        var config = new ClickHouseSinkConnectorConfig(Map.of("replication.history.enable", "true",
                // The existing engine wins when its column differs from configuration.
                "replacingmergetree.delete.column", businessFlag ? "_is_deleted" : "is_deleted"));
        Map<String, String> columns = new LinkedHashMap<>();
        columns.put("organisation_id", "String"); columns.put("account_id", "String"); columns.put("name", "String");
        if (businessFlag) columns.put("is_deleted", "UInt8");
        columns.put("_valid_from", "DateTime"); columns.put("_valid_to", "DateTime");
        columns.put("_operation", "String"); columns.put("_version", "UInt64"); columns.put(deleteColumn, "UInt8");
        var builder = SchemaBuilder.struct().field("organisation_id", Schema.STRING_SCHEMA)
                .field("account_id", Schema.STRING_SCHEMA).field("name", Schema.STRING_SCHEMA);
        if (businessFlag) builder.field("is_deleted", Schema.INT8_SCHEMA);
        Schema schema = builder.build();
        try (Connection connection = new ClickHouseDataSource(CLICKHOUSE.getJdbcUrl())
                .getConnection(CLICKHOUSE.getUsername(), CLICKHOUSE.getPassword())) {
            String ddl = "CREATE TABLE accounts (" + columns.entrySet().stream()
                    .map(c -> "`" + c.getKey() + "` " + c.getValue()).collect(java.util.stream.Collectors.joining(","))
                    + ") ENGINE=ReplacingMergeTree(_version," + deleteColumn + ") ORDER BY (organisation_id,account_id,_valid_to)";
            connection.createStatement().execute("DROP TABLE IF EXISTS accounts");
            connection.createStatement().execute(ddl);
            connection.createStatement().execute("SYSTEM STOP MERGES accounts");
            String businessValue = businessFlag ? "1," : "";
            connection.createStatement().execute("INSERT INTO accounts VALUES "
                    + "('org-1','account-A','Alice'," + businessValue + "'2026-01-01 00:00:00','2100-01-01 00:00:00','C',1,0),"
                    + "('org-1','account-B','Bob'," + businessValue + "'2026-01-01 00:00:00','2100-01-01 00:00:00','C',1,0)");
            Struct before = row(schema, "Alice", businessFlag);
            Struct after = row(schema, "Alicia", businessFlag);
            execute(connection, config, columns, deleteColumn, record(before, after, CDC_OPERATION.UPDATE, 1789992000L));
            assertSiblingUntouched(connection);
            assertEquals(1, count(connection, "account_id='account-A' AND name='Alice'"
                    + " AND _valid_to=toDateTime('2026-09-21 12:00:00') AND `" + deleteColumn + "`=0"));
            assertEquals(1, count(connection, "name='Alicia' AND `" + deleteColumn + "`=0"
                    + (businessFlag ? " AND is_deleted=1" : "")));

            execute(connection, config, columns, deleteColumn, record(after, null, CDC_OPERATION.DELETE, 1789992060L));
            assertSiblingUntouched(connection);
            assertEquals(1, count(connection, "account_id='account-A' AND _operation='D' AND `" + deleteColumn + "`=1"
                    + (businessFlag ? " AND is_deleted=1" : "")));
            if (businessFlag) assertEquals(0, count(connection, "is_deleted!=1"));
        }
    }

    private static void execute(Connection connection, ClickHouseSinkConnectorConfig config, Map<String, String> columns,
                                String deleteColumn, ClickHouseStruct record) throws Exception {
        Map<MutablePair<String, Map<String, Integer>>, List<ClickHouseStruct>> queries = new LinkedHashMap<>();
        assertTrue(new GroupInsertQueryWithBatchRecords().groupQueryWithRecords(List.of(record), queries,
                new LinkedHashMap<>(), config, "accounts", "default", connection, columns));
        var executor = new PreparedStatementExecutor(deleteColumn, true, null, "_version", "default", ZoneId.of("UTC"));
        assertTrue(executor.addToPreparedStatementBatch("test.accounts", queries, new BlockMetaData(), config,
                connection, "accounts", columns, DBMetadata.TABLE_ENGINE.REPLACING_MERGE_TREE));
    }

    private static Struct row(Schema schema, String name, boolean businessFlag) {
        var row = new Struct(schema).put("organisation_id", "org-1").put("account_id", "account-A").put("name", name);
        if (businessFlag) row.put("is_deleted", (byte) 1);
        return row;
    }

    private static ClickHouseStruct record(Struct before, Struct after, CDC_OPERATION operation, long seconds) {
        var record = new ClickHouseStruct();
        record.setTopic("test.accounts"); record.setKafkaPartition(0); record.setKafkaOffset(seconds);
        record.setPrimaryKey(new ArrayList<>(List.of("organisation_id", "account_id")));
        record.setBeforeStruct(before); record.setAfterStruct(after); record.setCdcOperation(operation);
        record.setTsSec(seconds); record.setTs_ms(seconds * 1000); record.setGtid(seconds);
        return record;
    }

    private static long count(Connection connection, String predicate) throws Exception {
        try (var statement = connection.createStatement(); var result = statement.executeQuery("SELECT count() FROM accounts WHERE " + predicate)) {
            assertTrue(result.next()); return result.getLong(1);
        }
    }

    private static void assertSiblingUntouched(Connection connection) throws Exception {
        // Raw rows, with background merges stopped: catch even transient false history or tombstones.
        assertEquals(1, count(connection, "account_id='account-B'"));
        assertEquals(1, count(connection, "account_id='account-B' AND name='Bob' AND _version=1"
                + " AND _valid_from=toDateTime('2026-01-01 00:00:00') AND _valid_to=toDateTime('2100-01-01 00:00:00')"));
    }
}
