package com.altinity.clickhouse.sink.connector.db.batch;

import com.altinity.clickhouse.sink.connector.ClickHouseSinkConnectorConfig;
import com.altinity.clickhouse.sink.connector.converters.ClickHouseConverter.CDC_OPERATION;
import com.altinity.clickhouse.sink.connector.db.QueryFormatter;
import com.altinity.clickhouse.sink.connector.model.ClickHouseStruct;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.Test;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class ReplicationHistoryKeyTest {
    private static final Schema ROW = SchemaBuilder.struct()
            .field("organisation_id", Schema.STRING_SCHEMA)
            .field("account_id", Schema.OPTIONAL_STRING_SCHEMA).build();

    private ClickHouseStruct record(Struct before, Struct after) {
        ClickHouseStruct record = new ClickHouseStruct();
        record.setPrimaryKey(new ArrayList<>(java.util.List.of("organisation_id", "account_id")));
        record.setBeforeStruct(before);
        record.setAfterStruct(after);
        record.setCdcOperation(after == null ? CDC_OPERATION.DELETE : CDC_OPERATION.UPDATE);
        record.setTs_ms(1789992000000L);
        record.setTsSec(1789992000L);
        record.setGtid(12345L);
        return record;
    }

    private Struct row(String account) {
        return new Struct(ROW).put("organisation_id", "org-1").put("account_id", account);
    }

    private ReplicationHistoryHandler handler() {
        return new ReplicationHistoryHandler(new QueryFormatter(), null);
    }

    @Test
    void keepsEveryKeyComponentAndUsesBeforeImageForChangedKeys() {
        var params = handler().buildUpdateQueryParams(record(row("old-account"), row("new-account")));
        assertEquals(Map.of("organisation_id", "org-1", "account_id", "old-account"), params.getPrimaryKeyValues());
        assertEquals(params.getPrimaryKeyValues(), handler().buildUpdateQueryParams(record(row("old-account"), null)).getPrimaryKeyValues());
        assertEquals("new-account", handler().buildUpdateQueryParams(record(null, row("new-account"))).getPrimaryKeyValues().get("account_id"));
    }

    @Test
    void rejectsIncompleteKeysInsteadOfBroadeningThePredicate() {
        assertThrows(IllegalArgumentException.class, () -> handler().buildUpdateQueryParams(record(row(null), row("account-A"))));
        var missing = new Struct(SchemaBuilder.struct().field("organisation_id", Schema.STRING_SCHEMA).build())
                .put("organisation_id", "org-1");
        assertThrows(IllegalArgumentException.class, () -> handler().buildUpdateQueryParams(record(missing, null)));
        var noKey = record(null, row("account-A"));
        noKey.setPrimaryKey(new ArrayList<>());
        assertThrows(IllegalArgumentException.class, () -> handler().buildUpdateQueryParams(noKey));
    }

    @Test
    void quotesAllStringKeyComponentsIncludingBackslashesAndWrappedTypes() {
        var params = handler().buildUpdateQueryParams(record(row("a\\'b"), null));
        var query = handler().generateDeleteQuery("accounts",
                Map.of("organisation_id", "LowCardinality(String)", "account_id", "String"), params).left;
        String predicate = "WHERE `organisation_id`='org-1' AND `account_id`='a\\\\''b' AND";
        assertEquals(2, query.split(java.util.regex.Pattern.quote(predicate), -1).length - 1);
        assertThrows(IllegalArgumentException.class, () -> handler().generateDeleteQuery("accounts",
                Map.of("organisation_id", "String"), params));
    }

    @Test
    void customDeleteColumnPreservesBusinessIsDeletedAndFiltersBothBranches() {
        var config = new ClickHouseSinkConnectorConfig(Map.of("replacingmergetree.delete.column", "_is_deleted"));
        var custom = new ReplicationHistoryHandler(config, ZoneId.of("UTC"));
        var record = record(row("account-A"), row("account-A"));
        var params = custom.buildUpdateQueryParams(record);
        Map<String, String> columns = new LinkedHashMap<>(Map.of("organisation_id", "String", "account_id", "String",
                "is_deleted", "UInt8", "_is_deleted", "UInt8", "_valid_to", "DateTime", "_version", "UInt64"));
        var update = custom.generateUpdateQuery("accounts", ROW.fields(), columns, params);
        var delete = custom.generateDeleteQuery("accounts", columns, params);
        for (String sql : java.util.List.of(update.left, delete.left)) {
            assertEquals(2, sql.split("AND `_is_deleted` = 0", -1).length - 1);
            assertFalse(sql.contains("AND `is_deleted` = 0"));
        }
        assertTrue(update.right.containsKey("is_deleted"));
        assertFalse(update.right.containsKey("_is_deleted"));
        assertFalse(PreparedStatementFieldMapper.isUnboundByDesign("is_deleted", "_is_deleted"));
        assertTrue(PreparedStatementFieldMapper.isUnboundByDesign("_is_deleted", "_is_deleted"));
    }

    @Test
    void postgresHistoryUsesTheSameVersionAndTimestampAsPlainInserts() {
        var record = record(null, row("account-A"));
        record.setTsSec(-1);
        record.setGtid(-1);
        record.setSequenceNumber(1790586515666000001L);
        var params = handler().buildUpdateQueryParams(record);
        assertEquals(record.getSequenceNumber(), params.getVersion() + 1);
        assertEquals("2026-09-21 12:00:00", params.getBinlogRecordTimestamp());
        record.setVersion(12345L);
        assertEquals(12345L, handler().buildUpdateQueryParams(record).getVersion() + 1);
        record.setTs_ms(0);
        record.setDebezium_ts_ms(1789992000000L);
        assertEquals("2026-09-21 12:00:00", handler().buildUpdateQueryParams(record).getBinlogRecordTimestamp());
    }

}
