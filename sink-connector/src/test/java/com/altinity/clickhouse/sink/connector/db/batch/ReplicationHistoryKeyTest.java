package com.altinity.clickhouse.sink.connector.db.batch;

import com.altinity.clickhouse.sink.connector.ClickHouseSinkConnectorConfig;
import com.altinity.clickhouse.sink.connector.converters.ClickHouseConverter.CDC_OPERATION;
import com.altinity.clickhouse.sink.connector.converters.ClickHouseDataTypeMapper;
import com.clickhouse.data.ClickHouseDataType;
import com.altinity.clickhouse.sink.connector.db.QueryFormatter;
import com.altinity.clickhouse.sink.connector.model.ClickHouseStruct;
import io.debezium.data.VariableScaleDecimal;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
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

    @ParameterizedTest
    @ValueSource(strings = {"9007199254740993.000000000000000001", "-123.4500", "1E+20", "0.000000000000000001"})
    void decodesVariableScaleNumericKeysWithoutLosingPrecision(String number) throws Exception {
        BigDecimal value = new BigDecimal(number);
        Schema decimalSchema = VariableScaleDecimal.schema();
        Schema schema = SchemaBuilder.struct().field("organisation_id", Schema.STRING_SCHEMA)
                .field("account_id", decimalSchema).build();
        byte[] bytes = value.unscaledValue().toByteArray();
        ByteBuffer buffer = ByteBuffer.allocateDirect(bytes.length + 2);
        buffer.put((byte) 99).put(bytes).put((byte) 99).flip();
        buffer.position(1).limit(bytes.length + 1);
        for (Object carrier : java.util.List.of(bytes, buffer.asReadOnlyBuffer())) {
            Struct decimal = new Struct(decimalSchema).put("scale", value.scale()).put("value", carrier);
            Struct before = new Struct(schema).put("organisation_id", "org-1").put("account_id", decimal);
            var params = handler().buildUpdateQueryParams(record(before, before));
            assertEquals(value, params.getPrimaryKeyValues().get("account_id"));
            var columns = Map.of("organisation_id", "String", "account_id", "Decimal(64,18)");
            String expected = "`account_id`=CAST('" + value.toPlainString() + "', 'Decimal(64,18)')";
            for (String sql : java.util.List.of(handler().generateDeleteQuery("accounts", columns, params).left,
                    handler().generateUpdateQuery("accounts", schema.fields(), columns, params).left)) {
                assertEquals(2, sql.split(java.util.regex.Pattern.quote(expected), -1).length - 1);
                assertFalse(sql.contains("Struct{"));
                assertFalse(sql.contains("[B@"));
            }
            var bound = new java.util.concurrent.atomic.AtomicReference<Object>();
            var ps = (java.sql.PreparedStatement) java.lang.reflect.Proxy.newProxyInstance(
                    getClass().getClassLoader(), new Class<?>[]{java.sql.PreparedStatement.class}, (proxy, method, args) -> {
                        assertEquals("setBigDecimal", method.getName());
                        assertEquals(1, args[0]);
                        bound.set(args[1]);
                        return null;
                    });
            assertTrue(ClickHouseDataTypeMapper.convert(Schema.Type.STRUCT, VariableScaleDecimal.LOGICAL_NAME,
                    decimal, 1, ps, new ClickHouseSinkConnectorConfig(Map.of()), ClickHouseDataType.Decimal, ZoneId.of("UTC")));
            assertEquals(value, bound.get());
            if (carrier instanceof ByteBuffer) {
                assertEquals(1, ((ByteBuffer) carrier).position());
                assertEquals(bytes.length + 1, ((ByteBuffer) carrier).limit());
            }
        }
    }

    @Test
    void fixedScaleDecimalKeyAlsoUsesAnExactTypedLiteral() {
        Schema schema = SchemaBuilder.struct().field("organisation_id", Schema.STRING_SCHEMA)
                .field("account_id", Decimal.schema(18)).build();
        BigDecimal value = new BigDecimal("9007199254740993.000000000000000001");
        Struct before = new Struct(schema).put("organisation_id", "org-1").put("account_id", value);
        var params = handler().buildUpdateQueryParams(record(before, null));
        String sql = handler().generateDeleteQuery("accounts",
                Map.of("organisation_id", "String", "account_id", "Nullable(Decimal(38,18))"), params).left;
        assertTrue(sql.contains("`account_id`=CAST('9007199254740993.000000000000000001', 'Nullable(Decimal(38,18))')"));
    }

}
