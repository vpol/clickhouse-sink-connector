package com.altinity.clickhouse.sink.connector.db.batch;

import com.altinity.clickhouse.sink.connector.ClickHouseSinkConnectorConfig;
import com.altinity.clickhouse.sink.connector.converters.ClickHouseConverter;
import com.altinity.clickhouse.sink.connector.db.DBMetadata;
import com.altinity.clickhouse.sink.connector.model.ClickHouseStruct;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.Assert;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.sql.PreparedStatement;
import java.sql.Types;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

public class PreparedStatementFieldMapperTest {

    @Test
    public void replacingMergeTreeDeleteColumnIsSetForInsertWhenDeletesAreIgnored() throws Exception {
        PreparedStatementFieldMapper mapper = new PreparedStatementFieldMapper(
                "is_deleted", true, null, "_version", "skadi", ZoneId.of("UTC"));
        RecordingPreparedStatement ps = new RecordingPreparedStatement();
        ClickHouseSinkConnectorConfig config = configWithIgnoreDelete(true);
        ClickHouseStruct record = record(ClickHouseConverter.CDC_OPERATION.CREATE);
        Struct sourceStruct = new Struct(SchemaBuilder.struct().name("campaigns").build());

        mapper.insertPreparedStatement(
                columnIndexes(),
                ps.proxy(),
                java.util.List.of(),
                record,
                sourceStruct,
                false,
                config,
                columnTypes(),
                DBMetadata.TABLE_ENGINE.SHARED_REPLACING_MERGE_TREE,
                "campaigns");

        Assert.assertEquals(Long.valueOf(123L), ps.longValues.get(1));
        Assert.assertEquals(Integer.valueOf(0), ps.intValues.get(2));
        Assert.assertEquals("is_deleted should be bound after the initial missing-field null",
                Integer.valueOf(Types.OTHER), ps.nullValues.get(2));
    }

    @Test
    public void replacingMergeTreeDeleteColumnStaysNotDeletedForDeleteWhenDeletesAreIgnored() throws Exception {
        PreparedStatementFieldMapper mapper = new PreparedStatementFieldMapper(
                "is_deleted", true, null, "_version", "skadi", ZoneId.of("UTC"));
        RecordingPreparedStatement ps = new RecordingPreparedStatement();
        ClickHouseSinkConnectorConfig config = configWithIgnoreDelete(true);
        ClickHouseStruct record = record(ClickHouseConverter.CDC_OPERATION.DELETE);
        Struct sourceStruct = new Struct(SchemaBuilder.struct().name("campaigns").build());

        mapper.insertPreparedStatement(
                columnIndexes(),
                ps.proxy(),
                java.util.List.of(),
                record,
                sourceStruct,
                false,
                config,
                columnTypes(),
                DBMetadata.TABLE_ENGINE.SHARED_REPLACING_MERGE_TREE,
                "campaigns");

        Assert.assertEquals(Integer.valueOf(0), ps.intValues.get(2));
    }

    private static ClickHouseSinkConnectorConfig configWithIgnoreDelete(boolean ignoreDelete) {
        Map<String, String> config = new HashMap<>();
        config.put("connector.class", "io.debezium.connector.postgresql.PostgresConnector");
        config.put("ignore_delete", Boolean.toString(ignoreDelete));
        return new ClickHouseSinkConnectorConfig(config);
    }

    private static ClickHouseStruct record(ClickHouseConverter.CDC_OPERATION operation) {
        ClickHouseStruct record = new ClickHouseStruct();
        record.setCdcOperation(operation);
        record.setVersion(123L);
        return record;
    }

    private static Map<String, Integer> columnIndexes() {
        Map<String, Integer> indexes = new LinkedHashMap<>();
        indexes.put("_version", 1);
        indexes.put("is_deleted", 2);
        return indexes;
    }

    private static Map<String, String> columnTypes() {
        Map<String, String> types = new LinkedHashMap<>();
        types.put("_version", "UInt64");
        types.put("is_deleted", "UInt8");
        return types;
    }

    private static class RecordingPreparedStatement implements InvocationHandler {
        private final Map<Integer, Integer> intValues = new HashMap<>();
        private final Map<Integer, Long> longValues = new HashMap<>();
        private final Map<Integer, Integer> nullValues = new HashMap<>();

        private PreparedStatement proxy() {
            return (PreparedStatement) Proxy.newProxyInstance(
                    PreparedStatement.class.getClassLoader(),
                    new Class<?>[]{PreparedStatement.class},
                    this);
        }

        @Override
        public Object invoke(Object proxy, java.lang.reflect.Method method, Object[] args) {
            if ("setInt".equals(method.getName())) {
                intValues.put((Integer) args[0], (Integer) args[1]);
                return null;
            }
            if ("setLong".equals(method.getName())) {
                longValues.put((Integer) args[0], (Long) args[1]);
                return null;
            }
            if ("setNull".equals(method.getName())) {
                nullValues.put((Integer) args[0], (Integer) args[1]);
                return null;
            }
            if ("toString".equals(method.getName())) {
                return "RecordingPreparedStatement";
            }
            return defaultValue(method.getReturnType());
        }

        private static Object defaultValue(Class<?> returnType) {
            if (returnType == Boolean.TYPE) {
                return false;
            }
            if (returnType == Byte.TYPE) {
                return (byte) 0;
            }
            if (returnType == Short.TYPE) {
                return (short) 0;
            }
            if (returnType == Integer.TYPE) {
                return 0;
            }
            if (returnType == Long.TYPE) {
                return 0L;
            }
            if (returnType == Float.TYPE) {
                return 0f;
            }
            if (returnType == Double.TYPE) {
                return 0d;
            }
            return null;
        }
    }
}
