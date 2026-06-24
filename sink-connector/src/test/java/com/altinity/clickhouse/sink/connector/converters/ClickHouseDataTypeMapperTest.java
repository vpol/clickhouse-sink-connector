package com.altinity.clickhouse.sink.connector.converters;

import com.altinity.clickhouse.sink.connector.ClickHouseSinkConnectorConfig;
import com.altinity.clickhouse.sink.connector.db.BaseDbWriter;
import com.altinity.clickhouse.sink.connector.db.HikariDbSource;
import com.clickhouse.data.ClickHouseDataType;
import com.clickhouse.jdbc.ClickHouseConnection;
import io.debezium.data.VariableScaleDecimal;
import io.debezium.time.Date;
import io.debezium.time.Time;
import org.apache.kafka.connect.data.Schema;
import org.junit.Assert;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.ClickHouseContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.sql.*;
import java.time.ZoneId;
import java.lang.reflect.Proxy;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

@Testcontainers
public class ClickHouseDataTypeMapperTest {

//    @Container
//    private ClickHouseContainer clickHouseContainer = new ClickHouseContainer("clickhouse/clickhouse-server:latest")
//            .withInitScript("./datatypes.sql");

    @Test
    public void getClickHouseDataType() {
        ClickHouseDataType chDataType = ClickHouseDataTypeMapper.getClickHouseDataType(Schema.Type.INT16, null);
        Assert.assertTrue(chDataType.name().equalsIgnoreCase("INT16"));

        chDataType = ClickHouseDataTypeMapper.getClickHouseDataType(Schema.Type.INT32, null);
        Assert.assertTrue(chDataType.name().equalsIgnoreCase("INT32"));

        chDataType = ClickHouseDataTypeMapper.getClickHouseDataType(Schema.BYTES_SCHEMA.type(), null);
        Assert.assertTrue(chDataType.name().equalsIgnoreCase("String"));

        chDataType = ClickHouseDataTypeMapper.getClickHouseDataType(Schema.INT32_SCHEMA.type(), Time.SCHEMA_NAME);
        Assert.assertTrue(chDataType.name().equalsIgnoreCase("String"));

        chDataType = ClickHouseDataTypeMapper.getClickHouseDataType(Schema.INT32_SCHEMA.type(), Date.SCHEMA_NAME);
        Assert.assertTrue(chDataType.name().equalsIgnoreCase("Date32"));

        chDataType = ClickHouseDataTypeMapper.getClickHouseDataType(Schema.Type.STRUCT, VariableScaleDecimal.LOGICAL_NAME);
        Assert.assertTrue(chDataType.name().equalsIgnoreCase("Decimal"));

    }

    @Test
    public void convertArrayAcceptsEmptyListImplementation() throws SQLException {
        AtomicReference<String> typeName = new AtomicReference<>();
        AtomicReference<Object[]> elements = new AtomicReference<>();

        boolean converted = ClickHouseDataTypeMapper.convert(
                Schema.Type.ARRAY,
                Schema.Type.STRING.name(),
                Collections.emptyList(),
                1,
                preparedStatementCapturingArray(typeName, elements),
                null,
                null,
                ZoneId.of("UTC"));

        Assert.assertTrue(converted);
        Assert.assertEquals("String", typeName.get());
        Assert.assertArrayEquals(new Object[0], elements.get());
    }

    @Test
    public void convertArrayAcceptsImmutableListImplementation() throws SQLException {
        AtomicReference<String> typeName = new AtomicReference<>();
        AtomicReference<Object[]> elements = new AtomicReference<>();

        boolean converted = ClickHouseDataTypeMapper.convert(
                Schema.Type.ARRAY,
                Schema.Type.STRING.name(),
                List.of("api", "post"),
                1,
                preparedStatementCapturingArray(typeName, elements),
                null,
                null,
                ZoneId.of("UTC"));

        Assert.assertTrue(converted);
        Assert.assertEquals("String", typeName.get());
        Assert.assertArrayEquals(new Object[]{"api", "post"}, elements.get());
    }

    private PreparedStatement preparedStatementCapturingArray(
            AtomicReference<String> typeName,
            AtomicReference<Object[]> elements) {
        Connection connection = (Connection) Proxy.newProxyInstance(
                Connection.class.getClassLoader(),
                new Class<?>[]{Connection.class},
                (proxy, method, args) -> {
                    if ("createArrayOf".equals(method.getName())) {
                        typeName.set((String) args[0]);
                        elements.set((Object[]) args[1]);
                        return Proxy.newProxyInstance(
                                Array.class.getClassLoader(),
                                new Class<?>[]{Array.class},
                                (arrayProxy, arrayMethod, arrayArgs) ->
                                        defaultValue(arrayMethod.getReturnType()));
                    }
                    return defaultValue(method.getReturnType());
                });
        return (PreparedStatement) Proxy.newProxyInstance(
                PreparedStatement.class.getClassLoader(),
                new Class<?>[]{PreparedStatement.class},
                (proxy, method, args) -> {
                    if ("getConnection".equals(method.getName())) {
                        return connection;
                    }
                    return defaultValue(method.getReturnType());
                });
    }

    private Object defaultValue(Class<?> returnType) {
        if (!returnType.isPrimitive()) {
            return null;
        }
        if (returnType == boolean.class) {
            return false;
        }
        if (returnType == void.class) {
            return null;
        }
        return 0;
    }

}
