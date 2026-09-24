package com.altinity.clickhouse.sink.connector.db.batch;

import com.altinity.clickhouse.sink.connector.model.ClickHouseStruct;
import com.altinity.clickhouse.sink.connector.ClickHouseSinkConnectorConfig;
import com.altinity.clickhouse.sink.connector.converters.ClickHouseConverter;
import com.altinity.clickhouse.sink.connector.db.DBMetadata;
import com.altinity.clickhouse.sink.connector.model.BlockMetaData;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.Assert;
import org.junit.jupiter.api.Test;

import java.time.ZoneId;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * ReplacingMergeTree deduplicates by SORTING KEY, so an UPDATE that changes a
 * sorting-key column writes the new row at a different key and leaves the
 * pre-update row in place forever: MySQL holds one row, ClickHouse holds two.
 *
 * <p>Measured on ClickHouse 24.8.14, keyed table {@code ORDER BY (id)}, three
 * rows, {@code UPDATE id=1 -> id=9}:</p>
 *
 * <pre>
 *   mysql        2 rows   (id 2, 9)
 *   clickhouse   3 rows   (id 1, 2, 9)   the pre-update row survives
 * </pre>
 *
 * <p>This is why it is not merely a keyless-table concern: any UPDATE of a
 * sorting-key column orphans the old row. It is simply guaranteed to happen on
 * a keyless table, whose sorting key is every column.</p>
 *
 * <p>The tombstone must be emitted ONLY when the key actually changes. For a
 * same-key UPDATE the delete marker and the new row would land on the same
 * sorting key, and ReplacingMergeTree resolves a version tie by insertion
 * order. Measured: with the tombstone written second, the live row is
 * permanently lost (FINAL returns 0 rows). Hence the same-key case must emit
 * the after-image alone.</p>
 */
public class PreparedStatementExecutorSortingKeyTombstoneTest {

    private static final Schema ROW_SCHEMA = SchemaBuilder.struct()
            .field("id", Schema.OPTIONAL_INT32_SCHEMA)
            .field("val", Schema.OPTIONAL_STRING_SCHEMA)
            .build();

    private PreparedStatementExecutor executorWithSortingKey(List<String> sortingKeyColumns) {
        return new PreparedStatementExecutor("is_deleted", true, "sign", "_version",
                "testdb", ZoneId.of("UTC"), () -> sortingKeyColumns);
    }

    private ClickHouseStruct updateRecord(Integer beforeId, String beforeVal,
                                          Integer afterId, String afterVal) {
        ClickHouseStruct record = new ClickHouseStruct();
        record.setBeforeStruct(new Struct(ROW_SCHEMA).put("id", beforeId).put("val", beforeVal));
        record.setAfterStruct(new Struct(ROW_SCHEMA).put("id", afterId).put("val", afterVal));
        return record;
    }

    @Test
    public void testSharedReplacingUpdateBindsOldKeyTombstoneBeforeNewRow() throws Exception {
        for (int afterId : new int[]{1, 9}) {
            List<Map<Integer, Object>> batches = new ArrayList<>();
            Map<Integer, Object> bound = new HashMap<>();
            PreparedStatement statement = (PreparedStatement) Proxy.newProxyInstance(
                    PreparedStatement.class.getClassLoader(), new Class<?>[]{PreparedStatement.class},
                    (proxy, method, args) -> {
                        if (method.getName().startsWith("set") && args != null
                                && args.length >= 2 && args[0] instanceof Integer) {
                            bound.put((Integer) args[0], "setNull".equals(method.getName()) ? null : args[1]);
                        } else if ("addBatch".equals(method.getName())) {
                            batches.add(new HashMap<>(bound));
                        } else if ("executeBatch".equals(method.getName())) {
                            return new int[batches.size()];
                        }
                        return null;
                    });
            Connection connection = (Connection) Proxy.newProxyInstance(
                    Connection.class.getClassLoader(), new Class<?>[]{Connection.class},
                    (proxy, method, args) -> "prepareStatement".equals(method.getName()) ? statement : null);

            ClickHouseStruct record = updateRecord(1, "before", afterId, "after");
            record.setCdcOperation(ClickHouseConverter.CDC_OPERATION.UPDATE);
            record.setVersion(123L);
            record.setBeforeModifiedFields(ROW_SCHEMA.fields());
            record.setAfterModifiedFields(ROW_SCHEMA.fields());
            Map<String, Integer> indexes = Map.of("id", 1, "val", 2, "_version", 3, "is_deleted", 4);
            Map<MutablePair<String, Map<String, Integer>>, List<ClickHouseStruct>> queries = new HashMap<>();
            queries.put(MutablePair.of("INSERT INTO testdb.rows VALUES (?, ?, ?, ?)", indexes), List.of(record));

            Assert.assertTrue(executorWithSortingKey(List.of("id")).addToPreparedStatementBatch(
                    "testdb.rows", queries, new BlockMetaData(),
                    new ClickHouseSinkConnectorConfig(Map.of(
                            "connector.class", "io.debezium.connector.postgresql.PostgresConnector")),
                    connection, "rows",
                    Map.of("id", "Int32", "val", "String", "_version", "UInt64", "is_deleted", "UInt8"),
                    DBMetadata.TABLE_ENGINE.SHARED_REPLACING_MERGE_TREE));

            Assert.assertEquals(afterId == 1 ? 1 : 2, batches.size());
            if (afterId != 1) {
                Assert.assertEquals(1, batches.get(0).get(1));
                Assert.assertEquals(1, batches.get(0).get(4));
            }
            Map<Integer, Object> liveRow = batches.get(batches.size() - 1);
            Assert.assertEquals(afterId, liveRow.get(1));
            Assert.assertEquals(0, liveRow.get(4));
            Assert.assertEquals(123L, liveRow.get(3));
        }
    }

    /**
     * The orphaning case: a sorting-key column changes.
     */
    @Test
    public void testSortingKeyColumnChangeRequiresTombstone() {
        PreparedStatementExecutor executor = executorWithSortingKey(Arrays.asList("id"));
        Assert.assertTrue("an UPDATE that moves the row to a new sorting key must tombstone the old one",
                executor.updateRelocatesSortingKey(updateRecord(1, "a", 9, "a")));
    }

    /**
     * The dangerous case: the key is unchanged, so a tombstone would create a
     * same-key version tie that can drop the live row.
     */
    @Test
    public void testNonSortingKeyColumnChangeNeedsNoTombstone() {
        PreparedStatementExecutor executor = executorWithSortingKey(Arrays.asList("id"));
        Assert.assertFalse("a same-key UPDATE must not be tombstoned -- the tie can delete the live row",
                executor.updateRelocatesSortingKey(updateRecord(1, "a", 1, "b")));
    }

    /**
     * A keyless table orders by every column, so any value change relocates it.
     */
    @Test
    public void testKeylessTableAllColumnsSortingKeyDetectsAnyChange() {
        PreparedStatementExecutor executor = executorWithSortingKey(Arrays.asList("id", "val"));
        Assert.assertTrue("with an all-columns sorting key, any changed column relocates the row",
                executor.updateRelocatesSortingKey(updateRecord(1, "a", 1, "b")));
    }

    /**
     * A no-op UPDATE (MySQL permits {@code UPDATE t SET a = a}) changes nothing
     * and must not produce a tombstone.
     */
    @Test
    public void testNoOpUpdateNeedsNoTombstone() {
        PreparedStatementExecutor executor = executorWithSortingKey(Arrays.asList("id", "val"));
        Assert.assertFalse("a no-op UPDATE must not be tombstoned",
                executor.updateRelocatesSortingKey(updateRecord(1, "a", 1, "a")));
    }

    /**
     * NULL transitions are ordinary value changes under a nullable sorting key,
     * which the all-columns fallback always is.
     */
    @Test
    public void testNullTransitionInSortingKeyIsAChange() {
        PreparedStatementExecutor executor = executorWithSortingKey(Arrays.asList("id", "val"));
        Assert.assertTrue("NULL -> value must count as relocating the row",
                executor.updateRelocatesSortingKey(updateRecord(1, null, 1, "a")));
        Assert.assertTrue("value -> NULL must count as relocating the row",
                executor.updateRelocatesSortingKey(updateRecord(1, "a", 1, null)));
        Assert.assertFalse("NULL -> NULL is not a change",
                executor.updateRelocatesSortingKey(updateRecord(1, null, 1, null)));
    }

    /**
     * An unknown or empty sorting key must degrade to the previous behaviour
     * rather than emit a speculative tombstone.
     */
    @Test
    public void testUnknownSortingKeyEmitsNoTombstone() {
        PreparedStatementExecutor executor = executorWithSortingKey(new ArrayList<>());
        Assert.assertFalse("an empty/unknown sorting key must not produce a tombstone",
                executor.updateRelocatesSortingKey(updateRecord(1, "a", 9, "b")));
    }

    /**
     * A sorting key may be an expression over columns the CDC record does not
     * carry (for example {@code toDate(deleted_time)} in replication-history
     * mode). Absent columns are skipped rather than guessed at.
     */
    @Test
    public void testSortingKeyColumnAbsentFromRecordIsSkipped() {
        PreparedStatementExecutor executor = executorWithSortingKey(Arrays.asList("deleted_time"));
        Assert.assertFalse("a sorting-key column absent from the record must not force a tombstone",
                executor.updateRelocatesSortingKey(updateRecord(1, "a", 9, "b")));
    }

    /**
     * A DELETE carries no after-image; it already writes its own tombstone.
     */
    @Test
    public void testMissingAfterImageEmitsNoTombstone() {
        PreparedStatementExecutor executor = executorWithSortingKey(Arrays.asList("id"));
        ClickHouseStruct record = new ClickHouseStruct();
        record.setBeforeStruct(new Struct(ROW_SCHEMA).put("id", 1).put("val", "a"));
        Assert.assertFalse("a record without an after image is not a relocating UPDATE",
                executor.updateRelocatesSortingKey(record));
    }
}
