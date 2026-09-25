package com.altinity.clickhouse.debezium.embedded.cdc;

import com.altinity.clickhouse.debezium.embedded.parser.SourceRecordParserService;
import com.altinity.clickhouse.sink.connector.ClickHouseSinkConnectorConfig;
import com.altinity.clickhouse.sink.connector.executor.DebeziumOffsetManagement;
import com.altinity.clickhouse.sink.connector.model.ClickHouseStruct;
import com.altinity.clickhouse.sink.connector.model.RoutedBatch;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.LinkedBlockingQueue;

import static org.junit.jupiter.api.Assertions.*;

/** Exercises the real parser, batch handoff and offset acknowledgement path. */
class TrailingControlBatchCompletionTest {
    private static final Schema KEY = SchemaBuilder.struct().field("id", Schema.STRING_SCHEMA).build();
    private static final Schema ROW = SchemaBuilder.struct().optional()
            .field("id", Schema.STRING_SCHEMA).field("payload_uuid", Schema.OPTIONAL_STRING_SCHEMA).build();
    private static final Schema SOURCE = SchemaBuilder.struct()
            .field("db", Schema.STRING_SCHEMA).field("ts_ms", Schema.INT64_SCHEMA).build();
    private static final Schema ENVELOPE = SchemaBuilder.struct()
            .field("before", ROW).field("after", ROW).field("source", SOURCE)
            .field("op", Schema.STRING_SCHEMA).field("ts_ms", Schema.INT64_SCHEMA).build();
    private final DebeziumChangeEventCapture capture = new DebeziumChangeEventCapture();
    private final LinkedBlockingQueue<List<ClickHouseStruct>> queue = new LinkedBlockingQueue<>();
    private final RecordingCommitter committer = new RecordingCommitter();

    @AfterEach
    void cleanUp() throws Exception {
        // Offset management is shared by all capture instances in the test JVM.
        for (String name : List.of("inFlightBatches", "completedBatches")) {
            Field field = DebeziumOffsetManagement.class.getDeclaredField(name);
            field.setAccessible(true);
            ((Map<?, ?>) field.get(null)).clear();
        }
        while (DebeziumOffsetManagement.hasUnwrittenBatches()) {
            DebeziumOffsetManagement.batchHandoffFailed();
        }
        capture.singleThreadDebeziumEventExecutor.shutdownNow();
    }

    @Test
    void ignoredDeleteAndTombstoneFinishWithoutAnotherSourceEvent() throws Exception {
        ChangeEvent<SourceRecord, SourceRecord> delete = row("d", 10, "rows");
        handle(List.of(delete, tombstone(10)), true);
        assertEquals(0, committer.finished, "handoff must not commit an unwritten batch");
        acknowledge(take());
        assertEquals(List.of(delete), committer.processed);
        assertEquals(1, committer.finished);
    }

    @Test
    void ordinaryDeleteWithTombstoneAlsoFinishes() throws Exception {
        handle(List.of(row("d", 10, "rows"), tombstone(10)), false);
        acknowledge(take());
        assertEquals(1, committer.finished);
    }

    @Test
    void mixedInsertsUpdatesAndDeletesFinishOnceAtTheLastDataOffset() throws Exception {
        var insert = row("c", 10, "rows");
        var update = row("u", 20, "rows");
        var delete = row("d", 30, "rows");
        handle(List.of(heartbeat(5), insert, tombstone(10), update, delete,
                tombstone(30), heartbeat(40), heartbeat(50)), true);
        List<ClickHouseStruct> batch = take();
        assertEquals(List.of(false, false, true), batch.stream().map(ClickHouseStruct::isLastRecordInBatch).toList());
        acknowledge(batch);
        assertEquals(List.of(insert, update, delete), committer.processed);
        assertEquals(1, committer.finished);
        assertEquals(30L, committer.processed.get(2).value().sourceOffset().get("lsn"),
                "do not substitute the later heartbeat offset for the acknowledged data offset");
    }

    @Test
    void multipleDeletesAndTombstonesFinishOneBatch() throws Exception {
        handle(List.of(row("d", 10, "rows"), tombstone(10), row("d", 20, "rows"), tombstone(20)), true);
        acknowledge(take());
        assertEquals(2, committer.processed.size());
        assertEquals(1, committer.finished);
    }

    @Test
    void insertUpdateAndSnapshotRowsFollowedByControlRecordsFinish() throws Exception {
        long offset = 10;
        for (String operation : List.of("c", "u", "r")) {
            handle(List.of(row(operation, offset, "rows"), heartbeat(offset + 1)), true);
            acknowledge(take());
            offset += 10;
        }
        assertEquals(3, committer.processed.size());
        assertEquals(3, committer.finished);
    }

    @Test
    void dataOnlyBatchStillFinishesExactlyOnce() throws Exception {
        handle(List.of(row("c", 10, "rows"), row("u", 20, "rows")), true);
        acknowledge(take());
        assertEquals(2, committer.processed.size());
        assertEquals(1, committer.finished);
    }

    @Test
    void controlOnlyBatchStillCommitsItsLatestOffset() throws Exception {
        var latest = heartbeat(30);
        handle(List.of(tombstone(10), heartbeat(20), latest), true);
        assertTrue(queue.isEmpty());
        assertEquals(List.of(latest), committer.processed);
        assertEquals(1, committer.finished);
    }

    @Test
    void failedOrPendingWriteBlocksLaterControlOffsetUntilRetrySucceeds() throws Exception {
        handle(List.of(row("d", 10, "rows"), tombstone(10)), true);
        List<ClickHouseStruct> failed = take();
        DebeziumOffsetManagement.addToBatchTimestamps(failed);
        // processBatch removes a failed attempt from in-flight, but retains
        // its handoff registration until a retry has actually persisted it.
        DebeziumOffsetManagement.removeFromBatchTimestamps(failed);
        handle(List.of(heartbeat(20)), true);
        assertTrue(committer.processed.isEmpty());
        assertEquals(0, committer.finished);
        acknowledge(failed);
        assertEquals(1, committer.processed.size());
        assertEquals(1, committer.finished);
    }

    @Test
    void routedLastGroupWaitsForAnOlderInflightGroup() throws Exception {
        LinkedBlockingQueue<RoutedBatch> routed = new LinkedBlockingQueue<>();
        setCaptureField("threadPoolSize", 3);
        setCaptureField("routedRecords", routed);
        handle(List.of(row("c", 10, "first"), row("d", 20, "second"), tombstone(20)), true);
        List<List<ClickHouseStruct>> groups = List.of(routed.remove().getBatch(), routed.remove().getBatch());
        List<ClickHouseStruct> first = groups.stream().filter(b -> b.get(0).getTopic().endsWith(".first")).findFirst().orElseThrow();
        List<ClickHouseStruct> last = groups.stream().filter(b -> b.get(0).getTopic().endsWith(".second")).findFirst().orElseThrow();
        DebeziumOffsetManagement.addToBatchTimestamps(first);
        DebeziumOffsetManagement.addToBatchTimestamps(last);
        assertFalse(DebeziumOffsetManagement.checkIfBatchCanBeCommitted(last));
        assertTrue(committer.processed.isEmpty());
        assertEquals(0, committer.finished);
        assertTrue(DebeziumOffsetManagement.checkIfBatchCanBeCommitted(first));
        assertEquals(List.of(10L, 20L), committer.processed.stream()
                .map(r -> r.value().sourceOffset().get("lsn")).toList());
        assertEquals(1, committer.finished);
    }

    @Test
    void commitFailureIsPropagatedAndTheBatchCanBeRetried() throws Exception {
        handle(List.of(row("d", 10, "rows"), tombstone(10)), true);
        List<ClickHouseStruct> batch = take();
        committer.failFinish = true;
        assertThrows(IllegalStateException.class, () -> acknowledge(batch));
        assertTrue(DebeziumOffsetManagement.hasUnwrittenBatches());
        committer.failFinish = false;
        acknowledge(batch);
        assertEquals(1, committer.finished);
        assertFalse(DebeziumOffsetManagement.hasUnwrittenBatches());
    }

    private void handle(List<ChangeEvent<SourceRecord, SourceRecord>> events, boolean ignoreDelete) throws Exception {
        setCaptureField("records", queue);
        ClickHouseSinkConnectorConfig config = new ClickHouseSinkConnectorConfig(Map.of(
                "connector.class", "io.debezium.connector.postgresql.PostgresConnector",
                "single.threaded", "false", "ignore_delete", Boolean.toString(ignoreDelete)));
        capture.handleChangeEventBatch(events, committer, new Properties(), new SourceRecordParserService(), config);
    }

    private void setCaptureField(String name, Object value) throws Exception {
        Field field = DebeziumChangeEventCapture.class.getDeclaredField(name);
        field.setAccessible(true);
        field.set(capture, value);
    }

    private List<ClickHouseStruct> take() {
        assertEquals(1, queue.size());
        List<ClickHouseStruct> batch = queue.remove();
        assertFalse(batch.isEmpty());
        return batch;
    }

    private void acknowledge(List<ClickHouseStruct> batch) throws InterruptedException {
        DebeziumOffsetManagement.addToBatchTimestamps(batch);
        assertTrue(DebeziumOffsetManagement.checkIfBatchCanBeCommitted(batch));
    }

    private static ChangeEvent<SourceRecord, SourceRecord> row(String operation, long offset, String table) {
        Struct before = new Struct(ROW).put("id", "key");
        Struct after = new Struct(ROW).put("id", "key").put("payload_uuid", "00000000-0000-0000-0000-000000000001");
        Struct value = new Struct(ENVELOPE).put("op", operation).put("ts_ms", offset)
                .put("source", new Struct(SOURCE).put("db", "sink").put("ts_ms", offset))
                .put("before", operation.equals("d") || operation.equals("u") ? before : null)
                .put("after", operation.equals("d") ? null : after);
        return event(offset, "sink.public." + table, ENVELOPE, value);
    }

    private static ChangeEvent<SourceRecord, SourceRecord> tombstone(long offset) {
        return event(offset, "sink.public.rows", null, null);
    }

    private static ChangeEvent<SourceRecord, SourceRecord> heartbeat(long offset) {
        Schema schema = SchemaBuilder.struct().field("ts_ms", Schema.INT64_SCHEMA).build();
        return event(offset, "__debezium-heartbeat.sink", schema, new Struct(schema).put("ts_ms", offset));
    }

    private static ChangeEvent<SourceRecord, SourceRecord> event(long offset, String topic, Schema schema, Struct value) {
        SourceRecord record = new SourceRecord(Map.of("server", "sink"), Map.of("lsn", offset),
                topic, 0, KEY, new Struct(KEY).put("id", "key"), schema, value, offset);
        return new ChangeEvent<>() {
            public SourceRecord key() { return null; }
            public SourceRecord value() { return record; }
            public String destination() { return record.topic(); }
            public Integer partition() { return 0; }
        };
    }

    private static class RecordingCommitter implements DebeziumEngine.RecordCommitter<ChangeEvent<SourceRecord, SourceRecord>> {
        private final List<ChangeEvent<SourceRecord, SourceRecord>> processed = new ArrayList<>();
        private int finished;
        private boolean failFinish;

        public void markProcessed(ChangeEvent<SourceRecord, SourceRecord> record) { processed.add(record); }
        public void markBatchFinished() {
            if (failFinish) { throw new IllegalStateException("Simulated offset commit failure"); }
            finished++;
        }
        public void markProcessed(ChangeEvent<SourceRecord, SourceRecord> record, DebeziumEngine.Offsets offsets) {
            markProcessed(record);
        }
        public DebeziumEngine.Offsets buildOffsets() { return (key, value) -> { }; }
    }
}
