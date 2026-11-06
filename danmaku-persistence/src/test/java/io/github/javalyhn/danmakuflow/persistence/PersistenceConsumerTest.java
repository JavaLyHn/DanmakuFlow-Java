package io.github.javalyhn.danmakuflow.persistence;

import io.github.javalyhn.danmakuflow.protocol.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.h2.jdbcx.JdbcDataSource;
import org.h2.tools.RunScript;
import java.io.InputStreamReader;
import java.math.BigInteger;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Consumer;
import static org.junit.jupiter.api.Assertions.*;

/** Kafka MockConsumer advances real poll positions; H2 covers replay at the JDBC boundary only. */
class PersistenceConsumerTest {
    final ProtocolCodec codec = new ProtocolCodec();
    final TopicPartition p0 = new TopicPartition("events", 0), p1 = new TopicPartition("events", 1);
    final DanmakuEvent event = new DanmakuEvent(UUID.randomUUID(), BigInteger.ZERO, "room", BigInteger.ONE,
            "hello", Instant.parse("2026-11-06T00:00:00Z"));

    // Committing before durable write, or committing only one partition, breaks this.
    @Test void commitsNextOffsetsOnlyAfterWholeBatchWrite() {
        var kafka = kafka();
        var written = new ArrayList<DanmakuEvent>();
        var loop = loop(kafka, batch -> {
            assertNull(kafka.committed(Set.of(p0)).get(p0));
            written.addAll(batch);
        }, 0);
        kafka.schedulePollTask(() -> { add(kafka, p0, 7, good()); add(kafka, p1, 12, good()); });
        kafka.schedulePollTask(() -> { assertEquals(8, kafka.committed(Set.of(p0)).get(p0).offset());
            assertEquals(13, kafka.committed(Set.of(p1)).get(p1).offset()); stop(loop); });
        loop.run();
        assertEquals(2, written.size()); assertTrue(kafka.closed());
    }

    // Partial decode followed by partial write would persist a valid prefix of poison batch.
    @Test void malformedBatchNeverWritesOrCommitsAndSeeksEveryPartition() {
        var kafka = kafka();
        var loop = loop(kafka, batch -> fail("must decode whole batch first"), 0);
        kafka.schedulePollTask(() -> { add(kafka, p0, 7, good()); add(kafka, p1, 12, "bad json"); });
        assertThrows(IllegalStateException.class, loop::run);
        assertTrue(kafka.closed());
        assertEquals(Map.of(p0, 7L, p1, 12L), kafka.seeks);
        assertEquals(0, kafka.commits);
    }

    // A dropped retry, unbounded retry or lost partition rewind breaks this.
    @Test void failedWritesRewindAndExhaustFiniteBudgetWithoutCommit() {
        var kafka = kafka(); var attempts = new int[1];
        var loop = loop(kafka, batch -> { attempts[0]++; throw new IllegalStateException("db down"); }, 2);
        for (int i = 0; i < 3; i++) kafka.schedulePollTask(() -> {
            assertEquals(7, kafka.position(p0)); add(kafka, p0, 7, good()); add(kafka, p1, 12, good()); });
        assertThrows(IllegalStateException.class, loop::run);
        assertEquals(3, attempts[0]); assertEquals(0, kafka.commits);
        assertEquals(Map.of(p0, 7L, p1, 12L), kafka.seeks); assertTrue(kafka.closed());
    }

    // Kafka commit failure must replay safely through the real JDBC unique event key.
    @Test void uncertainOffsetCommitReplaysWithoutDuplicateDatabaseRow() throws Exception {
        var ds = new JdbcDataSource(); ds.setURL("jdbc:h2:mem:" + UUID.randomUUID() + ";MODE=MySQL;DB_CLOSE_DELAY=-1");
        try (var c = ds.getConnection(); var reader = new InputStreamReader(getClass().getResourceAsStream("/h2-fixture.sql"))) {
            RunScript.execute(c, reader);
        }
        var repository = new JdbcDanmakuRepository(ds); var kafka = kafka(); kafka.failCommit = true;
        var attempts = new int[1];
        var loop = loop(kafka, batch -> { attempts[0]++; try { repository.writeBatch(batch); }
            catch (Exception e) { throw new IllegalStateException(e); } }, 1);
        kafka.schedulePollTask(() -> add(kafka, p0, 7, good()));
        kafka.schedulePollTask(() -> add(kafka, p0, 7, good()));
        kafka.schedulePollTask(() -> stop(loop)); loop.run();
        assertEquals(2, attempts[0]); assertEquals(8, kafka.lastCommitted.get(p0).offset());
        assertEquals(1, repository.findHistory("room", event.sendTime(), event.sendTime(), 100).size());
    }

    // Stop during a write must wait for it, close on owner, and leave offsets uncommitted.
    @Test void shutdownWaitsForWriteAndDoesNotCommitUnprocessedData() throws Exception {
        var kafka = kafka(); var entered = new CountDownLatch(1); var finish = new CountDownLatch(1);
        var loop = loop(kafka, batch -> { entered.countDown(); try { assertTrue(finish.await(2, TimeUnit.SECONDS)); }
            catch (InterruptedException e) { throw new AssertionError(e); } }, 1);
        kafka.schedulePollTask(() -> add(kafka, p0, 7, good()));
        var failure = new java.util.concurrent.atomic.AtomicReference<Throwable>();
        var owner = new Thread(() -> { try { loop.run(); } catch (Throwable t) { failure.set(t); } });
        owner.start(); assertTrue(entered.await(2, TimeUnit.SECONDS)); stop(loop);
        assertTrue(owner.isAlive()); finish.countDown(); owner.join(2000);
        assertFalse(owner.isAlive()); assertNull(failure.get()); assertTrue(kafka.closed());
        assertEquals(owner, kafka.closedBy); assertEquals(0, kafka.commits);
    }

    // Wakeup must also interrupt backoff rather than wait through the configured retry delay.
    @Test void stopWakesLongRetryBackoff() throws Exception {
        var kafka = kafka(); var failed = new CountDownLatch(1);
        var loop = loop(kafka, batch -> { failed.countDown(); throw new IllegalStateException("db"); }, 3, Duration.ofSeconds(30));
        kafka.schedulePollTask(() -> add(kafka, p0, 7, good()));
        var owner = new Thread(loop); owner.start(); assertTrue(failed.await(2, TimeUnit.SECONDS)); stop(loop);
        owner.join(2000); assertFalse(owner.isAlive()); assertTrue(kafka.closed()); assertEquals(0, kafka.commits);
    }

    // Context failure after Kafka construction but before run must release the client.
    @Test void contextClosesConsumerThatHasNotStarted() {
        var kafka = kafka(); var loop = loop(kafka, batch -> {}, 1);
        try (var context = new org.springframework.context.annotation.AnnotationConfigApplicationContext()) {
            context.registerBean("consumer", PersistenceConsumer.class, () -> loop);
            context.refresh();
        }
        assertTrue(kafka.closed(), "unstarted Kafka client must not leak on partial startup failure");
        assertThrows(IllegalStateException.class, loop::run);
    }

    // Interrupt during retry must preserve the owner flag and close on that owner.
    @Test void interruptedRetryPreservesFlagAndClosesConsumer() throws Exception {
        var kafka = kafka(); var failed = new CountDownLatch(1);
        var interrupted = new java.util.concurrent.atomic.AtomicBoolean();
        var loop = loop(kafka, batch -> { failed.countDown(); throw new IllegalStateException("db"); }, 3, Duration.ofSeconds(30));
        kafka.schedulePollTask(() -> add(kafka, p0, 7, good()));
        var owner = new Thread(() -> { loop.run(); interrupted.set(Thread.currentThread().isInterrupted()); });
        owner.start(); assertTrue(failed.await(2, TimeUnit.SECONDS)); owner.interrupt(); owner.join(2000);
        assertFalse(owner.isAlive()); assertTrue(interrupted.get()); assertTrue(kafka.closed());
    }
    private String good() { return codec.encodeStoredEvent(event); }
    private TrackingConsumer kafka() {
        var kafka = new TrackingConsumer(); kafka.assign(List.of(p0, p1));
        kafka.updateBeginningOffsets(Map.of(p0, 7L, p1, 12L)); kafka.seek(p0, 7); kafka.seek(p1, 12);
        kafka.seeks.clear(); return kafka;
    }
    private void add(TrackingConsumer kafka, TopicPartition p, long offset, String value) {
        kafka.addRecord(new ConsumerRecord<>(p.topic(), p.partition(), offset, "key", value));
    }
    private PersistenceConsumer loop(TrackingConsumer kafka, Consumer<List<DanmakuEvent>> writer, int retries) {
        return loop(kafka, writer, retries, Duration.ZERO);
    }
    private PersistenceConsumer loop(TrackingConsumer kafka, Consumer<List<DanmakuEvent>> writer, int retries, Duration delay) {
        return new PersistenceConsumer(kafka, writer, codec, "events", retries, delay);
    }
    private void stop(PersistenceConsumer loop) { loop.requestStop(); }
    static class TrackingConsumer extends MockConsumer<String, String> {
        final Map<TopicPartition, Long> seeks = new HashMap<>();
        final Map<TopicPartition, OffsetAndMetadata> lastCommitted = new HashMap<>();
        int commits; boolean failCommit; Thread closedBy;
        TrackingConsumer() { super(OffsetResetStrategy.EARLIEST); }
        @Override public synchronized void subscribe(Collection<String> topics) { /* fixture assignment survives subscription */ }
        @Override public synchronized void seek(TopicPartition p, long offset) { super.seek(p, offset); seeks.put(p, offset); }
        @Override public synchronized void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets, Duration timeout) {
            if (failCommit) { failCommit = false; throw new CommitFailedException("uncertain commit"); }
            super.commitSync(offsets); commits++; lastCommitted.putAll(offsets);
        }
        @Override public void close(Duration timeout) { closedBy = Thread.currentThread(); super.close(timeout); }
    }
}
