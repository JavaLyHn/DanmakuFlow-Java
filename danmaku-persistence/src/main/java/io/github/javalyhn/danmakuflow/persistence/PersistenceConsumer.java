package io.github.javalyhn.danmakuflow.persistence;

import io.github.javalyhn.danmakuflow.protocol.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/** Run once on its owning thread. Only requestStop may be called from another thread. */
public final class PersistenceConsumer implements Runnable, AutoCloseable {
    private final org.apache.kafka.clients.consumer.Consumer<String, String> kafka;
    private final Consumer<List<DanmakuEvent>> writer;
    private final ProtocolCodec codec;
    private final String topic;
    private final int maxRetries;
    private final Duration retryDelay;
    private final AtomicBoolean started = new AtomicBoolean();
    private final Object stopMonitor = new Object();
    private volatile boolean stopping;
    private volatile boolean closed;

    public PersistenceConsumer(org.apache.kafka.clients.consumer.Consumer<String, String> kafka,
            Consumer<List<DanmakuEvent>> writer, ProtocolCodec codec, String topic,
            int maxRetries, Duration retryDelay) {
        this.kafka = Objects.requireNonNull(kafka); this.writer = Objects.requireNonNull(writer);
        this.codec = Objects.requireNonNull(codec); this.topic = Objects.requireNonNull(topic);
        if (maxRetries < 0 || maxRetries > 20 || retryDelay.isNegative() || retryDelay.compareTo(Duration.ofSeconds(30)) > 0)
            throw new IllegalArgumentException("Invalid retry budget/delay");
        this.maxRetries = maxRetries; this.retryDelay = retryDelay;
    }

    public void requestStop() {
        synchronized (stopMonitor) { stopping = true; stopMonitor.notifyAll(); }
        kafka.wakeup();
    }

    /** Spring closes an unstarted client during failed context startup; a running owner closes itself. */
    @Override public void close() {
        if (started.compareAndSet(false, true)) {
            try { kafka.close(Duration.ofSeconds(5)); }
            finally { closed = true; }
        } else if (!closed) requestStop();
    }

    @Override public void run() {
        if (!started.compareAndSet(false, true)) throw new IllegalStateException("Consumer can run only once");
        try {
            kafka.subscribe(List.of(topic));
            int failures = 0;
            while (!stopping && !Thread.currentThread().isInterrupted()) {
                var records = kafka.poll(Duration.ofMillis(500));
                if (stopping || records.isEmpty()) continue;
                var first = new HashMap<TopicPartition, Long>();
                var next = new HashMap<TopicPartition, OffsetAndMetadata>();
                for (var partition : records.partitions()) {
                    var batch = records.records(partition);
                    first.put(partition, batch.getFirst().offset());
                    next.put(partition, new OffsetAndMetadata(batch.getLast().offset() + 1));
                }
                try {
                    var events = new ArrayList<DanmakuEvent>(records.count());
                    for (var record : records) events.add(codec.decodeStoredEvent(record.value()));
                    writer.accept(List.copyOf(events)); // repository returns only after transaction commit
                    if (stopping) break; // no offset commit during shutdown
                    kafka.commitSync(next, Duration.ofSeconds(5));
                    failures = 0;
                } catch (WakeupException e) {
                    throw e;
                } catch (RuntimeException failure) {
                    // Rewind every partition of this poll, including records decoded successfully.
                    for (var entry : first.entrySet()) {
                        if (kafka.assignment().contains(entry.getKey())) kafka.seek(entry.getKey(), entry.getValue());
                    }
                    if (++failures > maxRetries)
                        throw new IllegalStateException("Persistence retry budget exhausted; offsets remain uncommitted", failure);
                    System.err.println("Persistence batch failed; retry " + failures + "/" + maxRetries
                            + " (" + failure.getClass().getSimpleName() + ")");
                    awaitRetry(failures);
                }
            }
        } catch (WakeupException e) {
            if (!stopping) throw e;
        } finally {
            try { kafka.close(Duration.ofSeconds(5)); }
            finally { closed = true; }
        }
    }

    private void awaitRetry(int failure) {
        long millis = Math.min(30_000, retryDelay.toMillis() * (1L << (failure - 1)));
        long deadline = System.nanoTime() + Duration.ofMillis(millis).toNanos();
        synchronized (stopMonitor) {
            while (!stopping && millis > 0) {
                try { stopMonitor.wait(millis); }
                catch (InterruptedException e) { Thread.currentThread().interrupt(); stopping = true; return; }
                millis = Math.max(0, Duration.ofNanos(deadline - System.nanoTime()).toMillis());
            }
        }
    }
}
