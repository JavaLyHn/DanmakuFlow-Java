package io.github.javalyhn.danmakuflow.gateway;

import io.github.javalyhn.danmakuflow.protocol.DanmakuEvent;
import io.github.javalyhn.danmakuflow.protocol.ProtocolCodec;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import java.math.BigInteger;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.BooleanSupplier;
import static org.junit.jupiter.api.Assertions.*;

class KafkaEventPublisherTest {
    final ProtocolCodec codec = new ProtocolCodec();
    final DanmakuEvent event = new DanmakuEvent(UUID.fromString("10000000-0000-0000-0000-000000000001"),
            BigInteger.ZERO, "room-a", BigInteger.ONE, "hello", Instant.parse("2026-11-06T00:00:00Z"));

    static MockProducer<String, String> producer() {
        return new MockProducer<>(false, null, new StringSerializer(), new StringSerializer());
    }
    KafkaEventPublisher publisher(Producer<String, String> producer, int inflight, int queue) {
        return new KafkaEventPublisher(producer, codec, "danmakuflow.danmaku.v1", inflight, queue, Duration.ofMillis(150));
    }
    static void eventually(BooleanSupplier condition) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
        while (!condition.getAsBoolean() && System.nanoTime() < deadline) Thread.sleep(2);
        assertTrue(condition.getAsBoolean());
    }
    static Throwable failed(CompletableFuture<?> result) throws Exception {
        return assertThrows(ExecutionException.class, () -> result.get(2, TimeUnit.SECONDS)).getCause();
    }

    @Test void completionWaitsForBrokerAckAndRetryKeepsStoredIdentity() throws Exception {
        var producer = producer();
        try (var publisher = publisher(producer, 2, 2)) {
            var first = publisher.publish(event).toCompletableFuture();
            eventually(() -> producer.history().size() == 1);
            assertFalse(first.isDone());
            var record = producer.history().getFirst();
            assertEquals("danmakuflow.danmaku.v1", record.topic());
            assertEquals("room-a", record.key());
            assertEquals(event, codec.decodeStoredEvent(record.value()));
            var rejection = new IllegalStateException("broker rejection");
            producer.errorNext(rejection);
            assertSame(rejection, failed(first));
            var retry = publisher.publish(event).toCompletableFuture();
            eventually(() -> producer.history().size() == 2);
            assertEquals(record.value(), producer.history().get(1).value());
            assertFalse(retry.isDone());
            producer.completeNext();
            retry.get(2, TimeUnit.SECONDS);
        }
        assertTrue(producer.closed());
    }

    @Test void inflightOverloadReleasesOnlyAfterCallbackEvenWhenCallerCancels() throws Exception {
        var producer = producer();
        try (var publisher = publisher(producer, 1, 1)) {
            var first = publisher.publish(event).toCompletableFuture();
            eventually(() -> producer.history().size() == 1);
            assertInstanceOf(RejectedExecutionException.class, failed(publisher.publish(event).toCompletableFuture()));
            first.cancel(false);
            assertInstanceOf(RejectedExecutionException.class, failed(publisher.publish(event).toCompletableFuture()));
            producer.completeNext();
            var next = publisher.publish(event).toCompletableFuture();
            eventually(() -> producer.history().size() == 2);
            producer.completeNext();
            next.get(2, TimeUnit.SECONDS);
        }
    }

    @Test void synchronousSendFailureIsExceptionalAndReleasesAdmission() throws Exception {
        var producer = new MockProducer<String, String>(false, null, new StringSerializer(), new StringSerializer()) {
            boolean fail = true;
            @Override public synchronized Future<RecordMetadata> send(ProducerRecord<String, String> record, Callback callback) {
                if (fail) { fail = false; throw new IllegalStateException("send failed"); }
                return super.send(record, callback);
            }
        };
        try (var publisher = publisher(producer, 1, 1)) {
            assertEquals("send failed", failed(publisher.publish(event).toCompletableFuture()).getMessage());
            var second = publisher.publish(event).toCompletableFuture();
            eventually(() -> producer.history().size() == 1);
            producer.completeNext();
            second.get(2, TimeUnit.SECONDS);
        }
    }

    @Test void blockingSendIsOffCallerAndQueueIsBoundedAndRejectionReleasesCapacity() throws Exception {
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var worker = new ArrayBlockingQueue<Thread>(1);
        var producer = new MockProducer<String, String>(false, null, new StringSerializer(), new StringSerializer()) {
            @Override public Future<RecordMetadata> send(ProducerRecord<String, String> record, Callback callback) {
                if (entered.getCount() != 0) {
                    worker.add(Thread.currentThread()); entered.countDown();
                    try { release.await(3, TimeUnit.SECONDS); }
                    catch (InterruptedException e) { Thread.currentThread().interrupt(); throw new IllegalStateException(e); }
                }
                return super.send(record, callback);
            }
        };
        try (var publisher = publisher(producer, 4, 1)) {
            var first = publisher.publish(event).toCompletableFuture();
            assertTrue(entered.await(2, TimeUnit.SECONDS));
            assertNotSame(Thread.currentThread(), worker.poll());
            var second = publisher.publish(event).toCompletableFuture();
            assertInstanceOf(RejectedExecutionException.class, failed(publisher.publish(event).toCompletableFuture()));
            release.countDown();
            eventually(() -> producer.history().size() == 2);
            producer.completeNext(); producer.completeNext();
            first.get(2, TimeUnit.SECONDS); second.get(2, TimeUnit.SECONDS);
            var third = publisher.publish(event).toCompletableFuture();
            eventually(() -> producer.history().size() == 3);
            producer.completeNext(); third.get(2, TimeUnit.SECONDS);
        } finally { release.countDown(); }
    }

    @Test void closeDrainsSuccessfulAckAndRejectsNewWork() throws Exception {
        var producer = producer();
        var publisher = publisher(producer, 1, 1);
        var result = publisher.publish(event).toCompletableFuture();
        eventually(() -> producer.history().size() == 1);
        var closing = CompletableFuture.runAsync(publisher::close);
        eventually(() -> publisher.publish(event).toCompletableFuture().isCompletedExceptionally());
        producer.completeNext();
        result.get(2, TimeUnit.SECONDS); closing.get(2, TimeUnit.SECONDS);
        assertTrue(producer.closed());
        assertInstanceOf(RejectedExecutionException.class, failed(publisher.publish(event).toCompletableFuture()));
        publisher.close();
    }

    @Test void closeTimesOutUnackedAndQueuedRecordsAndInterruptsBlockedSend() throws Exception {
        var entered = new CountDownLatch(1);
        var interrupted = new CountDownLatch(1);
        var producer = new MockProducer<String, String>(false, null, new StringSerializer(), new StringSerializer()) {
            @Override public Future<RecordMetadata> send(ProducerRecord<String, String> record, Callback callback) {
                entered.countDown();
                try { new CountDownLatch(1).await(); }
                catch (InterruptedException e) { interrupted.countDown(); Thread.currentThread().interrupt(); }
                throw new IllegalStateException("interrupted send");
            }
        };
        var publisher = publisher(producer, 2, 1);
        var first = publisher.publish(event).toCompletableFuture();
        assertTrue(entered.await(2, TimeUnit.SECONDS));
        var queued = publisher.publish(event).toCompletableFuture();
        assertTimeoutPreemptively(Duration.ofSeconds(2), publisher::close);
        failed(first); failed(queued);
        assertTrue(interrupted.await(2, TimeUnit.SECONDS));
        assertTrue(producer.closed());
    }

    @Test void closeFailsRecordsStillAwaitingAck() throws Exception {
        var producer = producer();
        var publisher = publisher(producer, 1, 1);
        var result = publisher.publish(event).toCompletableFuture();
        eventually(() -> producer.history().size() == 1);
        assertTimeoutPreemptively(Duration.ofSeconds(2), publisher::close);
        failed(result);
        assertTrue(producer.closed());
    }

    @Test void productionPropertiesEnforceDurabilityAndFiniteResourceLimits() {
        var properties = KafkaEventPublisher.productionProperties("broker:9092");
        assertEquals("broker:9092", properties.getProperty("bootstrap.servers"));
        assertEquals("all", properties.getProperty("acks"));
        assertEquals("true", properties.getProperty("enable.idempotence"));
        assertEquals("8388608", properties.getProperty("buffer.memory"));
        assertEquals("1000", properties.getProperty("max.block.ms"));
        assertEquals("5000", properties.getProperty("request.timeout.ms"));
        assertEquals("15000", properties.getProperty("delivery.timeout.ms"));
        assertEquals("2", properties.getProperty("linger.ms"));
        assertEquals("16384", properties.getProperty("batch.size"));
        assertTrue(Integer.parseInt(properties.getProperty("max.in.flight.requests.per.connection")) <= 5);
        assertThrows(IllegalArgumentException.class, () -> KafkaEventPublisher.productionProperties(" "));
    }
}
