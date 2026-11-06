package io.github.javalyhn.danmakuflow.gateway;

import io.github.javalyhn.danmakuflow.protocol.ProtocolCodec;
import org.junit.jupiter.api.Test;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import static org.junit.jupiter.api.Assertions.*;

class GatewayApplicationTest {
    @Test void defaultsAndOverridesReachActualBoundaries() {
        var settings = GatewaySettings.from(Map.of());
        assertEquals("127.0.0.1", settings.server().host());
        assertEquals(8080, settings.server().port());
        assertEquals(100000, settings.server().maxConnections());
        assertEquals(64, settings.server().maxPendingFrames());
        assertEquals(524288, settings.server().maxPendingBytes());
        assertEquals(10000, settings.limits().maxRooms());
        assertEquals(4096, settings.limits().maxPending());
        assertEquals("localhost:9092", settings.kafkaBootstrap());
        assertEquals("danmakuflow.danmaku.v1", settings.kafkaTopic());
        assertEquals(256, settings.kafkaMaxInFlight()); assertEquals(128, settings.kafkaQueueCapacity());
        assertEquals(Duration.ofSeconds(5), settings.kafkaCloseTimeout());
        assertEquals("redis://127.0.0.1:6379/0", settings.redisUri());
        var changed = GatewaySettings.from(Map.of("DANMAKU_GATEWAY_PORT", "0", "DANMAKU_GATEWAY_MAX_CONNECTIONS", "12",
                "DANMAKU_KAFKA_TOPIC", "custom", "DANMAKU_KAFKA_CLOSE_TIMEOUT_MS", "41"));
        assertEquals(0, changed.server().port()); assertEquals(12, changed.server().maxConnections());
        assertEquals(12, changed.limits().maxConnections()); assertEquals("custom", changed.kafkaTopic());
        assertEquals(Duration.ofMillis(41), changed.kafkaCloseTimeout());
    }
    @Test void invalidConfigurationAndDiagnosticsDoNotEchoCredentials() {
        for (var entry : Map.of("DANMAKU_GATEWAY_PORT", "65536", "DANMAKU_GATEWAY_MAX_ROOMS", "0",
                "DANMAKU_GATEWAY_MAX_PENDING", "2147483647", "DANMAKU_GATEWAY_CLIENT_MAX_PENDING_BYTES", "-1",
                "DANMAKU_KAFKA_MAX_IN_FLIGHT", "zero", "DANMAKU_KAFKA_QUEUE_CAPACITY", "0",
                "DANMAKU_KAFKA_CLOSE_TIMEOUT_MS", "0", "DANMAKU_REDIS_URI", "password-secret").entrySet()) {
            var error = assertThrows(IllegalArgumentException.class, () -> GatewaySettings.from(Map.of(entry.getKey(), entry.getValue())));
            assertFalse(error.toString().contains("password-secret"));
        }
        assertFalse(GatewaySettings.from(Map.of("DANMAKU_REDIS_URI", "redis://user:password-secret@localhost:6379/0"))
                .toString().contains("password-secret"));
    }
    @Test void malformedRedisDatabaseFailsBeforeAnyResourceAcquisition() {
        assertAll(List.of("redis://localhost:6379/not-a-number", "redis://localhost:6379/-1",
                "redis://localhost:6379/2147483648", "rediss://user:password-secret@localhost:6379/not-a-number")
                .stream().map(uri -> (org.junit.jupiter.api.function.Executable) () -> {
                    var fixture = new Fixture();
                    var error = assertThrows(IllegalArgumentException.class, () -> {
                        var settings = GatewaySettings.from(Map.of("DANMAKU_REDIS_URI", uri));
                        try (var runtime = new GatewayRuntime(settings, fixture, Duration.ofMillis(80))) {
                            runtime.start();
                        }
                    });
                    assertTrue(fixture.events.isEmpty(), "Invalid configuration must not reach a resource factory");
                    assertFalse(error.toString().contains("password-secret"));
                    assertNull(error.getCause());
                    assertEquals(0, error.getSuppressed().length);
                }));
    }
    @Test void validStandaloneRedisDatabaseAuthenticationAndTlsSettingsRemainAccepted() {
        for (String uri : List.of("redis://127.0.0.1:6379/0", "redis://user:password-secret@localhost:6379/7",
                "rediss://user:password-secret@localhost:6379/7", "redis://localhost:6379/2147483647")) {
            var settings = assertDoesNotThrow(() -> GatewaySettings.from(Map.of("DANMAKU_REDIS_URI", uri)));
            assertEquals(uri, settings.redisUri());
            assertFalse(settings.toString().contains("password-secret"));
        }
    }
    @Test void closesInDependencyOrderAndWaitsForCore() throws Exception {
        var fixture = new Fixture(); fixture.coreClosed = new CompletableFuture<>();
        var runtime = fixture.runtime(); runtime.start();
        var executor = Executors.newSingleThreadExecutor();
        try {
            var closing = executor.submit(runtime::close);
            assertTrue(fixture.coreCloseCalled.await(1, TimeUnit.SECONDS));
            assertFalse(fixture.events.contains("redis-close"));
            fixture.coreClosed.complete(null); closing.get(1, TimeUnit.SECONDS);
            assertEquals(List.of("kafka", "redis", "core", "listener", "start", "listener-close", "core-close", "redis-close", "kafka-close"), fixture.events);
            runtime.close(); assertEquals(9, fixture.events.size());
        } finally { fixture.coreClosed.complete(null); executor.shutdownNow(); runtime.close(); }
    }
    @Test void everyAcquisitionFailureClosesOnlyAcquiredResourcesAndHidesCauses() {
        for (int fail = 1; fail <= 5; fail++) {
            var fixture = new Fixture(); fixture.failAt = fail;
            var error = assertThrows(IllegalStateException.class, fixture.runtime()::start);
            assertNull(error.getCause()); assertFalse(error.toString().contains("password-secret"));
            var acquired = Math.min(fail - 1, 4);
            var expected = new ArrayList<String>();
            if (acquired >= 4) expected.add("listener-close");
            if (acquired >= 3) expected.add("core-close");
            if (acquired >= 2) expected.add("redis-close");
            if (acquired >= 1) expected.add("kafka-close");
            assertEquals(expected, fixture.events.stream().filter(s -> s.endsWith("-close")).toList());
        }
    }
    @Test void closeFailureStillAttemptsAllResources() {
        var fixture = new Fixture(); fixture.failCleanup = true;
        var runtime = fixture.runtime(); runtime.start();
        assertThrows(IllegalStateException.class, runtime::close);
        assertEquals(List.of("listener-close", "core-close", "redis-close", "kafka-close"),
                fixture.events.stream().filter(s -> s.endsWith("-close")).toList());
        runtime.close();
    }
    @Test void coreTimeoutIsFiniteAndDoesNotSkipAdapters() {
        var fixture = new Fixture(); fixture.coreClosed = new CompletableFuture<>();
        var runtime = fixture.runtime(); runtime.start();
        assertTimeoutPreemptively(Duration.ofSeconds(1), () -> assertThrows(IllegalStateException.class, runtime::close));
        assertTrue(fixture.events.contains("redis-close")); assertTrue(fixture.events.contains("kafka-close"));
    }
    @Test void springOwnsRuntimeAndClosesItOnce() {
        var fixture = new Fixture();
        try (var context = GatewayApplication.application(GatewaySettings.from(Map.of()), fixture).run()) {
            assertNotNull(context.getBean(GatewayRuntime.class));
            assertEquals("start", fixture.events.getLast());
        }
        assertEquals(1, Collections.frequency(fixture.events, "kafka-close"));
    }
    @Test void springFailedStartupCleansPartialRuntime() {
        var fixture = new Fixture(); fixture.failAt = 5;
        assertThrows(RuntimeException.class, () -> GatewayApplication.application(GatewaySettings.from(Map.of()), fixture).run());
        assertEquals(1, Collections.frequency(fixture.events, "listener-close"));
        assertEquals(1, Collections.frequency(fixture.events, "kafka-close"));
    }
    @Test void helpDoesNotStartSpringOrReadInvalidEnvironment() {
        assertDoesNotThrow(() -> GatewayApplication.main(new String[]{"--help"}));
    }
    private static final class Fixture implements GatewayRuntime.Factory {
        final List<String> events = new CopyOnWriteArrayList<>();
        final CountDownLatch coreCloseCalled = new CountDownLatch(1);
        CompletableFuture<Void> coreClosed = CompletableFuture.completedFuture(null);
        int failAt; int stage; boolean failCleanup;
        GatewayRuntime runtime() { return new GatewayRuntime(GatewaySettings.from(Map.of()), this, Duration.ofMillis(80)); }
        void acquire(String name) { events.add(name); if (++stage == failAt) throw new IllegalStateException("redis://password-secret"); }
        void cleanup(String name) { events.add(name + "-close"); if (failCleanup) throw new IllegalStateException("password-secret"); }
        public GatewayRuntime.Owned<EventPublisher> publisher(GatewaySettings s, ProtocolCodec codec) {
            acquire("kafka"); return new GatewayRuntime.Owned<>(event -> CompletableFuture.completedFuture(null), () -> cleanup("kafka"));
        }
        public GatewayRuntime.RedisResource redis(GatewaySettings s) {
            acquire("redis"); return new GatewayRuntime.RedisResource(null, null, () -> cleanup("redis"));
        }
        public GatewayRuntime.CoreResource core(GatewaySettings s, ProtocolCodec codec, EventPublisher publisher, GatewayRuntime.RedisResource redis) {
            acquire("core"); return new GatewayRuntime.CoreResource(null, () -> { cleanup("core"); coreCloseCalled.countDown(); return coreClosed; });
        }
        public GatewayRuntime.Listener listener(GatewaySettings s, ProtocolCodec codec, GatewayRuntime.CoreResource core) {
            acquire("listener"); return new GatewayRuntime.Listener() {
                public void start() { acquire("start"); }
                public void close() { cleanup("listener"); }
            };
        }
    }
}
