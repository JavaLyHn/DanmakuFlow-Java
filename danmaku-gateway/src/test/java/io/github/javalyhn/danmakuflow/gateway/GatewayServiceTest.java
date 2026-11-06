package io.github.javalyhn.danmakuflow.gateway;

import io.github.javalyhn.danmakuflow.protocol.*;
import org.junit.jupiter.api.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.*;
import static org.junit.jupiter.api.Assertions.*;

class GatewayServiceTest {
    final Bus bus = new Bus();
    final Stats stats = new Stats();
    final BlockingQueue<CompletableFuture<Void>> acks = new LinkedBlockingQueue<>();
    GatewayService gateway;
    @BeforeEach void setup() {
        gateway = new GatewayService(e -> { var f = new CompletableFuture<Void>(); acks.add(f); return f; },
                bus, stats, new ProtocolCodec(), Clock.systemUTC(), UUID::randomUUID,
                new GatewayService.Limits(10, 20, 2));
    }
    @AfterEach void cleanup() throws Exception { if (gateway != null) gateway.closeAsync().toCompletableFuture().get(4, TimeUnit.SECONDS); }
    static void done(CompletionStage<?> stage) throws Exception { stage.toCompletableFuture().get(2, TimeUnit.SECONDS); }
    static void eventually(BooleanSupplier check) throws Exception {
        long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
        while (!check.getAsBoolean() && System.nanoTime() < until) Thread.sleep(5);
        assertTrue(check.getAsBoolean());
    }
    Client join(String room) throws Exception { var c = new Client(room); done(gateway.join(c)); return c; }
    @Test void ackGatesExactlyOnceFanoutAndIsolatesRooms() throws Exception {
        var a = join("r"); var b = join("r"); var other = join("s");
        var sent = gateway.handle(a, new ClientCommand.Chat("hello"));
        var ack = acks.poll(2, TimeUnit.SECONDS); assertNotNull(ack);
        assertTrue(a.frames.isEmpty()); assertTrue(bus.published.isEmpty());
        ack.complete(null); done(sent);
        eventually(() -> a.frames.size() == 1 && b.frames.size() == 1);
        assertSame(a.frames.getFirst(), b.frames.getFirst()); assertTrue(other.frames.isEmpty());
    }
    @Test void publishFailureClosesSenderAndReleasesPermit() throws Exception {
        var a = join("r"); var sent = gateway.handle(a, new ClientCommand.Chat("hello"));
        acks.poll(2, TimeUnit.SECONDS).completeExceptionally(new IllegalStateException());
        assertThrows(ExecutionException.class, () -> done(sent));
        eventually(() -> a.code == 1011); assertTrue(bus.published.isEmpty());
        assertEquals(1, gateway.failures());
    }
    @Test void joinWaitsForSubscriptionAndLeaveDuringSubscribeCleansUp() throws Exception {
        bus.subscribeGate = new CompletableFuture<>(); var a = new Client("r");
        var joined = gateway.join(a); eventually(() -> bus.listeners.containsKey("r"));
        assertFalse(joined.toCompletableFuture().isDone()); gateway.leave(a); gateway.leave(a);
        bus.subscribeGate.complete(null);
        eventually(() -> bus.unsubscribed.contains("r") && stats.removed.contains("r"));
        assertTrue(joined.toCompletableFuture().isCompletedExceptionally());
    }
    @Test void newJoinWaitsForOldUnsubscribeAndOldListenerCannotBroadcast() throws Exception {
        var a = join("r"); var old = bus.listeners.get("r");
        bus.unsubscribeGate = new CompletableFuture<>(); gateway.leave(a);
        eventually(() -> bus.unsubscribed.contains("r")); var b = new Client("r"); var joined = gateway.join(b);
        assertFalse(joined.toCompletableFuture().isDone());
        bus.unsubscribeGate.complete(null); done(joined); old.accept("stale");
        Thread.sleep(30); assertTrue(b.frames.isEmpty());
    }
    @Test void failedSubscribeRollsBackEveryWaitingClient() throws Exception {
        bus.subscribeGate = new CompletableFuture<>(); var a = new Client("r"); var b = new Client("r");
        var x = gateway.join(a); var y = gateway.join(b);
        eventually(() -> bus.listeners.containsKey("r")); bus.subscribeGate.completeExceptionally(new RuntimeException());
        assertThrows(ExecutionException.class, () -> done(x)); assertThrows(ExecutionException.class, () -> done(y));
        eventually(() -> a.code == 1011 && b.code == 1011);
    }
    @Test void statsCountConnectionsSkipOverlapAndRecoverAfterFailure() throws Exception {
        var a = join("r"); var b = join("r"); stats.gate = new CompletableFuture<>();
        gateway.tick(); eventually(() -> stats.counts.size() == 1); gateway.tick();
        assertEquals(List.of(2), stats.counts); gateway.leave(b);
        stats.gate.completeExceptionally(new RuntimeException()); eventually(() -> gateway.failures() == 1);
        stats.gate = CompletableFuture.completedFuture(new RoomStats(1, 4)); gateway.tick();
        eventually(() -> a.frames.size() == 1); assertTrue(b.frames.isEmpty());
        assertEquals(List.of(2, 1), stats.counts); assertTrue(bus.published.isEmpty());
        assertEquals(Duration.ofSeconds(7), stats.ttl);
    }
    @Test void overloadReleasesPermitsAndShutdownRejectsWork() throws Exception {
        var a = join("r"); var b = join("r"); var c = join("r");
        var x = gateway.handle(a, new ClientCommand.Chat("a")); var y = gateway.handle(b, new ClientCommand.Chat("b"));
        eventually(() -> acks.size() == 2);
        assertThrows(ExecutionException.class, () -> done(gateway.handle(c, new ClientCommand.Like(1))));
        acks.remove().complete(null); acks.remove().complete(null); done(x); done(y);
        done(gateway.handle(a, new ClientCommand.Like(2))); assertEquals(2, stats.likes);
        done(gateway.closeAsync()); assertTrue(stats.removed.contains("r"));
        assertThrows(ExecutionException.class, () -> done(gateway.join(new Client("s"))));
    }
    @Test void rejectedSendRemovesSlowClient() throws Exception {
        var a = join("r"); a.accept = false; bus.listeners.get("r").accept("payload");
        eventually(() -> a.code == 1013 && stats.removed.contains("r"));
    }
    @Test void synchronousLikeFailureReleasesPermitAndHasNoEcho() throws Exception {
        var a = join("r"); stats.throwLikes = true;
        assertThrows(ExecutionException.class, () -> done(gateway.handle(a, new ClientCommand.Like(1))));
        assertEquals(1011, a.code); assertTrue(a.frames.isEmpty());
        stats.throwLikes = false; var b = join("s");
        done(gateway.handle(b, new ClientCommand.Like(3))); assertEquals(3, stats.likes);
    }
    @Test void busPublishFailureAfterAckIsVisible() throws Exception {
        var a = join("r"); bus.failPublish = true;
        var sent = gateway.handle(a, new ClientCommand.Chat("hello"));
        acks.poll(2, TimeUnit.SECONDS).complete(null);
        assertThrows(ExecutionException.class, () -> done(sent));
        assertEquals(1011, a.code); assertTrue(a.frames.isEmpty()); assertEquals(1, gateway.failures());
    }
    @Test void cleanupWaitsForRefreshBeforeRemovingReportAndStartingNextGeneration() throws Exception {
        var a = join("r"); stats.gate = new CompletableFuture<>(); gateway.tick();
        eventually(() -> stats.counts.size() == 1); gateway.leave(a);
        eventually(() -> bus.unsubscribed.contains("r"));
        var b = new Client("r"); var joining = gateway.join(b);
        assertFalse(joining.toCompletableFuture().isDone()); assertFalse(stats.removed.contains("r"));
        stats.gate.complete(new RoomStats(1, 1)); done(joining);
        assertTrue(stats.removed.contains("r")); assertTrue(b.frames.isEmpty());
    }
    @Test void shutdownDoesNotWaitForUnacknowledgedChatAndSuppressesLatePublish() throws Exception {
        var a = join("r"); var sent = gateway.handle(a, new ClientCommand.Chat("hello"));
        var ack = acks.poll(2, TimeUnit.SECONDS); assertNotNull(ack);
        done(gateway.closeAsync()); ack.complete(null);
        assertTrue(sent.toCompletableFuture().isCompletedExceptionally()); assertTrue(bus.published.isEmpty());
        assertEquals(1001, a.code);
    }
    @Test void connectionAndRoomLimitsRejectExplicitlyAndRecover() throws Exception {
        done(gateway.closeAsync());
        gateway = new GatewayService(e -> CompletableFuture.completedFuture(null), bus, stats,
                new ProtocolCodec(), Clock.systemUTC(), UUID::randomUUID, new GatewayService.Limits(1, 2, 2));
        var a = join("r"); var b = join("r"); var c = new Client("r");
        assertThrows(ExecutionException.class, () -> done(gateway.join(c))); assertEquals(1013, c.code);
        gateway.leave(a); var other = new Client("s");
        assertThrows(ExecutionException.class, () -> done(gateway.join(other))); assertEquals(1013, other.code);
        gateway.leave(b); eventually(() -> stats.removed.contains("r")); join("s");
    }
    @Test void shutdownBoundsBlockedAdapterAndReportsTimeout() throws Exception {
        done(gateway.closeAsync()); var entered = new CountDownLatch(1); var release = new CountDownLatch(1);
        gateway = new GatewayService(e -> {
            entered.countDown();
            try { release.await(); } catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); }
            return CompletableFuture.completedFuture(null);
        }, bus, stats, new ProtocolCodec(), Clock.systemUTC(), UUID::randomUUID, GatewayService.Limits.defaults());
        try {
            var a = join("r"); gateway.handle(a, new ClientCommand.Chat("hello"));
            assertTrue(entered.await(2, TimeUnit.SECONDS));
            long before = System.nanoTime(); var shutdown = gateway.closeAsync();
            assertTrue(System.nanoTime() - before < TimeUnit.MILLISECONDS.toNanos(200));
            var failure = assertThrows(ExecutionException.class,
                    () -> shutdown.toCompletableFuture().get(3, TimeUnit.SECONDS));
            assertInstanceOf(TimeoutException.class, failure.getCause());
        } finally { release.countDown(); gateway.close(); gateway = null; }
    }
    @Test void fanoutDoesNotBlockUnrelatedAdmissionOrLeave() throws Exception {
        var entered = new CountDownLatch(1); var release = new CountDownLatch(1);
        var slow = new Client("r") {
            public boolean send(byte[] frame) { entered.countDown(); awaitLatch(release); return true; }
        };
        done(gateway.join(slow)); var other = join("s");
        var caller = Executors.newSingleThreadExecutor();
        try {
            bus.listeners.get("r").accept("payload"); assertTrue(entered.await(1, TimeUnit.SECONDS));
            caller.submit(() -> { gateway.leave(other); return gateway.join(new Client("s")); }).get(300, TimeUnit.MILLISECONDS);
        } finally { release.countDown(); caller.shutdownNow(); }
    }
    @Test void shutdownInitiationAndDeadlineDoNotWaitForClientCleanup() throws Exception {
        var entered = new CountDownLatch(1); var release = new CountDownLatch(1);
        var slow = new Client("r") {
            public void close(int code, String reason) { entered.countDown(); awaitLatch(release); }
        };
        done(gateway.join(slow)); var caller = Executors.newSingleThreadExecutor();
        try {
            var stage = caller.submit(() -> gateway.closeAsync()).get(300, TimeUnit.MILLISECONDS);
            assertTrue(entered.await(1, TimeUnit.SECONDS));
            var error = assertThrows(ExecutionException.class, () -> stage.toCompletableFuture().get(3, TimeUnit.SECONDS));
            assertInstanceOf(TimeoutException.class, error.getCause());
        } finally { release.countDown(); caller.shutdownNow(); gateway = null; }
    }
    @Test void unsubscribeFailureMakesShutdownFail() throws Exception {
        join("r"); bus.unsubscribeGate = CompletableFuture.failedFuture(new IllegalStateException("unsubscribe"));
        try { assertThrows(ExecutionException.class, () -> done(gateway.closeAsync())); } finally { gateway = null; }
    }
    @Test void priorReportCleanupFailureMakesShutdownFail() throws Exception {
        var a = join("r"); stats.failRemove = true; gateway.leave(a);
        eventually(() -> gateway.failures() == 1);
        try { assertThrows(ExecutionException.class, () -> done(gateway.closeAsync())); } finally { gateway = null; }
    }
    @Test void saturatedWorkersCannotDiscardLocalShutdownCleanup() throws Exception {
        done(gateway.closeAsync()); var entered = new CountDownLatch(2); var release = new CountDownLatch(1);
        gateway = new GatewayService(e -> {
            entered.countDown(); awaitLatch(release); return CompletableFuture.completedFuture(null);
        }, bus, stats, new ProtocolCodec(), Clock.systemUTC(), UUID::randomUUID, GatewayService.Limits.defaults());
        try {
            var a = join("r"); var b = join("r");
            gateway.handle(a, new ClientCommand.Chat("a")); gateway.handle(b, new ClientCommand.Chat("b"));
            assertTrue(entered.await(1, TimeUnit.SECONDS));
            var pending = new Client("pending"); var joining = gateway.join(pending);
            var shutdown = gateway.closeAsync();
            assertThrows(ExecutionException.class, () -> shutdown.toCompletableFuture().get(3, TimeUnit.SECONDS));
            assertEquals(1001, a.code); assertEquals(1001, b.code); assertEquals(1001, pending.code);
            assertTrue(joining.toCompletableFuture().isCompletedExceptionally());
        } finally { release.countDown(); gateway = null; }
    }
    @Test void deferredReadinessCannotReadyReplacementDuringUnsubscribe() throws Exception {
        bus.subscribeGate = new CompletableFuture<>(); bus.unsubscribeGate = new CompletableFuture<>();
        var a = new Client("r"); gateway.join(a); eventually(() -> bus.listeners.containsKey("r"));
        var entered = new CountDownLatch(1); var release = new CountDownLatch(1);
        // Gate exactly before the old notification enumerates membership. No production test hooks.
        var roomsField = GatewayService.class.getDeclaredField("rooms"); roomsField.setAccessible(true);
        var room = ((Map<?, ?>) roomsField.get(gateway)).get("r");
        var clientsField = room.getClass().getDeclaredField("clients"); clientsField.setAccessible(true);
        @SuppressWarnings("unchecked") Set<Object> original = (Set<Object>) clientsField.get(room);
        clientsField.set(room, new AbstractSet<Object>() {
            final java.util.concurrent.atomic.AtomicBoolean first = new java.util.concurrent.atomic.AtomicBoolean(true);
            public Iterator<Object> iterator() {
                if (first.compareAndSet(true, false)) { entered.countDown(); awaitLatch(release); }
                return original.iterator();
            }
            public int size() { return original.size(); }
            public boolean add(Object value) { return original.add(value); }
            public boolean remove(Object value) { return original.remove(value); }
        });
        var completion = Executors.newSingleThreadExecutor();
        try {
            var notification = completion.submit(() -> bus.subscribeGate.complete(null));
            assertTrue(entered.await(1, TimeUnit.SECONDS)); gateway.leave(a);
            eventually(() -> bus.unsubscribed.contains("r"));
            var b = new Client("r"); var joining = gateway.join(b);
            release.countDown(); notification.get(1, TimeUnit.SECONDS);
            assertFalse(joining.toCompletableFuture().isDone());
            bus.subscribeGate = new CompletableFuture<>(); bus.unsubscribeGate.complete(null);
            eventually(() -> stats.removed.contains("r"));
            assertFalse(joining.toCompletableFuture().isDone()); bus.subscribeGate.complete(null); done(joining);
        } finally {
            release.countDown(); bus.unsubscribeGate.complete(null); bus.subscribeGate.complete(null);
            completion.shutdownNow();
        }
    }
    static void awaitLatch(CountDownLatch latch) {
        boolean interrupted = false;
        while (true) { try { latch.await(); break; } catch (InterruptedException e) { interrupted = true; } }
        if (interrupted) Thread.currentThread().interrupt();
    }
    static class Client implements GatewayClient {
        final ConnectionIdentity identity; final List<byte[]> frames = new CopyOnWriteArrayList<>();
        volatile int code; volatile boolean accept = true;
        Client(String room) { identity = ConnectionIdentity.parse("1", "n", room); }
        public ConnectionIdentity identity() { return identity; }
        public boolean send(byte[] frame) { if (!accept) return false; frames.add(frame); return true; }
        public void close(int code, String reason) { this.code = code; }
    }
    static class Bus implements RoomBus {
        final Map<String, Consumer<String>> listeners = new ConcurrentHashMap<>();
        final List<String> published = new CopyOnWriteArrayList<>(), unsubscribed = new CopyOnWriteArrayList<>();
        volatile CompletableFuture<Void> subscribeGate = CompletableFuture.completedFuture(null);
        volatile CompletableFuture<Void> unsubscribeGate = CompletableFuture.completedFuture(null);
        volatile boolean failPublish;
        public CompletionStage<Void> subscribe(String r, Consumer<String> l) { listeners.put(r,l); return subscribeGate; }
        public CompletionStage<Void> unsubscribe(String r) { unsubscribed.add(r); return unsubscribeGate; }
        public CompletionStage<Void> publish(String r, String p) {
            if (failPublish) return CompletableFuture.failedFuture(new IllegalStateException("publish failed"));
            published.add(p); listeners.get(r).accept(p); return CompletableFuture.completedFuture(null);
        }
    }
    static class Stats implements RoomStatisticsStore {
        final List<Integer> counts = new CopyOnWriteArrayList<>(); final Set<String> removed = ConcurrentHashMap.newKeySet();
        volatile long likes; volatile Duration ttl;
        volatile boolean throwLikes;
        volatile boolean failRemove;
        volatile CompletableFuture<RoomStats> gate = CompletableFuture.completedFuture(new RoomStats(0,0));
        public CompletionStage<Void> addLikes(String r, long count) {
            if (throwLikes) throw new IllegalStateException("like failed");
            likes += count; return CompletableFuture.completedFuture(null);
        }
        public CompletionStage<RoomStats> refresh(String r, String i, int online, Duration ttl) { this.ttl=ttl; counts.add(online); return gate; }
        public CompletionStage<Void> remove(String r, String i) { removed.add(r); return failRemove
                ? CompletableFuture.failedFuture(new IllegalStateException("remove")) : CompletableFuture.completedFuture(null); }
    }
}
