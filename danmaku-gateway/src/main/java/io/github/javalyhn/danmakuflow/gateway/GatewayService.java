package io.github.javalyhn.danmakuflow.gateway;

import io.github.javalyhn.danmakuflow.protocol.*;
import java.nio.charset.StandardCharsets;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.*;

/** Thread-safe room core. Adapter invocations run on owned workers; no caller waits for I/O.
 * Completion callbacks and client methods must remain nonblocking. Different messages may reorder.
 */
public final class GatewayService implements AutoCloseable {
    public record Limits(int maxRooms, int maxConnections, int maxPending) {
        public Limits {
            if (maxRooms < 1 || maxConnections < 1 || maxPending < 1
                    || (long) maxRooms * 4 + (long) maxPending * 2 + 4 > Integer.MAX_VALUE)
                throw new IllegalArgumentException("Invalid gateway limits");
        }
        public static Limits defaults() { return new Limits(10000, 100000, 4096); }
    }
    private enum Phase { SUBSCRIBING, READY, UNSUBSCRIBING, REMOVING, FAILED }
    private static final Duration TTL = Duration.ofSeconds(7);
    private final EventPublisher publisher;
    private final RoomBus bus;
    private final RoomStatisticsStore stats;
    private final ProtocolCodec codec;
    private final Clock clock;
    private final Supplier<UUID> ids;
    private final Limits limits;
    private final String instanceId = UUID.randomUUID().toString();
    private final ThreadPoolExecutor workers;
    private final ThreadPoolExecutor lifecycle;
    private final ScheduledExecutorService scheduler;
    private final ScheduledFuture<?> ticker;
    private final Semaphore permits;
    private final Semaphore broadcastPermits;
    private final Map<String, Room> rooms = new ConcurrentHashMap<>();
    private final IdentityHashMap<GatewayClient, Member> members = new IdentityHashMap<>();
    private final Set<CompletableFuture<Void>> operations = ConcurrentHashMap.newKeySet();
    private final CompletableFuture<Void> closed = new CompletableFuture<>();
    private volatile boolean closing;
    private volatile boolean localCleanupDone;
    private Throwable cleanupFailure;
    private final ThreadLocal<List<Runnable>> effects = new ThreadLocal<>();
    private boolean stoppingWorkers;
    private long failureCount;
    private static final class Member {
        final GatewayClient client;
        final Room room;
        final long targetGeneration;
        final CompletableFuture<Void> ready = new CompletableFuture<>();
        Member(GatewayClient client, Room room, long targetGeneration) {
            this.client = client; this.room = room; this.targetGeneration = targetGeneration;
        }
    }
    private static final class Room {
        final String id;
        final Set<Member> clients = ConcurrentHashMap.newKeySet();
        Phase phase = Phase.SUBSCRIBING;
        long generation;
        boolean refreshing;
        boolean unsubscribed;
        Room(String id) { this.id = id; }
    }
    public GatewayService(EventPublisher publisher, RoomBus bus, RoomStatisticsStore stats,
                          ProtocolCodec codec, Clock clock, Supplier<UUID> eventIds, Limits limits) {
        this.publisher = Objects.requireNonNull(publisher); this.bus = Objects.requireNonNull(bus);
        this.stats = Objects.requireNonNull(stats); this.codec = Objects.requireNonNull(codec);
        this.clock = Objects.requireNonNull(clock); this.ids = Objects.requireNonNull(eventIds);
        this.limits = Objects.requireNonNull(limits); permits = new Semaphore(limits.maxPending());
        broadcastPermits = new Semaphore(limits.maxPending());
        ThreadFactory factory = task -> { var t = new Thread(task, "gateway-worker"); t.setDaemon(true); return t; };
        workers = new ThreadPoolExecutor(2, 2, 0, TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(limits.maxRooms() * 4 + limits.maxPending() * 2 + 4), factory);
        // Exactly one task per service lifetime. Application saturation cannot discard local cleanup.
        lifecycle = new ThreadPoolExecutor(1, 1, 0, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<>(1), task -> {
            var thread = new Thread(task, "gateway-cleanup"); thread.setDaemon(true); return thread;
        });
        scheduler = Executors.newSingleThreadScheduledExecutor(task -> {
            var t = new Thread(task, "gateway-ticker"); t.setDaemon(true); return t;
        });
        ticker = scheduler.scheduleAtFixedRate(this::tick, 3, 3, TimeUnit.SECONDS);
    }
    private <T> T locked(Supplier<T> action) {
        if (Thread.holdsLock(this)) return action.get();
        List<Runnable> pending = new ArrayList<>();
        T result;
        synchronized (this) {
            effects.set(pending);
            try { result = action.get(); } finally { effects.remove(); }
        }
        for (Runnable effect : pending) effect.run();
        return result;
    }
    private void transition(Runnable action) { locked(() -> { action.run(); return null; }); }
    private void defer(Runnable effect) {
        if (Thread.holdsLock(this)) effects.get().add(effect); else effect.run();
    }
    public CompletionStage<Void> join(GatewayClient client) {
        String roomId = Objects.requireNonNull(client).identity().roomId();
        return locked(() -> joinState(client, roomId));
    }
    public void leave(GatewayClient client) { transition(() -> leaveState(client)); }
    public CompletionStage<Void> handle(GatewayClient client, ClientCommand command) {
        return locked(() -> handleState(client, command));
    }
    public String instanceId() { return instanceId; }
    public synchronized long failures() { return failureCount; }
    private CompletionStage<Void> joinState(GatewayClient client, String id) {
        Objects.requireNonNull(client);
        if (closing) return reject(client, 1013, "Gateway closed");
        Member existing = members.get(client);
        if (existing != null) return existing.ready.minimalCompletionStage();
        if (members.size() >= limits.maxConnections()) return reject(client, 1013, "Connection limit");
        Room room = rooms.get(id);
        boolean fresh = room == null;
        if (fresh && rooms.size() >= limits.maxRooms()) return reject(client, 1013, "Room limit");
        if (fresh) { room = new Room(id); rooms.put(id, room); }
        if (room.phase == Phase.FAILED) return reject(client, 1011, "Room unavailable");
        long targetGeneration = room.generation + (fresh || room.phase == Phase.UNSUBSCRIBING
                || room.phase == Phase.REMOVING ? 1 : 0);
        Member member = new Member(client, room, targetGeneration); members.put(client, member); room.clients.add(member);
        if (room.phase == Phase.READY) defer(() -> notifyReady(member, targetGeneration));
        if (fresh) subscribe(room);
        return member.ready.minimalCompletionStage();
    }
    private void subscribe(Room room) {
        room.phase = Phase.SUBSCRIBING; room.unsubscribed = false;
        long generation = ++room.generation;
        invoke(() -> bus.subscribe(room.id, payload -> receive(room, generation, payload)), (ignored, error) -> {
            if (!current(room)) return;
            if (error != null) {
                failureCount++; room.phase = Phase.FAILED;
                defer(() -> {
                    evict(room, 1011, "Subscribe failed");
                    transition(() -> { if (current(room)) { room.phase = Phase.READY; retire(room); } });
                });
                return;
            }
            room.phase = Phase.READY;
            if (closing || room.clients.isEmpty()) retire(room);
            else defer(() -> {
                for (Member member : room.clients) {
                    notifyReady(member, generation);
                }
            });
        });
    }
    private void notifyReady(Member member, long confirmedGeneration) {
        boolean confirmed = locked(() -> !closing && current(member.room)
                && member.room.phase == Phase.READY && member.room.generation == confirmedGeneration
                && member.targetGeneration == confirmedGeneration && members.get(member.client) == member);
        if (confirmed) member.ready.complete(null);
    }
    private void leaveState(GatewayClient client) {
        Member member = members.remove(client);
        if (member == null) return;
        member.room.clients.remove(member);
        defer(() -> member.ready.completeExceptionally(new CancellationException("Client departed")));
        if (member.room.clients.isEmpty() && member.room.phase == Phase.READY) retire(member.room);
    }
    private void retire(Room room) {
        room.phase = Phase.UNSUBSCRIBING;
        invoke(() -> bus.unsubscribe(room.id), (ignored, error) -> {
            if (!current(room)) return;
            if (error != null) { poison(room, "Unsubscribe failed", error); return; }
            room.unsubscribed = true; removeReport(room);
        });
    }
    private void removeReport(Room room) {
        if (!room.unsubscribed || room.refreshing || room.phase == Phase.REMOVING) return;
        room.phase = Phase.REMOVING;
        invoke(() -> stats.remove(room.id, instanceId), (ignored, error) -> {
            if (!current(room)) return;
            if (error != null) { poison(room, "Statistics cleanup failed", error); return; }
            if (!closing && !room.clients.isEmpty()) subscribe(room);
            else { rooms.remove(room.id); finishCloseIfEmpty(); }
        });
    }
    private void poison(Room room, String reason, Throwable error) {
        failureCount++; room.phase = Phase.FAILED;
        if (cleanupFailure == null) cleanupFailure = error;
        defer(() -> {
            evict(room, 1011, reason);
            transition(() -> {
                if (closing) { rooms.remove(room.id, room); finishCloseIfEmpty(); }
            });
        });
    }
    private boolean current(Room room) { return !closed.isDone() && rooms.get(room.id) == room; }
    private CompletionStage<Void> handleState(GatewayClient client, ClientCommand command) {
        Member member = members.get(client);
        if (closing || member == null || !member.ready.isDone() || member.room.phase != Phase.READY)
            return CompletableFuture.failedFuture(new IllegalStateException("Client not ready"));
        if (!permits.tryAcquire()) return reject(client, 1013, "Gateway overloaded");
        var result = new CompletableFuture<Void>(); operations.add(result);
        result.whenComplete((v,e) -> { synchronized (this) { operations.remove(result); permits.release(); } });
        if (command instanceof ClientCommand.Chat chat) {
            invoke(() -> {
                DanmakuEvent event = codec.newEvent(client.identity(), chat, clock.instant(), ids.get());
                return publisher.publish(event).thenApply(ignored -> event);
            }, (event, error) -> {
                if (error != null) { commandFailed(client, result, error); return; }
                if (closing) { defer(() -> result.completeExceptionally(new CancellationException("Gateway closed"))); return; }
                invoke(() -> bus.publish(event.roomId(), codec.encodeDanmaku(event)),
                        (ignored, publishError) -> completeCommand(client, result, publishError));
            });
        } else if (command instanceof ClientCommand.Like like) {
            invoke(() -> stats.addLikes(member.room.id, like.count()),
                    (ignored, error) -> completeCommand(client, result, error));
        } else defer(() -> result.completeExceptionally(new IllegalArgumentException("Unknown command")));
        return result.minimalCompletionStage();
    }
    private void completeCommand(GatewayClient client, CompletableFuture<Void> result, Throwable error) {
        if (error == null) defer(() -> result.complete(null)); else commandFailed(client, result, error);
    }
    private void commandFailed(GatewayClient client, CompletableFuture<Void> result, Throwable error) {
        failureCount++; disconnect(client, 1011, "Publish failed"); defer(() -> result.completeExceptionally(error));
    }
    private void receive(Room room, long generation, String payload) {
        transition(() -> receiveState(room, generation, payload));
    }
    private void receiveState(Room room, long generation, String payload) {
        if (closing || !current(room) || room.generation != generation || room.phase != Phase.READY) return;
        if (payload == null || payload.length() > ProtocolCodec.MAX_FRAME_BYTES) { failureCount++; return; }
        if (!broadcastPermits.tryAcquire()) { failureCount++; evict(room, 1013, "Gateway overloaded"); return; }
        try {
            workers.execute(() -> {
                try {
                    byte[] bytes = payload.getBytes(StandardCharsets.UTF_8);
                    if (bytes.length > ProtocolCodec.MAX_FRAME_BYTES) { synchronized (this) { failureCount++; } return; }
                    broadcast(room, generation, bytes);
                } finally { broadcastPermits.release(); }
            });
        } catch (RejectedExecutionException error) { broadcastPermits.release(); failureCount++; evict(room, 1013, "Gateway overloaded"); }
    }
    private void broadcast(Room room, long generation, byte[] bytes) {
        for (Member member : room.clients) {
            boolean active = locked(() -> !closing && current(room) && room.generation == generation
                    && room.phase == Phase.READY && members.get(member.client) == member);
            if (!active) continue;
            boolean accepted;
            try { accepted = member.client.send(bytes); } catch (RuntimeException error) { accepted = false; }
            if (!accepted) disconnect(member.client, 1013, "Slow client");
        }
    }
    /** Also useful for an explicit refresh; the owned scheduler invokes this every three seconds. */
    public void tick() {
        if (closing || !tickQueued.compareAndSet(false, true)) return;
        try {
            workers.execute(() -> {
                try { for (Room room : rooms.values()) transition(() -> refresh(room)); }
                finally { tickQueued.set(false); }
            });
        } catch (RejectedExecutionException error) { tickQueued.set(false); }
    }
    private final java.util.concurrent.atomic.AtomicBoolean tickQueued = new java.util.concurrent.atomic.AtomicBoolean();
    private void refresh(Room room) {
        if (closing || !current(room) || room.phase != Phase.READY || room.refreshing || room.clients.isEmpty()) return;
        room.refreshing = true; int online = room.clients.size();
        invoke(() -> stats.refresh(room.id, instanceId, online, TTL), (value, error) -> {
            room.refreshing = false;
            if (!current(room)) return;
            if (error != null) failureCount++;
            else if (!closing && room.phase == Phase.READY) {
                long generation = room.generation;
                defer(() -> {
                    try { broadcast(room, generation, codec.encodeStats(room.id, value).getBytes(StandardCharsets.UTF_8)); }
                    catch (RuntimeException invalid) { transition(() -> failureCount++); }
                });
            }
            if (room.phase == Phase.UNSUBSCRIBING) removeReport(room);
        });
    }
    private <T> void invoke(Supplier<CompletionStage<T>> action, BiConsumer<T, Throwable> completion) {
        try {
            workers.execute(() -> {
                try {
                    Objects.requireNonNull(action.get()).whenComplete((value,error) -> {
                        transition(() -> completion.accept(value,error));
                    });
                } catch (Throwable error) { transition(() -> completion.accept(null,error)); }
            });
        } catch (RejectedExecutionException error) { transition(() -> completion.accept(null,error)); }
    }
    private CompletionStage<Void> reject(GatewayClient client, int code, String reason) {
        disconnect(client, code, reason);
        return CompletableFuture.failedFuture(new RejectedExecutionException(reason));
    }
    private void disconnect(GatewayClient client, int code, String reason) {
        defer(() -> {
            leave(client);
            try { client.close(code, reason); } catch (RuntimeException ignored) { /* Transport already gone. */ }
        });
    }
    private void evict(Room room, int code, String reason) {
        defer(() -> { for (Member member : room.clients) disconnect(member.client, code, reason); });
    }
    /** Stops admission immediately; population-wide cleanup always runs on owned workers. */
    public CompletionStage<Void> closeAsync() {
        return locked(() -> {
            if (!closing) {
                closing = true; ticker.cancel(false);
                scheduler.schedule(this::timeoutClose, 2, TimeUnit.SECONDS);
                lifecycle.execute(this::cleanupRooms);
                lifecycle.shutdown(); // Graceful only: never discard the sole local cleanup task.
            }
            return closed.minimalCompletionStage();
        });
    }
    private void cleanupRooms() {
        try {
            for (Room room : rooms.values()) {
                evict(room, 1001, "Gateway shutdown");
                transition(() -> { if (room.phase == Phase.FAILED) rooms.remove(room.id, room); });
            }
            transition(this::finishCloseIfEmpty);
        } finally {
            localCleanupDone = true;
            if (closed.isDone()) rooms.clear();
        }
    }
    private void finishCloseIfEmpty() {
        if (!closing || !rooms.isEmpty() || stoppingWorkers) return;
        stoppingWorkers = true;
        defer(() -> {
            for (var operation : operations) operation.completeExceptionally(new CancellationException("Gateway closed"));
            workers.shutdown(); observeTermination();
        });
    }
    private void observeTermination() {
        if (closed.isDone()) return;
        if (workers.isTerminated() && lifecycle.isTerminated()) {
            Throwable error = locked(() -> cleanupFailure);
            if (error == null) closed.complete(null); else closed.completeExceptionally(error);
            scheduler.shutdownNow();
        } else scheduler.schedule(this::observeTermination, 10, TimeUnit.MILLISECONDS);
    }
    private void timeoutClose() {
        // Deadline completion never requires the state monitor or any client callback.
        closed.completeExceptionally(new TimeoutException("Gateway cleanup timed out"));
        workers.shutdownNow(); scheduler.shutdownNow();
        if (localCleanupDone) rooms.clear();
        for (var operation : operations) operation.completeExceptionally(new CancellationException("Gateway closed"));
    }
    /** Nonblocking; use closeAsync() to observe cleanup completion. */
    @Override public void close() { closeAsync(); }
}
