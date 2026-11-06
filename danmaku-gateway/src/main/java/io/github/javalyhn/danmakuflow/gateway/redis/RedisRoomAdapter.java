package io.github.javalyhn.danmakuflow.gateway.redis;

import io.github.javalyhn.danmakuflow.gateway.*;
import io.github.javalyhn.danmakuflow.protocol.RoomStats;
import java.time.Duration;
import java.math.BigInteger;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.*;

/** Standalone Redis only: the atomic scripts intentionally span multiple keys. */
public final class RedisRoomAdapter implements RoomBus, RoomStatisticsStore, AutoCloseable {
    public static final String DEFAULT_URI = "redis://127.0.0.1:6379/0";
    static final String HEARTBEAT = """
        redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[2])
        redis.call('SADD', KEYS[2], ARGV[3])
        local result = {redis.call('GET', KEYS[3]) or '0'}
        for _, member in ipairs(redis.call('SMEMBERS', KEYS[2])) do
          local count = redis.call('GET', ARGV[4] .. member)
          if count then table.insert(result, count)
          else redis.call('SREM', KEYS[2], member) end
        end
        return result
        """;
    static final String REMOVE = """
        redis.call('DEL', KEYS[1])
        redis.call('SREM', KEYS[2], ARGV[1])
        return {}
        """;
    interface Commands {
        void listener(BiConsumer<String,String> listener);
        CompletionStage<Void> subscribe(String channel);
        CompletionStage<Void> unsubscribe(String channel);
        CompletionStage<Long> publish(String channel, String payload);
        CompletionStage<Long> increment(String key, long count);
        CompletionStage<List<Object>> eval(String script, String[] keys, String... args);
        CompletionStage<Void> close();
    }
    private static final class Subscription {
        Consumer<String> callback;
        CompletionStage<Void> ack;
        CompletableFuture<Void> removal;
        Subscription(Consumer<String> callback) { this.callback = callback; }
    }
    private final Commands commands;
    private final Map<String,Subscription> rooms = new HashMap<>();
    private final Set<CompletableFuture<?>> pending = new HashSet<>();
    private CompletableFuture<Void> closing;
    public static RedisRoomAdapter createProduction(String redisUri) {
        return new RedisRoomAdapter(LettuceCommands.open(redisUri));
    }
    RedisRoomAdapter(Commands commands) {
        this.commands = Objects.requireNonNull(commands);
        commands.listener(this::message);
    }
    private void message(String channel, String payload) {
        Consumer<String> callback;
        synchronized (this) { var entry = rooms.get(channel); callback = entry == null ? null : entry.callback; }
        if (callback != null) callback.accept(payload);
    }
    public synchronized CompletionStage<Void> subscribe(String room, Consumer<String> callback) {
        String channel = channel(room);
        if (closing != null || rooms.containsKey(channel)) return failed();
        var entry = new Subscription(Objects.requireNonNull(callback));
        rooms.put(channel, entry);
        entry.ack = invoke(() -> commands.subscribe(channel));
        entry.ack.whenComplete((v,e) -> { if (e != null) unsubscribe(room); });
        return entry.ack;
    }
    public synchronized CompletionStage<Void> unsubscribe(String room) {
        if (closing != null) return failed();
        String channel = channel(room); var entry = rooms.get(channel);
        if (entry == null) return CompletableFuture.completedFuture(null);
        entry.callback = null;
        if (entry.removal != null) return entry.removal;
        entry.removal = new CompletableFuture<>();
        // Subscribe must settle before unsubscribe: a late ACK must not recreate a departed room.
        entry.ack.whenComplete((v,e) -> {
            synchronized (this) {
                if (closing != null) { entry.removal.completeExceptionally(unavailable()); return; }
                invoke(() -> commands.unsubscribe(channel)).whenComplete((ignored,error) -> {
                    synchronized (this) { if (error == null) rooms.remove(channel,entry); }
                    if (error == null) entry.removal.complete(null); else entry.removal.completeExceptionally(unavailable());
                });
            }
        });
        return entry.removal;
    }
    public synchronized CompletionStage<Void> publish(String room, String payload) {
        return invoke(() -> commands.publish(channel(room), payload)).thenApply(v -> null);
    }
    public synchronized CompletionStage<Void> addLikes(String room, long count) {
        if (count < 0) return failed();
        return invoke(() -> commands.increment("room:" + room + ":likes", count)).thenApply(v -> {
            if (v == null || v < 0) throw unavailable(); return null;
        });
    }
    public synchronized CompletionStage<RoomStats> refresh(String room, String instance, int online, Duration ttl) {
        if (online < 0 || ttl == null || ttl.toMillis() <= 0) return failed();
        String prefix = "room:" + room;
        return invoke(() -> commands.eval(HEARTBEAT,
            new String[]{prefix + ":online:" + instance, prefix + ":servers", prefix + ":likes"},
            Integer.toString(online), Long.toString(ttl.toMillis()), instance, prefix + ":online:"))
            .thenApply(RedisRoomAdapter::parse);
    }
    private static RoomStats parse(List<Object> values) {
        if (values == null || values.isEmpty()) throw unavailable();
        long likes = integer(values.getFirst()).longValueExact();
        BigInteger online = BigInteger.ZERO;
        for (int i=1; i<values.size(); i++) online = online.add(integer(values.get(i)));
        return new RoomStats(online.longValueExact(), likes);
    }
    private static BigInteger integer(Object value) {
        if (!(value instanceof String text) || !text.matches("[0-9]+")) throw unavailable();
        BigInteger number = new BigInteger(text);
        if (number.compareTo(BigInteger.valueOf(Long.MAX_VALUE)) > 0) throw unavailable();
        return number;
    }
    public synchronized CompletionStage<Void> remove(String room, String instance) {
        String prefix = "room:" + room;
        return invoke(() -> commands.eval(REMOVE, new String[]{prefix + ":online:" + instance, prefix + ":servers"}, instance)).thenApply(v -> null);
    }
    private <T> CompletableFuture<T> invoke(Supplier<CompletionStage<T>> action) {
        if (closing != null) return failed();
        var result = new CompletableFuture<T>(); pending.add(result);
        try {
            action.get().whenComplete((v,e) -> {
                synchronized (this) { pending.remove(result); }
                if (e == null) result.complete(v); else result.completeExceptionally(unavailable());
            });
        } catch (RuntimeException e) { pending.remove(result); result.completeExceptionally(unavailable()); }
        return result;
    }
    public synchronized CompletionStage<Void> closeAsync() {
        if (closing != null) return closing;
        closing = new CompletableFuture<>();
        commands.listener(null);
        for (var entry : rooms.values()) {
            entry.callback = null;
            if (entry.removal != null) entry.removal.completeExceptionally(unavailable());
        }
        rooms.clear();
        for (var future : List.copyOf(pending)) future.completeExceptionally(unavailable());
        pending.clear();
        try {
            commands.close().whenComplete((v,e) -> {
                if (e == null) closing.complete(null); else closing.completeExceptionally(unavailable());
            });
        } catch (RuntimeException e) { closing.completeExceptionally(unavailable()); }
        return closing.orTimeout(6, TimeUnit.SECONDS);
    }
    public void close() { closeAsync().toCompletableFuture().join(); }
    private static String channel(String room) { return "room:" + room + ":pubsub"; }
    private static IllegalStateException unavailable() { return new IllegalStateException("Redis operation failed or adapter unavailable"); }
    private static <T> CompletableFuture<T> failed() { return CompletableFuture.failedFuture(unavailable()); }
}
