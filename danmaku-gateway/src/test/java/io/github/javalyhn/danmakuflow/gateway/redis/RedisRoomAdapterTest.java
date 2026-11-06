package io.github.javalyhn.danmakuflow.gateway.redis;

import org.junit.jupiter.api.Test;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.BiConsumer;
import static org.junit.jupiter.api.Assertions.*;

class RedisRoomAdapterTest {
    @Test void productionReconnectExcludesAmbiguousWritesButKeepsSubscriptionsAndScripts() {
        var options = LettuceCommands.productionOptions();
        var filter = options.getReplayFilter();
        assertAll(
            () -> assertTrue(filter.test(new io.lettuce.core.protocol.Command<>(io.lettuce.core.protocol.CommandType.INCRBY, null))),
            () -> assertTrue(filter.test(new io.lettuce.core.protocol.Command<>(io.lettuce.core.protocol.CommandType.PUBLISH, null))),
            () -> assertFalse(filter.test(new io.lettuce.core.protocol.Command<>(io.lettuce.core.protocol.CommandType.EVAL, null))),
            () -> assertFalse(filter.test(new io.lettuce.core.protocol.Command<>(io.lettuce.core.protocol.CommandType.SUBSCRIBE, null))),
            () -> assertFalse(filter.test(new io.lettuce.core.protocol.Command<>(io.lettuce.core.protocol.CommandType.UNSUBSCRIBE, null))),
            () -> assertFalse(filter.test(null)),
            () -> assertTrue(options.isAutoReconnect()),
            () -> assertTrue(options.getTimeoutOptions().isTimeoutCommands()));
    }
    @Test void subscriptionWaitsForAckAndDeliversOnlyOneBusEcho() {
        Fake f = new Fake(); var a = new RedisRoomAdapter(f); List<String> messages = new ArrayList<>();
        var subscribed = a.subscribe("r", messages::add).toCompletableFuture();
        assertFalse(subscribed.isDone()); assertEquals("room:r:pubsub", f.channel);
        f.sub.complete(null); subscribed.join();
        a.publish("r", "hello"); assertTrue(messages.isEmpty());
        f.listener.accept("room:r:pubsub", "hello"); assertEquals(List.of("hello"), messages);
        assertTrue(a.subscribe("r", messages::add).toCompletableFuture().isCompletedExceptionally());
    }
    @Test void unsubscribeDuringSubscribeDetachesAndWaitsForLateAck() {
        Fake f = new Fake(); var a = new RedisRoomAdapter(f); List<String> messages = new ArrayList<>();
        a.subscribe("r", messages::add); var done = a.unsubscribe("r").toCompletableFuture();
        assertFalse(done.isDone()); assertEquals(0, f.unsubCalls);
        f.listener.accept("room:r:pubsub", "stale"); assertTrue(messages.isEmpty());
        f.sub.complete(null); assertEquals(1, f.unsubCalls); assertFalse(done.isDone());
        f.unsub.complete(null); done.join();
        assertFalse(a.subscribe("r", messages::add).toCompletableFuture().isCompletedExceptionally());
    }
    @Test void failedSubscriptionDetachesListenerAndCleansChannel() {
        Fake f = new Fake(); var a = new RedisRoomAdapter(f); List<String> messages = new ArrayList<>();
        var done = a.subscribe("r", messages::add).toCompletableFuture();
        f.sub.completeExceptionally(new IllegalStateException("secret"));
        assertTrue(done.isCompletedExceptionally());
        f.listener.accept("room:r:pubsub", "stale"); assertTrue(messages.isEmpty());
        assertEquals(1, f.unsubCalls);
    }
    @Test void likesUseExactKeyAndPropagateOverflowWithoutRetry() {
        Fake f = new Fake(); var a = new RedisRoomAdapter(f);
        var done = a.addLikes("x", Long.MAX_VALUE).toCompletableFuture();
        assertEquals("room:x:likes", f.key); assertEquals(Long.MAX_VALUE, f.count); assertFalse(done.isDone());
        f.increment.completeExceptionally(new ArithmeticException("overflow"));
        assertThrows(CompletionException.class, done::join); assertEquals(1, f.increments);
        assertTrue(a.addLikes("x", -1).toCompletableFuture().isCompletedExceptionally());
    }
    @Test void heartbeatPreservesIntegersAboveDoublePrecision() {
        Fake f = new Fake(); var a = new RedisRoomAdapter(f);
        var done = a.refresh("r", "i", 3, Duration.ofSeconds(7)).toCompletableFuture();
        assertArrayEquals(new String[]{"room:r:online:i", "room:r:servers", "room:r:likes"}, f.keys);
        assertArrayEquals(new String[]{"3", "7000", "i", "room:r:online:"}, f.args); assertFalse(done.isDone());
        f.eval.complete(List.of("9223372036854775807", "9007199254740993", "2"));
        var stats = done.join(); assertEquals(Long.MAX_VALUE, stats.likes()); assertEquals(9007199254740995L, stats.online());
    }
    @Test void aggregateRejectsOverflowNegativeAndMalformedValues() {
        for (List<Object> values : List.<List<Object>>of(List.of("0", "9223372036854775807", "1"), List.of("-1", "0"), List.of("0", "1.5"))) {
            Fake f = new Fake(); var a = new RedisRoomAdapter(f);
            var done = a.refresh("r", "i", 0, Duration.ofSeconds(7)).toCompletableFuture();
            f.eval.complete(values); assertThrows(CompletionException.class, done::join);
        }
    }
    @Test void removeUsesOwnHeartbeatAndMembership() {
        Fake f = new Fake(); var a = new RedisRoomAdapter(f);
        var done = a.remove("r", "i").toCompletableFuture();
        assertArrayEquals(new String[]{"room:r:online:i", "room:r:servers"}, f.keys);
        assertArrayEquals(new String[]{"i"}, f.args); assertFalse(done.isDone());
        f.eval.complete(List.of()); done.join();
    }
    @Test void closeCancelsPendingDetachesListenerAndIsIdempotent() {
        Fake f = new Fake(); var a = new RedisRoomAdapter(f);
        var sub = a.subscribe("r", ignored -> fail()).toCompletableFuture();
        var likes = a.addLikes("r", 1).toCompletableFuture();
        var close = a.closeAsync().toCompletableFuture();
        assertTrue(sub.isCompletedExceptionally()); assertTrue(likes.isCompletedExceptionally());
        assertNull(f.listener); assertFalse(close.isDone());
        f.sub.complete(null); assertEquals(0, f.unsubCalls);
        assertTrue(a.publish("r", "x").toCompletableFuture().isCompletedExceptionally());
        a.closeAsync(); assertEquals(1, f.closes); f.closed.complete(null); close.join();
    }
    static final class Fake implements RedisRoomAdapter.Commands {
        BiConsumer<String,String> listener; String channel, key; long count; String[] keys,args;
        int unsubCalls, increments, closes;
        CompletableFuture<Void> sub = new CompletableFuture<>(), unsub = new CompletableFuture<>(), closed = new CompletableFuture<>();
        CompletableFuture<Long> increment = new CompletableFuture<>();
        CompletableFuture<List<Object>> eval = new CompletableFuture<>();
        public void listener(BiConsumer<String,String> value) { listener=value; }
        public CompletionStage<Void> subscribe(String c) {channel=c; return sub;}
        public CompletionStage<Void> unsubscribe(String c) {channel=c; unsubCalls++; return unsub;}
        public CompletionStage<Long> publish(String c,String p) {channel=c; return CompletableFuture.completedFuture(1L);}
        public CompletionStage<Long> increment(String k,long c) {key=k;count=c;increments++;return increment;}
        public CompletionStage<List<Object>> eval(String s,String[] k,String... a) {keys=k;args=a;return eval;}
        public CompletionStage<Void> close() {closes++;return closed;}
    }
}
