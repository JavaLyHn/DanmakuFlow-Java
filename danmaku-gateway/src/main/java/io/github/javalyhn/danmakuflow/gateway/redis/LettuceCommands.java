package io.github.javalyhn.danmakuflow.gateway.redis;

import io.lettuce.core.*;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.pubsub.*;
import io.lettuce.core.resource.*;
import io.lettuce.core.protocol.CommandType;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.BiConsumer;

/** Owns all Lettuce resources. No retry of ambiguous non-idempotent INCRBY failures. */
final class LettuceCommands implements RedisRoomAdapter.Commands {
    private DefaultClientResources resources;
    private RedisClient client;
    private StatefulRedisConnection<String,String> ordinary;
    private StatefulRedisPubSubConnection<String,String> pubsub;
    private volatile BiConsumer<String,String> receiver;
    private final RedisPubSubAdapter<String,String> listener = new RedisPubSubAdapter<>() {
        @Override public void message(String channel, String message) {
            var target = receiver;
            if (target != null) target.accept(channel,message);
        }
    };
    static LettuceCommands open(String uri) {
        var owned = new LettuceCommands();
        try {
            RedisURI address = RedisURI.create(uri == null ? RedisRoomAdapter.DEFAULT_URI : uri);
            address.setTimeout(Duration.ofSeconds(2));
            owned.resources = DefaultClientResources.builder().ioThreadPoolSize(2).computationThreadPoolSize(2).build();
            owned.client = RedisClient.create(owned.resources, address);
            owned.client.setOptions(productionOptions());
            owned.ordinary = owned.client.connect();
            owned.pubsub = owned.client.connectPubSub();
            owned.pubsub.addListener(owned.listener);
            return owned;
        } catch (RuntimeException e) {
            try { owned.close().toCompletableFuture().orTimeout(6,TimeUnit.SECONDS).join(); }
            catch (RuntimeException ignored) { /* All resource shutdowns have already been initiated. */ }
            // Do not attach an exception that may contain a credential-bearing URI.
            throw new IllegalStateException("Redis initialization failed");
        }
    }
    static ClientOptions productionOptions() {
        return ClientOptions.builder().autoReconnect(true).requestQueueSize(1024)
            // Lettuce's true predicate result EXCLUDES replay. A lost response must
            // not increment likes or broadcast twice; the command timeout fails it.
            .replayFilter(command -> command != null &&
                (command.getType() == CommandType.INCRBY || command.getType() == CommandType.PUBLISH))
            .disconnectedBehavior(ClientOptions.DisconnectedBehavior.REJECT_COMMANDS)
            .socketOptions(SocketOptions.builder().connectTimeout(Duration.ofSeconds(2)).build())
            .timeoutOptions(TimeoutOptions.enabled(Duration.ofSeconds(2))).build();
    }
    public void listener(BiConsumer<String,String> value) { receiver = value; }
    public CompletionStage<Void> subscribe(String channel) { return pubsub.async().subscribe(channel); }
    public CompletionStage<Void> unsubscribe(String channel) { return pubsub.async().unsubscribe(channel); }
    public CompletionStage<Long> publish(String channel,String payload) { return ordinary.async().publish(channel,payload); }
    public CompletionStage<Long> increment(String key,long count) { return ordinary.async().incrby(key,count); }
    public CompletionStage<List<Object>> eval(String script,String[] keys,String... args) {
        return ordinary.async().eval(script, ScriptOutputType.MULTI, keys,args);
    }
    public CompletionStage<Void> close() {
        receiver = null;
        List<CompletableFuture<?>> tasks = new ArrayList<>();
        if (pubsub != null) {
            pubsub.removeListener(listener);
            attempt(tasks, () -> pubsub.closeAsync());
        }
        if (ordinary != null) attempt(tasks, () -> ordinary.closeAsync());
        if (client != null) attempt(tasks, () -> client.shutdownAsync(0,2,TimeUnit.SECONDS));
        if (resources != null) {
            var done = new CompletableFuture<Void>(); tasks.add(done);
            try { resources.shutdown(0,2,TimeUnit.SECONDS).addListener(f -> {
                if (f.isSuccess()) done.complete(null); else done.completeExceptionally(new IllegalStateException("Redis shutdown failed"));
            }); } catch (RuntimeException e) { done.completeExceptionally(new IllegalStateException("Redis shutdown failed")); }
        }
        return CompletableFuture.allOf(tasks.toArray(CompletableFuture[]::new));
    }
    private static void attempt(List<CompletableFuture<?>> tasks, java.util.function.Supplier<CompletionStage<Void>> action) {
        try { tasks.add(action.get().toCompletableFuture()); }
        catch (RuntimeException e) { tasks.add(CompletableFuture.failedFuture(new IllegalStateException("Redis shutdown failed"))); }
    }
}
