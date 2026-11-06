package io.github.javalyhn.danmakuflow.gateway;

import io.github.javalyhn.danmakuflow.gateway.redis.RedisRoomAdapter;
import io.github.javalyhn.danmakuflow.protocol.ProtocolCodec;
import java.time.*;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.function.Supplier;

/** Single Spring-owned resource boundary; owns every successfully returned factory resource. */
public final class GatewayRuntime implements AutoCloseable {
    record Owned<T>(T value, AutoCloseable cleanup) { }
    record RedisResource(RoomBus bus, RoomStatisticsStore statistics, AutoCloseable cleanup) { }
    record CoreResource(GatewayService service, Supplier<CompletionStage<Void>> closeAsync) { }
    interface Listener extends AutoCloseable { void start() throws Exception; }
    interface Factory {
        Owned<EventPublisher> publisher(GatewaySettings settings, ProtocolCodec codec);
        RedisResource redis(GatewaySettings settings);
        CoreResource core(GatewaySettings settings, ProtocolCodec codec, EventPublisher publisher, RedisResource redis);
        Listener listener(GatewaySettings settings, ProtocolCodec codec, CoreResource core);
    }
    static final class ProductionFactory implements Factory {
        public Owned<EventPublisher> publisher(GatewaySettings settings, ProtocolCodec codec) {
            var publisher = KafkaEventPublisher.createProduction(settings.kafkaBootstrap(), codec, settings.kafkaTopic(),
                    settings.kafkaMaxInFlight(), settings.kafkaQueueCapacity(), settings.kafkaCloseTimeout());
            return new Owned<>(publisher, publisher);
        }
        public RedisResource redis(GatewaySettings settings) {
            var redis = RedisRoomAdapter.createProduction(settings.redisUri());
            return new RedisResource(redis, redis, redis);
        }
        public CoreResource core(GatewaySettings settings, ProtocolCodec codec, EventPublisher publisher, RedisResource redis) {
            var core = new GatewayService(publisher, redis.bus(), redis.statistics(), codec,
                    Clock.systemUTC(), UUID::randomUUID, settings.limits());
            return new CoreResource(core, core::closeAsync);
        }
        public Listener listener(GatewaySettings settings, ProtocolCodec codec, CoreResource core) {
            var server = new NettyGatewayServer(core.service(), codec, settings.server());
            return new Listener() {
                public void start() throws Exception { server.start(); }
                public void close() { server.close(); }
            };
        }
    }
    private final GatewaySettings settings;
    private final Factory factory;
    private final Duration coreCloseTimeout;
    private Owned<EventPublisher> publisher;
    private RedisResource redis;
    private CoreResource core;
    private Listener listener;
    private boolean started;
    private boolean closed;
    GatewayRuntime(GatewaySettings settings, Factory factory, Duration coreCloseTimeout) {
        this.settings = settings; this.factory = factory; this.coreCloseTimeout = coreCloseTimeout;
        if (coreCloseTimeout.isNegative() || coreCloseTimeout.isZero()) throw new IllegalArgumentException("Invalid close timeout");
    }
    public synchronized void start() {
        if (started || closed) throw new IllegalStateException("Gateway lifecycle already started or closed");
        started = true;
        try {
            var codec = new ProtocolCodec();
            publisher = factory.publisher(settings, codec);
            redis = factory.redis(settings);
            core = factory.core(settings, codec, publisher.value(), redis);
            listener = factory.listener(settings, codec, core);
            listener.start();
        } catch (Throwable failure) {
            try { close(); } catch (Throwable ignored) { /* all acquired resources already attempted */ }
            // Do not attach credential-bearing client causes or suppressed exceptions to Spring's log chain.
            throw new IllegalStateException("Gateway startup failed; check service availability and configuration");
        }
    }
    @Override public synchronized void close() {
        if (closed) return;
        closed = true;
        boolean failed = false;
        boolean interrupted = Thread.interrupted();
        try {
            if (listener != null) try { listener.close(); } catch (Throwable error) { failed = true; }
            if (core != null) {
                long deadline = System.nanoTime() + coreCloseTimeout.toNanos();
                try {
                    var completion = core.closeAsync().get().toCompletableFuture();
                    for (;;) {
                        try { completion.get(Math.max(0, deadline - System.nanoTime()), TimeUnit.NANOSECONDS); break; }
                        catch (InterruptedException error) { interrupted = true; }
                    }
                } catch (Throwable error) { failed = true; }
            }
            if (redis != null) try { redis.cleanup().close(); } catch (Throwable error) { failed = true; }
            if (publisher != null) try { publisher.cleanup().close(); } catch (Throwable error) { failed = true; }
        } finally { if (interrupted) Thread.currentThread().interrupt(); }
        if (failed) throw new IllegalStateException("Gateway cleanup failed or exceeded its resource deadline");
    }
}
