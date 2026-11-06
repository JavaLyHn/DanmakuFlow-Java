package io.github.javalyhn.danmakuflow.gateway;

import io.lettuce.core.RedisURI;
import java.net.URI;
import java.time.Duration;
import java.util.Map;

/** Validated before any external resources are acquired. Never print credential-bearing values. */
public record GatewaySettings(NettyGatewayServer.Settings server, GatewayService.Limits limits,
        String kafkaBootstrap, String kafkaTopic, int kafkaMaxInFlight, int kafkaQueueCapacity,
        Duration kafkaCloseTimeout, String redisUri) {
    public static GatewaySettings from(Map<String, String> env) {
        String host = value(env, "DANMAKU_GATEWAY_HOST", "127.0.0.1");
        int port = (int) number(env, "DANMAKU_GATEWAY_PORT", 8080, 0, 65535);
        int connections = integer(env, "DANMAKU_GATEWAY_MAX_CONNECTIONS", 100000);
        var limits = new GatewayService.Limits(integer(env, "DANMAKU_GATEWAY_MAX_ROOMS", 10000),
                connections, integer(env, "DANMAKU_GATEWAY_MAX_PENDING", 4096));
        var server = new NettyGatewayServer.Settings(host, port, connections,
                integer(env, "DANMAKU_GATEWAY_CLIENT_MAX_PENDING_FRAMES", 64),
                number(env, "DANMAKU_GATEWAY_CLIENT_MAX_PENDING_BYTES", 524288, 1, Long.MAX_VALUE));
        String uri = value(env, "DANMAKU_REDIS_URI", "redis://127.0.0.1:6379/0");
        try {
            var parsed = URI.create(uri);
            if (!("redis".equals(parsed.getScheme()) || "rediss".equals(parsed.getScheme()))
                    || parsed.getHost() == null || parsed.getPort() > 65535 || parsed.getFragment() != null)
                throw new IllegalArgumentException();
            // Use the adapter's pure parser before any resource factory can run.
            RedisURI.create(uri);
        } catch (RuntimeException e) { throw new IllegalArgumentException("Invalid DANMAKU_REDIS_URI"); }
        String topic = value(env, "DANMAKU_KAFKA_TOPIC", "danmakuflow.danmaku.v1");
        if (topic.length() > 249 || !topic.matches("[a-zA-Z0-9._-]+") || topic.equals(".") || topic.equals(".."))
            throw new IllegalArgumentException("Invalid DANMAKU_KAFKA_TOPIC");
        return new GatewaySettings(server, limits,
                value(env, "DANMAKU_KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"), topic,
                integer(env, "DANMAKU_KAFKA_MAX_IN_FLIGHT", 256),
                integer(env, "DANMAKU_KAFKA_QUEUE_CAPACITY", 128),
                Duration.ofMillis(number(env, "DANMAKU_KAFKA_CLOSE_TIMEOUT_MS", 5000, 1, 60000)), uri);
    }
    private static String value(Map<String, String> env, String key, String fallback) {
        String result = env.getOrDefault(key, fallback);
        if (result == null || result.isBlank()) throw new IllegalArgumentException("Invalid " + key);
        return result;
    }
    private static int integer(Map<String, String> env, String key, int fallback) {
        return (int) number(env, key, fallback, 1, Integer.MAX_VALUE);
    }
    private static long number(Map<String, String> env, String key, long fallback, long min, long max) {
        try {
            long result = Long.parseLong(value(env, key, Long.toString(fallback)));
            if (result < min || result > max) throw new NumberFormatException();
            return result;
        } catch (IllegalArgumentException e) { throw new IllegalArgumentException("Invalid " + key); }
    }
    @Override public String toString() { return "GatewaySettings[values redacted]"; }
}
