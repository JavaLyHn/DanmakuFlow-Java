package io.github.javalyhn.danmakuflow.persistence;

import com.zaxxer.hikari.HikariConfig;
import java.net.URI;
import java.time.Duration;
import java.util.*;

/** All service settings are environment variables; constructing settings opens no connections. */
public record PersistenceSettings(String dbUrl, String dbUser, String dbPassword, String bootstrap,
        String topic, String group, int retries, Duration retryDelay) {
    public static PersistenceSettings from(Map<String, String> env) {
        String url = required(env, "DANMAKU_DB_URL");
        String user = required(env, "DANMAKU_DB_USER");
        if (!url.startsWith("jdbc:mysql://")) throw new IllegalArgumentException("DANMAKU_DB_URL must select MySQL");
        URI uri;
        try { uri = URI.create(url.substring(5)); }
        catch (IllegalArgumentException e) { throw new IllegalArgumentException("Invalid MySQL database URL"); }
        if (uri.getHost() == null || uri.getUserInfo() != null || uri.getFragment() != null
                || uri.getPath() == null || !uri.getPath().matches("/[A-Za-z0-9_]+"))
            throw new IllegalArgumentException("Select an explicit dedicated MySQL schema");
        // Driver URL properties override configured properties. Keep finite I/O timeouts authoritative.
        if (uri.getQuery() != null) throw new IllegalArgumentException("DB URL query properties are unsupported; timeouts are managed");
        int retries = number(env, "DANMAKU_PERSISTENCE_MAX_RETRIES", 5, 0, 20);
        int delay = number(env, "DANMAKU_PERSISTENCE_RETRY_DELAY_MS", 200, 1, 30_000);
        return new PersistenceSettings(url, user, env.get("DANMAKU_DB_PASSWORD"),
                value(env, "DANMAKU_KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
                value(env, "DANMAKU_KAFKA_TOPIC", "danmakuflow.danmaku.v1"),
                value(env, "DANMAKU_KAFKA_GROUP_ID", "danmakuflow-persistence-v1"), retries, Duration.ofMillis(delay));
    }
    public Properties consumerProperties() {
        var p = new Properties();
        p.setProperty("bootstrap.servers", bootstrap); p.setProperty("group.id", group);
        p.setProperty("enable.auto.commit", "false"); p.setProperty("auto.offset.reset", "earliest");
        p.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        p.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        p.setProperty("max.poll.records", "500"); p.setProperty("fetch.max.bytes", "4194304");
        p.setProperty("max.partition.fetch.bytes", "1048576");
        p.setProperty("request.timeout.ms", "10000"); p.setProperty("default.api.timeout.ms", "15000");
        p.setProperty("max.poll.interval.ms", "300000");
        return p;
    }
    public HikariConfig poolConfig() {
        var c = new HikariConfig(); c.setJdbcUrl(dbUrl); c.setUsername(dbUser); c.setPassword(dbPassword);
        c.setMaximumPoolSize(4); c.setMinimumIdle(0); c.setConnectionTimeout(5000);
        c.setValidationTimeout(2000); c.setInitializationFailTimeout(1);
        c.addDataSourceProperty("rewriteBatchedStatements", "true");
        c.addDataSourceProperty("connectTimeout", "5000"); c.addDataSourceProperty("socketTimeout", "10000");
        return c;
    }
    private static String required(Map<String, String> env, String key) {
        var value = env.get(key); if (value == null || value.isBlank()) throw new IllegalArgumentException(key + " is required");
        return value;
    }
    private static String value(Map<String, String> env, String key, String fallback) {
        return env.containsKey(key) ? required(env, key) : fallback;
    }
    private static int number(Map<String, String> env, String key, int fallback, int min, int max) {
        try { int n = Integer.parseInt(env.getOrDefault(key, Integer.toString(fallback)));
            if (n < min || n > max) throw new NumberFormatException(); return n; }
        catch (NumberFormatException e) { throw new IllegalArgumentException(key + " outside supported range"); }
    }
}
