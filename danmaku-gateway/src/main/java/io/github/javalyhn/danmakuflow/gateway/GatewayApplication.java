package io.github.javalyhn.danmakuflow.gateway;

import org.springframework.boot.*;
import org.springframework.context.annotation.Bean;
import java.time.Duration;
import java.util.Map;

/** Non-web Boot context: the single owned runtime starts Netty and closes in dependency order. */
@SpringBootConfiguration(proxyBeanMethods = false)
public class GatewayApplication {
    public static void main(String[] args) {
        if (args.length == 1 && args[0].equals("--help")) {
            System.out.println(help());
            return;
        }
        application(GatewaySettings.from(System.getenv()), new GatewayRuntime.ProductionFactory()).run(args);
    }
    static SpringApplication application(GatewaySettings settings, GatewayRuntime.Factory factory) {
        var app = new SpringApplication(GatewayApplication.class);
        app.setWebApplicationType(WebApplicationType.NONE);
        app.setDefaultProperties(Map.of("spring.main.banner-mode", "off"));
        app.addInitializers(context -> {
            context.getBeanFactory().registerSingleton("gatewaySettings", settings);
            context.getBeanFactory().registerSingleton("gatewayFactory", factory);
        });
        return app;
    }
    @Bean(initMethod = "start", destroyMethod = "close")
    GatewayRuntime gatewayRuntime(GatewaySettings settings, GatewayRuntime.Factory factory) {
        return new GatewayRuntime(settings, factory, Duration.ofSeconds(3));
    }
    static String help() {
        return """
                DanmakuFlow gateway (non-web Spring Boot; Netty owns /ws).
                Requires external standalone Redis and Kafka. No database or schema migration.
                Environment (defaults):
                  DANMAKU_GATEWAY_HOST=127.0.0.1
                  DANMAKU_GATEWAY_PORT=8080 (0 selects an ephemeral port)
                  DANMAKU_GATEWAY_MAX_ROOMS=10000
                  DANMAKU_GATEWAY_MAX_CONNECTIONS=100000
                  DANMAKU_GATEWAY_MAX_PENDING=4096
                  DANMAKU_GATEWAY_CLIENT_MAX_PENDING_FRAMES=64
                  DANMAKU_GATEWAY_CLIENT_MAX_PENDING_BYTES=524288
                  DANMAKU_KAFKA_BOOTSTRAP_SERVERS=localhost:9092
                  DANMAKU_KAFKA_TOPIC=danmakuflow.danmaku.v1
                  DANMAKU_KAFKA_MAX_IN_FLIGHT=256
                  DANMAKU_KAFKA_QUEUE_CAPACITY=128
                  DANMAKU_KAFKA_CLOSE_TIMEOUT_MS=5000 (1..60000)
                  DANMAKU_REDIS_URI=redis://127.0.0.1:6379/0 (redis or rediss; credentials supported)
                Limits must be positive; port must be 0..65535. Core close wait: 3 seconds.
                --help creates no context, clients, threads, or listener.
                """;
    }
}
