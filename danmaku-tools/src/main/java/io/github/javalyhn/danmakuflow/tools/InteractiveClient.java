package io.github.javalyhn.danmakuflow.tools;

import java.io.*;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.*;

/** Plain JDK interactive WebSocket client; no Spring application context is started. */
public final class InteractiveClient {
    private static final Duration CONNECT_TIMEOUT = Duration.ofSeconds(10);
    private static final Duration WRITE_TIMEOUT = Duration.ofSeconds(5);
    private static final Duration SHUTDOWN_TIMEOUT = Duration.ofSeconds(2);

    private InteractiveClient() {}

    public static void main(String[] args) {
        System.exit(run(args, System.in, System.out, System.err));
    }

    static int run(String[] args, InputStream input, PrintStream output, PrintStream errors) {
        ClientOptions options;
        try { options = ClientOptions.parse(args); }
        catch (IllegalArgumentException invalid) { errors.println(invalid.getMessage()); return 2; }
        if (options.help()) {
            output.println("Usage: java -jar danmaku-tools-0.1.0-SNAPSHOT.jar [--url] <ws/wss URL>");
            output.println("Without a URL: ws://127.0.0.1:8080/ws?uid=1&name=java-cli&room=test");
            output.println("Enter chat text (max 500 code points), /like [positive count], or /quit.");
            output.println("--help / -h displays this help without connecting. One connection; no reconnection.");
            return 0;
        }
        ExecutorService executor = Executors.newFixedThreadPool(2, Thread.ofPlatform().daemon(true).name("danmaku-http-", 0).factory());
        HttpClient client = HttpClient.newBuilder().connectTimeout(CONNECT_TIMEOUT).executor(executor).build();
        ClientSession session = new ClientSession(output, errors, WRITE_TIMEOUT);
        CompletableFuture<WebSocket> connecting = null;
        try {
            connecting = client.newWebSocketBuilder().connectTimeout(CONNECT_TIMEOUT).buildAsync(options.uri(), session);
            connecting.get(CONNECT_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            return runConnected(input, session, errors);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            errors.println("Client interrupted");
            return 1;
        } catch (ExecutionException | TimeoutException | RuntimeException failed) {
            errors.println("WebSocket connection failed or timed out");
            return 1;
        } finally {
            if (connecting != null && !connecting.isDone()) connecting.cancel(true);
            session.stop();
            client.shutdownNow();
            executor.shutdownNow();
            try {
                client.awaitTermination(SHUTDOWN_TIMEOUT);
                executor.awaitTermination(SHUTDOWN_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            } catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); }
        }
    }

    static int runConnected(InputStream input, ClientSession session, PrintStream errors)
            throws InterruptedException, ExecutionException {
        startConsole(input, session, errors);
        return session.finished().get();
    }

    private static void startConsole(InputStream input, ClientSession session, PrintStream errors) {
        Thread.ofPlatform().daemon(true).name("danmaku-console").start(() -> {
            // System.in is caller-owned. A blocked daemon reader never keeps the CLI alive.
            BufferedReader reader = new BufferedReader(new InputStreamReader(input, StandardCharsets.UTF_8));
            try {
                while (!session.finished().isDone()) {
                    String line = reader.readLine();
                    if (line == null) { session.sendLine("/quit"); return; }
                    session.sendLine(line);
                }
            } catch (IOException failed) {
                session.fail("Console input failed");
            }
        });
    }
}
