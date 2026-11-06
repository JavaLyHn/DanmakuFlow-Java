package io.github.javalyhn.danmakuflow.tools;

import java.io.PrintStream;
import java.net.http.WebSocket;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.concurrent.*;

/** Single connection; console writes are serialized and bounded, callbacks signal completion. */
final class ClientSession implements WebSocket.Listener {
    private final CommandParser commands = new CommandParser();
    private final TextFragments fragments = new TextFragments();
    private final CompletableFuture<Integer> finished = new CompletableFuture<>();
    private final PrintStream output;
    private final PrintStream errors;
    private final Duration writeTimeout;
    private WebSocket socket;

    ClientSession(PrintStream output, PrintStream errors, Duration writeTimeout) {
        this.output = output;
        this.errors = errors;
        this.writeTimeout = writeTimeout;
    }

    CompletableFuture<Integer> finished() { return finished; }

    @Override public synchronized void onOpen(WebSocket webSocket) {
        socket = webSocket;
        if (finished.isDone()) webSocket.abort();
        else webSocket.request(1);
    }

    @Override public synchronized CompletionStage<?> onText(WebSocket webSocket, CharSequence data, boolean last) {
        if (finished.isDone()) return CompletableFuture.completedFuture(null);
        try {
            fragments.accept(data, last).ifPresent(output::println);
            webSocket.request(1);
        } catch (IllegalArgumentException invalid) {
            fail(invalid.getMessage());
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override public synchronized CompletionStage<?> onBinary(WebSocket webSocket, ByteBuffer data, boolean last) {
        fail("Unexpected binary server message");
        return CompletableFuture.completedFuture(null);
    }

    @Override public synchronized CompletionStage<?> onClose(WebSocket webSocket, int statusCode, String reason) {
        finished.complete(0);
        return CompletableFuture.completedFuture(null);
    }

    @Override public synchronized void onError(WebSocket webSocket, Throwable error) {
        fail("WebSocket connection failed");
    }

    synchronized void sendLine(String line) {
        if (finished.isDone() || socket == null) return;
        CommandParser.Action action;
        try { action = commands.parse(line); }
        catch (IllegalArgumentException invalid) { errors.println(invalid.getMessage()); return; }
        try {
            CompletableFuture<WebSocket> write = action.quit()
                    ? socket.sendClose(WebSocket.NORMAL_CLOSURE, "bye") : socket.sendText(action.json(), true);
            write.get(writeTimeout.toMillis(), TimeUnit.MILLISECONDS);
            if (action.quit()) finished.complete(0);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            fail("WebSocket write interrupted");
        } catch (ExecutionException | TimeoutException | RuntimeException failed) {
            fail("WebSocket write failed or timed out");
        }
    }

    synchronized void stop() {
        finished.complete(0);
        if (socket != null) socket.abort();
    }

    synchronized void fail(String message) {
        if (finished.isDone()) return;
        errors.println(message);
        if (socket != null) socket.abort();
        finished.complete(1);
    }
}
