package io.github.javalyhn.danmakuflow.tools;

import io.github.javalyhn.danmakuflow.protocol.ClientCommand;
import io.github.javalyhn.danmakuflow.protocol.ProtocolCodec;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.json.JsonMapper;

import java.io.*;
import java.net.http.WebSocket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;

class InteractiveClientTest {
    private final CommandParser parser = new CommandParser();

    @Test void chatIsEscapedAndRoundTripsThroughProtocol() {
        String content = "quote\" slash\\ newline\n emoji 😀";
        String json = parser.parse(content).json();
        assertEquals(new ClientCommand.Chat(content), new ProtocolCodec().decodeClient(json));
        assertEquals(101, JsonMapper.builder().build().readTree(json).get("type").intValue());
        assertFalse(json.contains("\n"));
    }

    @Test void chatUsesProtocolCodepointAndUnicodeValidation() {
        assertDoesNotThrow(() -> parser.parse("😀".repeat(500)));
        assertThrows(IllegalArgumentException.class, () -> parser.parse("😀".repeat(501)));
        assertThrows(IllegalArgumentException.class, () -> parser.parse("\uD83D"));
        assertEquals(new ClientCommand.Chat("/other"), new ProtocolCodec().decodeClient(parser.parse("/other").json()));
    }

    @Test void likesApplyDefaultsAndAcceptSignedLongMaximum() {
        var codec = new ProtocolCodec();
        assertEquals(new ClientCommand.Like(1), codec.decodeClient(parser.parse("/like").json()));
        assertEquals(new ClientCommand.Like(7), codec.decodeClient(parser.parse("/like 7").json()));
        assertEquals(new ClientCommand.Like(Long.MAX_VALUE), codec.decodeClient(parser.parse("/like 9223372036854775807").json()));
    }

    @Test void rejectsInvalidLikeCommandsAndRecognizesQuit() {
        for (String input : List.of("/like -1", "/like 0", "/like 9223372036854775808", "/like 1.5", "/like 1 2", "/like nope"))
            assertThrows(IllegalArgumentException.class, () -> parser.parse(input), input);
        assertTrue(parser.parse("/quit").quit());
    }

    @Test void fragmentsEmitOnlyAtFinalAndResetForNextMessage() {
        var fragments = new TextFragments();
        assertTrue(fragments.accept("{\"a\":", false).isEmpty());
        assertEquals(Optional.of("{\"a\":1}"), fragments.accept("1}", true));
        assertEquals(Optional.of("{}"), fragments.accept("{}", true));
    }

    @Test void utf8LimitCountsMultibyteAndSplitSurrogatePairsExactly() {
        var fragments = new TextFragments();
        assertTrue(fragments.accept("a".repeat(8188) + "\uD83D", false).isEmpty());
        assertEquals(8190, fragments.accept("\uDE00", true).orElseThrow().length());
        assertEquals("中".repeat(2730) + "ab", new TextFragments().accept("中".repeat(2730) + "ab", true).orElseThrow());
        assertThrows(IllegalArgumentException.class, () -> new TextFragments().accept("中".repeat(2731), true));
        var tooLarge = new TextFragments();
        tooLarge.accept("a".repeat(8189) + "\uD83D", false);
        assertThrows(IllegalArgumentException.class, () -> tooLarge.accept("\uDE00", true));
    }

    @Test void invalidSurrogateSequencesAndUnterminatedSurrogateAreRejected() {
        assertThrows(IllegalArgumentException.class, () -> new TextFragments().accept("\uDE00", true));
        assertThrows(IllegalArgumentException.class, () -> new TextFragments().accept("\uD83D", true));
        var fragments = new TextFragments();
        fragments.accept("\uD83D", false);
        assertThrows(IllegalArgumentException.class, () -> fragments.accept("x", true));
    }

    @Test void optionsAcceptExplicitWsAndWssAndRejectCredentialsOrInvalidInput() {
        assertEquals("wss://example.org/ws", ClientOptions.parse(new String[]{"--url", "wss://example.org/ws"}).uri().toString());
        assertEquals("ws", ClientOptions.parse(new String[0]).uri().getScheme());
        for (String input : List.of("https://example.org/ws", "ws://user:secret@example.org/ws", "ws:///ws", "ws://example.org/#fragment", "not a url"))
            assertThrows(IllegalArgumentException.class, () -> ClientOptions.parse(new String[]{input}));
        for (String[] args : List.of(new String[]{"--unknown"}, new String[]{"--url"}, new String[]{"ws://a/ws", "ws://b/ws"}))
            assertThrows(IllegalArgumentException.class, () -> ClientOptions.parse(args));
    }

    @Test void helpReturnsWithoutReadingInputOrConnectingAndErrorsDoNotEchoSecrets() {
        var output = new ByteArrayOutputStream();
        InputStream unreadable = new InputStream() { public int read() { fail("help must not read input"); return -1; } };
        assertEquals(0, InteractiveClient.run(new String[]{"--help"}, unreadable, new PrintStream(output), System.err));
        assertTrue(output.toString(StandardCharsets.UTF_8).contains("/like"));
        var errors = new ByteArrayOutputStream();
        assertEquals(2, InteractiveClient.run(new String[]{"ws://user:secret@example.org/ws"}, unreadable, System.out, new PrintStream(errors)));
        assertFalse(errors.toString(StandardCharsets.UTF_8).contains("secret"));
    }

    @Test void listenerPrintsOnceRequestsMoreAndRemoteCloseCompletesWithoutConsole() {
        var output = new ByteArrayOutputStream();
        var session = new ClientSession(new PrintStream(output), System.err, Duration.ofMillis(50));
        var socket = new FakeSocket();
        session.onOpen(socket);
        session.onText(socket, "{\"ok\":", false);
        assertEquals("", output.toString(StandardCharsets.UTF_8));
        session.onText(socket, "true}", true);
        assertEquals("{\"ok\":true}" + System.lineSeparator(), output.toString(StandardCharsets.UTF_8));
        assertEquals(3, socket.requests);
        session.onClose(socket, 1000, "done");
        assertTrue(session.finished().isDone());
        session.sendLine("late");
        assertTrue(socket.sent.isEmpty());
    }

    @Test void invalidCommandsKeepSessionUsableAndQuitSendsNormalClose() {
        var session = new ClientSession(System.out, new PrintStream(new ByteArrayOutputStream()), Duration.ofMillis(50));
        var socket = new FakeSocket();
        session.onOpen(socket);
        session.sendLine("/like -1");
        assertFalse(session.finished().isDone());
        session.sendLine("hello");
        assertEquals(new ClientCommand.Chat("hello"), new ProtocolCodec().decodeClient(socket.sent.getFirst()));
        session.sendLine("/quit");
        assertEquals(1000, socket.closeCode);
        assertTrue(session.finished().isDone());
        session.sendLine("late");
        assertEquals(1, socket.sent.size());
    }

    @Test void oversizedFramesAndWriteTimeoutTerminateAndAbort() {
        var errors = new PrintStream(new ByteArrayOutputStream());
        var session = new ClientSession(System.out, errors, Duration.ofMillis(20));
        var socket = new FakeSocket();
        session.onOpen(socket);
        session.onText(socket, "x".repeat(8193), true);
        assertTrue(session.finished().isDone());
        assertTrue(socket.aborted);
        var stalled = new ClientSession(System.out, errors, Duration.ofMillis(20));
        var blockedSocket = new FakeSocket();
        blockedSocket.write = new CompletableFuture<>();
        stalled.onOpen(blockedSocket);
        assertTimeoutPreemptively(Duration.ofSeconds(1), () -> stalled.sendLine("hello"));
        assertTrue(stalled.finished().isDone());
        assertTrue(blockedSocket.aborted);
    }

    @Test void connectedWriteTimeoutReturnsFailureStatus() {
        var errors = new PrintStream(new ByteArrayOutputStream());
        var session = new ClientSession(System.out, errors, Duration.ofMillis(20));
        var socket = new FakeSocket();
        socket.write = new CompletableFuture<>();
        session.onOpen(socket);
        session.sendLine("hello");
        assertTimeoutPreemptively(Duration.ofSeconds(1), () ->
                assertEquals(1, InteractiveClient.runConnected(InputStream.nullInputStream(), session, errors)));
    }

    @Test void connectedListenerAndFrameFailuresReturnFailureStatusWithoutLeakingDetails() throws Exception {
        for (String cause : List.of("listener", "oversized", "malformed", "binary")) {
            var output = new ByteArrayOutputStream();
            var errors = new PrintStream(output);
            var session = new ClientSession(System.out, errors, Duration.ofMillis(20));
            var socket = new FakeSocket();
            session.onOpen(socket);
            switch (cause) {
                case "listener" -> session.onError(socket, new IOException("secret"));
                case "oversized" -> session.onText(socket, "x".repeat(8193), true);
                case "malformed" -> session.onText(socket, "\uD83D", true);
                case "binary" -> session.onBinary(socket, ByteBuffer.wrap(new byte[]{1}), true);
            }
            assertEquals(1, InteractiveClient.runConnected(InputStream.nullInputStream(), session, errors), cause);
            assertTrue(socket.aborted, cause);
            assertFalse(output.toString(StandardCharsets.UTF_8).contains("secret"));
        }
    }

    @Test void connectedConsoleFailureReturnsFailureStatusWithoutLeakingDetails() {
        var output = new ByteArrayOutputStream();
        var errors = new PrintStream(output);
        var session = new ClientSession(System.out, errors, Duration.ofMillis(20));
        var socket = new FakeSocket();
        session.onOpen(socket);
        InputStream failingInput = new InputStream() {
            @Override public int read() throws IOException { throw new IOException("secret"); }
        };
        assertTimeoutPreemptively(Duration.ofSeconds(1), () ->
                assertEquals(1, InteractiveClient.runConnected(failingInput, session, errors)));
        assertTrue(socket.aborted);
        assertFalse(output.toString(StandardCharsets.UTF_8).contains("secret"));
    }

    @Test void connectedQuitRemoteCloseAndEofReturnSuccessAndInvalidCommandsRemainRecoverable() throws Exception {
        var errors = new PrintStream(new ByteArrayOutputStream());
        for (String cause : List.of("quit", "remote", "eof")) {
            var session = new ClientSession(System.out, errors, Duration.ofMillis(20));
            var socket = new FakeSocket();
            session.onOpen(socket);
            session.sendLine("/like -1");
            assertFalse(session.finished().isDone());
            if (cause.equals("quit")) session.sendLine("/quit");
            if (cause.equals("remote")) session.onClose(socket, 1000, "normal");
            assertTimeoutPreemptively(Duration.ofSeconds(1), () ->
                    assertEquals(0, InteractiveClient.runConnected(InputStream.nullInputStream(), session, errors), cause));
            if (!cause.equals("remote")) assertEquals(1000, socket.closeCode);
        }
    }

    @Test void serverErrorClosesFailWithoutPrintingUntrustedReasons() {
        for (int code : List.of(1008, 1011, 1013)) {
            var bytes = new ByteArrayOutputStream();
            var session = new ClientSession(System.out, new PrintStream(bytes), Duration.ofMillis(20));
            var socket = new FakeSocket();
            session.onOpen(socket);
            session.onClose(socket, code, "secret\nforged diagnostic");
            assertEquals(1, session.finished().join(), "close code " + code);
            String diagnostic = bytes.toString(StandardCharsets.UTF_8);
            assertTrue(diagnostic.contains(Integer.toString(code)));
            assertFalse(diagnostic.contains("secret"));
            assertFalse(diagnostic.contains("forged"));
        }
    }

    @Test void normalClosesAndCompletedLocalShutdownStaySuccessful() {
        for (int code : List.of(1000, 1001)) {
            var bytes = new ByteArrayOutputStream();
            var session = new ClientSession(System.out, new PrintStream(bytes), Duration.ofMillis(20));
            var socket = new FakeSocket();
            session.onOpen(socket);
            session.onClose(socket, code, "secret");
            assertEquals(0, session.finished().join());
            assertEquals("", bytes.toString(StandardCharsets.UTF_8));
        }
        for (boolean quit : List.of(true, false)) {
            var bytes = new ByteArrayOutputStream();
            var session = new ClientSession(System.out, new PrintStream(bytes), Duration.ofMillis(20));
            var socket = new FakeSocket();
            session.onOpen(socket);
            if (quit) session.sendLine("/quit"); else session.stop();
            session.onClose(socket, 1011, "secret");
            assertEquals(0, session.finished().join());
            assertEquals("", bytes.toString(StandardCharsets.UTF_8));
        }
    }

    static class FakeSocket implements WebSocket {
        final List<String> sent = new ArrayList<>();
        CompletableFuture<WebSocket> write = CompletableFuture.completedFuture(this);
        int requests, closeCode;
        boolean aborted;
        public CompletableFuture<WebSocket> sendText(CharSequence text, boolean last) { sent.add(text.toString()); return write; }
        public CompletableFuture<WebSocket> sendBinary(ByteBuffer data, boolean last) { throw new UnsupportedOperationException(); }
        public CompletableFuture<WebSocket> sendPing(ByteBuffer message) { return CompletableFuture.completedFuture(this); }
        public CompletableFuture<WebSocket> sendPong(ByteBuffer message) { return CompletableFuture.completedFuture(this); }
        public CompletableFuture<WebSocket> sendClose(int statusCode, String reason) { closeCode = statusCode; return CompletableFuture.completedFuture(this); }
        public void request(long n) { requests += (int)n; }
        public String getSubprotocol() { return ""; }
        public boolean isOutputClosed() { return closeCode != 0 || aborted; }
        public boolean isInputClosed() { return aborted; }
        public void abort() { aborted = true; }
    }
}
