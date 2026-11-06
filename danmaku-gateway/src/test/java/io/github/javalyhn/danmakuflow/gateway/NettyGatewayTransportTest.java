package io.github.javalyhn.danmakuflow.gateway;

import io.github.javalyhn.danmakuflow.protocol.*;
import io.netty.channel.*;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.*;
import io.netty.handler.codec.http.websocketx.*;
import io.netty.util.ReferenceCountUtil;
import org.junit.jupiter.api.*;
import java.net.*;
import java.net.http.*;
import java.nio.ByteBuffer;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Consumer;
import static org.junit.jupiter.api.Assertions.*;

class NettyGatewayTransportTest {
    @Test void encodedRouteUpgradesThroughFullPipelineAndPreservesRawQuery() throws Exception {
        var codec = new ProtocolCodec();
        var service = new GatewayService(e -> CompletableFuture.completedFuture(null),
            new GatewayServiceTest.Bus(), new GatewayServiceTest.Stats(), codec,
            Clock.systemUTC(), UUID::randomUUID, new GatewayService.Limits(5, 10, 10));
        var server = new NettyGatewayServer(service, codec, new NettyGatewayServer.Settings("127.0.0.1", 0, 5, 8, 65536));
        try {
            var address = server.start();
            assertEquals("HTTP/1.1 404 Not Found", upgradeStatus(address, "/%77rong?uid=1&room=r"));
            assertEquals("HTTP/1.1 101 Switching Protocols",
                upgradeStatus(address, "/%77s?uid=1&name=A%2BB&room=r%2Fz"));
        } finally {
            server.close();
            service.closeAsync().toCompletableFuture().get(4, TimeUnit.SECONDS);
        }
        var channel = new EmbeddedChannel(new HandshakeValidator());
        try {
            channel.writeInbound(new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET,
                "/%77s?uid=1&name=A%2BB&room=r%2Fz"));
            FullHttpRequest forwarded = channel.readInbound();
            assertNotNull(forwarded);
            try { assertEquals("/ws?uid=1&name=A%2BB&room=r%2Fz", forwarded.uri()); }
            finally { forwarded.release(); }
            assertEquals("A+B", channel.attr(HandshakeValidator.IDENTITY).get().name());
        } finally { channel.finishAndReleaseAll(); }
    }
    static String upgradeStatus(InetSocketAddress address, String uri) throws Exception {
        try (var socket = new Socket()) {
            socket.connect(address, 2000);
            socket.setSoTimeout(2000);
            var request = "GET " + uri + " HTTP/1.1\r\nHost: 127.0.0.1:" + address.getPort()
                + "\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Version: 13"
                + "\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n";
            socket.getOutputStream().write(request.getBytes(java.nio.charset.StandardCharsets.US_ASCII));
            var reader = new java.io.BufferedReader(new java.io.InputStreamReader(socket.getInputStream(), java.nio.charset.StandardCharsets.US_ASCII));
            return assertDoesNotThrow(reader::readLine, "Validated routes must receive an HTTP response within two seconds");
        }
    }
    @Test void validatesExactRouteIdentityAndDuplicateParameters() {
        for (String uri : List.of("/ws?uid=-1&room=r", "/ws?uid=18446744073709551616&room=r", "/ws?uid=1", "/ws?uid=1&uid=2&room=r", "/ws?uid=1&room=%ZZ"))
            assertEquals(400, response(uri, HttpMethod.GET));
        assertEquals(400, response("/ws?uid=1&room=r" + "&x".repeat(1100) + "&uid=2", HttpMethod.GET));
        assertEquals(404, response("/ws/extra?uid=1&room=r", HttpMethod.GET));
        assertEquals(405, response("/ws?uid=1&room=r", HttpMethod.POST));
        var channel = new EmbeddedChannel(new HandshakeValidator());
        try {
            var request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/ws?uid=18446744073709551615&room=r");
            assertTrue(channel.writeInbound(request));
            assertEquals("18446744073709551615", channel.attr(HandshakeValidator.IDENTITY).get().userId().toString());
            assertEquals("", channel.attr(HandshakeValidator.IDENTITY).get().name());
            ReferenceCountUtil.release(channel.readInbound());
        } finally { channel.finishAndReleaseAll(); }
    }
    int response(String uri, HttpMethod method) {
        var channel = new EmbeddedChannel(new HandshakeValidator());
        try {
            channel.writeInbound(new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, method, uri));
            FullHttpResponse response = channel.readOutbound();
            assertNotNull(response, "Invalid request must receive an HTTP rejection");
            try { return response.status().code(); } finally { response.release(); }
        } finally { channel.finishAndReleaseAll(); }
    }
    @Test void wholeHandshakeDeadlineExpiresWithoutHeaders() {
        var channel = new EmbeddedChannel(new NettyGatewayServer.HandshakeDeadline());
        try {
            channel.advanceTimeBy(5, TimeUnit.SECONDS);
            channel.runScheduledPendingTasks();
            assertFalse(channel.isActive());
        } finally { channel.finishAndReleaseAll(); }
    }
    @Test void completedHandshakeCancelsDeadlineForQuietViewer() {
        var channel = new EmbeddedChannel(new NettyGatewayServer.HandshakeDeadline());
        try {
            channel.pipeline().fireUserEventTriggered(new WebSocketServerProtocolHandler.HandshakeComplete("/ws", new DefaultHttpHeaders(), null));
            channel.advanceTimeBy(6, TimeUnit.SECONDS);
            channel.runScheduledPendingTasks();
            assertTrue(channel.isActive());
        } finally { channel.finishAndReleaseAll(); }
    }
    @Test void slowWritesHaveFrameAndByteCapsAndReleaseOnFailure() {
        var held = new ArrayList<ChannelPromise>();
        var frames = new ArrayList<Object>();
        var channel = new EmbeddedChannel(new ChannelOutboundHandlerAdapter() {
            @Override public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) { frames.add(msg); held.add(promise); }
        });
        try {
            var client = new NettyGatewayClient(channel, ConnectionIdentity.parse("1", "", "r"), 2, 5);
            assertTrue(client.send(new byte[3]));
            assertFalse(client.send(new byte[3]));
            assertTrue(client.send(new byte[2]));
            assertFalse(client.send(new byte[0]));
            held.removeFirst().setFailure(new IllegalStateException());
            ReferenceCountUtil.release(frames.removeFirst());
            assertTrue(client.send(new byte[3]));
        } finally {
            held.forEach(p -> p.tryFailure(new IllegalStateException()));
            frames.forEach(ReferenceCountUtil::release);
            channel.finishAndReleaseAll();
        }
    }
    @Test void loopbackPausesForSubscriptionThenAckGatesFanoutAndCleansUp() throws Exception {
        var ready = new CompletableFuture<Void>();
        var shutdownReady = new CompletableFuture<Void>();
        var subscribed = new CountDownLatch(1);
        var ack = new CompletableFuture<Void>();
        var events = new LinkedBlockingQueue<DanmakuEvent>();
        var listeners = new ConcurrentHashMap<String, Consumer<String>>();
        RoomBus bus = new RoomBus() {
            public CompletionStage<Void> subscribe(String room, Consumer<String> listener) { if (room.equals("failure")) return CompletableFuture.failedFuture(new IllegalStateException()); listeners.put(room, listener); subscribed.countDown(); return room.equals("r") ? ready : room.equals("shutdown") ? shutdownReady : CompletableFuture.completedFuture(null); }
            public CompletionStage<Void> unsubscribe(String room) { listeners.remove(room); return CompletableFuture.completedFuture(null); }
            public CompletionStage<Void> publish(String room, String payload) { listeners.get(room).accept(payload); return CompletableFuture.completedFuture(null); }
        };
        RoomStatisticsStore stats = new RoomStatisticsStore() {
            public CompletionStage<Void> addLikes(String r, long n) { return CompletableFuture.completedFuture(null); }
            public CompletionStage<RoomStats> refresh(String r, String i, int n, Duration t) { return CompletableFuture.completedFuture(new RoomStats(n, 0)); }
            public CompletionStage<Void> remove(String r, String i) { return CompletableFuture.completedFuture(null); }
        };
        var codec = new ProtocolCodec();
        var service = new GatewayService(e -> { events.add(e); return ack; }, bus, stats, codec, Clock.systemUTC(), UUID::randomUUID, new GatewayService.Limits(5, 10, 10));
        var server = new NettyGatewayServer(service, codec, new NettyGatewayServer.Settings("127.0.0.1", 0, 5, 8, 65536));
        var sockets = new ArrayList<WebSocket>();
        try (var http = HttpClient.newHttpClient()) {
            var address = server.start();
            // A second bind on the occupied port must fail and terminate its own event loops.
            try (var failed = new NettyGatewayServer(service, codec, new NettyGatewayServer.Settings("127.0.0.1", address.getPort(), 5, 8, 65536))) {
                assertThrows(IllegalStateException.class, failed::start);
            }
            // Five idle TCP handshakes consume the same admission cap as upgraded clients.
            var idle = new ArrayList<Socket>();
            try {
                for (int i = 0; i < 5; i++) { idle.add(new Socket("127.0.0.1", address.getPort())); int expected = i + 1; GatewayServiceTest.eventually(() -> server.activeConnections() == expected); }
                try (var rejected = new Socket("127.0.0.1", address.getPort())) {
                    rejected.setSoTimeout(2000);
                    assertEquals(-1, rejected.getInputStream().read());
                }
            } finally { for (var socket : idle) socket.close(); }
            GatewayServiceTest.eventually(() -> server.activeConnections() == 0);
            var a = new Listener(); var b = new Listener(); var c = new Listener();
            var ws = connect(http, address, "r", a); sockets.add(ws);
            assertTrue(subscribed.await(2, TimeUnit.SECONDS));
            ws.sendText("{\"type\":101,\"data\":{\"content\":\"hello\"}}", true).get(2, TimeUnit.SECONDS);
            assertNull(events.poll(100, TimeUnit.MILLISECONDS));
            ready.complete(null);
            assertEquals("hello", events.poll(2, TimeUnit.SECONDS).content());
            sockets.add(connect(http, address, "r", b));
            sockets.add(connect(http, address, "other", c));
            assertNull(a.text.poll(100, TimeUnit.MILLISECONDS));
            ack.complete(null);
            String first = a.text.poll(2, TimeUnit.SECONDS);
            assertNotNull(first); assertEquals(first, b.text.poll(2, TimeUnit.SECONDS));
            assertTrue(first.contains("\"user_id\":18446744073709551615"));
            assertNull(c.text.poll(100, TimeUnit.MILLISECONDS));
            ws.sendText("{\"type\":101,", false).get(2, TimeUnit.SECONDS);
            ws.sendText("\"data\":{\"content\":\"fragment\"}}", true).get(2, TimeUnit.SECONDS);
            assertEquals("fragment", events.poll(2, TimeUnit.SECONDS).content());
            ws.sendClose(1000, "done").get(2, TimeUnit.SECONDS);
            assertEquals(1000, a.closed.get(2, TimeUnit.SECONDS));
            sockets.get(1).sendBinary(ByteBuffer.wrap(new byte[]{1}), true).get(2, TimeUnit.SECONDS);
            assertEquals(1003, b.closed.get(2, TimeUnit.SECONDS));
            sockets.get(2).sendText("x".repeat(5000), false).get(2, TimeUnit.SECONDS);
            sockets.get(2).sendText("x".repeat(5000), true).get(2, TimeUnit.SECONDS);
            assertEquals(1009, c.closed.get(2, TimeUnit.SECONDS));
            var invalid = new Listener();
            var invalidSocket = connect(http, address, "invalid", invalid); sockets.add(invalidSocket);
            invalidSocket.sendText("{}", true).get(2, TimeUnit.SECONDS);
            assertEquals(1008, invalid.closed.get(2, TimeUnit.SECONDS));
            var failure = new Listener();
            sockets.add(connect(http, address, "failure", failure));
            assertEquals(1011, failure.closed.get(2, TimeUnit.SECONDS));
            var shutdown = new Listener();
            sockets.add(connect(http, address, "shutdown", shutdown));
            GatewayServiceTest.eventually(() -> listeners.containsKey("shutdown"));
            var stopped = service.closeAsync();
            assertEquals(1001, shutdown.closed.get(2, TimeUnit.SECONDS));
            shutdownReady.complete(null);
            stopped.toCompletableFuture().get(4, TimeUnit.SECONDS);
        } finally {
            sockets.forEach(WebSocket::abort); ready.complete(null); shutdownReady.complete(null); ack.complete(null);
            server.close(); service.closeAsync().toCompletableFuture().get(4, TimeUnit.SECONDS);
        }
        assertTrue(listeners.isEmpty());
    }
    static WebSocket connect(HttpClient http, InetSocketAddress address, String room, Listener listener) throws Exception {
        return http.newWebSocketBuilder().connectTimeout(Duration.ofSeconds(2)).buildAsync(URI.create("ws://127.0.0.1:" + address.getPort() + "/ws?uid=18446744073709551615&room=" + room), listener).get(2, TimeUnit.SECONDS);
    }
    static class Listener implements WebSocket.Listener {
        final BlockingQueue<String> text = new LinkedBlockingQueue<>();
        final CompletableFuture<Integer> closed = new CompletableFuture<>();
        final StringBuilder partial = new StringBuilder();
        public void onOpen(WebSocket ws) { ws.request(1); }
        public CompletionStage<?> onText(WebSocket ws, CharSequence data, boolean last) { partial.append(data); if(last) { text.add(partial.toString()); partial.setLength(0); } ws.request(1); return null; }
        public CompletionStage<?> onClose(WebSocket ws, int code, String reason) { closed.complete(code); return null; }
        public void onError(WebSocket ws, Throwable error) { closed.completeExceptionally(error); }
    }
}
