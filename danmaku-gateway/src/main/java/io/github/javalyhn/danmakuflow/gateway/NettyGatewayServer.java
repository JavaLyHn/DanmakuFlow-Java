package io.github.javalyhn.danmakuflow.gateway;

import io.github.javalyhn.danmakuflow.protocol.ProtocolCodec;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.*;
import io.netty.handler.codec.http.websocketx.*;
import java.net.InetSocketAddress;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/** Owns transport resources only. Start/close are bounded blocking lifecycle calls, not event-loop calls. */
public final class NettyGatewayServer implements AutoCloseable {
    public record Settings(String host, int port, int maxConnections, int maxPendingFrames, long maxPendingBytes) {
        public Settings {
            if (host == null || host.isBlank() || port < 0 || port > 65535 || maxConnections < 1 || maxPendingFrames < 1 || maxPendingBytes < 1)
                throw new IllegalArgumentException("Invalid gateway transport settings");
        }
        public static Settings defaults() { return new Settings("127.0.0.1", 8080, 100000, 64, 524288); }
    }
    private final GatewayService service;
    private final ProtocolCodec codec;
    private final Settings settings;
    private final Set<Channel> children = ConcurrentHashMap.newKeySet();
    private final AtomicInteger accepted = new AtomicInteger();
    private EventLoopGroup acceptors;
    private EventLoopGroup workers;
    private Channel listener;
    private boolean closed;
    public NettyGatewayServer(GatewayService service, ProtocolCodec codec, Settings settings) {
        this.service = java.util.Objects.requireNonNull(service); this.codec = java.util.Objects.requireNonNull(codec); this.settings = java.util.Objects.requireNonNull(settings);
    }
    public synchronized InetSocketAddress start() throws InterruptedException {
        if (closed || acceptors != null) throw new IllegalStateException("Server already started or closed");
        try {
            acceptors = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
            workers = new MultiThreadIoEventLoopGroup(2, NioIoHandler.newFactory());
            var bind = new ServerBootstrap().group(acceptors, workers).channel(NioServerSocketChannel.class)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override protected void initChannel(SocketChannel channel) {
                        children.add(channel);
                        int count = accepted.incrementAndGet();
                        channel.closeFuture().addListener(f -> { children.remove(channel); accepted.decrementAndGet(); });
                        if (count > settings.maxConnections()) { channel.close(); return; }
                        channel.pipeline().addLast(new HttpServerCodec(), new HttpObjectAggregator(8192),
                            new HandshakeValidator(), new WebSocketServerProtocolHandler(WebSocketServerProtocolConfig.newBuilder()
                                .websocketPath("/ws").checkStartsWith(true).maxFramePayloadLength(8192).handshakeTimeoutMillis(5000).build()),
                            new HandshakeDeadline(), new WebSocketFrameAggregator(8192), new GatewayFrameHandler(service, codec, settings));
                    }
                }).bind(settings.host(), settings.port());
            listener = bind.channel();
            if (!bind.await(5, TimeUnit.SECONDS)) throw new IllegalStateException("Bind timed out");
            if (!bind.isSuccess()) throw new IllegalStateException("Gateway bind failed", bind.cause());
            return (InetSocketAddress) listener.localAddress();
        } catch (RuntimeException | InterruptedException failure) { close(); throw failure; }
    }
    public int activeConnections() { return accepted.get(); }
    @Override public synchronized void close() {
        if (closed) return;
        closed = true;
        if (listener != null) listener.close().awaitUninterruptibly(2000);
        for (Channel channel : children) channel.close();
        var a = acceptors == null ? null : acceptors.shutdownGracefully(0, 2, TimeUnit.SECONDS);
        var w = workers == null ? null : workers.shutdownGracefully(0, 2, TimeUnit.SECONDS);
        boolean ok = (a == null || a.awaitUninterruptibly(3000)) & (w == null || w.awaitUninterruptibly(3000));
        if (!ok) throw new IllegalStateException("Gateway event loop shutdown timed out");
    }
    static final class HandshakeDeadline extends ChannelInboundHandlerAdapter {
        private io.netty.util.concurrent.ScheduledFuture<?> deadline;
        @Override public void channelActive(ChannelHandlerContext ctx) throws Exception {
            deadline = ctx.executor().schedule(() -> { ctx.close(); }, 5, TimeUnit.SECONDS);
            super.channelActive(ctx);
        }
        @Override public void userEventTriggered(ChannelHandlerContext ctx, Object event) throws Exception {
            if (event instanceof WebSocketServerProtocolHandler.HandshakeComplete) cancel();
            super.userEventTriggered(ctx, event);
        }
        @Override public void channelInactive(ChannelHandlerContext ctx) throws Exception { cancel(); super.channelInactive(ctx); }
        private void cancel() { if (deadline != null) deadline.cancel(false); }
    }
}
