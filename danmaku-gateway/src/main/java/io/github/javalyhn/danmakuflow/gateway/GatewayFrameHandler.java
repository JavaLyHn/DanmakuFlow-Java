package io.github.javalyhn.danmakuflow.gateway;

import io.github.javalyhn.danmakuflow.protocol.*;
import io.netty.channel.*;
import io.netty.handler.codec.TooLongFrameException;
import io.netty.handler.codec.http.websocketx.*;
import java.util.ArrayDeque;
import java.util.concurrent.RejectedExecutionException;

final class GatewayFrameHandler extends SimpleChannelInboundHandler<WebSocketFrame> {
    private final GatewayService service;
    private final ProtocolCodec codec;
    private final NettyGatewayServer.Settings settings;
    private final ArrayDeque<TextWebSocketFrame> pending = new ArrayDeque<>();
    private long pendingBytes;
    private NettyGatewayClient client;
    private boolean ready;
    GatewayFrameHandler(GatewayService service, ProtocolCodec codec, NettyGatewayServer.Settings settings) {
        this.service = service; this.codec = codec; this.settings = settings;
    }
    @Override public void userEventTriggered(ChannelHandlerContext ctx, Object event) throws Exception {
        if (event instanceof WebSocketServerProtocolHandler.HandshakeComplete) {
            ctx.channel().config().setAutoRead(false);
            client = new NettyGatewayClient(ctx.channel(), ctx.channel().attr(HandshakeValidator.IDENTITY).get(), settings.maxPendingFrames(), settings.maxPendingBytes());
            service.join(client).whenComplete((unused, failure) -> {
                try { ctx.executor().execute(() -> {
                    if (!ctx.channel().isActive()) { service.leave(client); clear(); return; }
                    if (failure != null) { clear(); return; } // Core owns the failure/shutdown close code.
                    ready = true;
                    while (!pending.isEmpty()) {
                        var frame = pending.removeFirst(); pendingBytes -= frame.content().readableBytes();
                        try { command(frame); } finally { frame.release(); }
                    }
                    ctx.channel().config().setAutoRead(true);
                }); } catch (RejectedExecutionException stopped) { service.leave(client); }
            });
        }
        ctx.fireUserEventTriggered(event);
    }
    @Override protected void channelRead0(ChannelHandlerContext ctx, WebSocketFrame frame) {
        if (frame instanceof BinaryWebSocketFrame) { close(ctx, 1003, "Text required"); return; }
        if (!(frame instanceof TextWebSocketFrame text)) return;
        if (client == null) { ctx.close(); return; }
        if (!ready) {
            int size = text.content().readableBytes();
            if (pending.size() >= settings.maxPendingFrames() || size > settings.maxPendingBytes() - pendingBytes) {
                clear(); client.close(1013, "Pending input limit"); return;
            }
            pending.add(text.retain()); pendingBytes += size; return;
        }
        command(text);
    }
    private void command(TextWebSocketFrame frame) {
        try { service.handle(client, codec.decodeClient(frame.text())); }
        catch (ProtocolException invalid) { client.close(1008, "Invalid command"); }
    }
    private void close(ChannelHandlerContext ctx, int code, String reason) {
        if (client != null) client.close(code, reason);
        else ctx.close();
    }
    private void clear() { while (!pending.isEmpty()) pending.removeFirst().release(); pendingBytes = 0; }
    @Override public void channelInactive(ChannelHandlerContext ctx) throws Exception { clear(); if (client != null) service.leave(client); super.channelInactive(ctx); }
    @Override public void handlerRemoved(ChannelHandlerContext ctx) { clear(); }
    @Override public void exceptionCaught(ChannelHandlerContext ctx, Throwable error) {
        close(ctx, error instanceof TooLongFrameException ? 1009 : 1008, "Invalid frame");
    }
}
