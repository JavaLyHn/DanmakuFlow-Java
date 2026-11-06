package io.github.javalyhn.danmakuflow.gateway;

import io.github.javalyhn.danmakuflow.protocol.ConnectionIdentity;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.handler.codec.http.websocketx.*;
import java.util.concurrent.atomic.AtomicBoolean;

final class NettyGatewayClient implements GatewayClient {
    private final Channel channel;
    private final ConnectionIdentity identity;
    private final int maxFrames;
    private final long maxBytes;
    private int pendingFrames;
    private long pendingBytes;
    private final AtomicBoolean closing = new AtomicBoolean();
    NettyGatewayClient(Channel channel, ConnectionIdentity identity, int maxFrames, long maxBytes) {
        this.channel = channel; this.identity = identity; this.maxFrames = maxFrames; this.maxBytes = maxBytes;
    }
    @Override public ConnectionIdentity identity() { return identity; }
    @Override public boolean send(byte[] bytes) {
        synchronized (this) {
            if (closing.get() || !channel.isActive() || pendingFrames >= maxFrames || bytes.length > maxBytes - pendingBytes) return false;
            pendingFrames++; pendingBytes += bytes.length;
        }
        // Copy protects the core's shared immutable array from transport buffer mutation.
        TextWebSocketFrame frame = null;
        try {
            frame = new TextWebSocketFrame(Unpooled.copiedBuffer(bytes));
            channel.writeAndFlush(frame).addListener(f -> release(bytes.length));
            return true;
        } catch (RuntimeException failure) {
            if (frame != null) frame.release();
            release(bytes.length); return false;
        }
    }
    private synchronized void release(int length) { pendingFrames--; pendingBytes -= length; }
    @Override public void close(int code, String reason) {
        if (!closing.compareAndSet(false, true)) return;
        channel.writeAndFlush(new CloseWebSocketFrame(code, reason)).addListener(ChannelFutureListener.CLOSE);
    }
}
