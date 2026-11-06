package io.github.javalyhn.danmakuflow.gateway;

import io.netty.channel.*;
import io.netty.handler.codec.http.websocketx.WebSocketFrame;
import io.netty.util.ReferenceCountUtil;
import java.nio.channels.ClosedChannelException;

/** Event-loop-confined budget shared by application and protocol-generated frames. */
final class OutboundFrameBudget extends ChannelOutboundHandlerAdapter {
    private final int maxFrames;
    private final long maxBytes;
    private int pendingFrames;
    private long pendingBytes;
    private boolean closing;

    OutboundFrameBudget(int maxFrames, long maxBytes) {
        this.maxFrames = maxFrames;
        this.maxBytes = maxBytes;
    }

    @Override public void write(ChannelHandlerContext ctx, Object message, ChannelPromise promise) {
        if (!(message instanceof WebSocketFrame frame)) {
            ctx.write(message, promise);
            return;
        }
        int bytes = frame.content().readableBytes();
        if (closing || pendingFrames >= maxFrames || bytes > maxBytes - pendingBytes) {
            closing = true;
            ReferenceCountUtil.release(frame);
            promise.tryFailure(new ClosedChannelException());
            // Close below the WebSocket protocol handler: a congested peer cannot flush a close frame.
            ctx.close();
            return;
        }
        pendingFrames++;
        pendingBytes += bytes;
        ChannelPromise tracked = promise.unvoid();
        tracked.addListener(f -> { pendingFrames--; pendingBytes -= bytes; });
        ctx.write(message, tracked);
    }
}
