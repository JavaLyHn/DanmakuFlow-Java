package io.github.javalyhn.danmakuflow.gateway;

import io.github.javalyhn.danmakuflow.protocol.ConnectionIdentity;
import io.netty.channel.*;
import io.netty.handler.codec.http.*;
import io.netty.util.AttributeKey;

/** Development identity only: deployment must provide authentication and origin restrictions. */
final class HandshakeValidator extends SimpleChannelInboundHandler<FullHttpRequest> {
    static final AttributeKey<ConnectionIdentity> IDENTITY = AttributeKey.valueOf(HandshakeValidator.class, "identity");
    @Override protected void channelRead0(ChannelHandlerContext ctx, FullHttpRequest request) {
        HttpResponseStatus status = null;
        try {
            if (!request.decoderResult().isSuccess()) throw new IllegalArgumentException();
            var query = new QueryStringDecoder(request.uri(), java.nio.charset.StandardCharsets.UTF_8, true, 8192);
            if (!query.path().equals("/ws")) status = HttpResponseStatus.NOT_FOUND;
            else if (!request.method().equals(HttpMethod.GET)) status = HttpResponseStatus.METHOD_NOT_ALLOWED;
            else {
                var params = query.parameters();
                for (String key : new String[]{"uid", "name", "room"})
                    if (params.containsKey(key) && params.get(key).size() != 1) throw new IllegalArgumentException();
                var identity = ConnectionIdentity.parse(value(params, "uid"), value(params, "name"), value(params, "room"));
                // The upgrade handler matches the raw path; retain the validated query verbatim.
                int queryStart = request.uri().indexOf('?');
                request.setUri("/ws" + (queryStart < 0 ? "" : request.uri().substring(queryStart)));
                ctx.channel().attr(IDENTITY).set(identity);
            }
        } catch (RuntimeException invalid) { status = HttpResponseStatus.BAD_REQUEST; }
        if (status == null) ctx.fireChannelRead(request.retain());
        else {
            var response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, status);
            response.headers().setInt(HttpHeaderNames.CONTENT_LENGTH, 0);
            ctx.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
        }
    }
    private static String value(java.util.Map<String, java.util.List<String>> params, String key) {
        var values = params.get(key); return values == null ? null : values.getFirst();
    }
}
