package io.github.javalyhn.danmakuflow.gateway;
import io.github.javalyhn.danmakuflow.protocol.ConnectionIdentity;
/** Methods must be nonblocking and thread safe. Identity is stable for the connection.
 * send must not modify the shared immutable frame, including after returning.
 * Returning false signals a full/closed transport. close must be idempotent. */
public interface GatewayClient {
 ConnectionIdentity identity();
 boolean send(byte[] immutableFrame);
 void close(int code, String reason);
}
