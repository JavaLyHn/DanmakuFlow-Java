package io.github.javalyhn.danmakuflow.protocol;

/** Invalid untrusted protocol input. Messages never include the input payload. */
public final class ProtocolException extends IllegalArgumentException {
    public ProtocolException(String message) { super(message); }
}
