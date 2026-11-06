package io.github.javalyhn.danmakuflow.tools;

import io.github.javalyhn.danmakuflow.protocol.ProtocolCodec;
import java.util.Optional;

/** Bounded UTF-8 accounting across WebSocket callback and UTF-16 surrogate boundaries. */
final class TextFragments {
    private final StringBuilder text = new StringBuilder();
    private int bytes;
    private boolean pendingHigh;

    Optional<String> accept(CharSequence chunk, boolean last) {
        for (int index = 0; index < chunk.length(); index++) {
            char c = chunk.charAt(index);
            if (pendingHigh) {
                if (!Character.isLowSurrogate(c)) throw invalidUnicode();
                bytes += 4;
                pendingHigh = false;
            } else if (Character.isHighSurrogate(c)) {
                pendingHigh = true;
            } else {
                if (Character.isLowSurrogate(c)) throw invalidUnicode();
                bytes += c < 0x80 ? 1 : c < 0x800 ? 2 : 3;
            }
            if (bytes > ProtocolCodec.MAX_FRAME_BYTES)
                throw new IllegalArgumentException("Server text exceeds 8192 UTF-8 bytes");
            text.append(c);
        }
        if (!last) return Optional.empty();
        if (pendingHigh) throw invalidUnicode();
        String complete = text.toString();
        text.setLength(0);
        bytes = 0;
        return Optional.of(complete);
    }

    private static IllegalArgumentException invalidUnicode() {
        return new IllegalArgumentException("Server text contains invalid Unicode");
    }
}
