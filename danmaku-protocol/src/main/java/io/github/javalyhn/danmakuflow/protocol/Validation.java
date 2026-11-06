package io.github.javalyhn.danmakuflow.protocol;

import java.math.BigInteger;

final class Validation {
    private static final BigInteger UINT64_MAX = new BigInteger("18446744073709551615");
    private Validation() {}

    static BigInteger uint64(BigInteger value) {
        if (value == null || value.signum() < 0 || value.compareTo(UINT64_MAX) > 0)
            throw new ProtocolException("Expected unsigned 64-bit integer");
        return value;
    }

    static String text(String value, int maximum, boolean nonempty) {
        if (value == null || (nonempty && value.isEmpty()))
            throw new ProtocolException("Required text missing");
        int points = 0;
        for (int index = 0; index < value.length(); index++) {
            char c = value.charAt(index);
            if (Character.isHighSurrogate(c)) {
                if (++index >= value.length() || !Character.isLowSurrogate(value.charAt(index)))
                    throw new ProtocolException("Invalid Unicode text");
            } else if (Character.isLowSurrogate(c)) {
                throw new ProtocolException("Invalid Unicode text");
            }
            if (++points > maximum) throw new ProtocolException("Text exceeds code point limit");
        }
        return value;
    }

    static String room(String value) { return text(value, ProtocolCodec.MAX_ROOM_CODE_POINTS, true); }
    static String content(String value) { return text(value, ProtocolCodec.MAX_CONTENT_CODE_POINTS, false); }
}
