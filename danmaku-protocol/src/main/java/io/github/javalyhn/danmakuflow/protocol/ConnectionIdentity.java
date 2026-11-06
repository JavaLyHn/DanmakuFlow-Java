package io.github.javalyhn.danmakuflow.protocol;

import java.math.BigInteger;

public record ConnectionIdentity(BigInteger userId, String name, String roomId) {
    public ConnectionIdentity {
        Validation.uint64(userId);
        name = Validation.text(name == null ? "" : name, Integer.MAX_VALUE, false);
        Validation.room(roomId);
    }

    public static ConnectionIdentity parse(String uid, String name, String room) {
        if (uid == null || uid.isEmpty()) throw new ProtocolException("Required user id missing");
        for (int i = 0; i < uid.length(); i++)
            if (uid.charAt(i) < '0' || uid.charAt(i) > '9')
                throw new ProtocolException("User id must contain decimal ASCII digits");
        return new ConnectionIdentity(Validation.uint64(new BigInteger(uid)), name, room);
    }
}
