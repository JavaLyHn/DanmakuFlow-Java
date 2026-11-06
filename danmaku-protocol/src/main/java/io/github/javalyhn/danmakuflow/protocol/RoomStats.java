package io.github.javalyhn.danmakuflow.protocol;

public record RoomStats(long online, long likes) {
    public RoomStats {
        if (online < 0 || likes < 0) throw new ProtocolException("Room statistics must be nonnegative");
    }
}
