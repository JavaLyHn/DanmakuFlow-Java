package io.github.javalyhn.danmakuflow.protocol;

import java.math.BigInteger;
import java.time.Instant;
import java.util.UUID;

/** eventId is the stable persistence deduplication key; id is zero until assigned by storage. */
public record DanmakuEvent(UUID eventId, BigInteger id, String roomId, BigInteger userId,
                           String content, Instant sendTime) {
    private static final Instant MIN_TIME = Instant.parse("1000-01-01T00:00:00Z");
    private static final Instant MAX_TIME = Instant.parse("9999-12-31T23:59:59.999999999Z");

    public DanmakuEvent {
        if (eventId == null) throw new ProtocolException("Required event id missing");
        Validation.uint64(id);
        Validation.uint64(userId);
        Validation.room(roomId);
        Validation.content(content);
        if (sendTime == null || sendTime.isBefore(MIN_TIME) || sendTime.isAfter(MAX_TIME))
            throw new ProtocolException("Timestamp outside MySQL supported range");
    }
}
