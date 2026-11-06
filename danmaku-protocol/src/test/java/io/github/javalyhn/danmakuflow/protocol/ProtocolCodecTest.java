package io.github.javalyhn.danmakuflow.protocol;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.jupiter.params.provider.CsvSource;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;
import java.math.BigInteger;
import java.time.Instant;
import java.util.UUID;
import static org.junit.jupiter.api.Assertions.*;

class ProtocolCodecTest {
    private final ProtocolCodec codec = new ProtocolCodec();
    private final JsonMapper json = JsonMapper.builder().build();
    private static final BigInteger MAX = new BigInteger("18446744073709551615");
    private static final UUID EVENT = UUID.fromString("89cb03cc-c45c-48a9-80d6-13ba903420fa");
    private static final Instant TIME = Instant.parse("2026-11-06T01:02:03.123456789Z");

    @ParameterizedTest @ValueSource(strings={"0", "18446744073709551615"})
    void unsignedIdentityAndEventRemainExact(String uid) {
        var identity = ConnectionIdentity.parse(uid, null, "room");
        assertEquals("", identity.name());
        var event = codec.newEvent(identity, new ClientCommand.Chat("hello"), TIME, EVENT);
        assertEquals(BigInteger.ZERO, event.id());
        assertEquals(new BigInteger(uid), event.userId());
        assertEquals(EVENT, event.eventId());
        JsonNode body = json.readTree(codec.encodeDanmaku(event)).get("data");
        assertTrue(body.get("user_id").isIntegralNumber());
        assertEquals(new BigInteger(uid), body.get("user_id").bigIntegerValue());
        assertEquals(event, codec.decodeStoredEvent(codec.encodeStoredEvent(event)));
    }

    @ParameterizedTest @ValueSource(strings={"", "-1", "+1", "1.0", "1e1", " 1", "1 ", "١", "18446744073709551616"})
    void rejectsInvalidIdentity(String uid) {
        assertThrows(ProtocolException.class, () -> ConnectionIdentity.parse(uid, "", "room"));
    }

    @Test void constructorsEnforceIdentityAndRoomBounds() {
        assertThrows(ProtocolException.class, () -> ConnectionIdentity.parse(null, "", "r"));
        assertThrows(ProtocolException.class, () -> new ConnectionIdentity(BigInteger.valueOf(-1), "", "r"));
        assertThrows(ProtocolException.class, () -> new ConnectionIdentity(MAX.add(BigInteger.ONE), "", "r"));
        assertThrows(ProtocolException.class, () -> new ConnectionIdentity(null, "", "r"));
        assertThrows(ProtocolException.class, () -> ConnectionIdentity.parse("0", "", ""));
        assertThrows(ProtocolException.class, () -> ConnectionIdentity.parse("0", "", null));
        assertEquals("😀".repeat(50), ConnectionIdentity.parse("0", null, "😀".repeat(50)).roomId());
        assertThrows(ProtocolException.class, () -> ConnectionIdentity.parse("0", "", "😀".repeat(51)));
        assertThrows(ProtocolException.class, () -> ConnectionIdentity.parse("0", "", "\uD800"));
    }

    @Test void acceptsUnicodeAtContentBoundaryAndRejectsBeyond() {
        String content = "😀".repeat(500);
        assertEquals(new ClientCommand.Chat(content), codec.decodeClient("{\"type\":101,\"data\":{\"content\":" + json.writeValueAsString(content) + "}}"));
        assertThrows(ProtocolException.class, () -> new ClientCommand.Chat(content + "x"));
        assertThrows(ProtocolException.class, () -> new ClientCommand.Chat(null));
        assertEquals("", new ClientCommand.Chat("").content());
        assertThrows(ProtocolException.class, () -> new ClientCommand.Chat("\uD800"));
        assertThrows(ProtocolException.class, () -> new ClientCommand.Chat("\uDC00"));
        assertThrows(ProtocolException.class, () -> codec.decodeClient("{\"type\":101,\"data\":{\"content\":\"\\ud800\"}}"));
    }

    @Test void ignoresSpoofedServerMetadataAndUnknownFields() {
        var chat = (ClientCommand.Chat) codec.decodeClient("""
                {"type":101,"room_id":"spoof","extra":true,"data":{"content":"hello","user_id":9,"id":99,"room_id":"spoof","send_time":"bad","unknown":[]}}
                """);
        var event = codec.newEvent(ConnectionIdentity.parse("7", "name", "real"), chat, TIME, EVENT);
        assertEquals("real", event.roomId());
        assertEquals(BigInteger.valueOf(7), event.userId());
        assertEquals(BigInteger.ZERO, event.id());
        assertEquals(TIME, event.sendTime());
    }

    @ParameterizedTest @ValueSource(strings={"{\"type\":103}","{\"type\":103,\"data\":null}","{\"type\":103,\"data\":{}}","{\"type\":103,\"data\":{\"count\":null}}","{\"type\":103,\"data\":{\"count\":0}}"})
    void defaultsMissingNullAndZeroLikesToOne(String input) {
        assertEquals(new ClientCommand.Like(1), codec.decodeClient(input));
    }

    @ParameterizedTest @ValueSource(strings={"1", "10", "9223372036854775807"})
    void acceptsSignedLongLikeRange(String count) {
        assertEquals(new ClientCommand.Like(Long.parseLong(count)), codec.decodeClient("{\"type\":103,\"data\":{\"count\":" + count + "}}"));
    }

    // Intentional Go compatibility fix: Redis INCRBY cannot safely receive unsigned overflow or invalid counts.
    @ParameterizedTest @ValueSource(strings={"-1", "1.0", "1e0", "\"1\"", "9223372036854775808", "18446744073709551615", "true"})
    void rejectsLikesThatCannotBeRedisSignedIncrements(String count) {
        assertThrows(ProtocolException.class, () -> codec.decodeClient("{\"type\":103,\"data\":{\"count\":" + count + "}}"));
    }

    @Test void directLikeAndStatsRejectNegativeValues() {
        assertEquals(1, new ClientCommand.Like(0).count());
        assertThrows(ProtocolException.class, () -> new ClientCommand.Like(-1));
        assertThrows(ProtocolException.class, () -> new RoomStats(-1, 0));
        assertThrows(ProtocolException.class, () -> new RoomStats(0, -1));
    }

    @Test void outgoingEnvelopeUsesLegacyNamesAndNumericTypes() {
        var event = new DanmakuEvent(EVENT, MAX, "room", MAX, "hello", TIME);
        JsonNode envelope = json.readTree(codec.encodeDanmaku(event));
        assertEquals(101, envelope.get("type").intValue());
        assertEquals("room", envelope.get("room_id").stringValue());
        JsonNode data = envelope.get("data");
        assertEquals(5, data.size());
        assertEquals(MAX, data.get("id").bigIntegerValue());
        assertEquals(MAX, data.get("user_id").bigIntegerValue());
        assertEquals("room", data.get("room_id").stringValue());
        assertEquals("hello", data.get("content").stringValue());
        assertEquals("2026-11-06T01:02:03.123456789Z", data.get("send_time").stringValue());
        JsonNode stats = json.readTree(codec.encodeStats("room", new RoomStats(3, 7)));
        assertEquals(102, stats.get("type").intValue());
        assertEquals("room", stats.get("room_id").stringValue());
        assertEquals(3, stats.get("data").get("online").longValue());
        assertEquals(7, stats.get("data").get("likes").longValue());
    }

    @Test void storedBodyKeepsStableDedupIdentityAndNormalizesOffset() {
        String stored = """
                {"event_id":"89cb03cc-c45c-48a9-80d6-13ba903420fa","id":18446744073709551615,"room_id":"r","user_id":0,"content":"x","send_time":"2026-11-06T09:02:03.123456789+08:00"}
                """;
        var decoded = codec.decodeStoredEvent(stored);
        assertEquals(EVENT, decoded.eventId());
        assertEquals(TIME, decoded.sendTime());
        assertEquals(MAX, decoded.id());
        JsonNode output = json.readTree(codec.encodeStoredEvent(decoded));
        assertEquals(6, output.size());
        assertEquals(EVENT.toString(), output.get("event_id").stringValue());
        assertEquals("2026-11-06T01:02:03.123456789Z", output.get("send_time").stringValue());
        assertEquals(decoded, codec.decodeStoredEvent(codec.encodeStoredEvent(decoded)));
        assertThrows(ProtocolException.class, () -> codec.decodeStoredEvent(stored.replace("\"event_id\":\"89cb03cc-c45c-48a9-80d6-13ba903420fa\",", "")));
    }

    @Test void eventsEnforceMysqlDatesAndRequiredFields() {
        assertThrows(ProtocolException.class, () -> new DanmakuEvent(null, BigInteger.ZERO, "r", MAX, "", TIME));
        assertThrows(ProtocolException.class, () -> new DanmakuEvent(EVENT, null, "r", MAX, "", TIME));
        assertThrows(ProtocolException.class, () -> new DanmakuEvent(EVENT, MAX.add(BigInteger.ONE), "r", MAX, "", TIME));
        assertThrows(ProtocolException.class, () -> new DanmakuEvent(EVENT, BigInteger.ZERO, "r", MAX, "", null));
        for (String time : new String[]{"0999-12-31T23:59:59Z", "+10000-01-01T00:00:00Z"})
            assertThrows(ProtocolException.class, () -> new DanmakuEvent(EVENT, BigInteger.ZERO, "r", MAX, "", Instant.parse(time)));
        for (String time : new String[]{"1000-01-01T00:00:00Z", "9999-12-31T23:59:59.999999999Z"})
            assertEquals(Instant.parse(time), new DanmakuEvent(EVENT, BigInteger.ZERO, "r", MAX, "", Instant.parse(time)).sendTime());
    }

    @ParameterizedTest @ValueSource(strings={"null", "[]", "{}", "{\"type\":999}", "{\"type\":\"101\"}", "{\"type\":101.0}", "{\"type\":101}", "{\"type\":101,\"data\":null}", "{\"type\":101,\"data\":[]}", "{\"type\":101,\"data\":{}}", "{\"type\":101,\"data\":{\"content\":3}}", "{\"type\":103,\"data\":1}", "{", "{\"type\":103} {}", "{\"type\":103} true"})
    void rejectsMalformedAndInvalidCommands(String input) {
        assertThrows(ProtocolException.class, () -> codec.decodeClient(input));
    }

    @Test void rejectsFrameBytesAndDeepUnknownFieldsWithoutLeakingInput() {
        String secret = "PRIVATE_PAYLOAD";
        ProtocolException malformed = assertThrows(ProtocolException.class, () -> codec.decodeClient("{" + secret));
        assertFalse(malformed.getMessage().contains(secret));
        assertThrows(ProtocolException.class, () -> codec.decodeClient(null));
        String oversized = "{\"type\":103,\"unknown\":\"" + "界".repeat(2800) + "\"}";
        assertTrue(oversized.length() < 8192);
        assertThrows(ProtocolException.class, () -> codec.decodeClient(oversized));
        assertThrows(ProtocolException.class, () -> codec.decodeClient("{\"type\":103,\"unknown\":" + "[".repeat(100) + "0" + "]".repeat(100) + "}"));
        assertThrows(ProtocolException.class, () -> codec.decodeStoredEvent("{} {}"));
    }

    @ParameterizedTest @ValueSource(strings={"\"1\"", "1.0", "-1", "18446744073709551616"})
    void storedUnsignedNumbersRejectCoercion(String id) {
        String stored = "{\"event_id\":\"" + EVENT + "\",\"id\":" + id + ",\"room_id\":\"r\",\"user_id\":0,\"content\":\"x\",\"send_time\":\"2026-11-06T01:02:03Z\"}";
        assertThrows(ProtocolException.class, () -> codec.decodeStoredEvent(stored));
    }

    @ParameterizedTest @ValueSource(strings={
            "2026-11-06T24:00:00Z", "2026-11-06T01:02:03+08:00:30",
            "2026-11-06T01:02:03+08:00:00", "2026-11-06T01:02:03+24:00",
            "2026-11-06T01:02:03-24:00", "2026-11-06T01:02:03+01:60",
            "2026-11-06T01:02:03-01:60", "2026-11-06T01:02:03+8:00",
            "2026-11-06t01:02:03Z", "2026-11-06T01:02:03z",
            "2026-11-06T01:02:03.1234567890Z", "2026-11-06T01:02:03.Z",
            "2026-11-06T01:02:03,5Z", "2026-11-06T01:02:03",
            "2026-02-30T01:02:03Z", "2025-02-29T01:02:03Z",
            "2016-12-31T23:59:60Z"})
    void storedTimestampsRejectOutsideGoCompatibleProfile(String timestamp) {
        ProtocolException error = assertThrows(ProtocolException.class,
                () -> codec.decodeStoredEvent(storedWithTimestamp(timestamp)));
        assertFalse(error.getMessage().contains(timestamp));
        assertNull(error.getCause());
    }

    @ParameterizedTest @CsvSource({
            "2026-11-06T01:02:03.123456789+23:59, 2026-11-05T01:03:03.123456789Z",
            "2026-11-06T01:02:03.123456789-23:59, 2026-11-07T01:01:03.123456789Z",
            "2024-02-29T23:59:59.1+19:00, 2024-02-29T04:59:59.100Z"})
    void storedTimestampsNormalizeOffsetsBeyondJavaZoneOffsetLimit(String timestamp, String expected) {
        var event = codec.decodeStoredEvent(storedWithTimestamp(timestamp));
        assertEquals(Instant.parse(expected), event.sendTime());
        assertEquals(expected, json.readTree(codec.encodeStoredEvent(event)).get("send_time").stringValue());
    }

    @ParameterizedTest @ValueSource(strings={"1000-01-01T00:00:00+23:59", "9999-12-31T23:59:59-23:59"})
    void storedOffsetsCannotShiftTimestampOutsideMysqlUtcRange(String timestamp) {
        assertThrows(ProtocolException.class, () -> codec.decodeStoredEvent(storedWithTimestamp(timestamp)));
    }

    private static String storedWithTimestamp(String timestamp) {
        return "{\"event_id\":\"89cb03cc-c45c-48a9-80d6-13ba903420fa\",\"id\":0,\"room_id\":\"r\",\"user_id\":0,\"content\":\"x\",\"send_time\":\"" + timestamp + "\"}";
    }
}
