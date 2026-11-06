package io.github.javalyhn.danmakuflow.protocol;

import tools.jackson.core.json.JsonFactory;
import tools.jackson.core.StreamReadConstraints;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ObjectNode;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.ResolverStyle;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Thread-safe JSON boundary for the legacy WebSocket envelopes and Java Kafka event body. */
public final class ProtocolCodec {
    public static final int MAX_FRAME_BYTES = 8192;
    public static final int MAX_CONTENT_CODE_POINTS = 500;
    public static final int MAX_ROOM_CODE_POINTS = 50;
    private static final Pattern STORED_TIMESTAMP = Pattern.compile(
            "([0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(?:\\.[0-9]{1,9})?)"
                    + "(Z|([+-])([0-9]{2}):([0-9]{2}))");
    private static final DateTimeFormatter LOCAL_TIMESTAMP = DateTimeFormatter.ISO_LOCAL_DATE_TIME
            .withResolverStyle(ResolverStyle.STRICT);
    private final JsonMapper mapper;

    public ProtocolCodec() {
        mapper = JsonMapper.builder(JsonFactory.builder().streamReadConstraints(
                StreamReadConstraints.builder().maxNestingDepth(64).maxStringLength(MAX_FRAME_BYTES)
                        .maxNumberLength(64).build()).build())
                .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS).build();
    }

    public ClientCommand decodeClient(String json) {
        JsonNode root = read(json);
        JsonNode type = root.get("type");
        if (type == null || !type.isIntegralNumber()) throw new ProtocolException("Invalid command type");
        BigInteger code = type.bigIntegerValue();
        JsonNode data = root.get("data");
        if (code.equals(BigInteger.valueOf(101))) {
            object(data);
            return new ClientCommand.Chat(string(data, "content"));
        }
        if (code.equals(BigInteger.valueOf(103))) {
            if (data == null || data.isNull()) return new ClientCommand.Like(1);
            object(data);
            JsonNode count = data.get("count");
            if (count == null || count.isNull()) return new ClientCommand.Like(1);
            if (!count.isIntegralNumber()) throw new ProtocolException("Invalid like count");
            BigInteger value = count.bigIntegerValue();
            if (value.signum() < 0 || value.compareTo(BigInteger.valueOf(Long.MAX_VALUE)) > 0)
                throw new ProtocolException("Like count outside signed 64-bit range");
            return new ClientCommand.Like(value.longValueExact());
        }
        throw new ProtocolException("Unknown command type");
    }

    public DanmakuEvent newEvent(ConnectionIdentity identity, ClientCommand.Chat chat, Instant now, UUID eventId) {
        if (identity == null || chat == null) throw new ProtocolException("Required event input missing");
        return new DanmakuEvent(eventId, BigInteger.ZERO, identity.roomId(), identity.userId(), chat.content(), now);
    }

    public String encodeDanmaku(DanmakuEvent event) {
        if (event == null) throw new ProtocolException("Required event missing");
        ObjectNode envelope = envelope(101, event.roomId());
        envelope.set("data", eventBody(event));
        return mapper.writeValueAsString(envelope);
    }

    public String encodeStats(String roomId, RoomStats stats) {
        if (stats == null) throw new ProtocolException("Required statistics missing");
        ObjectNode envelope = envelope(102, Validation.room(roomId));
        ObjectNode data = mapper.createObjectNode().put("online", stats.online()).put("likes", stats.likes());
        envelope.set("data", data);
        return mapper.writeValueAsString(envelope);
    }

    public String encodeStoredEvent(DanmakuEvent event) {
        if (event == null) throw new ProtocolException("Required event missing");
        return mapper.writeValueAsString(eventBody(event).put("event_id", event.eventId().toString()));
    }

    public DanmakuEvent decodeStoredEvent(String json) {
        JsonNode root = read(json);
        try {
            String eventId = string(root, "event_id");
            if (eventId.length() != 36) throw new ProtocolException("Invalid event id");
            return new DanmakuEvent(UUID.fromString(eventId), unsigned(root, "id"), string(root, "room_id"),
                    unsigned(root, "user_id"), string(root, "content"), parseStoredTimestamp(string(root, "send_time")));
        } catch (ProtocolException exception) {
            throw exception;
        } catch (RuntimeException exception) {
            throw new ProtocolException("Invalid stored event");
        }
    }

    /**
     * Go-compatible RFC3339 profile: uppercase T/Z, four-digit calendar year, optional
     * 1–9 fractional digits, and Z or numeric offsets through +/-23:59. Calendar and
     * clock fields are strict; leap seconds are rejected rather than normalized.
     * UTC conversion deliberately avoids ZoneOffset's narrower +/-18-hour range.
     */
    private static Instant parseStoredTimestamp(String timestamp) {
        Matcher match = STORED_TIMESTAMP.matcher(timestamp);
        if (!match.matches()) throw new ProtocolException("Invalid stored timestamp");
        LocalDateTime local = LocalDateTime.parse(match.group(1), LOCAL_TIMESTAMP);
        int offsetSeconds = 0;
        if (!match.group(2).equals("Z")) {
            int hours = Integer.parseInt(match.group(4));
            int minutes = Integer.parseInt(match.group(5));
            if (hours > 23 || minutes > 59) throw new ProtocolException("Invalid stored timestamp offset");
            offsetSeconds = (hours * 60 + minutes) * 60;
            if (match.group(3).equals("-")) offsetSeconds = -offsetSeconds;
        }
        return local.toInstant(ZoneOffset.UTC).minusSeconds(offsetSeconds);
    }

    private JsonNode read(String json) {
        if (json == null || json.length() > MAX_FRAME_BYTES)
            throw new ProtocolException("Missing or oversized JSON frame");
        Validation.text(json, Integer.MAX_VALUE, false);
        if (json.getBytes(StandardCharsets.UTF_8).length > MAX_FRAME_BYTES)
            throw new ProtocolException("Oversized JSON frame");
        try {
            JsonNode root = mapper.readTree(json);
            object(root);
            return root;
        } catch (ProtocolException exception) {
            throw exception;
        } catch (RuntimeException exception) {
            // Do not retain Jackson's source-bearing exception as a cause.
            throw new ProtocolException("Invalid JSON frame");
        }
    }

    private static void object(JsonNode node) {
        if (node == null || !node.isObject()) throw new ProtocolException("Expected JSON object");
    }

    private static String string(JsonNode node, String name) {
        JsonNode value = node.get(name);
        if (value == null || !value.isString()) throw new ProtocolException("Required string field missing or invalid");
        return value.stringValue();
    }

    private static BigInteger unsigned(JsonNode node, String name) {
        JsonNode value = node.get(name);
        if (value == null || !value.isIntegralNumber()) throw new ProtocolException("Required integer field missing or invalid");
        return Validation.uint64(value.bigIntegerValue());
    }

    private ObjectNode envelope(int type, String roomId) {
        return mapper.createObjectNode().put("type", type).put("room_id", roomId);
    }

    private ObjectNode eventBody(DanmakuEvent event) {
        return mapper.createObjectNode().put("id", event.id()).put("room_id", event.roomId())
                .put("user_id", event.userId()).put("content", event.content()).put("send_time", event.sendTime().toString());
    }
}
