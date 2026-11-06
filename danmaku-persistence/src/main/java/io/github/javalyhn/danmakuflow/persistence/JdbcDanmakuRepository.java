package io.github.javalyhn.danmakuflow.persistence;

import io.github.javalyhn.danmakuflow.protocol.DanmakuEvent;

import javax.sql.DataSource;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

/** JDBC storage only; the caller owns the DataSource and explicitly provisions a new database.
 * No migrations or service connections are started by constructing this repository.
 */
public final class JdbcDanmakuRepository {
    public static final int MAX_BATCH_SIZE = 500;
    public static final int MAX_HISTORY_ROWS = 100;

    private static final String INSERT = """
            INSERT INTO danmaku_messages (event_id, room_id, user_id, content, send_time)
            VALUES (?, ?, ?, ?, ?)
            ON DUPLICATE KEY UPDATE event_id = event_id
            """;
    private static final String HISTORY = """
            SELECT id, event_id, room_id, user_id, content, send_time
            FROM danmaku_messages
            WHERE room_id = ? AND send_time >= ? AND send_time <= ?
            ORDER BY send_time ASC, id ASC LIMIT ?
            """;

    private final DataSource dataSource;

    public JdbcDanmakuRepository(DataSource dataSource) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource");
    }

    /** Atomic batch, stable event UUID deduplication; incoming wire ids never become database ids.
     * Oversized batches are rejected before obtaining a connection. No input is truncated.
     */
    public void writeBatch(List<DanmakuEvent> events) throws SQLException {
        Objects.requireNonNull(events, "events");
        if (events.size() > MAX_BATCH_SIZE) throw new IllegalArgumentException("Batch exceeds 500 events");
        var batch = List.copyOf(events);
        if (batch.isEmpty()) return;
        try (var connection = dataSource.getConnection()) {
            boolean originalAutoCommit = connection.getAutoCommit();
            boolean restoreAutoCommit = true;
            Throwable failure = null;
            try {
                connection.setAutoCommit(false);
                try (var statement = connection.prepareStatement(INSERT)) {
                    for (var event : batch) {
                        statement.setBytes(1, uuidBytes(event.eventId()));
                        statement.setString(2, event.roomId());
                        statement.setBigDecimal(3, new BigDecimal(event.userId()));
                        statement.setString(4, event.content());
                        // DATETIME stores ISO local fields; explicit UTC avoids locale/calendar cutovers.
                        statement.setObject(5, LocalDateTime.ofInstant(
                                event.sendTime().truncatedTo(ChronoUnit.MILLIS), ZoneOffset.UTC));
                        statement.addBatch();
                    }
                    statement.executeBatch();
                }
                connection.commit();
            } catch (SQLException | RuntimeException | Error exception) {
                failure = exception;
                try { connection.rollback(); }
                catch (SQLException rollbackFailure) {
                    exception.addSuppressed(rollbackFailure);
                    // setAutoCommit(true) can commit an unresolved transaction after failed rollback.
                    // Close the failed connection without that implicit commit.
                    restoreAutoCommit = false;
                }
                throw exception;
            } finally {
                if (restoreAutoCommit) {
                    try { connection.setAutoCommit(originalAutoCommit); }
                    catch (SQLException restoreFailure) {
                        if (failure != null) failure.addSuppressed(restoreFailure);
                        else throw restoreFailure;
                    }
                }
            }
        }
    }

    /** Inclusive time interval in UTC, stable timestamp/id ordering, at most 100 rows.
     * DATETIME(3) storage truncates incoming event precision to milliseconds.
     * Query endpoints are rounded inward to the stored millisecond grid before driver encoding.
     */
    public List<DanmakuEvent> findHistory(String roomId, Instant fromInclusive, Instant toInclusive, int limit)
            throws SQLException {
        Objects.requireNonNull(roomId, "roomId");
        Objects.requireNonNull(fromInclusive, "fromInclusive");
        Objects.requireNonNull(toInclusive, "toInclusive");
        if (limit < 1 || limit > MAX_HISTORY_ROWS) throw new IllegalArgumentException("History limit must be 1..100");
        if (fromInclusive.isAfter(toInclusive)) throw new IllegalArgumentException("History interval is reversed");
        var lower = fromInclusive.truncatedTo(ChronoUnit.MILLIS);
        var upper = toInclusive.truncatedTo(ChronoUnit.MILLIS);
        if (!lower.equals(fromInclusive)) {
            // No representable point if both endpoints occupy the same stored millisecond.
            // Check before incrementing to avoid overflow at Instant.MAX.
            if (!lower.isBefore(upper)) return List.of();
            lower = lower.plusMillis(1);
        }
        try (var connection = dataSource.getConnection(); var statement = connection.prepareStatement(HISTORY)) {
            statement.setString(1, roomId);
            statement.setObject(2, LocalDateTime.ofInstant(lower, ZoneOffset.UTC));
            statement.setObject(3, LocalDateTime.ofInstant(upper, ZoneOffset.UTC));
            statement.setInt(4, limit);
            try (var result = statement.executeQuery()) {
                var events = new ArrayList<DanmakuEvent>();
                while (result.next()) {
                    var bytes = ByteBuffer.wrap(result.getBytes("event_id"));
                    var eventId = new UUID(bytes.getLong(), bytes.getLong());
                    events.add(new DanmakuEvent(eventId, result.getBigDecimal("id").toBigIntegerExact(),
                            result.getString("room_id"), result.getBigDecimal("user_id").toBigIntegerExact(),
                            result.getString("content"), result.getObject("send_time", LocalDateTime.class).toInstant(ZoneOffset.UTC)));
                }
                return List.copyOf(events);
            }
        }
    }

    private static byte[] uuidBytes(UUID id) {
        return ByteBuffer.allocate(16).putLong(id.getMostSignificantBits()).putLong(id.getLeastSignificantBits()).array();
    }
}
