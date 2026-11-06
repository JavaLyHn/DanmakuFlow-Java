package io.github.javalyhn.danmakuflow.persistence;

import io.github.javalyhn.danmakuflow.protocol.DanmakuEvent;
import org.h2.jdbcx.JdbcDataSource;
import org.h2.tools.RunScript;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.io.InputStreamReader;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.Calendar;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.TimeZone;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/** Real JDBC against isolated H2 fixtures; does not certify MySQL's dialect or driver. */
class JdbcDanmakuRepositoryTest {
    private static final Instant TIME = Instant.parse("2026-11-06T01:02:03.456789Z");
    private static final BigInteger MAX_UID = new BigInteger("18446744073709551615");
    private JdbcDataSource dataSource;

    @BeforeEach void fixture() throws Exception {
        dataSource = new JdbcDataSource();
        dataSource.setURL("jdbc:h2:mem:" + UUID.randomUUID() + ";MODE=MySQL;DB_CLOSE_DELAY=-1");
        try (var connection = dataSource.getConnection();
             var reader = new InputStreamReader(getClass().getResourceAsStream("/h2-fixture.sql"))) {
            RunScript.execute(connection, reader);
        }
    }

    // A missing insert, numeric narrowing, wire-id reuse, or local-zone conversion breaks this.
    @Test void storesExactUnsignedUidGeneratedIdUuidAndUtcMilliseconds() throws Throwable {
        var previous = TimeZone.getDefault();
        try {
            TimeZone.setDefault(TimeZone.getTimeZone("Pacific/Honolulu"));
            var event = event("Room", "你好😀", TIME);
            var repo = repository(dataSource);
            write(repo, List.of(event));
            var rows = history(repo, "Room", TIME.minusSeconds(1), TIME.plusSeconds(1), 100);
            assertEquals(1, rows.size());
            var stored = rows.getFirst();
            assertEquals(event.eventId(), stored.eventId());
            assertEquals(BigInteger.ONE, stored.id());
            assertEquals(MAX_UID, stored.userId());
            assertEquals("你好😀", stored.content());
            assertEquals(Instant.parse("2026-11-06T01:02:03.456Z"), stored.sendTime());
            try (var connection = dataSource.getConnection(); var statement = connection.createStatement();
                 var result = statement.executeQuery("SELECT event_id, send_time FROM danmaku_messages")) {
                assertTrue(result.next());
                var bytes = result.getBytes(1);
                var buffer = java.nio.ByteBuffer.wrap(bytes);
                assertEquals(event.eventId().getMostSignificantBits(), buffer.getLong());
                assertEquals(event.eventId().getLeastSignificantBits(), buffer.getLong());
                assertEquals(java.time.LocalDateTime.parse("2026-11-06T01:02:03.456"),
                        result.getObject(2, java.time.LocalDateTime.class));
            }
        } finally { TimeZone.setDefault(previous); }
    }

    // A duplicate that updates stored content, loses identity, or inserts twice breaks this.
    @Test void stableEventReplayIsNoOp() throws Throwable {
        var repo = repository(dataSource);
        var original = event("r", "original", TIME);
        write(repo, List.of(original));
        write(repo, List.of(new DanmakuEvent(original.eventId(), BigInteger.ZERO, "r", MAX_UID,
                "changed", TIME.plusSeconds(10))));
        var rows = history(repo, "r", TIME.minusSeconds(1), TIME.plusSeconds(20), 100);
        assertEquals(1, rows.size());
        assertEquals("original", rows.getFirst().content());
        assertEquals(BigInteger.ONE, rows.getFirst().id());
    }

    // Autocommit or committing the successful prefix would leave a visible row.
    @Test void middleBatchFailureRollsBackEveryRow() throws Throwable {
        try (var connection = dataSource.getConnection(); var statement = connection.createStatement()) {
            statement.execute("ALTER TABLE danmaku_messages ADD CONSTRAINT reject_bad CHECK(content <> 'bad')");
        }
        var repo = repository(dataSource);
        assertThrows(SQLException.class, () -> write(repo, List.of(event("r", "first", TIME),
                event("r", "bad", TIME), event("r", "last", TIME))));
        assertEquals(0, history(repo, "r", TIME.minusSeconds(1), TIME.plusSeconds(1), 100).size());
    }

    // Removing the secondary order, room predicate, inclusive bounds or limit breaks this.
    @Test void historyIsInclusiveRoomIsolatedOrderedAndCapped() throws Throwable {
        var repo = repository(dataSource);
        var end = TIME.plusSeconds(1).truncatedTo(java.time.temporal.ChronoUnit.MILLIS);
        var start = TIME.truncatedTo(java.time.temporal.ChronoUnit.MILLIS);
        write(repo, List.of(event("Room", "end", end), event("Room", "tie1", start),
                event("room", "other room", start), event("Room", "tie2", start),
                event("Room", "outside", end.plusSeconds(1))));
        assertEquals(List.of("tie1", "tie2", "end"), history(repo, "Room", start, end, 100)
                .stream().map(DanmakuEvent::content).toList());
        assertEquals(List.of("tie1", "tie2"), history(repo, "Room", start, end, 2)
                .stream().map(DanmakuEvent::content).toList());
        var many = new ArrayList<DanmakuEvent>();
        for (int i = 0; i < 105; i++) many.add(event("many", "row" + i, start));
        write(repo, many);
        assertEquals(100, history(repo, "many", start, end, 100).size());
        assertThrows(IllegalArgumentException.class, () -> history(repo, "r", start, end, 101));
        assertThrows(IllegalArgumentException.class, () -> history(repo, "r", start, end, 0));
        assertThrows(IllegalArgumentException.class, () -> history(repo, "r", end, start, 1));
    }

    // Exact Java room identity includes trailing spaces. MySQL collation needs real-DB verification.
    @Test void historySeparatesTrailingSpaceRooms() throws SQLException {
        var repo = repository(dataSource);
        write(repo, List.of(event("r", "plain", TIME), event("r ", "space", TIME)));
        assertEquals(List.of("plain"), history(repo, "r", TIME.minusSeconds(1), TIME.plusSeconds(1), 100)
                .stream().map(DanmakuEvent::content).toList());
        assertEquals(List.of("space"), history(repo, "r ", TIME.minusSeconds(1), TIME.plusSeconds(1), 100)
                .stream().map(DanmakuEvent::content).toList());
    }

    // A driver losing sub-microsecond precision must not widen an inclusive interval.
    @Test void fractionalBoundsExcludeRowsOutsideIntervalDespiteDriverPrecisionLoss() throws SQLException {
        var repo = repository(fieldEncodingDriverFixture());
        var first = Instant.parse("2026-11-06T01:02:03.456Z");
        var second = Instant.parse("2026-11-06T01:02:03.457Z");
        write(repo, List.of(event("r", "first", first), event("r", "second", second)));
        assertEquals(List.of("second"), history(repo, "r", first.plusNanos(1), second, 100)
                .stream().map(DanmakuEvent::content).toList());
        assertEquals(List.of("first"), history(repo, "r", first, second.minusNanos(1), 100)
                .stream().map(DanmakuEvent::content).toList());
    }

    // No stored millisecond is representable between these endpoints; do no DB work.
    @Test void intervalWithoutRepresentableMillisecondsReturnsBeforeConnecting() throws SQLException {
        var unavailable = (DataSource) Proxy.newProxyInstance(getClass().getClassLoader(),
                new Class<?>[]{DataSource.class}, (proxy, method, args) -> {
                    throw new AssertionError("Unexpected DataSource call: " + method.getName());
                });
        var repo = repository(unavailable);
        assertEquals(List.of(), history(repo, "r", Instant.parse("2026-11-06T01:02:03.456000001Z"),
                Instant.parse("2026-11-06T01:02:03.456999999Z"), 100));
    }

    // Inspect persisted ISO calendar fields, since symmetric write/read conversions can conceal errors.
    @Test void writesIsoDateFieldsAtMinimumAndBeforeCutoverRegardlessOfLocale() throws SQLException {
        var previousLocale = Locale.getDefault();
        var previousZone = TimeZone.getDefault();
        try {
            Locale.setDefault(Locale.forLanguageTag("th-TH-u-ca-buddhist"));
            TimeZone.setDefault(TimeZone.getTimeZone("Pacific/Honolulu"));
            var repo = repository(fieldEncodingDriverFixture());
            write(repo, List.of(event("r", "min", Instant.parse("1000-01-01T00:00:00Z")),
                    event("r", "cutover", Instant.parse("1500-03-01T12:34:56.789Z")),
                    event("r", "current", Instant.parse("2026-11-06T01:02:03.456Z"))));
            try (var connection = dataSource.getConnection(); var statement = connection.createStatement();
                 var rows = statement.executeQuery("SELECT send_time FROM danmaku_messages ORDER BY id")) {
                for (var expected : List.of("1000-01-01T00:00:00", "1500-03-01T12:34:56.789", "2026-11-06T01:02:03.456")) {
                    assertTrue(rows.next());
                    assertEquals(LocalDateTime.parse(expected), rows.getObject(1, LocalDateTime.class));
                }
            }
            assertEquals(List.of(Instant.parse("1000-01-01T00:00:00Z"), Instant.parse("1500-03-01T12:34:56.789Z"),
                    Instant.parse("2026-11-06T01:02:03.456Z")), history(repo, "r", Instant.parse("1000-01-01T00:00:00Z"),
                    Instant.parse("9999-12-31T23:59:59.999999999Z"), 100).stream().map(DanmakuEvent::sendTime).toList());
        } finally {
            Locale.setDefault(previousLocale);
            TimeZone.setDefault(previousZone);
        }
    }

    /** Boundary fixture, not Connector/J: emulate Calendar-field encoding and microsecond rounding
     * before delegating to real H2. H2 alone does not reproduce these documented driver hazards.
     */
    private DataSource fieldEncodingDriverFixture() {
        return (DataSource) Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[]{DataSource.class},
                (proxy, method, args) -> {
                    var connection = dataSource.getConnection();
                    return Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[]{Connection.class},
                            (connectionProxy, connectionMethod, connectionArgs) -> {
                                try {
                                    var result = connectionMethod.invoke(connection, connectionArgs);
                                    if (!(result instanceof PreparedStatement statement)) return result;
                                    return Proxy.newProxyInstance(getClass().getClassLoader(),
                                            new Class<?>[]{PreparedStatement.class}, (statementProxy, statementMethod, statementArgs) -> {
                                                if (statementMethod.getName().equals("setTimestamp") && statementArgs.length == 3) {
                                                    var calendar = (Calendar) ((Calendar) statementArgs[2]).clone();
                                                    var timestamp = (java.sql.Timestamp) statementArgs[1];
                                                    calendar.setTimeInMillis(timestamp.getTime());
                                                    var fields = LocalDateTime.of(calendar.get(Calendar.YEAR), calendar.get(Calendar.MONTH) + 1,
                                                            calendar.get(Calendar.DAY_OF_MONTH), calendar.get(Calendar.HOUR_OF_DAY),
                                                            calendar.get(Calendar.MINUTE), calendar.get(Calendar.SECOND), timestamp.getNanos());
                                                    statement.setObject((int) statementArgs[0], roundToMicros(fields));
                                                    return null;
                                                }
                                                if (statementMethod.getName().equals("setObject") && statementArgs[1] instanceof LocalDateTime fields) {
                                                    statementArgs[1] = roundToMicros(fields);
                                                }
                                                try { return statementMethod.invoke(statement, statementArgs); }
                                                catch (InvocationTargetException exception) { throw exception.getCause(); }
                                            });
                                } catch (InvocationTargetException exception) { throw exception.getCause(); }
                            });
                });
    }

    private LocalDateTime roundToMicros(LocalDateTime fields) {
        return fields.plusNanos(500).truncatedTo(java.time.temporal.ChronoUnit.MICROS);
    }

    // Empty/oversized batches must not obtain even a connection, or partially insert.
    @Test void emptyAndOversizeBatchesDoNoDatabaseWork() throws Throwable {
        var unavailable = (DataSource) Proxy.newProxyInstance(getClass().getClassLoader(),
                new Class<?>[]{DataSource.class}, (proxy, method, args) -> {
                    throw new AssertionError("Unexpected DataSource call: " + method.getName());
                });
        var repo = repository(unavailable);
        write(repo, List.of());
        assertThrows(IllegalArgumentException.class,
                () -> write(repo, Collections.nCopies(501, event("r", "x", TIME))));
    }

    // An off-by-one cap rejects a legal full batch.
    @Test void acceptsFullFiveHundredRowBatch() throws Throwable {
        var repo = repository(dataSource);
        var events = new ArrayList<DanmakuEvent>();
        for (int i = 0; i < 500; i++) events.add(event("r", "" + i, TIME));
        write(repo, events);
        try (var connection = dataSource.getConnection(); var statement = connection.createStatement();
             var rows = statement.executeQuery("SELECT COUNT(*) FROM danmaku_messages")) {
            assertTrue(rows.next());
            assertEquals(500, rows.getInt(1));
        }
    }

    // A pooled connection must be restored before close on successful and failed batches.
    @Test void restoresAutoCommitAndClosesConnectionsAfterSuccessAndFailure() throws Throwable {
        for (boolean fail : List.of(false, true)) {
            try (var connection = dataSource.getConnection(); var statement = connection.createStatement()) {
                statement.execute("ALTER TABLE danmaku_messages ADD CONSTRAINT IF NOT EXISTS reject_bad CHECK(content <> 'bad')");
            }
            var actions = new ArrayList<String>();
            var source = trackingSource(actions, false);
            var repo = repository(source);
            if (fail) assertThrows(SQLException.class,
                    () -> write(repo, List.of(event("r", "bad", TIME))));
            else write(repo, List.of(event("r", "ok", TIME)));
            assertEquals(List.of("auto:false", fail ? "rollback" : "commit", "auto:true", "close:true"), actions);
        }
    }

    // Rollback cleanup errors must remain attached to the original database failure.
    @Test void preservesOriginalFailureWhenRollbackAlsoFails() throws Throwable {
        try (var connection = dataSource.getConnection(); var statement = connection.createStatement()) {
            statement.execute("ALTER TABLE danmaku_messages ADD CONSTRAINT reject_bad CHECK(content <> 'bad')");
        }
        var actions = new ArrayList<String>();
        var repo = repository(trackingSource(actions, true));
        var failure = assertThrows(SQLException.class, () -> write(repo, List.of(event("r", "bad", TIME))));
        assertTrue(failure.getMessage().contains("REJECT_BAD"));
        assertTrue(java.util.Arrays.stream(failure.getSuppressed())
                .anyMatch(e -> "rollback failed".equals(e.getMessage())));
        assertTrue(actions.stream().anyMatch(a -> a.startsWith("close:")));
        assertFalse(actions.contains("auto:true"), "An unresolved transaction must never implicitly commit");
    }

    private DataSource trackingSource(List<String> actions, boolean rollbackFails) throws SQLException {
        var connection = dataSource.getConnection();
        var tracked = (Connection) Proxy.newProxyInstance(getClass().getClassLoader(),
                new Class<?>[]{Connection.class}, (proxy, method, args) -> {
                    if (method.getName().equals("setAutoCommit")) actions.add("auto:" + args[0]);
                    if (method.getName().equals("commit")) actions.add("commit");
                    if (method.getName().equals("rollback")) {
                        actions.add("rollback");
                        if (rollbackFails) {
                            connection.rollback();
                            throw new SQLException("rollback failed");
                        }
                    }
                    if (method.getName().equals("close")) actions.add("close:" + connection.getAutoCommit());
                    try { return method.invoke(connection, args); }
                    catch (InvocationTargetException exception) { throw exception.getCause(); }
                });
        return (DataSource) Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[]{DataSource.class},
                (proxy, method, args) -> {
                    if (method.getName().equals("getConnection")) return tracked;
                    throw new UnsupportedOperationException(method.getName());
                });
    }

    private DanmakuEvent event(String room, String content, Instant instant) {
        return new DanmakuEvent(UUID.randomUUID(), MAX_UID, room, MAX_UID, content, instant);
    }

    private JdbcDanmakuRepository repository(DataSource source) {
        return new JdbcDanmakuRepository(source);
    }

    private void write(JdbcDanmakuRepository repo, List<DanmakuEvent> events) throws SQLException {
        repo.writeBatch(events);
    }

    private List<DanmakuEvent> history(JdbcDanmakuRepository repo, String room, Instant from, Instant to, int limit)
            throws SQLException {
        return repo.findHistory(room, from, to, limit);
    }
}
