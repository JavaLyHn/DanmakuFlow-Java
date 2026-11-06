package io.github.javalyhn.danmakuflow.persistence;

import com.zaxxer.hikari.HikariDataSource;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.Test;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import java.sql.*;
import java.util.Map;
import static org.junit.jupiter.api.Assertions.*;

class PersistenceStartupTest {
    // Migrations cannot obtain a connection: context must close pool without constructing Kafka.
    @Test void migrationFailureClosesPoolAndPreventsConsumerConstruction() {
        var pool = new FailingPool(); var built = new boolean[1];
        var application = new PersistenceApplication();
        var context = new AnnotationConfigApplicationContext();
        context.registerBean("dataSource", HikariDataSource.class, () -> pool,
                definition -> definition.setDestroyMethodName("close"));
        context.registerBean("migratedSchema", Flyway.class, () -> application.migratedSchema(pool));
        context.registerBean("consumer", Runnable.class, () -> {
            context.getBean("migratedSchema"); built[0] = true; return () -> {};
        });
        try { assertThrows(RuntimeException.class, context::refresh); }
        finally { context.close(); }
        assertTrue(pool.closed); assertFalse(built[0]);
    }

    @Test void poolAndFlywaySafetyConfigurationDoesNotOpenConnections() {
        var settings = PersistenceSettings.from(Map.of("DANMAKU_DB_URL", "jdbc:mysql://localhost/dedicated",
                "DANMAKU_DB_USER", "writer"));
        var application = new PersistenceApplication();
        try (var pool = application.dataSource(settings)) {
            assertEquals(4, pool.getMaximumPoolSize()); assertEquals(0, pool.getMinimumIdle());
            assertEquals(5000, pool.getConnectionTimeout());
            assertEquals("10000", pool.getDataSourceProperties().getProperty("socketTimeout"));
            assertEquals("true", pool.getDataSourceProperties().getProperty("rewriteBatchedStatements"));
            assertNull(pool.getHikariPoolMXBean());
        }
    }
    static class FailingPool extends HikariDataSource {
        boolean closed;
        @Override public Connection getConnection() throws SQLException { throw new SQLException("isolated failure boundary"); }
        @Override public void close() { closed = true; super.close(); }
    }
}
