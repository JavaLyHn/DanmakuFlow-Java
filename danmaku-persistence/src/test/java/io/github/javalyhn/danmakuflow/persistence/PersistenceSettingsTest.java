package io.github.javalyhn.danmakuflow.persistence;
import org.junit.jupiter.api.Test;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;
class PersistenceSettingsTest {
    @Test void databaseUrlAndUserMustBeExplicit() {
        for (Map<String,String> env : List.<Map<String,String>>of(Map.of(), Map.of("DANMAKU_DB_URL", "jdbc:mysql://localhost/new_schema"))) {
            assertThrows(IllegalArgumentException.class, () -> PersistenceSettings.from(env));
        }
    }
    @Test void validConfigurationDoesNotNeedPasswordOrOpenServices() {
        var settings = PersistenceSettings.from(Map.of("DANMAKU_DB_URL", "jdbc:mysql://localhost/new_schema", "DANMAKU_DB_USER", "u"));
        assertNull(settings.dbPassword());
        var properties = settings.consumerProperties();
        assertEquals("false", properties.getProperty("enable.auto.commit"));
        assertEquals("500", properties.getProperty("max.poll.records"));
        assertEquals("danmakuflow-persistence-v1", properties.getProperty("group.id"));
    }
}
