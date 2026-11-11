package org.lich.simple.employee.event.service;

import io.confluent.kafka.schemaregistry.CompatibilityLevel;
import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.lich.simple.employee.event.SchemaCatalog;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class SchemaCompatibilityServiceTest {

    private static final String SCOPE = "compat-service-test";
    private static final String SUBJECT = "employees-value";

    private final SchemaCatalog catalog = new SchemaCatalog("mock://" + SCOPE, SUBJECT);

    @AfterAll
    static void cleanUp() {
        MockSchemaRegistry.dropScope(SCOPE);
    }

    private SchemaCompatibilityService service(String configured, boolean applyOnStartup) {
        return new SchemaCompatibilityService(catalog, SUBJECT, configured, applyOnStartup);
    }

    @Test
    void supportedLevelsComeFromConfluentEnum() {
        List<String> supported = SchemaCompatibilityService.supportedLevels();
        assertEquals(CompatibilityLevel.values().length, supported.size());
        assertTrue(supported.containsAll(List.of(
                "NONE", "BACKWARD", "BACKWARD_TRANSITIVE", "FORWARD", "FORWARD_TRANSITIVE", "FULL", "FULL_TRANSITIVE")));
    }

    @Test
    void parseIsCaseInsensitive() {
        assertEquals(CompatibilityLevel.FULL_TRANSITIVE, SchemaCompatibilityService.parse("full_transitive"));
    }

    @Test
    void parseRejectsUnknownLevelAndListsTheSupportedOnes() {
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> SchemaCompatibilityService.parse("SOMETIMES"));
        assertTrue(ex.getMessage().contains("SOMETIMES"));
        assertTrue(ex.getMessage().contains("BACKWARD_TRANSITIVE"));
    }

    @Test
    void invalidYamlValueFailsFastAtConstruction() {
        assertThrows(IllegalArgumentException.class, () -> service("NOPE", true));
    }

    @Test
    void startupAppliesTheLevelFromYaml() {
        service("FULL", true).run(null);
        assertEquals("FULL", service("FULL", true).current().get("compatibility"));
    }

    @Test
    void startupLeavesRegistryAloneWhenApplyOnStartupIsFalse() {
        SchemaCompatibilityService svc = service("FULL", false);
        svc.update("NONE");                 // level set earlier, e.g. through the API
        svc.run(null);                      // must NOT overwrite it with the YAML value
        assertEquals("NONE", svc.current().get("compatibility"));
    }

    @Test
    void levelCanBeChangedAtRuntime() {
        SchemaCompatibilityService svc = service("BACKWARD", false);
        svc.update("BACKWARD");
        assertEquals("BACKWARD", svc.current().get("compatibility"));
        svc.update("forward_transitive");
        assertEquals("FORWARD_TRANSITIVE", svc.current().get("compatibility"));
    }

    @Test
    void updateRejectsUnsupportedLevel() {
        assertThrows(IllegalArgumentException.class, () -> service("BACKWARD", false).update("bogus"));
    }

    @Test
    void currentReportsTheSupportedLevels() {
        SchemaCompatibilityService svc = service("BACKWARD", false);
        svc.update("BACKWARD");
        assertEquals(SchemaCompatibilityService.supportedLevels(), svc.current().get("supported"));
    }
}