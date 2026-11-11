package org.lich.simple.employee.event;

import org.lich.simple.employee.event.model.ResolvedSchema;
import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.json.JsonSchemaProvider;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Unit test: schema lookup against an in-process mock registry (no network, no Kafka). */
class SchemaCatalogTest {

    private static final String SCOPE = "schema-catalog-test";
    private static final String SUBJECT = "employees-value";

    private final SchemaCatalog catalog = new SchemaCatalog("mock://" + SCOPE, SUBJECT);

    @BeforeAll
    static void registerThreeVersions() throws Exception {
        SchemaRegistryClient client = MockSchemaRegistry.getClientForScope(SCOPE, List.of(new JsonSchemaProvider()));
        for (int v = 1; v <= 3; v++) {
            client.register(SUBJECT, TestSchemas.schema(v));
        }
    }

    @AfterAll
    static void cleanUp() {
        MockSchemaRegistry.dropScope(SCOPE);
    }

    @Test
    void listsAllThreeVersions() {
        assertEquals(List.of(1, 2, 3), catalog.versions());
    }

    @Test
    void latestResolvesToVersion3() {
        ResolvedSchema latest = catalog.resolve("latest");
        assertEquals(3, latest.version());
        assertTrue(latest.schema().canonicalString().contains("skills"));
    }

    @Test
    void numericVersionResolvesThatVersion() {
        ResolvedSchema v1 = catalog.resolve("1");
        assertEquals(1, v1.version());
        assertTrue(!v1.schema().canonicalString().contains("department"));

        ResolvedSchema v2 = catalog.resolve("2");
        assertEquals(2, v2.version());
        assertTrue(v2.schema().canonicalString().contains("department"));
    }

    @Test
    void unknownVersionIsRejected() {
        assertThrows(IllegalArgumentException.class, () -> catalog.resolve("99"));
    }

    @Test
    void nonNumericVersionIsRejected() {
        assertThrows(IllegalArgumentException.class, () -> catalog.resolve("abc"));
    }
}
