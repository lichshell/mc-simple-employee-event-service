package org.lich.simple.employee.event;

import io.confluent.kafka.schemaregistry.json.JsonSchema;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Unit test: the three schema versions evolve compatibly under BACKWARD rules (what the registry enforces). */
class SchemaEvolutionTest {

    @Test
    void v2CanReadV1Data() {
        assertTrue(TestSchemas.schema(2).isBackwardCompatible(TestSchemas.schema(1)).isEmpty());
    }

    @Test
    void v3CanReadV2Data() {
        assertTrue(TestSchemas.schema(3).isBackwardCompatible(TestSchemas.schema(2)).isEmpty());
    }

    @Test
    void v3CanReadV1Data() {
        assertTrue(TestSchemas.schema(3).isBackwardCompatible(TestSchemas.schema(1)).isEmpty());
    }

    @Test
    void addingANewRequiredFieldBreaksBackwardCompatibility() {
        String v3 = TestSchemas.text(3);
        String bad = v3
                .replace("\"skills\":", "\"badge\": { \"type\": \"string\" }, \"skills\":")
                .replace("\"required\": [\"employeeId\", \"firstName\", \"lastName\"]",
                         "\"required\": [\"employeeId\", \"firstName\", \"lastName\", \"badge\"]");
        JsonSchema v4Bad = new JsonSchema(bad);

        assertFalse(v4Bad.isBackwardCompatible(TestSchemas.schema(3)).isEmpty(),
                "a new required property must be reported as incompatible");
    }

    @Test
    void changingAFieldTypeBreaksBackwardCompatibility() {
        String bad = TestSchemas.text(3).replace(
                "\"email\":      { \"type\": \"string\" }",
                "\"email\":      { \"type\": \"integer\" }");
        assertFalse(new JsonSchema(bad).isBackwardCompatible(TestSchemas.schema(3)).isEmpty());
    }
}
