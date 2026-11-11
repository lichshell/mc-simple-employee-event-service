package org.lich.simple.employee.event;

import org.lich.simple.employee.event.model.Employee;
import org.lich.simple.employee.event.model.Employee.Location;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Unit test: the Employee record serialises into payloads that each schema version accepts or rejects. */
class EmployeePayloadTest {

    private final ObjectMapper mapper = new ObjectMapper();

    private final Employee v1Employee =
            new Employee("E-1", "Asha", "Rao", "asha@example.com", null, null, null, null, null,null);
    private final Employee v3Employee =
            new Employee("E-3", "Carla", "Diaz", "carla@example.com", "Platform", "Lead", "2022-07-01",
                    new Location("Melbourne", "AU"), List.of("kafka", "spring"),"demo");

    @Test
    void nullFieldsAreOmittedNotWrittenAsNull() {
        JsonNode node = mapper.valueToTree(v1Employee);
        assertFalse(node.has("department"));
        assertFalse(node.has("location"));
        assertEquals(4, node.size());
    }

    @Test
    void v1PayloadValidatesAgainstAllVersions() {
        JsonNode node = mapper.valueToTree(v1Employee);
        for (int v = 1; v <= 3; v++) {
            int version = v;
            Assertions.assertDoesNotThrow(() -> TestSchemas.schema(version).validate(node), "v" + version);
        }
    }

    @Test
    void v3PayloadValidatesAgainstV3() {
        JsonNode node = mapper.valueToTree(v3Employee);
        Assertions.assertDoesNotThrow(() -> TestSchemas.schema(4).validate(node));
    }

    @Test
    void v3PayloadIsRejectedByV1AndV2BecauseSchemasAreClosed() {
        JsonNode node = mapper.valueToTree(v3Employee);
        assertThrows(Exception.class, () -> TestSchemas.schema(1).validate(node));
        assertThrows(Exception.class, () -> TestSchemas.schema(2).validate(node));
    }

    @Test
    void missingRequiredFieldIsRejected() {
        Employee noLastName = new Employee("E-9", "Zed", null, null, null, null, null, null, null,null);
        JsonNode node = mapper.valueToTree(noLastName);
        assertThrows(Exception.class, () -> TestSchemas.schema(3).validate(node));
    }

    @Test
    void recordRoundTripsThroughJson() throws Exception {
        String json = mapper.writeValueAsString(v3Employee);
        assertEquals(v3Employee, mapper.readValue(json, Employee.class));
    }
}
