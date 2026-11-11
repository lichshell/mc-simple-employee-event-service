package org.lich.simple.employee.event;

import org.lich.simple.employee.event.listener.EmployeeEventListener;
import org.lich.simple.employee.event.listener.ReceivedEmployeeEvent;
import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.json.JsonSchemaProvider;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.web.servlet.MockMvc;

import java.time.Duration;
import java.util.List;
import java.util.Optional;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * Integration test (run by failsafe: mvn verify).
 * Real Spring context configured only by application.yml, a real embedded Kafka broker, and an
 * in-process mock Schema Registry (mock://employee-it) pre-loaded with the three schema versions.
 * Exercises: produce with each version -> Kafka -> JSON Schema deserializer -> @KafkaListener.
 */
@SpringBootTest(properties = {
        "app.schema-registry-url=mock://employee-it",
        "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}"
})
@EmbeddedKafka(partitions = 3, topics = "employees")
@AutoConfigureMockMvc
class EmployeeFlowIT {

    private static final String SCOPE = "employee-it";

    @Autowired MockMvc mvc;
    @Autowired EmployeeEventListener listener;

    @BeforeAll
    static void registerSchemas() throws Exception {
        SchemaRegistryClient client = MockSchemaRegistry.getClientForScope(SCOPE, List.of(new JsonSchemaProvider()));
        for (int v = 1; v <= 3; v++) {
            client.register("employees-value", TestSchemas.schema(v));
            System.out.println("Trace: 1 > " + v + " . " + TestSchemas.schema(v));
        }
    }

    @AfterAll
    static void dropRegistry() {
        MockSchemaRegistry.dropScope(SCOPE);
    }

    private Optional<ReceivedEmployeeEvent> received(String id) {
        return listener.recent().stream().filter(r -> id.equals(r.key())).findFirst();
    }

    private ReceivedEmployeeEvent awaitReceived(String id) {
        await().atMost(Duration.ofSeconds(20)).until(() -> received(id).isPresent());
        return received(id).orElseThrow();
    }

    @Test
    void registryHasThreeVersions() throws Exception {
        System.out.println("Trace: 2 > " );
        mvc.perform(get("/api/employees/versions"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.length()").value(3));
    }

    @Test
    void demoPublishesAllThreeSchemaVersionsAndListenerReadsThemAsEmployee() throws Exception {
        mvc.perform(post("/api/employees/demo"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.length()").value(3))
                .andExpect(jsonPath("$[0].schemaVersion").value(1))
                .andExpect(jsonPath("$[1].schemaVersion").value(2))
                .andExpect(jsonPath("$[2].schemaVersion").value(3));

        // v1 message: only core fields; newer fields deserialize as null in the v3-shaped POJO
        ReceivedEmployeeEvent v1 = awaitReceived("E-1001");
        assertEquals("Asha", v1.value().firstName());
        assertNull(v1.value().department());
        assertNull(v1.value().location());

        // v2 message
        ReceivedEmployeeEvent v2 = awaitReceived("E-1002");
        assertEquals("Payments", v2.value().department());
        assertNull(v2.value().skills());

        // v3 message: full shape
        ReceivedEmployeeEvent v3 = awaitReceived("E-1003");
        assertEquals("Melbourne", v3.value().location().city());
        assertEquals(List.of("kafka", "spring"), v3.value().skills());
    }

    @Test
    void defaultVersionIsLatestAndMessageIsConsumed() throws Exception {
        mvc.perform(post("/api/employees")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"employeeId":"E-2001","firstName":"Noor","lastName":"Aziz",
                                 "skills":["avro","json-schema"]}"""))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.schemaVersion").value(3));

        ReceivedEmployeeEvent r = awaitReceived("E-2001");
        assertEquals(List.of("avro", "json-schema"), r.value().skills());
        assertTrue(r.partition() >= 0);
    }

    @Test
    void fieldFromANewerVersionIsRejectedWhenPublishingWithAnOlderVersion() throws Exception {
        mvc.perform(post("/api/employees?version=1")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"employeeId":"E-3001","firstName":"Li","lastName":"Wei","department":"Risk"}"""))
                .andExpect(status().isUnprocessableEntity());
    }

    @Test
    void compatibilityIsAppliedFromYamlAtStartupAndCanBeChangedAtRuntime() throws Exception {
        // application.yml: app.schema.compatibility=BACKWARD, apply-on-startup=true
        mvc.perform(get("/api/schema/compatibility"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.compatibility").value("BACKWARD"))
                .andExpect(jsonPath("$.supported.length()").value(7));

        mvc.perform(put("/api/schema/compatibility")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"compatibility\":\"full_transitive\"}"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.compatibility").value("FULL_TRANSITIVE"));

        mvc.perform(put("/api/schema/compatibility")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"compatibility\":\"SOMETIMES\"}"))
                .andExpect(status().isUnprocessableEntity());

        // put it back so the other tests see the YAML value
        mvc.perform(put("/api/schema/compatibility")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"compatibility\":\"BACKWARD\"}"))
                .andExpect(status().isOk());
    }

    @Test
    void unknownVersionIsRejected() throws Exception {
        mvc.perform(post("/api/employees?version=99")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"employeeId":"E-4001","firstName":"Li","lastName":"Wei"}"""))
                .andExpect(status().isUnprocessableEntity());
    }

    @Test
    void missingRequiredFieldIsRejected() throws Exception {
        mvc.perform(post("/api/employees")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"employeeId":"E-5001","firstName":"NoLastName"}"""))
                .andExpect(status().isUnprocessableEntity());
    }
}
