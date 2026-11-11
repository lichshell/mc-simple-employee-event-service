package org.lich.simple.employee.event.controller;

import org.lich.simple.employee.event.SchemaCatalog;
import org.lich.simple.employee.event.TestSchemas;
import org.lich.simple.employee.event.listener.EmployeeEventListener;
import org.lich.simple.employee.event.listener.ReceivedEmployeeEvent;
import org.lich.simple.employee.event.model.Employee;
import org.lich.simple.employee.event.model.ResolvedSchema;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.SerializationException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.test.web.servlet.MockMvc;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/** Unit test: controller logic with Kafka and the registry mocked out (web slice only). */
@WebMvcTest(controllers = EmployeeEventController.class, properties = "app.topic=employees")
class EmployeeEventControllerTest {

    @Autowired MockMvc mvc;

    @MockBean KafkaTemplate<String, Object> template;
    @MockBean
    SchemaCatalog catalog;
    @MockBean
    EmployeeEventListener listener;

    @BeforeEach
    void stubKafkaSend() {
        RecordMetadata md = new RecordMetadata(new TopicPartition("employees", 1), 42L, 0, 0L, 0, 0);
        SendResult<String, Object> sent = new SendResult<>(new ProducerRecord<>("employees", "k", "v"), md);
        when(template.send(eq("employees"), any(String.class), any()))
                .thenReturn(CompletableFuture.completedFuture(sent));
        for (int v = 1; v <= 3; v++) {
            ResolvedSchema resolved = new ResolvedSchema(TestSchemas.schema(v), v, v);   // build first, then stub
            when(catalog.resolve(String.valueOf(v))).thenReturn(resolved);
        }
        ResolvedSchema latest = new ResolvedSchema(TestSchemas.schema(3), 3, 3);
        when(catalog.resolve("latest")).thenReturn(latest);
    }

    @Test
    void postUsesLatestVersionByDefault() throws Exception {
        mvc.perform(post("/api/employees")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"employeeId":"E-7","firstName":"Mia","lastName":"Lee"}"""))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.schemaVersion").value(3))
                .andExpect(jsonPath("$.partition").value(1))
                .andExpect(jsonPath("$.offset").value(42));
    }

    @Test
    void envelopeCarriesSchemaAndPayloadWithoutNullFields() throws Exception {
        mvc.perform(post("/api/employees?version=1")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"employeeId":"E-7","firstName":"Mia","lastName":"Lee"}"""))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.schemaVersion").value(1));

        ArgumentCaptor<Object> value = ArgumentCaptor.forClass(Object.class);
        verify(template).send(eq("employees"), eq("E-7"), value.capture());
        ObjectNode envelope = (ObjectNode) value.getValue();
        JsonNode payload = envelope.get("payload");
        assertEquals("Mia", payload.get("firstName").asText());
        assertEquals(3, payload.size());                  // no "department": null etc.
        assertEquals("Employee", envelope.get("schema").get("title").asText());
    }

    @Test
    void demoSendsOneMessagePerSchemaVersion() throws Exception {
        mvc.perform(post("/api/employees/demo"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.length()").value(3))
                .andExpect(jsonPath("$[0].schemaVersion").value(1))
                .andExpect(jsonPath("$[1].schemaVersion").value(2))
                .andExpect(jsonPath("$[2].schemaVersion").value(3));
        verify(template, times(3)).send(eq("employees"), any(String.class), any());
    }

    @Test
    void unknownSchemaVersionReturns422() throws Exception {
        when(catalog.resolve("99")).thenThrow(new IllegalArgumentException("version '99' not available"));
        mvc.perform(post("/api/employees?version=99")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"employeeId":"E-7","firstName":"Mia","lastName":"Lee"}"""))
                .andExpect(status().isUnprocessableEntity())
                .andExpect(jsonPath("$.error").value("version '99' not available"));
    }

    @Test
    void schemaViolationFromSerializerReturns422() throws Exception {
        when(template.send(eq("employees"), any(String.class), any()))
                .thenThrow(new SerializationException("JSON does not match schema"));
        mvc.perform(post("/api/employees?version=1")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"employeeId":"E-7","firstName":"Mia","lastName":"Lee","department":"X"}"""))
                .andExpect(status().isUnprocessableEntity())
                .andExpect(jsonPath("$.error").value("JSON does not match schema"));
    }

    @Test
    void versionsEndpointListsRegistryVersions() throws Exception {
        when(catalog.versions()).thenReturn(List.of(1, 2, 3));
        mvc.perform(get("/api/employees/versions"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.length()").value(3))
                .andExpect(jsonPath("$[2]").value(3));
    }

    @Test
    void receivedEndpointReturnsListenerContents() throws Exception {
        Employee e = new Employee("E-1", "A", "B", null, null, null, null, null, null,null);
        when(listener.recent()).thenReturn(List.of(new ReceivedEmployeeEvent("E-1", 0, 5L, e)));
        mvc.perform(get("/api/employees/received"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0].key").value("E-1"))
                .andExpect(jsonPath("$[0].offset").value(5))
                .andExpect(jsonPath("$[0].value.firstName").value("A"));
    }
}
