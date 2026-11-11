package org.lich.simple.employee.event.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.lich.simple.employee.event.SchemaCatalog;
import org.lich.simple.employee.event.listener.EmployeeEventListener;
import org.lich.simple.employee.event.listener.ReceivedEmployeeEvent;
import org.lich.simple.employee.event.model.Employee;
import org.lich.simple.employee.event.model.ResolvedSchema;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.confluent.kafka.schemaregistry.json.JsonSchemaUtils;
import org.apache.kafka.common.errors.SerializationException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionException;

@RestController
@RequestMapping("/api/employees")
public class EmployeeEventController {

    private final KafkaTemplate<String, Object> template;   // auto-configured from application.yml
    private final SchemaCatalog catalog;
    private final EmployeeEventListener listener;
    private final ObjectMapper mapper;
    private final String topic;

    public EmployeeEventController(KafkaTemplate<String, Object> template,
                                   SchemaCatalog catalog,
                                   EmployeeEventListener listener,
                                   ObjectMapper mapper,
                                   @Value("${app.topic}") String topic) {
        this.template = template;
        this.catalog = catalog;
        this.listener = listener;
        this.mapper = mapper;
        this.topic = topic;
    }

    /**
     * Publish an employee using a specific registered schema version (default: latest).
     * The payload is validated against that version; fields the version does not know
     * (e.g. department under version=1) are rejected because the schemas are closed.
     */
    @PostMapping
    public Map<String, Object> send(@RequestBody Employee employee,
                                    @RequestParam(defaultValue = "latest") String version) throws JsonProcessingException {
        return publish(employee, version);
    }

    /** Sends one sample employee per schema version (v1, v2, v3). */
    @PostMapping("/demo")
    public List<Map<String, Object>> demo() throws JsonProcessingException{
        List<Map<String, Object>> out = new ArrayList<>();
        out.add(publish(new Employee("E-1001", "Asha", "Rao", "asha.rao@example.com",
                null, null, null, null, null,null), "1"));
        out.add(publish(new Employee("E-1002", "Ben", "Cho", "ben.cho@example.com",
                "Payments", "Engineer", "2024-03-18", null, null,null), "2"));
        out.add(publish(new Employee("E-1003", "Carla", "Diaz", "carla.diaz@example.com",
                "Platform", "Lead Engineer", "2022-07-01",
                new Employee.Location("Melbourne", "AU"), List.of("kafka", "spring"),null), "3"));
        return out;
    }

    @GetMapping("/versions")
    public List<Integer> versions() {
        return catalog.versions();
    }

    @GetMapping("/received")
    public List<ReceivedEmployeeEvent> received() {
        return listener.recent();
    }

    private Map<String, Object> publish(Employee employee, String version) throws JsonProcessingException {
        ResolvedSchema resolved = catalog.resolve(version);
        // Envelope = explicit schema + payload. With auto.register.schemas=false the serializer
        // verifies this exact schema is already registered, then validates the payload against it.
//        System.out.println("Trace[1]:> " + resolved.id() + " / version : " + resolved.version());
        System.out.println("Trace[1]:> " + mapper.writeValueAsString(employee));
        ObjectNode envelope = JsonSchemaUtils.envelope(resolved.schema(), mapper.valueToTree(employee));
//        System.out.println("Trace[2]:> " + resolved.id() + " / isEmpty : " + envelope.isEmpty());
        var md = template.send(topic, employee.employeeId(), envelope).join().getRecordMetadata();
//        System.out.println("Trace[3]:> " + resolved.id() + " / offset : " + md.offset());
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("employeeId", employee.employeeId());
        result.put("topic", md.topic());
        result.put("partition", md.partition());
        result.put("offset", md.offset());
        result.put("schemaVersion", resolved.version());
        result.put("schemaId", resolved.id());
        return result;
    }

    @ExceptionHandler({SerializationException.class, CompletionException.class, IllegalArgumentException.class,
            JsonProcessingException.class})
    public ResponseEntity<Map<String, String>> onRejected(Exception ex) {
        Throwable root = ex;
        while (root.getCause() != null && root.getCause() != root) {
            root = root.getCause();
        }
        //org.everit.json.schema.ValidationException : invalid message against the target schema / Error serializing JSON message
        System.out.println("Trace[EXP]:> " + ex.getMessage() + " / rootCause : " + String.valueOf(root.getMessage()));
        return ResponseEntity.status(HttpStatus.UNPROCESSABLE_ENTITY)
                .body(Map.of("error", ex.getMessage() == null ? root.toString() : ex.getMessage(),
                             "rootCause", String.valueOf(root.getMessage())));
    }
}
