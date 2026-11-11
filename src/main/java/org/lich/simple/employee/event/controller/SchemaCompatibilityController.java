package org.lich.simple.employee.event.controller;


import org.lich.simple.employee.event.service.SchemaCompatibilityService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

/** Read or change the Employee subject's compatibility level at runtime. */
@RestController
@RequestMapping("/api/schema/compatibility")
public class SchemaCompatibilityController {

    public record CompatibilityRequest(String compatibility) { }

    private final SchemaCompatibilityService service;

    public SchemaCompatibilityController(SchemaCompatibilityService service) {
        this.service = service;
    }

    @GetMapping
    public Map<String, Object> get() {
        return service.current();
    }

    @PutMapping
    public Map<String, Object> change(@RequestBody CompatibilityRequest request) {
        service.update(request.compatibility());
        return service.current();
    }

    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<Map<String, Object>> invalidLevel(IllegalArgumentException ex) {
        return ResponseEntity.status(HttpStatus.UNPROCESSABLE_ENTITY)
                .body(Map.of("error", ex.getMessage()));
    }

    @ExceptionHandler(IllegalStateException.class)
    public ResponseEntity<Map<String, Object>> registryProblem(IllegalStateException ex) {
        return ResponseEntity.status(HttpStatus.BAD_GATEWAY)
                .body(Map.of("error", ex.getMessage()));
    }
}

