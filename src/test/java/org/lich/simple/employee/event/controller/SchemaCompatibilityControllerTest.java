package org.lich.simple.employee.event.controller;

import org.junit.jupiter.api.Test;
import org.lich.simple.employee.event.service.SchemaCompatibilityService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(SchemaCompatibilityController.class)
class SchemaCompatibilityControllerTest {


    @Autowired
    MockMvc mvc;
    @MockBean
    SchemaCompatibilityService service;

    private static Map<String, Object> state(String level) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("subject", "employees-value");
        m.put("compatibility", level);
        m.put("scope", "subject");
        m.put("supported", List.of("NONE", "BACKWARD", "FULL"));
        return m;
    }

    @Test
    void getReturnsCurrentLevelAndSupportedLevels() throws Exception {
        when(service.current()).thenReturn(state("BACKWARD"));
        mvc.perform(get("/api/schema/compatibility"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.compatibility").value("BACKWARD"))
                .andExpect(jsonPath("$.supported.length()").value(3));
    }

    @Test
    void putChangesTheLevelAndReturnsTheNewState() throws Exception {
        when(service.current()).thenReturn(state("FULL"));
        mvc.perform(put("/api/schema/compatibility")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"compatibility\":\"FULL\"}"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.compatibility").value("FULL"));
        verify(service).update("FULL");
    }

    @Test
    void putWithUnsupportedLevelReturns422() throws Exception {
        when(service.update("BOGUS")).thenThrow(new IllegalArgumentException("Unsupported compatibility level 'BOGUS'"));
        mvc.perform(put("/api/schema/compatibility")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"compatibility\":\"BOGUS\"}"))
                .andExpect(status().isUnprocessableEntity())
                .andExpect(jsonPath("$.error").value("Unsupported compatibility level 'BOGUS'"));
    }

    @Test
    void registryFailureReturns502() throws Exception {
        when(service.update("FULL")).thenThrow(new IllegalStateException("registry unreachable"));
        mvc.perform(put("/api/schema/compatibility")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"compatibility\":\"FULL\"}"))
                .andExpect(status().isBadGateway());
    }
}