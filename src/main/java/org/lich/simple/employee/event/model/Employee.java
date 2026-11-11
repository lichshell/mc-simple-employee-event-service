package org.lich.simple.employee.event.model;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.List;

/**
 * Superset of all three registered schema versions (v1 -> v2 -> v3).
 * Fields added in later versions are optional and NON_NULL-excluded, because the
 * registered schemas are closed (additionalProperties=false) and reject explicit nulls.
 *
 * v1: employeeId, firstName, lastName, email
 * v2: + department, jobTitle, hireDate
 * v3: + location, skills
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record Employee(
        String employeeId,
        String firstName,
        String lastName,
        String email,
        String department,
        String jobTitle,
        String hireDate,
        Location location,
        List<String> skills,
        String unit) {

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record Location(String city, String country) { }
}
