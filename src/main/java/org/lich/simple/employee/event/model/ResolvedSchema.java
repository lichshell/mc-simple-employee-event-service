package org.lich.simple.employee.event.model;

import io.confluent.kafka.schemaregistry.json.JsonSchema;

/** A schema version fetched from the registry. */
public record ResolvedSchema(JsonSchema schema, int version, int id) { }
