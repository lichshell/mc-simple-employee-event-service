package org.lich.simple.employee.event;

import io.confluent.kafka.schemaregistry.json.JsonSchema;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

/** Loads the registry-ready schema files shared with docker-compose (copied onto the test classpath by the pom). */
public final class TestSchemas {

    private TestSchemas() { }

    public static String text(int version) {
        String path = "/schemas/employee-v" + version + ".json";
        try (InputStream in = TestSchemas.class.getResourceAsStream(path)) {
            if (in == null) throw new IllegalStateException("Missing test resource " + path);
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    public static JsonSchema schema(int version) {
        return new JsonSchema(text(version));
    }
}
