package org.lich.simple.employee.event.service;

import io.confluent.kafka.schemaregistry.CompatibilityLevel;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.lich.simple.employee.event.SchemaCatalog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Owns the compatibility level of the Employee subject.
 * - On startup it applies app.schema.compatibility from application.yml (if app.schema.apply-on-startup=true).
 * - At any time the level can be read or changed (see SchemaCompatibilityController).
 * The set of valid levels comes from Confluent's CompatibilityLevel enum, not from a hard-coded list.
 */
@Component
public class SchemaCompatibilityService implements ApplicationRunner {


    private static final Logger log = LoggerFactory.getLogger(SchemaCompatibilityService.class);

    private final SchemaRegistryClient client;
    private final String subject;
    private final CompatibilityLevel configured;
    private final boolean applyOnStartup;

    public SchemaCompatibilityService(SchemaCatalog catalog,
                                      @Value("${app.subject}") String subject,
                                      @Value("${app.schema.compatibility:BACKWARD}") String configured,
                                      @Value("${app.schema.apply-on-startup:true}") boolean applyOnStartup) {
        this.client = catalog.client();
        this.subject = subject;
        this.configured = parse(configured);      // bad YAML value -> application fails fast at startup
        this.applyOnStartup = applyOnStartup;
    }

    @Override
    public void run(ApplicationArguments args) {
        if (!applyOnStartup) {
            log.info("app.schema.apply-on-startup=false: leaving compatibility of '{}' as it is in the registry", subject);
            return;
        }
        String applied = update(configured.name());
        log.info("Compatibility of subject '{}' set to {} from application.yml", subject, applied);
    }

    /** Every level the Confluent client library supports (NONE, BACKWARD, BACKWARD_TRANSITIVE, FORWARD, ...). */
    public static List<String> supportedLevels() {
        return Arrays.stream(CompatibilityLevel.values()).map(Enum::name).toList();
    }

    /** Case-insensitive; throws IllegalArgumentException that lists the supported levels. */
    public static CompatibilityLevel parse(String value) {
        CompatibilityLevel level = CompatibilityLevel.forName(value);
        if (level == null) {
            throw new IllegalArgumentException(
                    "Unsupported compatibility level '" + value + "'. Supported: " + supportedLevels());
        }
        return level;
    }

    /** Changes the subject's compatibility. Only affects schemas registered from now on. */
    public String update(String level) {
        CompatibilityLevel parsed = parse(level);
        try {
            return client.updateCompatibility(subject, parsed.name());
        } catch (IOException | RestClientException e) {
            throw new IllegalStateException("Could not update compatibility of '" + subject + "': " + e.getMessage(), e);
        }
    }

    public Map<String, Object> current() {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("subject", subject);
        try {
            out.put("compatibility", client.getCompatibility(subject));
            out.put("scope", "subject");
        } catch (RestClientException e) {
            if (e.getStatus() != 404) {
                throw new IllegalStateException("Could not read compatibility of '" + subject + "': " + e.getMessage(), e);
            }
            out.put("compatibility", null);          // no subject-level override -> registry global default applies
            out.put("scope", "global default (no subject-level setting)");
        } catch (IOException e) {
            throw new IllegalStateException("Could not read compatibility of '" + subject + "': " + e.getMessage(), e);
        }
        out.put("supported", supportedLevels());
        return out;
    }
}
