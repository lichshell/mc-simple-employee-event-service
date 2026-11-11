package org.lich.simple.employee.event;

import org.lich.simple.employee.event.model.ResolvedSchema;
import io.confluent.kafka.schemaregistry.SchemaProvider;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.json.JsonSchemaProvider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Looks up Employee schema versions in the Schema Registry. The URL comes from
 * application.yml; a "mock://scope" URL gives an in-process registry for tests.
 */
@Component
public class SchemaCatalog {

    private final SchemaRegistryClient client;
    private final String subject;

    public SchemaCatalog(@Value("${app.schema-registry-url}") String url,
                         @Value("${app.subject}") String subject) {
        List<SchemaProvider> providers = List.of(new JsonSchemaProvider());
        this.client = url.startsWith("mock://")
                ? MockSchemaRegistry.getClientForScope(url.substring("mock://".length()), providers)
                : new CachedSchemaRegistryClient(url, 100, providers, Map.of());
        this.subject = subject;
    }

    /** The underlying registry client (shared with SchemaCompatibilityService). */
    public SchemaRegistryClient client() {
        return client;
    }

    /** @param version a version number such as "2", or "latest" */
    public ResolvedSchema resolve(String version) {
        try {
            SchemaMetadata md = "latest".equalsIgnoreCase(version)
                    ? client.getLatestSchemaMetadata(subject)
                    : client.getSchemaMetadata(subject, Integer.parseInt(version));
            System.out.println("Trace[1]:> " + md.getVersion() + " / Schema Id : " + md.getId());
            System.out.println("Trace[2]:> " + md.getSchema());
            return new ResolvedSchema(new JsonSchema(md.getSchema()), md.getVersion(), md.getId());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("version must be a number or 'latest': " + version, e);
        } catch (IOException | RestClientException e) {
            throw new IllegalArgumentException(
                    "Schema '" + subject + "' version '" + version + "' not available: " + e.getMessage(), e);
        }
    }

    public List<Integer> versions() {
        try {
            return client.getAllVersions(subject);
        } catch (IOException | RestClientException e) {
            throw new IllegalArgumentException("Cannot list versions of '" + subject + "': " + e.getMessage(), e);
        }
    }
}
