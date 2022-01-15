package com.github.drnushooz.schema.snowflake.converter.registry;

import com.github.drnushooz.schema.snowflake.converter.conf.ConverterConfiguration;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RegistryClient {

    private static final Logger logger = LoggerFactory.getLogger(RegistryClient.class);
    private static final String schemaRegistryURL = ConverterConfiguration.getSchemaRegistryURL();
    private static final int schemaRegistryCacheSize = ConverterConfiguration.getSchemaRegistryCacheSize();

    public static Schema getSchemaFromRegistry(String subject) throws RestClientException, IOException {
        return getSchemaFromRegistry(subject, null);
    }

    public static Schema getSchemaFromRegistry(String subject, Integer version) throws RestClientException, IOException {
        if (ConverterConfiguration.isSchemaRegistryEnabled()) {
            SchemaRegistryClient registryClient = SchemaRegistryClientHolder.INSTANCE;
            Schema schemaFromRegistry;
            if (version == null || version == 0) {
                logger.info("Getting schema for subject: {} version: latest", subject);
                SchemaMetadata metadata = registryClient.getLatestSchemaMetadata(subject);
                schemaFromRegistry = registryClient.getByVersion(subject, metadata.getVersion(), false);
            } else {
                logger.info("Getting schema for subject: {} version: {}", subject, version);
                schemaFromRegistry = registryClient.getByVersion(subject, version, false);
            }
            return schemaFromRegistry;
        } else {
            return null;
        }
    }

    private static class SchemaRegistryClientHolder {

        public static final SchemaRegistryClient INSTANCE = new CachedSchemaRegistryClient(schemaRegistryURL, schemaRegistryCacheSize);

        static {
            logger.info("Initializing schema registry client to {} with cache size {}", schemaRegistryURL, schemaRegistryCacheSize);
        }
    }
}
