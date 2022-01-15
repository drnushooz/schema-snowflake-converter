package com.github.drnushooz.schema.snowflake.converter.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.github.drnushooz.schema.snowflake.converter.registry.RegistryClient;
import com.google.protobuf.Descriptors.DescriptorValidationException;
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import java.io.IOException;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Test;

public class TableDefinitionGeneratorTest {

    @Test
    void testGenerateFromAvro() {
        String avroSchema = "{\n"
            + "\t\t\"type\": \"record\",\n"
            + "\t\t\"name\": \"snack\",\n"
            + "\t\t\"fields\": [\n"
            + "\t\t\t{\"name\": \"name\", \"type\": \"string\"},\n"
            + "\t\t\t{\"name\": \"manufacturer\", \"type\": \"string\"},\n"
            + "\t\t\t{\"name\": \"calories\", \"type\": \"float\"},\n"
            + "\t\t\t{\"name\": \"color\", \"type\": [\"null\", \"string\"], \"default\": null}\n"
            + "\t\t]\n"
            + "\t}";
        String result = TableDefinitionGenerator.generateFromAvro(avroSchema);
        assertNotNull(result);
        assertEquals(3, StringUtils.countMatches(result, ","));
    }

    @Test
    void testGenerateFromJSON() throws JsonProcessingException {
        String jsonSchema = "{\n"
            + "  \"title\": \"Person\",\n"
            + "  \"type\": \"object\",\n"
            + "  \"properties\": {\n"
            + "    \"firstName\": {\n"
            + "      \"type\": \"string\",\n"
            + "      \"description\": \"The person's first name.\"\n"
            + "    },\n"
            + "    \"lastName\": {\n"
            + "      \"type\": \"string\",\n"
            + "      \"description\": \"The person's last name.\"\n"
            + "    },\n"
            + "    \"age\": {\n"
            + "      \"description\": \"Age in years which must be equal to or greater than zero.\",\n"
            + "      \"type\": \"integer\",\n"
            + "      \"minimum\": 0\n"
            + "    }\n"
            + "  }\n"
            + "}";
        String result = TableDefinitionGenerator.generateFromJSON(jsonSchema);
        assertNotNull(result);
        assertEquals(2, StringUtils.countMatches(result, ","));
    }

    @Test
    void testGenerateFromProtobuf() throws DescriptorValidationException {
        String protobufSchema = "message Subscriber {\n"
            + "\t\trequired string first_name = 1;\n"
            + "\t\trequired string last_name = 2;\n"
            + "\t\trequired string address = 3;\n"
            + "\t\toptional string city = 4;\n"
            + "\t\toptional int32 zipcode = 5;\n"
            + "\t}";
        String result = TableDefinitionGenerator.generateFromProtobuf(protobufSchema);
        assertNotNull(result);
        assertEquals(4, StringUtils.countMatches(result, ","));
    }
}
