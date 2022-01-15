package com.github.drnushooz.schema.snowflake.converter.core;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.drnushooz.schema.snowflake.converter.json.SingletonObjectMapper;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.DescriptorValidationException;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor.Type;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.squareup.wire.schema.internal.parser.ProtoFileElement;
import com.squareup.wire.schema.internal.parser.ProtoParser;
import io.apicurio.registry.utils.protobuf.schema.FileDescriptorUtils;
import io.apicurio.registry.utils.protobuf.schema.ProtobufSchema;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Stack;
import java.util.stream.Collectors;
import lombok.NonNull;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Parser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TableDefinitionGenerator {

    private static final Logger logger = LoggerFactory.getLogger(TableDefinitionGenerator.class);
    private static final org.apache.avro.Schema NULL_AVRO_SCHEMA = org.apache.avro.Schema.create(org.apache.avro.Schema.Type.NULL);
    private static final TypeReference<Map<String, Object>> STRING_OBJECT_MAP_TYPE = new TypeReference<>() {
    };

    /**
     * Generate Snowflake SQL table definition based on Avro schema.
     */
    public static String generateFromAvro(String schema) {
        Parser avroSchemaParser = new Parser();
        Schema avroSchema = avroSchemaParser.parse(schema);
        return generateFromAvro(avroSchema);
    }

    public static String generateFromAvro(org.apache.avro.Schema avroSchema) {
        if (avroSchema.getType() != org.apache.avro.Schema.Type.RECORD) {
            throw new IllegalArgumentException("Outermost type must be record!");
        }

        StringBuilder snowflakeQueryBuffer = new StringBuilder();
        snowflakeQueryBuffer.append("create table ").append(avroSchema.getName()).append("(");
        Iterator<Field> fieldsIterator = avroSchema.getFields().iterator();
        StringBuilder fieldType = new StringBuilder();

        Stack<Field> fieldsToProcess = new Stack<>();
        fieldsToProcess.push(fieldsIterator.next());
        while (!fieldsToProcess.isEmpty()) {
            Field curField = fieldsToProcess.pop();
            int prevSize = fieldsToProcess.size();
            String curFieldName = curField.name();
            @NonNull Schema curFieldSchema = curField.schema();
            Schema.Type curFieldType = curFieldSchema.getType();
            Optional<LogicalType> curFieldLogicalType = Optional.ofNullable(curFieldSchema.getLogicalType());

            switch (curFieldType) {
                case NULL:
                    throw new IllegalArgumentException("Standalone null fields are not supported for Avro by this converter");

                case BOOLEAN:
                    snowflakeQueryBuffer.append(String.format(" %s boolean,", curFieldName));
                    break;

                case BYTES:
                    fieldType.delete(0, fieldType.length());
                    fieldType.append(curFieldLogicalType.map(logicalType -> {
                            String fType = "binary";
                            if (logicalType instanceof LogicalTypes.Decimal) {
                                fType = "varchar";
                            }
                            return fType;
                        })
                        .orElse("binary"));
                    snowflakeQueryBuffer.append(String.format(" %s %s,", curFieldName, fieldType));
                    break;

                case FIXED:
                    fieldType.delete(0, fieldType.length());
                    fieldType.append(curFieldLogicalType.map(logicalType -> {
                            String fType = "binary";
                            if (logicalType instanceof LogicalTypes.Decimal) {
                                fType = "varchar";
                            } else if (curFieldSchema.getProp("size").equals("12")) {
                                // Duration logical type
                                fType = "timestamp_ntz";
                            }
                            return fType;
                        })
                        .orElse("binary"));
                    snowflakeQueryBuffer.append(String.format(" %s %s,", curFieldName, fieldType));
                    break;

                case DOUBLE:
                    snowflakeQueryBuffer.append(String.format(" %s double,", curFieldName));
                    break;

                case FLOAT:
                    snowflakeQueryBuffer.append(String.format(" %s float,", curFieldName));
                    break;

                case INT:
                    fieldType.delete(0, fieldType.length());
                    fieldType.append(curFieldLogicalType.map(logicalType -> {
                            String fType = "int";
                            if (logicalType instanceof LogicalTypes.Date) {
                                fType = "date";
                            } else if (logicalType instanceof LogicalTypes.TimeMillis) {
                                fType = "timestamp";
                            }
                            return fType;
                        })
                        .orElse("int"));
                    snowflakeQueryBuffer.append(String.format(" %s %s,", curFieldName, fieldType));
                    break;

                case LONG:
                    fieldType.delete(0, fieldType.length());
                    fieldType.append(
                        curFieldLogicalType.map(logicalType -> {
                                String fType = "bigint";
                                if (logicalType instanceof LogicalTypes.TimeMicros) {
                                    fType = "timestamp_ntz";
                                } else if (logicalType instanceof LogicalTypes.TimestampMillis) {
                                    fType = "timestamp";
                                } else if (logicalType instanceof LogicalTypes.TimestampMicros) {
                                    fType = "timestamp";
                                } else if (logicalType instanceof LogicalTypes.LocalTimestampMillis) {
                                    fType = "timestamp_tz";
                                } else if (logicalType instanceof LogicalTypes.LocalTimestampMicros) {
                                    fType = "timestamp_tz";
                                }
                                return fType;
                            })
                            .orElse("bigint"));
                    snowflakeQueryBuffer.append(String.format(" %s %s,", curFieldName, fieldType));
                    break;

                case ENUM:
                case STRING:
                    snowflakeQueryBuffer.append(String.format(" %s string,", curFieldName));
                    break;

                case ARRAY:
                    snowflakeQueryBuffer.append(String.format(" %s array,", curFieldName));
                    break;

                case MAP:
                    snowflakeQueryBuffer.append(String.format(" %s object,", curFieldName));
                    break;

                case UNION:
                    List<Schema> schemaTypes = curFieldSchema.getTypes();
                    if (schemaTypes.size() == 2 && schemaTypes.contains(NULL_AVRO_SCHEMA)) {
                        for (Schema memberType : schemaTypes) {
                            if (!memberType.equals(NULL_AVRO_SCHEMA)) {
                                fieldsToProcess.push(new Field(curFieldName, memberType));
                            }
                        }
                    } else {
                        snowflakeQueryBuffer.append(String.format(" %s variant,", curFieldName));
                    }
                    break;

                default:
                    IllegalArgumentException iae =
                        new IllegalArgumentException(String.format("Unknown type: %s for field: %s", curFieldType, curFieldName));
                    logger.error(String.format("Exception while trying to parse avro field: %s", curFieldName), iae);
                    throw iae;
            }

            // Check if any nested fields were added. If not, proceed to the next field in the record.
            if (prevSize == fieldsToProcess.size() && fieldsIterator.hasNext()) {
                fieldsToProcess.push(fieldsIterator.next());
            }
        }

        int firstBracketPos = snowflakeQueryBuffer.indexOf("(");
        snowflakeQueryBuffer.delete(firstBracketPos + 1, firstBracketPos + 2);
        snowflakeQueryBuffer.delete(snowflakeQueryBuffer.lastIndexOf(","), snowflakeQueryBuffer.length());
        snowflakeQueryBuffer.append(");");
        return snowflakeQueryBuffer.toString();
    }

    /**
     * Generate Snowflake SQL table definition based on JSON schema.
     */
    public static String generateFromJSON(String schema) throws JsonProcessingException {
        ObjectMapper objectMapper = SingletonObjectMapper.getInstance();
        JsonNode jsonSchema = objectMapper.readTree(schema);
        @NonNull String title = jsonSchema.get("title").asText();
        Map<String, Object> properties = objectMapper.convertValue(jsonSchema.get("properties"), STRING_OBJECT_MAP_TYPE);

        StringBuilder snowflakeQueryBuffer = new StringBuilder();
        snowflakeQueryBuffer.append("create table ").append(title).append("(");

        for (String curPropName : properties.keySet()) {
            Map<String, Object> valueMap = objectMapper.convertValue(properties.get(curPropName), STRING_OBJECT_MAP_TYPE);
            String curPropType = Objects.requireNonNull(valueMap.get("type")).toString().toLowerCase();

            switch (curPropType) {
                case "boolean":
                    snowflakeQueryBuffer.append(String.format(" %s boolean,", curPropName));
                    break;

                case "string":
                    String fieldType = "string";
                    if (valueMap.containsKey("maxLength")) {
                        int maxLength = Integer.parseInt(valueMap.get("maxLength").toString());
                        fieldType = String.format("varchar(%d)", maxLength);
                    }
                    snowflakeQueryBuffer.append(String.format(" %s %s,", curPropName, fieldType));
                    break;

                case "integer":
                    snowflakeQueryBuffer.append(String.format(" %s int,", curPropName));
                    break;

                case "number":
                    snowflakeQueryBuffer.append(String.format(" %s float,", curPropName));
                    break;

                case "object":
                    snowflakeQueryBuffer.append(String.format(" %s object,", curPropName));
                    break;

                case "array":
                    snowflakeQueryBuffer.append(String.format(" %s array,", curPropName));
                    break;

                case "":
                    snowflakeQueryBuffer.append(String.format(" %s variant,", curPropName));
                    break;

                case "null":
                    IllegalArgumentException iae =
                        new IllegalArgumentException("Standalone null fields are not supported for JSON by this converter");
                    logger.error(String.format("Exception while trying to parse field: %s", curPropName), iae);
                    throw iae;

                default:
                    IllegalArgumentException exc =
                        new IllegalArgumentException(String.format("Unknown type: %s for field: %s", curPropType, curPropName));
                    logger.error(String.format("Exception while trying to parse JSON field: %s", curPropName), exc);
                    throw exc;
            }
        }

        int firstBracketPos = snowflakeQueryBuffer.indexOf("(");
        snowflakeQueryBuffer.delete(firstBracketPos + 1, firstBracketPos + 2);
        snowflakeQueryBuffer.delete(snowflakeQueryBuffer.lastIndexOf(","), snowflakeQueryBuffer.length());
        snowflakeQueryBuffer.append(");");
        return snowflakeQueryBuffer.toString();
    }

    /**
     * Generate Snowflake SQL table definition based on Protobuf schema.
     */
    public static String generateFromProtobuf(String schema) throws DescriptorValidationException {
        ProtoFileElement fileElement = ProtoParser.Companion.parse(FileDescriptorUtils.DEFAULT_LOCATION, schema);
        FileDescriptor fileDescriptor = FileDescriptorUtils.protoFileToFileDescriptor(fileElement);
        return generateFromProtobuf(new ProtobufSchema(fileDescriptor, fileElement));
    }

    public static String generateFromProtobuf(ProtobufSchema protobufSchema) {
        FileDescriptor fileDescriptor = protobufSchema.getFileDescriptor();
        int messageDescriptorCount = fileDescriptor.getMessageTypes().size();
        if (messageDescriptorCount > 1) {
            IllegalArgumentException iae = new IllegalArgumentException(
                String.format("There should be only one outermost message type, found %d", messageDescriptorCount));
            logger.error("Error in parsing protobuf schema", iae);
            throw iae;
        }

        Descriptor outermostMessageType = fileDescriptor.getMessageTypes().get(0);
        List<FieldDescriptor> fieldDescriptors =
            outermostMessageType.getFields().stream().sorted(Comparator.comparingInt(FieldDescriptor::getNumber))
                .collect(Collectors.toList());

        StringBuilder snowflakeQueryBuffer = new StringBuilder();
        snowflakeQueryBuffer.append("create table ").append(outermostMessageType.getName()).append("(");

        for (FieldDescriptor curField : fieldDescriptors) {
            String curFieldName = curField.getName();
            Type curFieldType = curField.getType();
            switch (curFieldType) {
                case BOOL:
                    snowflakeQueryBuffer.append(String.format(" %s boolean,", curFieldName));
                    break;

                case INT32:
                case SINT32:
                case UINT32:
                case FIXED32:
                case SFIXED32:
                case INT64:
                case SINT64:
                case FIXED64:
                case SFIXED64:
                    snowflakeQueryBuffer.append(String.format(" %s int,", curFieldName));
                    break;

                case FLOAT:
                    snowflakeQueryBuffer.append(String.format(" %s float,", curFieldName));
                    break;

                case DOUBLE:
                    snowflakeQueryBuffer.append(String.format(" %s double,", curFieldName));
                    break;

                case STRING:
                case BYTES:
                case ENUM:
                    snowflakeQueryBuffer.append(String.format(" %s string,", curFieldName));
                    break;

                default:
                    if (curField.isRepeated()) {
                        snowflakeQueryBuffer.append(String.format(" %s array,", curFieldName));
                    } else if (curField.isMapField()) {
                        snowflakeQueryBuffer.append(String.format(" %s variant,", curFieldName));
                    } else {
                        IllegalArgumentException iae =
                            new IllegalArgumentException(String.format("Unknown type: %s for field: %s", curFieldType, curFieldName));
                        logger.error(String.format("Exception while trying to parse protobuf field: %s", curFieldName), iae);
                        throw iae;
                    }
            }
        }

        int firstBracketPos = snowflakeQueryBuffer.indexOf("(");
        snowflakeQueryBuffer.delete(firstBracketPos + 1, firstBracketPos + 2);
        snowflakeQueryBuffer.delete(snowflakeQueryBuffer.lastIndexOf(","), snowflakeQueryBuffer.length());
        snowflakeQueryBuffer.append(");");
        return snowflakeQueryBuffer.toString();
    }

    public static String generateFromRegistry(io.confluent.kafka.schemaregistry.client.rest.entities.Schema schemaFromRegistry)
        throws DescriptorValidationException, JsonProcessingException {
        String schemaType = schemaFromRegistry.getSchemaType();
        switch (schemaType) {
            case "AVRO":
                return generateFromAvro(schemaFromRegistry.getSchema());

            case "JSON":
                return generateFromJSON(schemaFromRegistry.getSchema());

            case "PROTOBUF":
                return generateFromProtobuf(schemaFromRegistry.getSchema());

            default:
                throw new IllegalArgumentException(String.format("Found invalid schema type: %s", schemaType));
        }
    }
}
