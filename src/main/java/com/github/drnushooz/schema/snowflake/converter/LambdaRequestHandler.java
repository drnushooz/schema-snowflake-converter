package com.github.drnushooz.schema.snowflake.converter;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyRequestEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyResponseEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.drnushooz.schema.snowflake.converter.core.TableDefinitionGenerator;
import com.github.drnushooz.schema.snowflake.converter.json.SingletonObjectMapper;
import com.github.drnushooz.schema.snowflake.converter.registry.RegistryClient;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import lombok.SneakyThrows;
import org.apache.commons.lang3.StringEscapeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LambdaRequestHandler implements RequestHandler<APIGatewayProxyRequestEvent, APIGatewayProxyResponseEvent> {

    private static final Logger logger = LoggerFactory.getLogger(LambdaRequestHandler.class);

    @SneakyThrows
    @Override
    public APIGatewayProxyResponseEvent handleRequest(APIGatewayProxyRequestEvent inputEvent, Context context) {
        ObjectMapper objectMapper = SingletonObjectMapper.getInstance();
        String httpMethod = inputEvent.getHttpMethod();
        String requestPath = inputEvent.getPath();
        ImmutableList<String> pathComponents =
            ImmutableList.copyOf(Stream.of(requestPath.split("/")).filter(t -> !t.isBlank()).collect(Collectors.toList()));
        String requestEntity = pathComponents.get(0);
        logger.info("Received event {} {}", httpMethod, requestPath);

        Map<String, String> headers = ImmutableMap.of("Content-Type", "application/json");
        APIGatewayProxyResponseEvent responseEvent = new APIGatewayProxyResponseEvent();
        responseEvent.withHeaders(headers).withIsBase64Encoded(false);
        ResponseBody responseBody;
        switch (httpMethod) {
            case "GET":
                if (requestEntity.equalsIgnoreCase("fromregistry")) {
                    String subjectName;
                    int version;
                    switch (pathComponents.size()) {
                        case 2:
                            subjectName = pathComponents.get(1);
                            try {
                                Schema schemaFromRegistry = Objects.requireNonNull(RegistryClient.getSchemaFromRegistry(subjectName));
                                String tableDefinition = TableDefinitionGenerator.generateFromRegistry(schemaFromRegistry);
                                responseBody = new ResponseBody(httpMethod, requestPath, tableDefinition);
                                responseEvent.withStatusCode(200).withBody(objectMapper.writeValueAsString(responseBody));
                            } catch (Exception e) {
                                logger.error("Exception while trying to pull schema for subject: " + subjectName, e);
                                responseBody = new ResponseBody(httpMethod, requestPath, e.getMessage());
                                responseEvent.withStatusCode(404).withBody(objectMapper.writeValueAsString(responseBody));
                            }
                            break;

                        case 3:
                            subjectName = pathComponents.get(1);
                            version = Integer.parseInt(pathComponents.get(2));
                            try {
                                Schema schemaFromRegistry =
                                    Objects.requireNonNull(RegistryClient.getSchemaFromRegistry(subjectName, version));
                                String tableDefinition = TableDefinitionGenerator.generateFromRegistry(schemaFromRegistry);
                                responseBody = new ResponseBody(httpMethod, requestPath, tableDefinition);
                                responseEvent.withStatusCode(200).withBody(objectMapper.writeValueAsString(responseBody));
                            } catch (Exception e) {
                                logger.error("Exception while trying to pull schema for subject: " + subjectName + "version: " + version,
                                    e);
                                responseBody = new ResponseBody(httpMethod, requestPath, e.getMessage());
                                responseEvent.withStatusCode(404).withBody(objectMapper.writeValueAsString(responseBody));
                            }
                            break;

                        default:
                            responseBody = new ResponseBody(httpMethod, requestPath, "Could not understand path parameters");
                            responseEvent.withStatusCode(400).withBody(objectMapper.writeValueAsString(responseBody));
                    }
                }
                break;

            case "POST":
                StringBuilder schemaFromRequest = new StringBuilder();
                try {
                    schemaFromRequest.append(StringEscapeUtils.unescapeJson(inputEvent.getBody()));
                    if (requestEntity.equalsIgnoreCase("fromavro")) {
                        String tableDefinition = TableDefinitionGenerator.generateFromAvro(schemaFromRequest.toString());
                        responseBody = new ResponseBody(httpMethod, requestPath, tableDefinition);
                        responseEvent.withStatusCode(200).withBody(objectMapper.writeValueAsString(responseBody));
                    } else if (requestEntity.equalsIgnoreCase("fromjson")) {
                        String tableDefinition = TableDefinitionGenerator.generateFromJSON(schemaFromRequest.toString());
                        responseBody = new ResponseBody(httpMethod, requestPath, tableDefinition);
                        responseEvent.withStatusCode(200).withBody(objectMapper.writeValueAsString(responseBody));
                    } else if (requestEntity.equalsIgnoreCase("fromprotobuf")) {
                        String tableDefinition = TableDefinitionGenerator.generateFromProtobuf(schemaFromRequest.toString());
                        responseBody = new ResponseBody(httpMethod, requestPath, tableDefinition);
                        responseEvent.withStatusCode(200).withBody(objectMapper.writeValueAsString(responseBody));
                    } else {
                        responseBody = new ResponseBody(httpMethod, requestPath, "Could not understand path parameters");
                        responseEvent.withStatusCode(400).withBody(objectMapper.writeValueAsString(responseBody));
                    }
                } catch (Exception e) {
                    StringBuilder logMessageBuilder = new StringBuilder();
                    logMessageBuilder.append("Exception while trying to process ").append(httpMethod).append(" ").append(requestPath);
                    if (schemaFromRequest.length() > 0) {
                        logMessageBuilder.append(" ").append(schemaFromRequest);
                    }
                    logger.error(logMessageBuilder.toString(), e);
                    responseBody = new ResponseBody(httpMethod, requestPath, "Exception trying to parse schema. " + e.getMessage());
                    responseEvent.withStatusCode(400).withBody(objectMapper.writeValueAsString(responseBody));
                }
                break;

            default:
                responseBody = new ResponseBody(httpMethod, requestPath, "Unsupported method for request path");
                responseEvent.withStatusCode(405).withBody(objectMapper.writeValueAsString(responseBody));
        }
        return responseEvent;
    }

    @AllArgsConstructor
    @Getter
    @Setter
    private static class ResponseBody {

        private String httpMethod;
        private String path;
        private String response;
    }
}
