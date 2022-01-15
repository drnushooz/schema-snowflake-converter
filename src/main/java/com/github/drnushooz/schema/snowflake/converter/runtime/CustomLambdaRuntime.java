package com.github.drnushooz.schema.snowflake.converter.runtime;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyRequestEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyResponseEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.drnushooz.schema.snowflake.converter.json.SingletonObjectMapper;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpHeaders;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.text.MessageFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A proxy for LambdaRequestHandler using Amazon Linux 2 custom runtime
 */
public class CustomLambdaRuntime {

    private static final Logger logger = LoggerFactory.getLogger(CustomLambdaRuntime.class);
    private static final HttpClient httpClient = HttpClient.newHttpClient();
    private static final ObjectMapper objectMapper = SingletonObjectMapper.getInstance();

    private static final String LAMBDA_VERSION_DATE = "2018-06-01";
    private static final String LAMBDA_RUNTIME_URL_TEMPLATE = "http://{0}/{1}/runtime/invocation/next";
    private static final String LAMBDA_INVOCATION_URL_TEMPLATE = "http://{0}/{1}/runtime/invocation/{2}/response";
    private static final String LAMBDA_INIT_ERROR_URL_TEMPLATE = "http://{0}/{1}/runtime/init/error";
    private static final String LAMBDA_ERROR_URL_TEMPLATE = "http://{0}/{1}/runtime/invocation/{2}/error";

    public static void main(String[] args) throws IOException, InterruptedException {
        Map<String, String> systemEnvironment = new HashMap<>(System.getenv());
        Set<Entry<Object, Object>> systemProperties = System.getProperties().entrySet();
        systemProperties.forEach(e -> systemEnvironment.put(e.getKey().toString(), e.getValue().toString()));

        if (!systemEnvironment.containsKey("AWS_LAMBDA_RUNTIME_API")) {
            systemEnvironment.put("SINGLE_LOOP", "true");
        }

        loadHandlerAndInvoke(ImmutableMap.copyOf(systemEnvironment));
    }

    private static void loadHandlerAndInvoke(ImmutableMap<String, String> systemEnvironment) throws IOException, InterruptedException {
        Optional<String> handlerOpt = Optional.ofNullable(systemEnvironment.get("_HANDLER"));
        if (handlerOpt.isEmpty()) {
            throw new IllegalArgumentException("_HANDLER property is not set");
        }

        String[] handlerComponents = handlerOpt.get().split("::");
        String methodName = "handleRequest";
        String className = handlerComponents[0];
        if (handlerComponents.length == 2) {
            methodName = handlerComponents[1];
        }

        Optional<Object> handlerInstanceOpt = Optional.empty();
        Optional<Method> handlerMethodOpt = Optional.empty();

        try {
            Class<?> handlerClass = Class.forName(className);
            handlerInstanceOpt = Optional.of(handlerClass.getConstructor().newInstance());
            handlerMethodOpt = Optional.of(handlerClass.getMethod(methodName, APIGatewayProxyRequestEvent.class, Context.class));
        } catch (Exception e) {
            String logMessage = String.format("Could not initialize lambda runtime with: %s::%s", className, methodName);
            logger.error(logMessage, e);
            String lambdaRuntimeApi = systemEnvironment.get("AWS_LAMBDA_RUNTIME_API");
            if (lambdaRuntimeApi != null) {
                String errorReportingUrl = MessageFormat.format(LAMBDA_INIT_ERROR_URL_TEMPLATE, lambdaRuntimeApi, LAMBDA_VERSION_DATE);
                APIGatewayProxyResponseEvent errorEvent = new APIGatewayProxyResponseEvent();
                errorEvent.withStatusCode(500).withBody(logMessage).withIsBase64Encoded(false);
                HttpRequest errorReportingRequest =
                    HttpRequest.newBuilder(URI.create(errorReportingUrl))
                        .POST(BodyPublishers.ofString(objectMapper.writeValueAsString(errorEvent))).build();
                httpClient.send(errorReportingRequest, BodyHandlers.discarding());
            }
        }

        if (handlerInstanceOpt.isPresent() && handlerMethodOpt.isPresent()) {
            pollAndProcessEvents(systemEnvironment, handlerInstanceOpt.get(), handlerMethodOpt.get());
        }
    }

    private static void pollAndProcessEvents(ImmutableMap<String, String> systemEnvironment, final Object handlerInstance,
        final Method handlerMethod) throws IOException, InterruptedException {
        boolean isSingleLoop = Boolean.parseBoolean(systemEnvironment.getOrDefault("SINGLE_LOOP", "false"));
        Optional<String> runtimeApiOpt = Optional.ofNullable(systemEnvironment.get("AWS_LAMBDA_RUNTIME_API"));
        String lambdaRuntimeUrl =
            runtimeApiOpt.map(runtimeApi -> MessageFormat.format(LAMBDA_RUNTIME_URL_TEMPLATE, runtimeApi, LAMBDA_VERSION_DATE)).orElse("");
        AtomicReference<String> awsRequestIdRef = new AtomicReference<>();
        try {
            do {
                Optional<Context> contextOpt = Optional.empty();
                Optional<String> eventBodyOpt = Optional.empty();
                awsRequestIdRef = new AtomicReference<>(UUID.randomUUID().toString());

                if (!lambdaRuntimeUrl.isBlank()) {
                    HttpRequest nextEventRequest = HttpRequest.newBuilder(URI.create(lambdaRuntimeUrl)).GET().build();
                    HttpResponse<String> eventResponse = httpClient.send(nextEventRequest, BodyHandlers.ofString());
                    HttpHeaders eventHeaders = eventResponse.headers();
                    awsRequestIdRef.set(eventHeaders.firstValue("Lambda-Runtime-Aws-Request-Id").orElse(awsRequestIdRef.get()));
                    Optional<String> xrayTraceIdOpt = eventHeaders.firstValue("Lambda-Runtime-Trace-Id");
                    xrayTraceIdOpt.ifPresent(xrayTraceId -> System.setProperty("com.amazonaws.xray.traceHeader", xrayTraceId));

                    contextOpt = Optional.of(new CustomRuntimeContext(awsRequestIdRef.get()));
                    eventBodyOpt = Optional.ofNullable(eventResponse.body());
                }

                if (eventBodyOpt.isPresent() && runtimeApiOpt.isPresent()) {
                    String eventBody = eventBodyOpt.get();
                    String runtimeApi = runtimeApiOpt.get();
                    APIGatewayProxyRequestEvent lambdaRequestEvent = objectMapper.readValue(eventBody, APIGatewayProxyRequestEvent.class);
                    APIGatewayProxyResponseEvent lambdaResponseEvent =
                        (APIGatewayProxyResponseEvent) handlerMethod.invoke(handlerInstance, lambdaRequestEvent, contextOpt.orElse(null));

                    String eventResponseUrl =
                        MessageFormat.format(LAMBDA_INVOCATION_URL_TEMPLATE, runtimeApi, LAMBDA_VERSION_DATE, awsRequestIdRef.get());
                    HttpRequest eventResultRequest =
                        HttpRequest.newBuilder().uri(URI.create(eventResponseUrl)).header("Content-Type", "application/json")
                            .POST(BodyPublishers.ofString(objectMapper.writeValueAsString(lambdaResponseEvent))).build();
                    httpClient.send(eventResultRequest, BodyHandlers.discarding());
                }
            } while (!isSingleLoop);
        } catch (Exception e) {
            String logMessage =
                String.format("Exception while invoking method %s::%s: %s", handlerInstance.getClass(), handlerMethod.getName(),
                    e.getMessage());
            logger.error(logMessage, e);

            Optional<String> lambdaRuntimeApiOpt = Optional.ofNullable(systemEnvironment.get("AWS_LAMBDA_RUNTIME_API"));
            if (lambdaRuntimeApiOpt.isPresent()) {
                String errorReportingUrl =
                    MessageFormat.format(LAMBDA_ERROR_URL_TEMPLATE, lambdaRuntimeApiOpt.get(), LAMBDA_VERSION_DATE, awsRequestIdRef.get());
                APIGatewayProxyResponseEvent errorEvent = new APIGatewayProxyResponseEvent();
                errorEvent.withStatusCode(500).withBody(logMessage).withIsBase64Encoded(false);
                HttpRequest errorReportingRequest = HttpRequest.newBuilder(URI.create(errorReportingUrl))
                    .POST(BodyPublishers.ofString(objectMapper.writeValueAsString(errorEvent))).build();
                httpClient.send(errorReportingRequest, BodyHandlers.discarding());
            }
        }
    }
}
