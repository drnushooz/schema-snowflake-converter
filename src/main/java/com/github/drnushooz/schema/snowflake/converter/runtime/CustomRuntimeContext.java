package com.github.drnushooz.schema.snowflake.converter.runtime;

import com.amazonaws.services.lambda.runtime.ClientContext;
import com.amazonaws.services.lambda.runtime.CognitoIdentity;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class CustomRuntimeContext implements Context {

    @Getter
    @NonNull
    private final String awsRequestId;

    @Getter
    private final LambdaLogger logger = new SystemOutLambdaLogger();

    @Override
    public String getLogGroupName() {
        return System.getenv("AWS_LAMBDA_LOG_GROUP_NAME");
    }

    @Override
    public String getLogStreamName() {
        return System.getenv("AWS_LAMBDA_LOG_STREAM_NAME");
    }

    @Override
    public String getFunctionName() {
        return System.getenv("AWS_LAMBDA_FUNCTION_NAME");
    }

    @Override
    public String getFunctionVersion() {
        return System.getenv("AWS_LAMBDA_FUNCTION_VERSION");
    }

    @Override
    public String getInvokedFunctionArn() {
        throw new UnsupportedOperationException();
    }

    @Override
    public CognitoIdentity getIdentity() {
        throw new UnsupportedOperationException();
    }

    @Override
    public ClientContext getClientContext() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getRemainingTimeInMillis() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getMemoryLimitInMB() {
        return Integer.parseInt(System.getenv("AWS_LAMBDA_FUNCTION_MEMORY_SIZE"));
    }
}
