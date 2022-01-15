package com.github.drnushooz.schema.snowflake.converter.runtime;

import com.amazonaws.services.lambda.runtime.LambdaLogger;
import java.nio.charset.Charset;
import lombok.NoArgsConstructor;

@NoArgsConstructor
public class SystemOutLambdaLogger implements LambdaLogger {

    @Override
    public void log(final String message) {
        System.out.println(message);
    }

    @Override
    public void log(final byte[] message) {
        log(new String(message, Charset.defaultCharset()));
    }
}
