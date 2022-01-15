package com.github.drnushooz.schema.snowflake.converter.json;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

public class SingletonObjectMapper {

    private static class ObjectMapperHolder {

        public static final ObjectMapper objectMapper =
            new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    public static ObjectMapper getInstance() {
        return ObjectMapperHolder.objectMapper;
    }
}
