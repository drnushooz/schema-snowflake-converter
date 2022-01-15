package com.github.drnushooz.schema.snowflake.converter.conf;

import java.util.Optional;
import lombok.Getter;

public class ConverterConfiguration {

    public static final String SCHEMA_REGISTRY_URL_KEY = "SCHEMA_REGISTRY_URL";
    public static final String SCHEMA_REGISTRY_CACHE_SIZE_KEY = "SCHEMA_REGISTRY_CACHE_SIZE";

    @Getter
    private static final String schemaRegistryURL = Optional.ofNullable(System.getenv(SCHEMA_REGISTRY_URL_KEY)).orElse("");

    @Getter
    private static final int schemaRegistryCacheSize =
        Optional.ofNullable(System.getenv(SCHEMA_REGISTRY_CACHE_SIZE_KEY)).map(Integer::parseInt).orElse(20);

    public static boolean isSchemaRegistryEnabled() {
        return !schemaRegistryURL.isBlank();
    }
}
