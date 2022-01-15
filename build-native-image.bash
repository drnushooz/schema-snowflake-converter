#!/bin/bash

export OUTPUT_JAR_PATH="target/schema-snowflake-converter-0.0.1-SNAPSHOT.jar"
export DOCKER_IMAGE_TAG="22.0.0.2-java11"

echo "Building jar"
mvn clean package -DskipTests
echo "Sleeping for 2 seconds to allow filesystem to settle"
sleep 2

echo "Building native image"
# shellcheck disable=SC2046
docker run --rm -v $(pwd):/working goodforgod/amazonlinux-graalvm:${DOCKER_IMAGE_TAG} -c \
    "native-image --verbose --enable-url-protocols=http,https --no-fallback \
    --report-unsupported-elements-at-runtime --allow-incomplete-classpath \
    --initialize-at-build-time=com.google.common.base.Preconditions,com.google.common.jimfs.SystemJimfsFileSystemProvider \
    -H:ReflectionConfigurationFiles=/working/src/main/resources/reflection-config.json \
    -H:ResourceConfigurationFiles=/working/src/main/resources/resources-config.json \
    -H:+ReportExceptionStackTraces -H:+JNI \
    -jar /working/${OUTPUT_JAR_PATH} /working/target/schema-snowflake-converter-amazonlinux"

rm -f target/schema-snowflake-converter*.build_artifacts.txt target/schema-snowflake-converter*.o

if [[ -f target/schema-snowflake-converter-amazonlinux ]]; then
    echo "Packaging image for lambda"
    cp target/schema-snowflake-converter-amazonlinux .
    rm -f schema-snowflake-converter-amazonlinux.zip
    zip schema-snowflake-converter-amazonlinux.zip schema-snowflake-converter-amazonlinux bootstrap
fi
