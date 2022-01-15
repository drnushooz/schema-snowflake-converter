# Schema to Snowflake table definition converter

This is a lambda which can convert Avro, Protobuf and JSON Schema schemas into equivalent snowflake table definitions. There is
built in support for Schema registry. The Lambda also contains a custom runtime for GraalVM with support for Amazon Linux 2 
which can be used to significantly reduce the cold boot time.

### How to build
```shell
mvn clean package
./build-native-image.bash
```
This will build `schema-snowflake-converter-amazonlinux.zip` which contains a `bootstrap` script and
a self-contained binary built on Amazon Linux 2 using GraalVM 11.

### APIs
The lambda can be used with API Gateway to exposes the following 4 APIs which

| Method | Path                                      | Parameters                               |
|--------|-------------------------------------------|------------------------------------------|
| `POST` | `/fromavro`                               | Avro schema in the body                  |
| `POST` | `/fromjson`                               | JSON schema in the body                  |
| `POST` | `/fromprotobuf`                           | Protobuf schema in the body              |
| `GET`  | `/fromregistry/<subjectname>/[<version>]` | Get schema from Schema registry instance |

### Schema registry integration
For Schema registry integration specify the HTTP endpoint in environment varible `SCHEMA_REGISTRY_URL`.
The client by default caches 20 schema IDs and the schema cache size can be changed using
`SCHEMA_REGISTRY_CACHE_SIZE` environment variable.
