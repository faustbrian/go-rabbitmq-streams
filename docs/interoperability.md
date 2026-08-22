# Language-neutral interoperability

## Current Laravel/PHP status

The pinned RabbitMQ source baseline does not list a supported PHP Streams
protocol client, and no RabbitMQ-organization PHP Streams client was found.
Third-party PHP implementations did not provide the production evidence needed
for a release claim. Consequently `rabbitstream` does **not** claim direct
Laravel/PHP Streams interoperability.

Protocol compatibility alone is insufficient. A future claim requires a
maintained PHP client plus real cross-language tests covering publish,
consume, binary payloads, metadata, confirmation, offsets, reconnection, TLS,
and supported RabbitMQ versions.

Until then, choose one of these application architectures:

1. consume the stream in a supported Go client and bridge an application event
   to Laravel through an owned HTTP, database/outbox, or queue boundary;
2. keep the relevant Laravel workload on the existing queue abstraction;
3. operate another RabbitMQ-supported Streams client as an explicit bridge.

Each bridge is a separate delivery boundary with its own retry, duplicate,
ordering, and failure semantics.

## Canonical wire contract

The RabbitMQ adapter uses one AMQP 1.0 data section and the following mapping.
JSON is optional; `Payload` remains opaque bytes.

| `rabbitstream.Message` | AMQP 1.0 field | Wire type |
| --- | --- | --- |
| `Payload` | single data section | binary |
| `ContentType` | message properties `content-type` | string |
| `MessageID` | message properties `message-id` | string or binary when consuming |
| `CorrelationID` | message properties `correlation-id` | string or binary when consuming |
| `Timestamp` | message properties `creation-time` | timestamp |
| `RoutingKey` | message annotation `x-rabbitstream-routing-key` | string |
| `Headers` | message annotations except the reserved routing key | string or binary values |
| `Properties` | application properties | string or binary values |
| `PublishingID` | RabbitMQ Streams publishing ID | signed 64-bit compatible nonnegative value |
| stream, partition, offset | broker delivery context | not application-controlled wire metadata |

The adapter sorts incoming annotation and application-property keys so Go
deliveries are deterministic. Unsupported key or value types fail validation;
they are not stringified. Multiple AMQP data sections are rejected. All bytes
are copied and validated against the configured limits after conversion.

## Example event contract

For tracking-event examples, applications may adopt these language-neutral
fields without making them mandatory package policy:

| Meaning | Field |
| --- | --- |
| event identifier | AMQP `message-id` |
| correlation or shipment identifier | AMQP `correlation-id` |
| event creation time | AMQP `creation-time` |
| content encoding | AMQP `content-type` |
| stable partition key | `x-rabbitstream-routing-key` annotation |
| trace context | `traceparent` and optional `tracestate` annotations |
| schema name/version | application properties such as `schema` and `schema-version` |
| domain body | opaque single data section |

W3C propagation is available in the optional OpenTelemetry adapter. Baggage is
excluded because it can carry customer or credential-like data. Duplicate W3C
fields fail closed during extraction.

## Compatibility test gate

Before adding a non-Go compatibility claim, pin and record:

- client name, version, source commit, and license;
- RabbitMQ server version and image digest;
- Go, foreign runtime, operating system, and architecture;
- exact wire fixture bytes or independently decoded values;
- TLS/authentication configuration;
- publish confirmation and consumer offset behavior;
- broker restart and reconnection results.

The test must publish in one language and consume in the other in both
directions when the foreign client supports both operations. A local
encode/decode test in one implementation is not interoperability evidence.
