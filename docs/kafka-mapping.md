# Kafka to RabbitMQ Streams mapping

This report is the design input for `rabbitstream`; it is not a claim that
Kafka and RabbitMQ Streams are interchangeable. It was checked against the
pinned sources in `specification/sources.lock.json`.

| Kafka concept or repository use | Classification | RabbitMQ design |
| --- | --- | --- |
| retained topic used by independent readers | Streams-native | one RabbitMQ stream and independently named consumers |
| partitioned high-volume tracking ingress | Super Streams-native | one Super Stream; backing streams are the ordering scopes |
| record key used for per-aggregate order | Super Streams-native | an explicit routing key and a reviewed stable routing strategy |
| topic partition | Super Streams-native | one backing stream; there is no global order across backing streams |
| consumer group distribution | requires redesign | named stream consumers track positions; Super Stream distribution and single-active-consumer behavior are not Kafka group protocols |
| earliest/latest/absolute/timestamp start | Streams-native | first, next, absolute offset, or timestamp; RabbitMQ clamps unavailable absolute and timestamp requests, so exact replay must inspect the retained range and fail closed before subscribing |
| committed group offset | Streams-native with different semantics | named consumer offset stored in the stream; storing is a separate effect after handler success |
| replay with normal group identity | requires redesign | a replay identity or no broker tracking; live-consumer progress must not be advanced |
| idempotent producer | Streams-native with narrower scope | one producer name per stream plus monotonically increasing publishing IDs; no concurrent instances with the same name |
| `acks=all` delivery result | Streams-native | publisher confirmation after a quorum has written; cancellation after transmission can still leave an ambiguous caller outcome |
| retry topic | Streams-native or existing `queue` | a retry stream for retained event redistribution, or `queue` for delayed job execution |
| dead-letter topic | Streams-native or existing `queue` | a dedicated dead-letter stream when retained history is required; otherwise a queue-owned terminal job path |
| Kafka read-process-write transaction | unsupported equivalent | RabbitMQ Streams does not provide the same atomic source-offset plus target-publish transaction; use explicit duplicate/loss windows or redesign |
| database/outbox transaction | application/database responsibility | commit domain state and outbox atomically, then publish at least once through a downstream adapter |
| Kafka telemetry adapter | adapter responsibility | add a separate RabbitMQ Streams observer adapter; do not make OpenTelemetry a core dependency |
| Kafka service lifecycle adapter | application/service composition responsibility | the stable producer and consumer already expose bounded start/run/close behavior; do not add a framework adapter until a concrete adoption workload requires additional lifecycle policy |
| work queue, delayed job, webhook execution, command | existing `queue` package | retain AMQP queue acknowledgement, retry, dead-letter, and process-and-remove semantics |

## Repository consumer inventory

This inventory covers current production modules, owned tooling, and adoption
fixtures that import or are defined by the Kafka contract. Each independently
releasable module requires a contract-specific migration; none is redirected
implicitly by the root `rabbitstream` module.

| Current module or use case | Classification | Migration decision |
| --- | --- | --- |
| `pkg/kafka` producer, consumer, replay, inspection, and retained-topic policy | Streams-native or Super Streams-native by configured topic shape | migrate each configured workload to the equivalent explicit stream policy; do not preserve Kafka-shaped APIs |
| `pkg/kafka` transactional producer and read-process-write transaction processor | unsupported / requires redesign | RabbitMQ Streams has no equivalent atomic source-offset plus target-publish transaction |
| `pkg/kafka/adapters/golog` | application responsibility | add a stream-specific observer adapter only if the stable root observation seam lacks required logging integration |
| `pkg/kafka/adapters/gotelemetry` | adapter responsibility | use `pkg/rabbitstream/otel`; separately review Kafka-only spans, attributes, and propagation semantics |
| `pkg/kafka/adapters/mskiam` | unsupported / remove for RabbitMQ | AWS MSK IAM is Kafka-specific; use RabbitMQ TLS and authentication policy instead |
| `pkg/kafka/kafkaservice` | application/service composition responsibility | no generic `rabbitstreamservice` module is added: the core lifecycle is explicit, and no current adoption contract justifies correlation or service-framework coupling |
| `pkg/kafka/benchmarks/clients` | validation tooling | replace with equivalent raw-client and policy-wrapper RabbitMQ Streams benchmarks, preserving durability and payloads |
| `pkg/kafka/kafkatest` | validation tooling | create RabbitMQ-specific fakes only for deterministic policy seams; real broker guarantees remain integration tests |
| `pkg/cloudevents/adapters/golib` Kafka transport | Streams-native | use the distinct structured JSON RabbitMQ Streams mapping in that adapter; it keeps broker state outside CloudEvents and does not invent an unverified binary AMQP binding |
| `pkg/event-sourcing/adapters/gokafka` dispatcher, handler, codec, and dead-letter path | application/database responsibility plus Streams-native transport | no generic Streams adapter is added: event-store position, dispatcher checkpoint, retry/dead-letter publication, source offset advancement, and duplicate windows require a workload-specific design before transport mapping |
| `pkg/event-sourcing/adapters/gotelemetry` Kafka propagation | adapter responsibility | use the language-neutral message and W3C propagation contract; do not couple event-sourcing telemetry to the root module |
| `pkg/outbox/adapters/gokafka` relay publisher | application/database responsibility plus Streams-native publisher | use `pkg/outbox/adapters/gorabbitstream`; it retains database/outbox transaction ownership and requires one confirmed publish without claiming atomic relay settlement |
| `pkg/service/integration/adoption` Kafka fixture | adoption fixture | add a separate RabbitMQ Streams adoption fixture after a service adapter exists; do not replace the Kafka fixture in place |
| delayed retry, command execution, webhook delivery, and jobs currently routed through messaging | existing `queue` package | keep queue semantics rather than translating them into retained streams |

The root package therefore has no dependency on Kafka, queue, outbox,
event-sourcing, service, CloudEvents, or telemetry.

## Hard incompatibilities

RabbitMQ exact-offset and timestamp subscriptions clamp out-of-range requests;
safe replay must compare the requested range with broker statistics first.
Broker offset storage is not transactional with a handler's database, HTTP, or
other external effect. Publisher deduplication is scoped to one named producer
and stream, tracks only a bounded set of producer names, and does not make the
end-to-end workflow exactly once. A Super Stream is a logical set of streams;
partition topology changes can change routing and never create global order.

The selected Go client's `ha` package is not the policy boundary: its reconnect
path recursively retries without a caller context or attempt bound, blocks
publishes while reconnecting, and logs underlying error strings. `rabbitstream`
must own bounded, cancellation-aware reconnection and safe diagnostics while
using the supported client for protocol operations.

## Laravel/PHP status

RabbitMQ's supported Stream protocol client list does not include PHP, and no
RabbitMQ-organization PHP Stream client exists as of the verification date.
The PHP projects found during research are new third-party implementations with
no production evidence suitable for a release claim. Consequently this module
must document a language-neutral AMQP 1.0 message contract but must not claim
direct Laravel/PHP Stream client interoperability. A Go bridge or another
supported client is an application architecture decision, not core behavior.

## Migration sequence

For every application workload:

1. record the topic, partitions, key, producer acknowledgement and idempotency
   settings, consumer groups, offset reset/replay policy, retry/DLQ behavior,
   transactions, retention, throughput, payload distribution, and dependencies;
2. classify the workload using the inventory categories above;
3. provision a stream or Super Stream and pin routing, retention, replication,
   TLS, permissions, and ownership outside the application;
4. define the binary/application-property wire contract and deploy compatible
   readers before writers;
5. run dual publication only with an explicit reconciliation key and duplicate
   policy; dual publication is not atomic;
6. build an independent RabbitMQ consumer position and validate ordering,
   duplicates, lag, backlog catch-up, replay, and failure recovery;
7. cut reads only after parity or deliberate semantic redesign is demonstrated;
8. retain rollback access to the Kafka history for the agreed window;
9. remove Kafka adapters and infrastructure only after every dependent module
   and application has migrated and its release gates pass.

Kafka transactions are a hard migration gate. A workload depending on atomic
offset commit plus target publication must be redesigned, commonly around an
application database/outbox or idempotent processing. It must not be labeled a
behaviorally equivalent Streams migration.
