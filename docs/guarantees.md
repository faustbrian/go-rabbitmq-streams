# Guarantees and responsibility boundaries

This document separates facts that are often collapsed into a misleading
"delivery guarantee". The pinned source baseline is recorded in
[`../specification/sources.lock.json`](../specification/sources.lock.json).

## Responsibility matrix

| Owner | Guarantees and responsibilities |
| --- | --- |
| RabbitMQ broker | persists and replicates stream data according to configured stream policy; confirms accepted publications; retains data according to operator configuration; stores named consumer offsets as separate broker operations |
| RabbitMQ Streams protocol and selected Go client | performs protocol negotiation, publishing, confirmations, subscriptions, offset queries/stores, and topology queries; exposes connection and broker failures available to the adapter |
| `rabbitstream` policy | validates and copies bounded data; limits in-flight work, retries, handlers, and shutdown; represents definite, rejected, and ambiguous publish outcomes; stores offsets only after successful processing policy; validates exact replay ranges |
| infrastructure/operator | provisions streams, Super Streams, partitions, replication, retention, listeners, certificates, users, permissions, monitoring, upgrades, and capacity |
| application | defines payload/schema semantics, routing identities, producer and consumer identity ownership, handler idempotency, external side-effect reconciliation, deployment order, and incident response |

## Publishing

A successful `DeliveryConfirmed` means the broker sent a positive publisher
confirmation. It does not mean every replica is healthy forever, a consumer
processed the record, or an external side effect committed.

Cancellation or timeout before transport acceptance is a definite non-send.
After `Send` succeeds, cancellation, confirmation timeout, or connection loss
is `DeliveryAmbiguous`: the caller must assume the broker may have retained the
record. A later retry can duplicate the record unless broker deduplication or
application idempotency recognizes it.

Named-producer publishing IDs are scoped to one producer name and one stream.
They are not a distributed transaction and do not cover downstream processing.
One active owner must allocate monotonically increasing IDs for that identity.

`PublishBatch` is not atomic. It validates the whole batch first, sends in
order, stops at the first failure, and reports later entries as not sent.

## Consumption and offset storage

The default consumer is at least once. It invokes a handler before storing that
delivery's offset. If the process fails after the side effect but before offset
storage, the event is redelivered. Handlers must therefore be idempotent or
reconcile duplicate side effects.

`OffsetStoreEveryMessages > 1` deliberately increases the crash window: on
restart, successfully processed messages since the last store can be replayed.
It never permits a failed message to be silently skipped within its partition.

Max concurrency applies across independent partition workers. Each partition
remains sequential. Batch consumption never combines partitions and stores
only the last offset after the complete batch succeeds.

Broker offset storage is not transactional with a database, HTTP request,
queue operation, or target stream publish.

## Retry and dead-letter streams

In-process retry is bounded and blocks progress for the affected partition.
Publishing a retry or dead-letter event is a new confirmed publish. Source
offset storage happens afterward. A crash between these operations can
duplicate the failure event; reversing the order could lose it, so the package
does not do that.

Queue-style per-message NACK or delayed redelivery is not fabricated. Use the
existing `queue` package when the workload is a delayed job rather than a
retained event history.

## Ordering and Super Streams

Ordering is defined only within one backing stream. Concurrent publishers can
observe different completion order even when broker order is valid. There is
no global order across Super Stream partitions.

Hash routing maps one routing key to one backing stream for the observed
ordered topology. Adding, removing, or reordering partitions can remap keys.
Applications requiring per-aggregate order must keep a stable key and review
topology changes as migrations.

## Retention and replay

Retention is operator policy, not application storage. An offset can disappear
before replay if retention advances. Replay first snapshots the exact retained
range and fails closed when an explicit start is no longer retained or an end
is not available.

Timestamp subscriptions can be clamped by RabbitMQ. They do not prove that an
exact historical instant is retained. Super Stream replay pins an ordered
topology and runs one partition at a time; it never claims global order.

Replay opens an isolated cursor, stores no offset, and never uses the live
consumer identity. `AllowSideEffects` is visible to the handler but cannot make
side effects transactional or idempotent.

## Lifecycle

Opened clients own their transport resources. Close is idempotent and bounded,
but a caller cancellation can stop waiting before internal cleanup finishes.
Applications must stop admission, cancel consumers, wait for handlers, close
producers and consumers, then flush caller-owned telemetry.

Observer delivery is best effort. Telemetry panics are contained and telemetry
failure never changes delivery correctness.
