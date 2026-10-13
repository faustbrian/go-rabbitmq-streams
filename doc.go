// Package rabbitstream provides bounded RabbitMQ Streams and Super Streams
// policy for retained event distribution. It is not a work-queue abstraction;
// applications requiring competing job workers and process-and-remove
// semantics should use the separate go-rabbitmq-queues family.
//
// NewProducer and NewConsumer wrap explicit, owned transports; NewReplayer uses
// a borrowed isolated source. Publish copies admitted bytes; handlers receive
// call-scoped deliveries and use Retain before keeping or asynchronously using
// them. Backpressure, confirmation and caller shutdown budgets are separate.
// A shutdown wait timeout is not cleanup completion: one owner continues joining
// admitted work and trusted context-free transport Close must eventually return.
//
// Confirmation and named offsets concern broker state, not atomic application
// effects. One-way offset storage has no durable receipt; rerouting and source
// storage are separate operations. Replay checkpoints/side-effect authority,
// effect idempotency, broker retention, ACLs and network policy belong to callers.
// Observers and handlers contain panics, but arbitrary provider panics may
// escape. Borrowed collaborators run synchronously; containment cannot force
// prompt completion or make arbitrary diagnostic data safe to log.
//
// The optional adapters/rabbitmq/v2 binding follows RabbitMQ Streams 4.3.5 and
// pinned client v1.8.3 interpretations; its specification/decision register owns
// protocol ambiguity and direct-service evidence. Both telemetry import paths
// intentionally retain published root-v1 types and are not v2 adapters. The core
// introduces no broker client, infrastructure goroutine or cgo dependency merely
// by import; clients must be constructed before use.
package rabbitstream
