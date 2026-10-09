# Lifecycle and adapter-path migration

## Root v2

Change root imports to `github.com/faustbrian/go-rabbitmq-streams/v2` and use
Go 1.27.2 or newer. Root v2 has distinct public type identities; replace the
entire root cohort used by an application rather than mixing v1 and v2 values.
Repository directories and the main branch do not change. Root tags are
`v2.x.y`; nested module tags retain their directory prefix.

The RabbitMQ adapter and facade require their own v2 releases to adopt these
root identities. Root publication does not claim that those releases are
available. The unchanged OpenTelemetry adapter/facade remain on their published
root-v1 cohort and are not compatible with root-v2 observer/configuration types.

Custom transports and replay sources must provide an explicit offset (including
offset zero), a bounded backing stream/partition identity, and messages within
the configured payload/metadata limits. Rejected deliveries cannot reach
handlers, failure publication, or offset storage. Shutdown timeouts bound the
individual caller's wait; they do not prove or permanently finalize cleanup.
Trusted callbacks and contextless transport operations must still return.

## RabbitMQ adapter and facade v2

Use `github.com/faustbrian/go-rabbitmq-streams/adapters/rabbitmq/v2` with
root-v2 values, or `github.com/faustbrian/go-rabbitmq-streams/rabbitmq/v2`
for the deprecated facade within that same cohort. Change imports together.
Facade-v2 publication follows adapter-v2; the existing v1 facade does not
silently adopt these types or controls.
Repository directories and main remain unchanged; tags are
`adapters/rabbitmq/v2.x.y` and `rabbitmq/v2.x.y`.

The adapter packages an attributed private client rather than relying on
downstream vendoring or replacement directives. Native frame, chunk, AMQP
count/depth and decompression limits can reject wire shapes previously
accepted by v1. Context cancellation reaches native opening, RPC and socket
operations; shutdown joins owned workers. Applications still own cooperative
callbacks, broker permissions, topology and end-to-end side-effect idempotency.
See the [security model](security-model.md) and
[source maintenance record](../adapters/rabbitmq/THIRD_PARTY_RABBITMQ_STREAM.md).

## Existing v1 adapter-path migration

The canonical optional integrations are now target-oriented modules:

| Released path | Canonical successor |
| --- | --- |
| `github.com/faustbrian/go-rabbitmq-streams/otel` | `github.com/faustbrian/go-rabbitmq-streams/adapters/otel` |
| `github.com/faustbrian/go-rabbitmq-streams/rabbitmq` | `github.com/faustbrian/go-rabbitmq-streams/adapters/rabbitmq` |

Both released paths remain supported deprecated compatibility facades. Their
exported type identities, errors, OpenTelemetry instrumentation scope, and
runtime behavior remain compatible. Existing applications may upgrade without
changing imports. New applications should use the canonical paths. Migration
requires only changing the import path and dependency; constructor and method
calls remain unchanged.

Root producers and consumers now expose `Shutdown(ctx)`. It starts the owned
terminal cleanup once, allows every concurrent caller to bound its own wait,
and returns the same terminal cleanup result to every caller that observes
completion. The existing `Close(ctx)` method delegates to `Shutdown(ctx)` and
remains supported for compatibility.

The next major release changes timeout handling: context and `CloseTimeout`
bound each caller's entire wait, including producer draining. A timeout does
not mean transport cleanup completed and is not cached as its terminal result.
One owner continues cleanup; a later call on the same value can observe the
actual close result. Cleanup completion still requires trusted handlers,
transport sends and contextless transport close operations to return. This
ownership correction does not provide preemptive supplier cancellation.

```go
if err := producer.Shutdown(ctx); err != nil {
    return err
}
if err := consumer.Shutdown(ctx); err != nil {
    return err
}
```

Do not retry application shutdown by constructing replacement transports.
After one caller starts shutdown, later callers should call `Shutdown` on the
same value with their own deadline and observe the shared terminal result.
