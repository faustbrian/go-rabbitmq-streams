# Lifecycle and adapter-path migration

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
