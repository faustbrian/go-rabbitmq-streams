# Rabbitstream OpenTelemetry adapter

`rabbitstreamotel` converts the root module's bounded `rabbitstream.Observation`
values into caller-owned OpenTelemetry metrics and propagates W3C Trace Context
through `rabbitstream.Message` headers. The root module remains telemetry-vendor
neutral.

## Quick start

```go
adapter, err := rabbitstreamotel.New(rabbitstreamotel.Config{
    MeterProvider: meterProvider,
    Limits:        rabbitstream.DefaultLimits(),
})
if err != nil {
    return err
}

producer, err := rabbitstream.NewProducer(rabbitstream.ProducerConfig{
    // transport and stream policy omitted
    Observer: adapter,
})
```

Inject trace context before publication without mutating caller-owned bytes:

```go
outbound, err := adapter.Inject(ctx, rabbitstream.Message{
    Stream:  "tracking.events",
    Payload: encoded,
})
if err != nil {
    return err
}
result, err := producer.Publish(ctx, outbound)
```

At consumption, extract the remote context before invoking application code:

```go
handlerContext, err := adapter.Extract(ctx, message)
if err != nil {
    return err
}
return handler(handlerContext, message)
```

Use the same `rabbitstream.Limits` as the producer and consumer. `Inject`
validates both the caller's input and the post-injection message. `Extract`
rejects out-of-bounds messages and ignores ambiguous duplicate W3C fields.

## API and ownership

- `New` creates instruments from a caller-owned `metric.MeterProvider`.
- `Adapter.Observe` is concurrency-safe, starts no goroutines, and never blocks
  on adapter-owned work.
- `Adapter.Inject` returns a deep-owned message and replaces stale
  `traceparent` and `tracestate` fields.
- `Adapter.Extract` borrows the message synchronously and returns a context with
  a remote W3C span context.
- the adapter does not create spans, configure exporters, install globals, or
  shut down the provider.

The caller must flush and shut down its OpenTelemetry provider after all
RabbitMQ Streams clients have stopped emitting observations.

## Metrics and privacy

The adapter emits only fixed metric names and the closed
`rabbitstream.error.category` dimension. It never records stream or Super
Stream names, partitions, routing keys, message IDs, offsets as dimensions,
payloads, arbitrary metadata, endpoints, credentials, or error text.

| Metric | Meaning |
| --- | --- |
| `rabbitstream.connection.state` | last observed connection state, `0` or `1` |
| `rabbitstream.reconnects` | reconnect attempts |
| `rabbitstream.publish.messages` | confirmed messages |
| `rabbitstream.publish.bytes` | attempted payload bytes |
| `rabbitstream.publish.confirmation.duration` | completion latency in seconds |
| `rabbitstream.publish.unconfirmed` | locally outstanding publish attempts |
| `rabbitstream.consumer.messages` | delivered messages |
| `rabbitstream.consumer.bytes` | delivered payload bytes |
| `rabbitstream.consumer.handler.duration` | handler duration in seconds |
| `rabbitstream.consumer.handler.retries` | in-process handler retry attempts |
| `rabbitstream.consumer.retry_stream.messages` | confirmed retry-stream publications |
| `rabbitstream.consumer.dead_letter.messages` | confirmed dead-letter-stream publications |
| `rabbitstream.consumer.failure_publish.errors` | failed retry/dead-letter publications |
| `rabbitstream.consumer.offset` | latest accepted broker offset store |
| `rabbitstream.stream.end_offset` | exact inspected stream end offset |
| `rabbitstream.consumer.lag` | exact inspected stored-offset lag |
| `rabbitstream.replay.messages` | replay progress messages |
| `rabbitstream.producer.shutdown.duration` | producer shutdown duration in seconds |
| `rabbitstream.consumer.shutdown.duration` | consumer shutdown duration in seconds |
| `rabbitstream.errors` | failures by closed stable category |

Telemetry provider panics during observation are contained. Instrument-creation
errors preserve their cause through `errors.Is` while the rendered
`OperationError` remains category-only. Callers must not log unwrapped causes
without application-owned redaction.

## Trace propagation

Only W3C `traceparent` and `tracestate` are propagated. Baggage is deliberately
excluded because it can carry customer or credential-like data. Header matching
uses ASCII case folding as required by the propagation contract; non-ASCII
application keys remain distinct. Invalid `traceparent` leaves the supplied
context unchanged, and duplicate W3C fields fail closed by leaving it unchanged.

Propagation is synchronous and does not check context cancellation. A nil
context is a validation error. It does not publish, consume, acknowledge,
retry, or advance an offset.

## Adoption and tradeoffs

Adopt this module when the root observation seam and explicit W3C propagation
are sufficient. Use application-owned tracing around publish and handler calls
when operation spans are required: the observation seam carries no context and
cannot prove producer creation/send links or handler parentage after completion.

The unconfirmed gauge is process-local telemetry reconstructed from observed
attempt and completion events. It is not broker state. Process loss, dropped
telemetry, or attaching the adapter after publication begins can make it differ
from broker-confirmation state. Provider failure can also drop an attempt or
completion signal; later values remain best-effort and are clamped at zero.

The selected client does not identify broker deduplication hits, so the root
observation contract and this adapter do not fabricate a duplicate counter.

## Security notes

- supply a bounded exporter and reader configuration; this module does not own
  export queues or timeouts;
- do not add caller-controlled metric attributes around this adapter;
- do not export baggage automatically;
- treat unwrapped provider errors as potentially sensitive;
- use verified TLS and root-module credential policy independently of telemetry.

## FAQ

### Does this enable OpenTelemetry globally?

No. Every provider is explicit and caller-owned.

### Does it create messaging spans?

No. Completion observations lack the context and lifecycle evidence needed for
honest OpenTelemetry producer and consumer span semantics.

### Can telemetry failure fail a publish or handler?

No. Root observers and this adapter contain observer panics. Propagation errors
are explicit validation results before transport or handler invocation.

### Is baggage propagated?

No. Only W3C Trace Context fields are supported.

## Release notes

See [CHANGELOG.md](CHANGELOG.md).

Use the [Golib documentation portal](https://github.com/faustbrian/golib/blob/main/docs/index.md)
to choose packages, compose services, and review repository-wide guarantees.
