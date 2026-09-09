# Rabbitstream OpenTelemetry adapter

`rabbitstreamotel` converts the root module's bounded `rabbitstream.Observation`
values into caller-owned OpenTelemetry metrics and propagates W3C Trace Context
through `rabbitstream.Message` headers. The root module remains telemetry-vendor
neutral.

This adapter belongs to Golib's integration and data movement family. See the
versioned [v1.4.0 ecosystem
index](https://github.com/faustbrian/go-library-tools/blob/v1.4.0/docs/ecosystem/README.md)
and its [Integration and data movement family](https://github.com/faustbrian/go-library-tools/blob/v1.4.0/docs/ecosystem/design-language.md#package-families-and-selection)
for package selection and shared ownership conventions.

## Install

```sh
go get github.com/faustbrian/go-rabbitmq-streams/adapters/otel@v1
```

## Package map

| Package | Use |
| --- | --- |
| `github.com/faustbrian/go-rabbitmq-streams/adapters/otel` | Add bounded metrics and W3C Trace Context propagation to root `rabbitstream` values. |

This module has no public subpackages. Use the independently released root
module for stream policy and the `adapters/rabbitmq` module for protocol
resources.

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

The compiling [`Example`](example_test.go) contains complete imports and setup.

## Guarantees and limitations

The [complete guide](docs/reference.md) defines ownership, failure semantics,
bounds, concurrency, security, and unsupported behavior. Do not infer
additional guarantees beyond the documented module boundary.

The adapter starts no goroutines, owns no OpenTelemetry provider or exporter,
and exposes no `Close` or `Shutdown`. Callers flush and shut down their provider
only after RabbitMQ Streams clients stop emitting observations.

## Documentation

- [Documentation index](docs/README.md)
- [Complete technical guide](docs/reference.md)
- [Go API reference](https://pkg.go.dev/github.com/faustbrian/go-rabbitmq-streams/adapters/otel)
- [Parent package documentation](https://github.com/faustbrian/go-rabbitmq-streams/blob/main/docs/README.md)
- [Operations and FAQ](https://github.com/faustbrian/go-rabbitmq-streams/blob/main/docs/operations.md)
- [Support](https://github.com/faustbrian/go-rabbitmq-streams/blob/main/SUPPORT.md)
- [Private vulnerability reporting](https://github.com/faustbrian/go-rabbitmq-streams/blob/main/SECURITY.md)

## Compatibility and support

This stable module requires Go 1.26.6 and follows Semantic Versioning. Use the
[parent support policy](https://github.com/faustbrian/go-rabbitmq-streams/blob/main/SUPPORT.md)
for adoption help and report vulnerabilities through the
[parent security policy](https://github.com/faustbrian/go-rabbitmq-streams/blob/main/SECURITY.md).

## License

MIT. See [LICENSE](LICENSE).
