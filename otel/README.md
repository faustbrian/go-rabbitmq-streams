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
go get github.com/faustbrian/go-rabbitmq-streams/otel@v1
```

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

The compiling examples in this module contain complete imports and setup.

## Guarantees and limitations

The [complete guide](docs/reference.md) defines ownership, failure semantics,
bounds, concurrency, security, and unsupported behavior. Do not infer
additional guarantees beyond the documented module boundary.

## Documentation

- [Documentation index](docs/README.md)
- [Complete technical guide](docs/reference.md)
- [Go API reference](https://pkg.go.dev/github.com/faustbrian/go-rabbitmq-streams/otel)
- [Parent package documentation](../docs/README.md)

## Compatibility and support

This module follows Semantic Versioning. Report vulnerabilities through the
[parent security policy](../SECURITY.md).

## License

MIT. See [LICENSE](LICENSE).
