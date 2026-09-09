# Rabbitstream OpenTelemetry compatibility facade

> Deprecated: use
> `github.com/faustbrian/go-rabbitmq-streams/adapters/otel`.

This stable v1 module preserves the released `rabbitstreamotel.Config` and
`rabbitstreamotel.Adapter` type identities, error classification,
OpenTelemetry instrumentation scope, and runtime behavior while delegating to
the target-oriented successor.

Existing consumers may continue to use:

```sh
go get github.com/faustbrian/go-rabbitmq-streams/otel@v1
```

New consumers should use:

```sh
go get github.com/faustbrian/go-rabbitmq-streams/adapters/otel@v1
```

Migration changes only the import path. Construction and method calls remain
unchanged. See the
[migration guide](https://github.com/faustbrian/go-rabbitmq-streams/blob/main/docs/migration.md),
the [canonical adapter](https://pkg.go.dev/github.com/faustbrian/go-rabbitmq-streams/adapters/otel),
and the [changelog](CHANGELOG.md).

This facade requires Go 1.26.6. It owns no provider, exporter, background work,
or shutdown lifecycle. Use the parent
[support](https://github.com/faustbrian/go-rabbitmq-streams/blob/main/SUPPORT.md)
and [security](https://github.com/faustbrian/go-rabbitmq-streams/blob/main/SECURITY.md)
policies.
