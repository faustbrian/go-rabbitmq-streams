# Rabbitstream RabbitMQ compatibility facade

> Deprecated: use
> `github.com/faustbrian/go-rabbitmq-streams/adapters/rabbitmq`.

This stable v1 module preserves the released `OpenProducer`, `OpenConsumer`,
`Inspector`, and `Replayer` API identities, error classification, and runtime
behavior while delegating to the target-oriented successor.

Existing consumers may continue to use:

```sh
go get github.com/faustbrian/go-rabbitmq-streams/rabbitmq@v1
```

New consumers should use:

```sh
go get github.com/faustbrian/go-rabbitmq-streams/adapters/rabbitmq@v1
```

Migration changes only the import path. Constructor and method calls remain
unchanged. See the
[migration guide](https://github.com/faustbrian/go-rabbitmq-streams/blob/main/docs/migration.md),
the [canonical adapter](https://pkg.go.dev/github.com/faustbrian/go-rabbitmq-streams/adapters/rabbitmq),
and the [changelog](CHANGELOG.md).
The released behavior retained by this facade remains documented in the
canonical adapter's
[specification decision register](https://github.com/faustbrian/go-rabbitmq-streams/blob/main/adapters/rabbitmq/docs/specification-decisions.md).

This facade requires Go 1.27.0. Broker resources and lifecycle remain owned by
the canonical adapter and returned root values. Use the parent
[support](https://github.com/faustbrian/go-rabbitmq-streams/blob/main/SUPPORT.md)
and [security](https://github.com/faustbrian/go-rabbitmq-streams/blob/main/SECURITY.md)
policies.
