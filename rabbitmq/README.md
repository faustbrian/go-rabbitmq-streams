# Rabbitstream RabbitMQ compatibility facade

> Deprecated: use
> `github.com/faustbrian/go-rabbitmq-streams/adapters/rabbitmq/v2`.

This v2 compatibility facade delegates `OpenProducer`, `OpenConsumer`,
`Inspector`, and `Replayer` to the canonical v2 adapter and its root-v2 policy
types. The v1 cohort remains separate; v1 values cannot be mixed with v2.

Install the v2 facade:

```sh
go get github.com/faustbrian/go-rabbitmq-streams/rabbitmq/v2@v2
```

New consumers should use:

```sh
go get github.com/faustbrian/go-rabbitmq-streams/adapters/rabbitmq/v2@v2
```

Migrate root and adapter or facade imports together. V2 adds bounded native
decoding and cooperative transport shutdown and rejects wire shapes outside
those bounds; this is not merely an import alias for the v1 release. See the
[migration guide](https://github.com/faustbrian/go-rabbitmq-streams/blob/main/docs/migration.md),
the [canonical adapter](https://pkg.go.dev/github.com/faustbrian/go-rabbitmq-streams/adapters/rabbitmq/v2),
and the [changelog](CHANGELOG.md).
The released behavior retained by this facade remains documented in the
canonical adapter's
[specification decision register](https://github.com/faustbrian/go-rabbitmq-streams/blob/main/adapters/rabbitmq/docs/specification-decisions.md).

This facade requires Go 1.27.2. Broker resources and lifecycle remain owned by
the canonical adapter and returned root values. Use the parent
[support](https://github.com/faustbrian/go-rabbitmq-streams/blob/main/SUPPORT.md)
and [security](https://github.com/faustbrian/go-rabbitmq-streams/blob/main/SECURITY.md)
policies.
