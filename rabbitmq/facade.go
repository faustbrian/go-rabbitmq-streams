// Package rabbitmq preserves the released RabbitMQ Streams client adapter
// import path while delegating behavior to the target-oriented successor.
//
// Deprecated: use github.com/faustbrian/go-rabbitmq-streams/adapters/rabbitmq.
package rabbitmq

import (
	"context"

	rabbitstream "github.com/faustbrian/go-rabbitmq-streams"
	successor "github.com/faustbrian/go-rabbitmq-streams/adapters/rabbitmq"
)

// OpenProducer preserves the released producer construction contract.
//
// Deprecated: use successor.OpenProducer.
func OpenProducer(
	ctx context.Context,
	connection rabbitstream.ConnectionConfig,
	config rabbitstream.ProducerConfig,
) (*rabbitstream.Producer, error) {
	return successor.OpenProducer(ctx, connection, config)
}

// OpenConsumer preserves the released consumer construction contract.
//
// Deprecated: use successor.OpenConsumer.
func OpenConsumer(
	ctx context.Context,
	connection rabbitstream.ConnectionConfig,
	config rabbitstream.ConsumerConfig,
) (*rabbitstream.Consumer, error) {
	return successor.OpenConsumer(ctx, connection, config)
}

// Inspector preserves the released inspector identity while delegating all
// broker behavior to the target-oriented successor.
//
// Deprecated: use successor.Inspector.
type Inspector struct {
	delegate *successor.Inspector
}

// NewInspector preserves the released validation and construction contract.
//
// Deprecated: use successor.NewInspector.
func NewInspector(
	connection rabbitstream.ConnectionConfig,
	limits rabbitstream.Limits,
) (*Inspector, error) {
	delegate, err := successor.NewInspector(connection, limits)
	if err != nil {
		return nil, err
	}
	return &Inspector{delegate: delegate}, nil
}

// Inspect preserves bounded read-only topology inspection.
func (inspector *Inspector) Inspect(
	ctx context.Context,
	request rabbitstream.InspectionRequest,
) (rabbitstream.InspectionResult, error) {
	return inspector.delegate.Inspect(ctx, request)
}

// StoredOffset preserves the broker-stored consumer offset query.
func (inspector *Inspector) StoredOffset(
	ctx context.Context,
	streamName string,
	consumerName string,
) (*uint64, error) {
	return inspector.delegate.StoredOffset(ctx, streamName, consumerName)
}

// Health preserves the bounded RabbitMQ dependency-health query.
func (inspector *Inspector) Health(ctx context.Context) rabbitstream.DependencyHealth {
	return inspector.delegate.Health(ctx)
}

// Replayer preserves the released replayer identity while delegating all
// broker behavior to the target-oriented successor.
//
// Deprecated: use successor.Replayer.
type Replayer struct {
	delegate *successor.Replayer
}

// NewReplayer preserves the released validation and construction contract.
//
// Deprecated: use successor.NewReplayer.
func NewReplayer(
	connection rabbitstream.ConnectionConfig,
	limits rabbitstream.Limits,
) (*Replayer, error) {
	delegate, err := successor.NewReplayer(connection, limits)
	if err != nil {
		return nil, err
	}
	return &Replayer{delegate: delegate}, nil
}

// Inspect preserves exact retained-range inspection.
func (replayer *Replayer) Inspect(
	ctx context.Context,
	request rabbitstream.ReplayRequest,
) (rabbitstream.RetainedRange, error) {
	return replayer.delegate.Inspect(ctx, request)
}

// Run preserves isolated replay without changing consumer offsets.
func (replayer *Replayer) Run(
	ctx context.Context,
	request rabbitstream.ReplayRequest,
	handler rabbitstream.ReplayHandler,
) error {
	return replayer.delegate.Run(ctx, request, handler)
}
