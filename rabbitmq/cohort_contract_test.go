//nolint:staticcheck // Compile the intentionally deprecated facade as a public consumer.
package rabbitmq_test

import (
	"context"
	"errors"
	"testing"

	legacy "github.com/faustbrian/go-rabbitmq-streams/rabbitmq/v2"
	rabbitstream "github.com/faustbrian/go-rabbitmq-streams/v2"
)

func TestFacadeV2CanceledOpenDoesNotResolveCredentials(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	connection := rabbitstream.ConnectionConfig{
		Endpoints:   []rabbitstream.Endpoint{{Host: "rabbitmq.invalid", Port: 5551}},
		Credentials: &unresolvedCredentials{t: t},
		Security:    rabbitstream.DevelopmentPlaintextSecurity(),
	}
	producer, err := legacy.OpenProducer(ctx, connection, rabbitstream.ProducerConfig{Stream: "events"})
	if producer != nil || !errors.Is(err, rabbitstream.ErrCanceled) || !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled producer open = %v, %v", producer, err)
	}
	consumer, err := legacy.OpenConsumer(ctx, connection, rabbitstream.ConsumerConfig{
		Stream: "events", ConsumerName: "worker",
		Start: rabbitstream.StartPosition{Kind: rabbitstream.OffsetStartBeginning},
	})
	if consumer != nil || !errors.Is(err, rabbitstream.ErrCanceled) || !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled consumer open = %v, %v", consumer, err)
	}
}

// These assignments exercise the nominal type contract used by real consumers.
var (
	_ func(context.Context, rabbitstream.ConnectionConfig, rabbitstream.ProducerConfig) (*rabbitstream.Producer, error) = legacy.OpenProducer
	_ func(context.Context, rabbitstream.ConnectionConfig, rabbitstream.ConsumerConfig) (*rabbitstream.Consumer, error) = legacy.OpenConsumer
	_ func(rabbitstream.ConnectionConfig, rabbitstream.Limits) (*legacy.Inspector, error)                               = legacy.NewInspector
	_ func(rabbitstream.ConnectionConfig, rabbitstream.Limits) (*legacy.Replayer, error)                                = legacy.NewReplayer
)
