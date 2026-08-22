package rabbitstream

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"testing"
	"time"
)

func TestMain(main *testing.M) {
	if err := verifyMutationSafetyBoundaries(); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	os.Exit(main.Run())
}

// verifyMutationSafetyBoundaries fails before the wider suite can exercise a
// broken zero-capacity producer or a worker that ignores cancellation. Those
// failures otherwise deadlock the process instead of producing a useful test
// failure.
func verifyMutationSafetyBoundaries() error {
	normalized, err := (ProducerConfig{Stream: "stream"}).Normalized()
	if err != nil {
		return fmt.Errorf("producer defaults: %w", err)
	}
	if normalized.Policy.MaxOutstanding != defaultMaxOutstanding {
		return fmt.Errorf("producer max outstanding = %d, want %d", normalized.Policy.MaxOutstanding, defaultMaxOutstanding)
	}

	ready := make(chan Message, 1)
	ready <- Message{Stream: "stream"}
	if message, ok := nextConsumerWorkerMessage(context.Background(), ready); !ok || message.Stream != "stream" {
		return fmt.Errorf("live consumer worker message = %#v, %t", message, ok)
	}

	ready <- Message{Stream: "stream"}
	canceled := &errOnlyCanceledContext{Context: context.Background()}
	if message, ok := nextConsumerWorkerMessage(canceled, ready); ok || !reflect.DeepEqual(message, Message{}) {
		return fmt.Errorf("canceled consumer worker message = %#v, %t", message, ok)
	}

	consumer, err := NewConsumer(ConsumerConfig{
		Stream: "stream", ConsumerName: "mutation-guard",
		Policy: ConsumerPolicy{CloseTimeout: time.Nanosecond},
	}, mutationGuardConsumerTransport{})
	if err != nil {
		return fmt.Errorf("consumer close guard: %w", err)
	}
	if err := consumer.Close(context.Background()); err != nil {
		return fmt.Errorf("consumer close before run: %w", err)
	}
	return nil
}

type mutationGuardConsumerTransport struct{}

func (mutationGuardConsumerTransport) Next(context.Context) (Message, error) {
	return Message{}, context.Canceled
}

func (mutationGuardConsumerTransport) StoreOffset(context.Context, string, uint64) error { return nil }
func (mutationGuardConsumerTransport) Close() error                                      { return nil }
