package rabbitmq

import (
	"context"
	"errors"

	"github.com/faustbrian/go-rabbitmq-streams/adapters/rabbitmq/v2/internal/rabbitmqstream/message"
	"github.com/faustbrian/go-rabbitmq-streams/adapters/rabbitmq/v2/internal/rabbitmqstream/stream"
	"github.com/faustbrian/go-rabbitmq-streams/v2"
)

type rabbitProducer interface {
	// Send submits one supported-client wire message.
	SendContext(context.Context, message.StreamMessage) error
	// NotifyPublishConfirmation returns the producer confirmation stream.
	NotifyPublishConfirmation() stream.ChannelPublishConfirm
	// NotifyClose returns the producer terminal event stream.
	NotifyClose() stream.ChannelClose
	// GetStreamName returns the backing stream owned by this producer.
	GetStreamName() string
	// Close releases the supported-client producer.
	Close() error
}

type rabbitConsumer interface {
	// StoreCustomOffset submits one broker offset-tracking command.
	StoreCustomOffsetContext(context.Context, int64) error
	// NotifyClose returns the consumer terminal event stream.
	NotifyClose() stream.ChannelClose
	// Close releases the supported-client consumer.
	Close() error
}

type rabbitEnvironment interface {
	// QueryPartitions returns the broker's ordered Super Stream backing streams.
	QueryPartitions(context.Context, string) ([]string, error)
	// StreamExists reports whether a direct stream exists.
	StreamExists(context.Context, string) (bool, error)
	// StreamStats returns supported-client stream statistics.
	StreamStats(context.Context, string) (*stream.StreamStats, error)
	// QueryOffset returns a named consumer's broker-stored offset.
	QueryOffset(context.Context, string, string) (int64, error)
	// NewConsumer opens one supported-client consumer.
	NewConsumer(context.Context, string, stream.MessagesHandler, *stream.ConsumerOptions) (rabbitConsumer, error)
	// Close releases the supported-client environment.
	Close() error
}

type producerEnvironment interface {
	rabbitEnvironment
	// NewProducer opens one supported-client producer.
	NewProducer(context.Context, string, *stream.ProducerOptions) (rabbitProducer, error)
}

type streamEnvironment struct{ environment *stream.Environment }

func validSuperStreamPartitionCount(count int) bool {
	return count > 0 && count <= rabbitstream.MaxSuperStreamPartitions
}

func validSuperStreamPartitions(partitions []string, limits rabbitstream.Limits) bool {
	if !validSuperStreamPartitionCount(len(partitions)) {
		return false
	}
	seen := make(map[string]struct{}, len(partitions))
	for _, partition := range partitions {
		if _, exists := seen[partition]; exists {
			return false
		}
		if err := (rabbitstream.InspectionRequest{Stream: partition}).Validate(limits); err != nil {
			return false
		}
		seen[partition] = struct{}{}
	}
	return true
}

// QueryPartitions delegates ordered topology lookup to the supported client.
func (environment *streamEnvironment) QueryPartitions(ctx context.Context, name string) ([]string, error) {
	return environment.environment.QueryPartitionsContext(ctx, name)
}

// StreamExists delegates existence lookup to the supported client.
func (environment *streamEnvironment) StreamExists(ctx context.Context, name string) (bool, error) {
	return environment.environment.StreamExistsContext(ctx, name)
}

// StreamStats delegates stream-statistics lookup to the supported client.
func (environment *streamEnvironment) StreamStats(ctx context.Context, name string) (*stream.StreamStats, error) {
	return environment.environment.StreamStatsContext(ctx, name)
}

// QueryOffset delegates broker offset lookup to the supported client.
func (environment *streamEnvironment) QueryOffset(ctx context.Context, consumerName string, streamName string) (int64, error) {
	return environment.environment.QueryOffsetContext(ctx, consumerName, streamName)
}

// NewConsumer wraps a supported-client consumer behind the private boundary.
func (environment *streamEnvironment) NewConsumer(
	ctx context.Context,
	name string,
	handler stream.MessagesHandler,
	options *stream.ConsumerOptions,
) (rabbitConsumer, error) {
	return environment.environment.NewConsumerContext(ctx, name, handler, options)
}

// NewProducer wraps a supported-client producer behind the private boundary.
func (environment *streamEnvironment) NewProducer(ctx context.Context, name string, options *stream.ProducerOptions) (rabbitProducer, error) {
	return environment.environment.NewProducerContext(ctx, name, options)
}

// Close joins native cleanup; the root shutdown owner bounds individual callers.
func (environment *streamEnvironment) Close() error {
	closeErr := environment.environment.Close()
	joinErr := environment.environment.WaitContext(context.Background())
	return errors.Join(closeErr, joinErr)
}
