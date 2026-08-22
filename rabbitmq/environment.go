package rabbitmq

import (
	"github.com/faustbrian/golib/pkg/rabbitstream"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/message"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"
)

type rabbitProducer interface {
	// Send submits one supported-client wire message.
	Send(message.StreamMessage) error
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
	StoreCustomOffset(int64) error
	// NotifyClose returns the consumer terminal event stream.
	NotifyClose() stream.ChannelClose
	// Close releases the supported-client consumer.
	Close() error
}

type rabbitEnvironment interface {
	// QueryPartitions returns the broker's ordered Super Stream backing streams.
	QueryPartitions(string) ([]string, error)
	// StreamExists reports whether a direct stream exists.
	StreamExists(string) (bool, error)
	// StreamStats returns supported-client stream statistics.
	StreamStats(string) (*stream.StreamStats, error)
	// QueryOffset returns a named consumer's broker-stored offset.
	QueryOffset(string, string) (int64, error)
	// NewConsumer opens one supported-client consumer.
	NewConsumer(string, stream.MessagesHandler, *stream.ConsumerOptions) (rabbitConsumer, error)
	// Close releases the supported-client environment.
	Close() error
}

type producerEnvironment interface {
	rabbitEnvironment
	// NewProducer opens one supported-client producer.
	NewProducer(string, *stream.ProducerOptions) (rabbitProducer, error)
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
func (environment *streamEnvironment) QueryPartitions(name string) ([]string, error) {
	return environment.environment.QueryPartitions(name)
}

// StreamExists delegates existence lookup to the supported client.
func (environment *streamEnvironment) StreamExists(name string) (bool, error) {
	return environment.environment.StreamExists(name)
}

// StreamStats delegates stream-statistics lookup to the supported client.
func (environment *streamEnvironment) StreamStats(name string) (*stream.StreamStats, error) {
	return environment.environment.StreamStats(name)
}

// QueryOffset delegates broker offset lookup to the supported client.
func (environment *streamEnvironment) QueryOffset(consumerName string, streamName string) (int64, error) {
	return environment.environment.QueryOffset(consumerName, streamName)
}

// NewConsumer wraps a supported-client consumer behind the private boundary.
func (environment *streamEnvironment) NewConsumer(
	name string,
	handler stream.MessagesHandler,
	options *stream.ConsumerOptions,
) (rabbitConsumer, error) {
	return environment.environment.NewConsumer(name, handler, options)
}

// NewProducer wraps a supported-client producer behind the private boundary.
func (environment *streamEnvironment) NewProducer(name string, options *stream.ProducerOptions) (rabbitProducer, error) {
	return environment.environment.NewProducer(name, options)
}

// Close releases the wrapped supported-client environment.
func (environment *streamEnvironment) Close() error { return environment.environment.Close() }
