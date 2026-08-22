package rabbitstream

import (
	"errors"
	"time"
)

const (
	defaultMaxStreamNameBytes    = 255
	defaultMaxRoutingKeyBytes    = 255
	defaultMaxPayloadBytes       = 1 << 20
	defaultMaxMetadataEntries    = 64
	defaultMaxMetadataKeyBytes   = 128
	defaultMaxMetadataValueBytes = 8 << 10
	defaultMaxMetadataBytes      = 64 << 10
	defaultMaxBatchMessages      = 256
	defaultMaxBatchBytes         = 8 << 20
	defaultMaxBufferedMessages   = 1024

	// RoutingKeyMetadata is the reserved AMQP message-annotation key used by
	// adapters to preserve a Message routing key on the wire.
	RoutingKeyMetadata = "x-rabbitstream-routing-key"
)

// Limits bounds all caller-controlled material retained by core operations.
// Values are bytes unless their name states otherwise.
type Limits struct {
	// MaxStreamNameBytes bounds stream, Super Stream, partition, and consumer names.
	MaxStreamNameBytes int
	// MaxRoutingKeyBytes bounds routing and partition keys.
	MaxRoutingKeyBytes int
	// MaxPayloadBytes bounds one message payload before transport allocation.
	MaxPayloadBytes int
	// MaxMetadataEntries bounds all header, property, and broker metadata entries.
	MaxMetadataEntries int
	// MaxMetadataKeyBytes bounds one metadata key.
	MaxMetadataKeyBytes int
	// MaxMetadataValueBytes bounds one metadata value or standard property.
	MaxMetadataValueBytes int
	// MaxMetadataBytes bounds aggregate message metadata.
	MaxMetadataBytes int
	// MaxBatchMessages bounds one synchronous batch.
	MaxBatchMessages int
	// MaxBatchBytes bounds aggregate payload and metadata bytes in a batch.
	MaxBatchBytes int
	// MaxBufferedMessages bounds asynchronous admission.
	MaxBufferedMessages int
}

// DefaultLimits returns conservative finite defaults. Callers may lower them;
// higher values must remain compatible with broker frame and resource policy.
func DefaultLimits() Limits {
	return Limits{
		MaxStreamNameBytes:    defaultMaxStreamNameBytes,
		MaxRoutingKeyBytes:    defaultMaxRoutingKeyBytes,
		MaxPayloadBytes:       defaultMaxPayloadBytes,
		MaxMetadataEntries:    defaultMaxMetadataEntries,
		MaxMetadataKeyBytes:   defaultMaxMetadataKeyBytes,
		MaxMetadataValueBytes: defaultMaxMetadataValueBytes,
		MaxMetadataBytes:      defaultMaxMetadataBytes,
		MaxBatchMessages:      defaultMaxBatchMessages,
		MaxBatchBytes:         defaultMaxBatchBytes,
		MaxBufferedMessages:   defaultMaxBufferedMessages,
	}
}

func (limits Limits) validate() error {
	if limits.MaxStreamNameBytes <= 0 || limits.MaxRoutingKeyBytes <= 0 ||
		limits.MaxPayloadBytes <= 0 || limits.MaxMetadataEntries <= 0 ||
		limits.MaxMetadataKeyBytes <= 0 || limits.MaxMetadataValueBytes <= 0 ||
		limits.MaxMetadataBytes <= 0 || limits.MaxBatchMessages <= 0 ||
		limits.MaxBatchBytes <= 0 || limits.MaxBufferedMessages <= 0 {
		return validationError(errors.New("message limits must be positive"))
	}
	return nil
}

// MetadataEntry preserves application-property and header order, including
// duplicate keys. Key and Value are borrowed unless the enclosing Message is
// retained by calling Retain.
type MetadataEntry struct {
	// Key is a bounded application or broker metadata name.
	Key string
	// Value is borrowed unless the enclosing Message is retained.
	Value []byte
}

// Message is the language-neutral byte and ordered-metadata model used for
// publishing. Exactly one of Stream and SuperStream must be set. Payload,
// Headers, and Properties are borrowed for the duration of a synchronous call;
// asynchronous retention must use Retain.
type Message struct {
	// Stream is the direct stream target or consumed backing stream.
	Stream string
	// SuperStream is the logical partitioned target when direct Stream is empty.
	SuperStream string
	// Partition identifies the selected backing stream after routing or delivery.
	Partition string
	// RoutingKey selects a Super Stream partition and scopes keyed ordering.
	RoutingKey string
	// PublishingID is the broker deduplication sequence when HasPublishingID is true.
	PublishingID uint64
	// HasPublishingID distinguishes an explicit zero ID from no publishing ID.
	HasPublishingID bool
	// Offset is the delivered stream offset when HasOffset is true.
	Offset uint64
	// HasOffset distinguishes offset zero from an unset producer message offset.
	HasOffset bool
	// Timestamp is application message time, not broker receipt time.
	Timestamp time.Time
	// ContentType is the language-neutral payload media type.
	ContentType string
	// MessageID is the stable application message identity.
	MessageID string
	// CorrelationID links related application messages without affecting routing.
	CorrelationID string
	// Payload is borrowed during synchronous calls and copied by Retain.
	Payload []byte
	// Headers preserves ordered message annotations other than RoutingKey.
	Headers []MetadataEntry
	// Properties preserves ordered application properties.
	Properties []MetadataEntry
	// BrokerMetadata contains bounded delivery diagnostics supplied by an adapter.
	BrokerMetadata []MetadataEntry
}

// Retain returns a deep owned copy of byte slices and ordered metadata.
func (message Message) Retain() Message {
	message.Payload = append([]byte(nil), message.Payload...)
	message.Headers = retainMetadata(message.Headers)
	message.Properties = retainMetadata(message.Properties)
	message.BrokerMetadata = retainMetadata(message.BrokerMetadata)
	return message
}

func retainMetadata(entries []MetadataEntry) []MetadataEntry {
	if entries == nil {
		return nil
	}
	retained := make([]MetadataEntry, len(entries))
	for index, entry := range entries {
		retained[index] = MetadataEntry{
			Key:   entry.Key,
			Value: append([]byte(nil), entry.Value...),
		}
	}
	return retained
}

// Validate checks target identity and every retained byte dimension.
func (message Message) Validate(limits Limits) error {
	if err := limits.validate(); err != nil {
		return err
	}
	if (message.Stream == "") == (message.SuperStream == "") {
		return validationError(errors.New("exactly one stream target is required"))
	}
	if message.Stream != "" && invalidIdentifier(message.Stream, limits.MaxStreamNameBytes) {
		return validationError(errors.New("stream name is invalid"))
	}
	if message.SuperStream != "" && invalidIdentifier(message.SuperStream, limits.MaxStreamNameBytes) {
		return validationError(errors.New("super stream name is invalid"))
	}
	if message.RoutingKey != "" && invalidIdentifier(message.RoutingKey, limits.MaxRoutingKeyBytes) {
		return validationError(errors.New("routing key is invalid"))
	}
	if len(message.Payload) > limits.MaxPayloadBytes {
		return &OperationError{
			Operation: OperationPublish,
			Category:  CategoryValidation,
			Cause:     ErrMessageTooLarge,
		}
	}
	if len(message.Headers)+len(message.Properties)+len(message.BrokerMetadata) > limits.MaxMetadataEntries {
		return validationError(errors.New("metadata entry count exceeds limit"))
	}
	metadataBytes := len(message.ContentType) + len(message.MessageID) + len(message.CorrelationID)
	if len(message.ContentType) > limits.MaxMetadataValueBytes ||
		len(message.MessageID) > limits.MaxMetadataValueBytes ||
		len(message.CorrelationID) > limits.MaxMetadataValueBytes {
		return validationError(errors.New("standard message property exceeds limit"))
	}
	if metadataBytes > limits.MaxMetadataBytes {
		return validationError(errors.New("aggregate metadata exceeds limit"))
	}
	for entryGroup, entries := range [][]MetadataEntry{
		message.Headers, message.Properties, message.BrokerMetadata,
	} {
		for _, entry := range entries {
			if entryGroup == 0 && entry.Key == RoutingKeyMetadata {
				return validationError(errors.New("routing key metadata is reserved"))
			}
			if invalidIdentifier(entry.Key, limits.MaxMetadataKeyBytes) ||
				len(entry.Value) > limits.MaxMetadataValueBytes {
				return validationError(errors.New("metadata entry exceeds limit"))
			}
			metadataBytes += len(entry.Key) + len(entry.Value)
			if metadataBytes > limits.MaxMetadataBytes {
				return validationError(errors.New("aggregate metadata exceeds limit"))
			}
		}
	}
	return nil
}

// ValidateDelivery checks the owned broker-delivery shape used by consumers,
// replay, and telemetry. A Super Stream delivery carries both its logical
// SuperStream and its backing Stream/Partition identity.
func (message Message) ValidateDelivery(limits Limits) error {
	if message.Partition == "" || message.Partition != message.Stream || !message.HasOffset ||
		(message.SuperStream != "" && invalidIdentifier(message.SuperStream, limits.MaxStreamNameBytes)) {
		return &OperationError{Operation: OperationConsume, Category: CategoryValidation}
	}
	publishShape := message
	publishShape.SuperStream = ""
	publishShape.Partition = ""
	publishShape.Offset = 0
	publishShape.HasOffset = false
	if err := publishShape.Validate(limits); err != nil {
		return &OperationError{Operation: OperationConsume, Category: CategoryValidation, Cause: err}
	}
	return nil
}

// ValidateBatch validates every message and bounds the count and aggregate
// payload-plus-metadata bytes. It does not mutate or retain its input.
func ValidateBatch(messages []Message, limits Limits) error {
	if err := limits.validate(); err != nil {
		return err
	}
	if len(messages) == 0 || len(messages) > limits.MaxBatchMessages {
		return validationError(errors.New("batch count exceeds limit"))
	}
	total := 0
	for _, message := range messages {
		if err := message.Validate(limits); err != nil {
			return err
		}
		total += len(message.Payload)
		total += len(message.ContentType) + len(message.MessageID) + len(message.CorrelationID)
		for _, entry := range message.Headers {
			total += len(entry.Key) + len(entry.Value)
		}
		for _, entry := range message.Properties {
			total += len(entry.Key) + len(entry.Value)
		}
		for _, entry := range message.BrokerMetadata {
			total += len(entry.Key) + len(entry.Value)
		}
		if total > limits.MaxBatchBytes {
			return validationError(errors.New("aggregate batch bytes exceed limit"))
		}
	}
	return nil
}
