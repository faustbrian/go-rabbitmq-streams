// Package message defines the mutable native wire-message seam consumed by the
// private RabbitMQ Go client v1.8.3 closure. amqp.AMQP10 is the Streams implementation;
// it is not the root-v2 Message or a thread-safe publication container.
// Each Send/BatchSend needs its own instance. Getter data and metadata are borrowed
// unless copied; setters and marshaling must not race publication. The adapter
// copies into and out of its narrower public model and owns protocol validation.
// Source/license provenance is retained in THIRD_PARTY_RABBITMQ_STREAM.md at the
// canonical adapter root; this hand-maintained seam has no generator or cgo.
package message

import "github.com/faustbrian/go-rabbitmq-streams/adapters/rabbitmq/v2/internal/rabbitmqstream/amqp"

// StreamMessage is the interface that wraps the basic methods to interact with a message
// in the context of a stream.
// The Streams implementation is amqp.AMQP10, not amqp.Message.
// The implementations are not meant to be thread-safe.
type StreamMessage interface {
	// MarshalBinary encodes the mutable wire value; implementations must return owned output
	// or retain it through the native publication/confirmation lifetime.
	MarshalBinary() ([]byte, error)
	// UnmarshalBinary replaces mutable decoded state; use bounded codec entrypoints for
	// hostile input and never race a send.
	UnmarshalBinary(data []byte) error
	// SetPublishingId marks an explicit wire publishing identity on this unique message.
	SetPublishingId(id int64)
	// GetPublishingId returns signed native wire bits; root admission separately limits
	// caller-selected IDs to MaxInt64.
	GetPublishingId() int64
	// HasPublishingId distinguishes explicit zero from the absence of a publishing ID.
	HasPublishingId() bool
	// GetData returns borrowed data-section slices, not a deep copy.
	GetData() [][]byte
	// GetMessageProperties returns borrowed standard-property state.
	GetMessageProperties() *amqp.MessageProperties
	// GetMessageAnnotations returns borrowed native annotation keys/values; the public
	// adapter supports only its documented bounded subset.
	GetMessageAnnotations() amqp.Annotations
	// GetApplicationProperties returns borrowed native application-property state.
	GetApplicationProperties() map[string]any

	// GetMessageHeader returns the borrowed native header; it is not a root policy field.
	GetMessageHeader() *amqp.MessageHeader
	// GetAMQPValue returns borrowed native AMQP-value body data; root Data mapping
	// does not imply representation of this value.
	GetAMQPValue() any
}
