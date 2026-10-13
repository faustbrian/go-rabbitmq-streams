package stream

// Event is a native terminal notification. Resource names, Reason and Err may
// contain peer/application data; it is not a root redacted Observation.
type Event struct {
	// Command identifies the native operation associated with close.
	Command uint16
	// StreamName is caller/broker-derived diagnostic data, not a metric label.
	StreamName string
	// Name is the native producer/consumer reference and can be sensitive.
	Name string
	// Reason is a native close reason, not a root closed category.
	Reason string
	// Err preserves a native cause; public exposure requires separate redaction.
	Err error
}

// ChannelClose is a native-owned terminal event receive channel; receivers must
// not close it.
type ChannelClose = <-chan Event

// ChannelPublishConfirm is a native-owned confirmation channel. Consumers must
// release backpressure before external-owner joins and must not close it.
type ChannelPublishConfirm chan []*ConfirmationStatus
