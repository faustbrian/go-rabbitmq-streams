package rabbitstream

import "time"

// ObservationKind is a stable low-cardinality lifecycle signal.
type ObservationKind string

const (
	// ObservationConnectionConnecting reports a bounded connection attempt.
	ObservationConnectionConnecting ObservationKind = "connection_connecting"
	// ObservationConnectionReady reports an established usable session.
	ObservationConnectionReady ObservationKind = "connection_ready"
	// ObservationConnectionLost reports loss of an established session.
	ObservationConnectionLost ObservationKind = "connection_lost"
	// ObservationReconnectAttempt reports an attempt to restore a lost session.
	ObservationReconnectAttempt ObservationKind = "reconnect_attempt"
	// ObservationAuthenticationError reports credential rejection or resolution failure.
	ObservationAuthenticationError ObservationKind = "authentication_error"
	// ObservationPublishAttempt reports one caller publish attempt.
	ObservationPublishAttempt ObservationKind = "publish_attempt"
	// ObservationPublishConfirmed reports one definitive broker confirmation.
	ObservationPublishConfirmed ObservationKind = "publish_confirmed"
	// ObservationPublishRejected reports one definitive broker rejection.
	ObservationPublishRejected ObservationKind = "publish_rejected"
	// ObservationPublishAmbiguous reports a sent message without observed certainty.
	ObservationPublishAmbiguous ObservationKind = "publish_ambiguous"
	// ObservationPublishError reports a publish failure before confirmation.
	ObservationPublishError ObservationKind = "publish_error"
	// ObservationConsumerMessage reports one delivered message.
	ObservationConsumerMessage ObservationKind = "consumer_message"
	// ObservationHandlerSuccess reports a successful handler invocation.
	ObservationHandlerSuccess ObservationKind = "handler_success"
	// ObservationHandlerError reports a failed handler invocation.
	ObservationHandlerError ObservationKind = "handler_error"
	// ObservationHandlerRetry reports a bounded in-process retry.
	ObservationHandlerRetry ObservationKind = "handler_retry"
	// ObservationRetryStreamPublished reports confirmed retry-stream publication.
	ObservationRetryStreamPublished ObservationKind = "retry_stream_published"
	// ObservationDeadLetterPublished reports confirmed dead-letter publication.
	ObservationDeadLetterPublished ObservationKind = "dead_letter_published"
	// ObservationFailurePublishError reports failed retry or dead-letter publication.
	ObservationFailurePublishError ObservationKind = "failure_publish_error"
	// ObservationOffsetStoreAccepted reports client acceptance of an offset-store command.
	ObservationOffsetStoreAccepted ObservationKind = "offset_store_accepted"
	// ObservationStreamEndOffset reports an observed retained end offset.
	ObservationStreamEndOffset ObservationKind = "stream_end_offset"
	// ObservationConsumerLag reports bounded numeric backlog.
	ObservationConsumerLag ObservationKind = "consumer_lag"
	// ObservationReplayProgress reports one replay progress point.
	ObservationReplayProgress ObservationKind = "replay_progress"
	// ObservationProducerShutdown reports bounded producer close duration.
	ObservationProducerShutdown ObservationKind = "producer_shutdown"
	// ObservationConsumerShutdown reports bounded consumer close duration.
	ObservationConsumerShutdown ObservationKind = "consumer_shutdown"
)

// Observation contains only bounded scalar data. It deliberately excludes
// stream names, routing keys, message IDs, payloads, headers, and credentials.
type Observation struct {
	// Kind identifies the stable signal.
	Kind ObservationKind
	// Count is a bounded event or message count.
	Count uint64
	// Bytes is a bounded payload-byte total.
	Bytes uint64
	// Value is a kind-specific bounded scalar such as offset or lag.
	Value uint64
	// Duration is a kind-specific bounded elapsed time.
	Duration time.Duration
	// Category classifies a failure without high-cardinality details.
	Category ErrorCategory
}

// Observer receives best-effort lifecycle signals. Implementations must not
// block indefinitely; their panics are contained and never affect delivery.
type Observer interface {
	// Observe receives one bounded signal and must return promptly.
	Observe(Observation)
}

func observe(observer Observer, observation Observation) {
	if observer == nil {
		return
	}
	defer func() { _ = recover() }()
	observer.Observe(observation)
}
