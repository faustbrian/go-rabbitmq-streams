package rabbitstream

import (
	"context"
	"errors"
	"time"
)

// InspectionRequest selects exactly one read-only stream or Super Stream
// target. ConsumerName optionally requests its broker-stored offset.
type InspectionRequest struct {
	// Stream selects one direct stream when SuperStream is empty.
	Stream string
	// SuperStream selects a logical partitioned stream when Stream is empty.
	SuperStream string
	// ConsumerName optionally requests its broker-stored offset.
	ConsumerName string
}

// Validate checks target and diagnostic identity bounds.
func (request InspectionRequest) Validate(limits Limits) error {
	if err := limits.validate(); err != nil {
		return &OperationError{Operation: OperationInspect, Category: CategoryValidation, Cause: err}
	}
	if (request.Stream == "") == (request.SuperStream == "") ||
		(request.Stream != "" && invalidIdentifier(request.Stream, limits.MaxStreamNameBytes)) ||
		(request.SuperStream != "" && invalidIdentifier(request.SuperStream, limits.MaxStreamNameBytes)) ||
		(request.ConsumerName != "" && invalidIdentifier(request.ConsumerName, limits.MaxStreamNameBytes)) {
		return &OperationError{
			Operation: OperationInspect, Category: CategoryValidation,
			Cause: errors.New("inspection request is invalid"),
		}
	}
	return nil
}

// StreamInspection reports broker facts without interpreting committed chunk
// IDs as exact stream end offsets.
type StreamInspection struct {
	// Stream identifies the direct or backing stream inspected.
	Stream string
	// Exists reports whether the broker found the stream.
	Exists bool
	// FirstOffset is the earliest retained offset when the broker exposes it.
	FirstOffset *uint64
	// LastOffset is the exact retained end offset when available.
	LastOffset *uint64
	// CommittedChunkID is broker chunk metadata and is not an exact end offset.
	CommittedChunkID *uint64
	// StoredOffset is the named consumer's broker-stored progress when requested.
	StoredOffset *uint64
	// Lag is the exact non-negative distance to LastOffset when both are known.
	Lag *uint64
}

// InspectionResult is a bounded snapshot. Super Stream partition ordering is
// the broker routing topology ordering observed for this request.
type InspectionResult struct {
	// SuperStream is the logical target, empty for a direct stream request.
	SuperStream string
	// Partitions contains one bounded snapshot per direct or backing stream.
	Partitions []StreamInspection
	// ObservedAt records when the snapshot was assembled.
	ObservedAt time.Time
}

// DependencyState separates dependency health from process liveness.
type DependencyState uint8

const (
	// DependencyHealthy reports a successful bounded broker diagnostic.
	DependencyHealthy DependencyState = iota
	// DependencyUnavailable reports a failed dependency diagnostic without process failure.
	DependencyUnavailable
)

// DependencyHealth is a bounded readiness/diagnostic result. RabbitMQ
// unavailability alone does not imply that the process should restart.
type DependencyHealth struct {
	// State is the bounded dependency result.
	State DependencyState
	// ObservedAt records when the diagnostic completed.
	ObservedAt time.Time
	// Category classifies an unavailable dependency without resource names.
	Category ErrorCategory
}

// Inspector exposes read-only broker topology and offset diagnostics.
type Inspector interface {
	// Inspect returns a bounded read-only topology and offset snapshot.
	Inspect(context.Context, InspectionRequest) (InspectionResult, error)
	// Health reports dependency state without changing broker topology.
	Health(context.Context) DependencyHealth
}
