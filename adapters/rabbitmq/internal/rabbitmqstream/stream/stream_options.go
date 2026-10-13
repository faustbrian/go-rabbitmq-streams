package stream

import (
	"fmt"
	"time"
)

// StreamOptions is a mutable private provisioning builder. Positive MaxAge is
// rounded to seconds; positive capacity values are sent as broker policy, not
// evidence that retention has completed.
type StreamOptions struct {
	// MaxAge requests broker retention age rounded to seconds, not deletion completion.
	MaxAge time.Duration
	// MaxLengthBytes borrows broker log capacity policy, not total memory.
	MaxLengthBytes *ByteCapacity
	// MaxSegmentSizeBytes borrows broker segment sizing policy.
	MaxSegmentSizeBytes *ByteCapacity
}

// SetMaxAge stores retention duration for broker provisioning, not proof of
// actual deletion or timestamp precision.
func (s *StreamOptions) SetMaxAge(maxAge time.Duration) *StreamOptions {
	s.MaxAge = maxAge
	return s
}

// SetMaxLengthBytes borrows a capacity builder for broker retention
// configuration, not a process-memory limit.
func (s *StreamOptions) SetMaxLengthBytes(maxLength *ByteCapacity) *StreamOptions {
	s.MaxLengthBytes = maxLength
	return s
}

// SetMaxSegmentSizeBytes borrows a capacity builder for broker segment
// configuration, not an exact deletion boundary.
func (s *StreamOptions) SetMaxSegmentSizeBytes(segmentSize *ByteCapacity) *StreamOptions {
	s.MaxSegmentSizeBytes = segmentSize
	return s
}

func (s StreamOptions) buildParameters() (map[string]string, error) {
	res := map[string]string{"queue-leader-locator": "least-leaders"}

	if s.MaxLengthBytes != nil {
		if s.MaxLengthBytes.error != nil {
			return nil, s.MaxLengthBytes.error
		}

		if s.MaxLengthBytes.bytes > 0 {
			res["max-length-bytes"] = fmt.Sprintf("%d", s.MaxLengthBytes.bytes)
		}
	}

	if s.MaxSegmentSizeBytes != nil {
		if s.MaxSegmentSizeBytes.error != nil {
			return nil, s.MaxSegmentSizeBytes.error
		}

		if s.MaxSegmentSizeBytes.bytes > 0 {
			res["stream-max-segment-size-bytes"] = fmt.Sprintf("%d", s.MaxSegmentSizeBytes.bytes)
		}
	}

	if s.MaxAge > 0 {
		res["max-age"] = fmt.Sprintf("%.0fs", s.MaxAge.Seconds())
	}
	return res, nil
}

// NewStreamOptions returns an empty mutable provisioning builder; omitted
// retention arguments leave broker defaults.
func NewStreamOptions() *StreamOptions {
	return &StreamOptions{}
}
