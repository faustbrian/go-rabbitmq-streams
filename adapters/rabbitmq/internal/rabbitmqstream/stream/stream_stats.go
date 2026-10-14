package stream

import (
	"errors"
	"fmt"
)

// StreamStats contains one broker statistics response. Chunk identifiers are
// not exact stream end offsets; missing/empty values must not be promoted to
// durable progress.
type StreamStats struct {
	stats      map[string]int64
	streamName string
}

func newStreamStats(stats map[string]int64, streamName string) *StreamStats {
	return &StreamStats{stats: stats, streamName: streamName}
}

// ErrEmptyStreamStatistic identifies the broker's explicit absent-value sentinel.
// Only this classification can establish an empty first-offset response.
var ErrEmptyStreamStatistic = errors.New("broker stream statistic has no value")

// ErrMissingStreamStatistic identifies an incomplete statistics response.
// It is not evidence that the stream is empty or positioned at offset zero.
var ErrMissingStreamStatistic = errors.New("broker stream statistic missing")

func (s *StreamStats) statistic(key string) (int64, error) {
	if s == nil {
		return 0, fmt.Errorf("%w: %s", ErrMissingStreamStatistic, key)
	}
	value, present := s.stats[key]
	if !present {
		return 0, fmt.Errorf("%w: %s", ErrMissingStreamStatistic, key)
	}
	if value == -1 {
		return -1, fmt.Errorf("%w: %s", ErrEmptyStreamStatistic, key)
	}
	return value, nil
}

// FirstOffset reads the earliest retained chunk statistic. The broker's -1
// sentinel is empty; a missing key is an error, not known offset zero.
func (s *StreamStats) FirstOffset() (int64, error) { return s.statistic("first_chunk_id") }

// LastOffset reads a chunk identifier, not the final record offset. Explicit -1
// is absent; a missing key is a statistics error.
//
// Deprecated: use CommittedChunkId for committed chunk metadata; use the public
// inspector's LastOffset for an exact observed logical record end.
func (s *StreamStats) LastOffset() (int64, error) { return s.statistic("last_chunk_id") }

// CommittedChunkId returns the first offset of the last broker-confirmed chunk,
// not its last logical record. Explicit -1 is absent; a missing key is an error.
func (s *StreamStats) CommittedChunkId() (int64, error) { return s.statistic("committed_chunk_id") }
