package stream

import "fmt"

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

// FirstOffset reads the broker first-chunk statistic. The -1 sentinel is absent;
// missing map keys otherwise read zero. The value can change with retention.
func (s *StreamStats) FirstOffset() (int64, error) {
	if s.stats["first_chunk_id"] == -1 {
		return -1, fmt.Errorf("FirstOffset not found for %s", s.streamName)
	}
	return s.stats["first_chunk_id"], nil
}

// LastOffset reports the last chunk identifier, not the final record offset.
// Only the native -1 sentinel is treated as absent; a missing map key reads zero.
//
// Deprecated: The method name may be misleading.
// It does not indicate the last offset of the stream. It indicates the last uncommitted chunk id. This information is not necessary. The user should use CommittedChunkId().
func (s *StreamStats) LastOffset() (int64, error) {
	if s.stats["last_chunk_id"] == -1 {
		return -1, fmt.Errorf("LastOffset not found for %s", s.streamName)
	}
	return s.stats["last_chunk_id"], nil
}

// CommittedChunkId reads the committed chunk identifier, not its final record.
// Only -1 is treated as absent; missing map keys otherwise read zero.
//
//	It is the offset of the first message in the last chunk confirmed by a quorum of the stream
//	cluster members (leader and replicas).
//
//	The committed chunk ID is a good indication of what the last offset of a stream can be at a
//	given time. The value can be stale as soon as the application reads it though, as the committed
//	chunk ID for a stream that is published to changes all the time.
//
//	return committed offset in this stream
//	Error if there is no committed chunk yet
func (s *StreamStats) CommittedChunkId() (int64, error) {
	if s.stats["committed_chunk_id"] == -1 {
		return -1, fmt.Errorf("CommittedChunkId not found for %s", s.streamName)
	}
	return s.stats["committed_chunk_id"], nil
}
