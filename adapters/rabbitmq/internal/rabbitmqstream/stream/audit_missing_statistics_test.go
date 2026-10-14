package stream

import "testing"

func TestMissingBrokerStatisticsNeverManufactureOffsetZero(t *testing.T) {
	for _, operation := range []struct {
		key  string
		read func(*StreamStats) (int64, error)
	}{
		{"first_chunk_id", (*StreamStats).FirstOffset},
		{"last_chunk_id", (*StreamStats).LastOffset},
		{"committed_chunk_id", (*StreamStats).CommittedChunkId},
	} {
		t.Run(operation.key, func(t *testing.T) {
			if _, err := operation.read(newStreamStats(map[string]int64{}, "tracking.events")); err == nil {
				t.Fatal("missing statistic was promoted to known progress")
			}
			if _, err := operation.read(newStreamStats(map[string]int64{operation.key: -1}, "tracking.events")); err == nil {
				t.Fatal("explicit absent sentinel was accepted")
			}
			for _, expected := range []int64{0, 5} {
				value, err := operation.read(newStreamStats(map[string]int64{operation.key: expected}, "tracking.events"))
				if value != expected || err != nil {
					t.Fatalf("actual statistic %d = %d, %v", expected, value, err)
				}
			}
		})
	}
}
