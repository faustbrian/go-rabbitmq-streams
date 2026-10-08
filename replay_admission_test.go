package rabbitstream

import (
	"context"
	"errors"
	"testing"
)

func TestReplayRejectsExcessTopologyBeforeRetention(t *testing.T) {
	source := &fakeReplaySource{retained: RetainedRange{Empty: true}}
	replayer, err := NewReplayer(DefaultLimits(), source, nil)
	if err != nil {
		t.Fatal(err)
	}
	excess := ReplayRequest{SuperStream: "super", Partition: "partition", ExpectedPartitions: make([]string, MaxSuperStreamPartitions+1), Start: StartPosition{Kind: OffsetStartBeginning}}
	for _, run := range []struct {
		name string
		fn   func(ReplayRequest) error
	}{
		{"inspect", func(request ReplayRequest) error {
			_, err := replayer.Inspect(context.Background(), request)
			return err
		}},
		{"run", func(request ReplayRequest) error {
			return replayer.Run(context.Background(), request, func(context.Context, ReplayDelivery) error { t.Fatal("invalid replay reached handler"); return nil })
		}},
	} {
		t.Run(run.name, func(t *testing.T) {
			baseline := testing.AllocsPerRun(10, func() {
				if !errors.Is(run.fn(ReplayRequest{}), ErrValidation) {
					t.Fatal("invalid target accepted")
				}
			})
			oversized := testing.AllocsPerRun(10, func() {
				if !errors.Is(run.fn(excess), ErrValidation) {
					t.Fatal("excess topology accepted")
				}
			})
			if oversized > baseline {
				t.Fatalf("excess topology retained before refusal: allocations=%g baseline=%g", oversized, baseline)
			}
		})
	}
}
