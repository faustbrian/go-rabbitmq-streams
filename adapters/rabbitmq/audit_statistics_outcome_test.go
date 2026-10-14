package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/faustbrian/go-rabbitmq-streams/adapters/rabbitmq/v2/internal/rabbitmqstream/stream"
	rabbitstream "github.com/faustbrian/go-rabbitmq-streams/v2"
)

func TestStatisticFailureNeverPublishesKnownEmptyRange(t *testing.T) {
	cause := errors.New("fixture statistic unavailable")
	stats := fakeStreamStatistics{firstErr: cause, committedErr: cause}
	inspection, err := inspectStreamStats(context.Background(), &fakeRabbitEnvironment{}, rabbitstream.StreamInspection{Stream: "tracking.events", Exists: true}, "", stats)
	if !errors.Is(err, cause) || !reflect.DeepEqual(inspection, rabbitstream.StreamInspection{}) {
		t.Fatal("failed statistic became successful inspection")
	}
	retained, err := retainedRangeFromStats(context.Background(), &fakeRabbitEnvironment{}, "tracking.events", stats)
	if !errors.Is(err, cause) || !reflect.DeepEqual(retained, rabbitstream.RetainedRange{}) {
		t.Fatal("failed statistic became known empty retained range")
	}
}

func TestPublicInspectorPreservesMissingStatisticCauseAndZeroResult(t *testing.T) {
	stats := &stream.StreamStats{}
	cause, missing := stats.FirstOffset()
	if missing == nil || cause != 0 {
		t.Fatal("absent broker statistic manufactured known offset")
	}
	env := &fakeRabbitEnvironment{exists: true, stats: stats, newConsumerErr: errors.New("false end reader admission")}
	inspector := &Inspector{limits: rabbitstream.DefaultLimits(), openEnvironment: func(context.Context) (rabbitEnvironment, error) { return env, nil }}
	got, err := inspector.Inspect(context.Background(), rabbitstream.InspectionRequest{Stream: "tracking.events"})
	if err == nil || !reflect.DeepEqual(got, rabbitstream.InspectionResult{}) || env.consumerCalls != 0 || env.closeCalls != 1 {
		t.Fatal("missing statistic produced partial facts or entered false end reader")
	}
}

func TestExplicitEmptyStatisticsPreserveKnownEmptyAndValidZero(t *testing.T) {
	empty := fakeStreamStatistics{firstErr: fmt.Errorf("owned absence: %w", stream.ErrEmptyStreamStatistic), committedErr: stream.ErrEmptyStreamStatistic}
	retained, err := retainedRangeFromStats(context.Background(), &fakeRabbitEnvironment{}, "tracking.events", empty)
	if err != nil || !retained.Empty {
		t.Fatal("explicit empty sentinel lost empty-range admission")
	}
	inspection, err := inspectStreamStats(context.Background(), &fakeRabbitEnvironment{}, rabbitstream.StreamInspection{Stream: "tracking.events", Exists: true}, "", empty)
	if err != nil || !inspection.Exists || inspection.FirstOffset != nil || inspection.LastOffset != nil || inspection.CommittedChunkID != nil {
		t.Fatal("explicit empty sentinel manufactured progress")
	}
	// Committed chunk zero is a real known value even when retained data is absent.
	present := fakeStreamStatistics{firstErr: stream.ErrEmptyStreamStatistic, committed: 0}
	inspection, err = inspectStreamStats(context.Background(), &fakeRabbitEnvironment{}, rabbitstream.StreamInspection{Stream: "tracking.events", Exists: true}, "", present)
	if err != nil || inspection.CommittedChunkID == nil || *inspection.CommittedChunkID != 0 {
		t.Fatal("legitimate committed zero became unknown")
	}
}
