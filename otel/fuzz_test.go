package rabbitstreamotel_test

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/faustbrian/golib/pkg/rabbitstream"
	rabbitstreamotel "github.com/faustbrian/golib/pkg/rabbitstream/otel"
	metricnoop "go.opentelemetry.io/otel/metric/noop"
	"go.opentelemetry.io/otel/trace"
)

func FuzzPropagationOwnershipAndBounds(f *testing.F) {
	f.Add("application", []byte("value"), []byte("payload"))
	f.Add("traceparent", []byte("malformed"), []byte(nil))
	f.Add("TraceState", []byte("vendor=value"), []byte{0, 1, 2})

	adapter, err := rabbitstreamotel.New(rabbitstreamotel.Config{
		MeterProvider: metricnoop.NewMeterProvider(), Limits: rabbitstream.DefaultLimits(),
	})
	if err != nil {
		f.Fatalf("New() error = %v", err)
	}
	ctx := trace.ContextWithSpanContext(context.Background(), trace.NewSpanContext(trace.SpanContextConfig{
		TraceID: trace.TraceID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
		SpanID:  trace.SpanID{17, 18, 19, 20, 21, 22, 23, 24},
	}))

	f.Fuzz(func(t *testing.T, key string, value []byte, payload []byte) {
		message := rabbitstream.Message{
			Stream: "tracking.events", Payload: payload,
			Headers: []rabbitstream.MetadataEntry{{Key: key, Value: value}},
		}
		original := message.Retain()
		injected, injectErr := adapter.Inject(ctx, message)
		if !bytes.Equal(message.Payload, original.Payload) ||
			!equalMetadata(message.Headers, original.Headers) {
			t.Fatal("Inject() mutated caller-owned message")
		}
		if injectErr != nil {
			return
		}
		if _, extractErr := adapter.Extract(context.Background(), injected); extractErr != nil {
			t.Fatalf("Extract(injected) error = %v", extractErr)
		}
	})
}

func FuzzObservationIsolation(f *testing.F) {
	f.Add("publish_attempt", uint64(1), uint64(128), int64(1))
	f.Add("customer-secret", ^uint64(0), ^uint64(0), int64(-1))

	adapter, err := rabbitstreamotel.New(rabbitstreamotel.Config{
		MeterProvider: metricnoop.NewMeterProvider(), Limits: rabbitstream.DefaultLimits(),
	})
	if err != nil {
		f.Fatalf("New() error = %v", err)
	}
	f.Fuzz(func(t *testing.T, kind string, count uint64, size uint64, duration int64) {
		adapter.Observe(rabbitstream.Observation{
			Kind: rabbitstream.ObservationKind(kind), Count: count, Bytes: size,
			Duration: time.Duration(duration), Category: rabbitstream.ErrorCategory(kind),
		})
	})
}

func equalMetadata(left []rabbitstream.MetadataEntry, right []rabbitstream.MetadataEntry) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index].Key != right[index].Key || !bytes.Equal(left[index].Value, right[index].Value) {
			return false
		}
	}
	return true
}
