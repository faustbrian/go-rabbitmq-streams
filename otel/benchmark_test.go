package rabbitstreamotel_test

import (
	"context"
	"testing"

	"github.com/faustbrian/golib/pkg/rabbitstream"
	rabbitstreamotel "github.com/faustbrian/golib/pkg/rabbitstream/otel"
	metricnoop "go.opentelemetry.io/otel/metric/noop"
	"go.opentelemetry.io/otel/trace"
)

func BenchmarkObserve(b *testing.B) {
	adapter := benchmarkAdapter(b)
	observation := rabbitstream.Observation{
		Kind: rabbitstream.ObservationConsumerMessage, Count: 1, Bytes: 1024,
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		adapter.Observe(observation)
	}
}

func BenchmarkInjectTraceContext(b *testing.B) {
	adapter := benchmarkAdapter(b)
	ctx := trace.ContextWithSpanContext(context.Background(), trace.NewSpanContext(trace.SpanContextConfig{
		TraceID: trace.TraceID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
		SpanID:  trace.SpanID{17, 18, 19, 20, 21, 22, 23, 24},
	}))
	message := rabbitstream.Message{
		Stream: "tracking.events", Payload: make([]byte, 1024),
		Headers: []rabbitstream.MetadataEntry{{Key: "content-type", Value: []byte("application/octet-stream")}},
	}
	b.ReportAllocs()
	b.SetBytes(int64(len(message.Payload)))
	b.ResetTimer()
	for range b.N {
		if _, err := adapter.Inject(ctx, message); err != nil {
			b.Fatal(err)
		}
	}
}

func benchmarkAdapter(b *testing.B) *rabbitstreamotel.Adapter {
	b.Helper()
	adapter, err := rabbitstreamotel.New(rabbitstreamotel.Config{
		MeterProvider: metricnoop.NewMeterProvider(), Limits: rabbitstream.DefaultLimits(),
	})
	if err != nil {
		b.Fatal(err)
	}
	return adapter
}
