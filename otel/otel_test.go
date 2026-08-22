package rabbitstreamotel_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/faustbrian/golib/pkg/rabbitstream"
	rabbitstreamotel "github.com/faustbrian/golib/pkg/rabbitstream/otel"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/trace"
)

func TestW3CPropagationRoundTripDoesNotMutateCallerMessage(t *testing.T) {
	t.Parallel()

	adapter, err := rabbitstreamotel.New(rabbitstreamotel.Config{
		MeterProvider: sdkmetric.NewMeterProvider(),
		Limits:        rabbitstream.DefaultLimits(),
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	spanContext := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    trace.TraceID{1, 2, 3},
		SpanID:     trace.SpanID{4, 5, 6},
		TraceFlags: trace.FlagsSampled,
		TraceState: mustTraceState(t, "vendor=value"),
		Remote:     false,
	})
	ctx := trace.ContextWithSpanContext(context.Background(), spanContext)
	original := rabbitstream.Message{
		Stream: "tracking.events", Payload: []byte("payload"),
		Headers: []rabbitstream.MetadataEntry{{Key: "existing", Value: []byte("value")}},
	}
	injected, err := adapter.Inject(ctx, original)
	if err != nil {
		t.Fatalf("Inject() error = %v", err)
	}
	if len(original.Headers) != 1 || original.Headers[0].Key != "existing" {
		t.Fatalf("Inject() mutated caller headers: %#v", original.Headers)
	}
	if len(injected.Headers) != 3 || string(injected.Payload) != "payload" {
		t.Fatalf("injected message = %#v", injected)
	}
	original.Payload[0] = 'X'
	if string(injected.Payload) != "payload" {
		t.Fatal("injected message retained caller payload ownership")
	}

	extracted, err := adapter.Extract(context.Background(), injected)
	if err != nil {
		t.Fatalf("Extract() error = %v", err)
	}
	got := trace.SpanContextFromContext(extracted)
	if !got.IsRemote() || got.TraceID() != spanContext.TraceID() ||
		got.SpanID() != spanContext.SpanID() || got.TraceFlags() != spanContext.TraceFlags() ||
		got.TraceState().String() != spanContext.TraceState().String() {
		t.Fatalf("extracted span context = %#v", got)
	}
}

func TestPropagationRespectsMessageBounds(t *testing.T) {
	t.Parallel()

	limits := rabbitstream.DefaultLimits()
	limits.MaxMetadataEntries = 1
	adapter, err := rabbitstreamotel.New(rabbitstreamotel.Config{
		MeterProvider: sdkmetric.NewMeterProvider(), Limits: limits,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	spanContext := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID: trace.TraceID{1}, SpanID: trace.SpanID{2}, TraceFlags: trace.FlagsSampled,
	})
	original := rabbitstream.Message{
		Stream: "tracking.events", Payload: []byte("payload"),
		Headers: []rabbitstream.MetadataEntry{{Key: "existing", Value: []byte("value")}},
	}
	_, err = adapter.Inject(trace.ContextWithSpanContext(context.Background(), spanContext), original)
	if !errors.Is(err, rabbitstream.ErrValidation) || len(original.Headers) != 1 {
		t.Fatalf("Inject() error = %v; original headers = %#v", err, original.Headers)
	}
}

func TestObserverEmitsClosedPayloadFreeMetrics(t *testing.T) {
	t.Parallel()

	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	adapter, err := rabbitstreamotel.New(rabbitstreamotel.Config{
		MeterProvider: provider, Limits: rabbitstream.DefaultLimits(),
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	adapter.Observe(rabbitstream.Observation{
		Kind: rabbitstream.ObservationReconnectAttempt, Count: 1,
	})
	adapter.Observe(rabbitstream.Observation{
		Kind: rabbitstream.ObservationPublishAttempt, Count: 2, Bytes: 128,
	})
	adapter.Observe(rabbitstream.Observation{
		Kind: rabbitstream.ObservationPublishConfirmed, Count: 2, Bytes: 128,
	})
	adapter.Observe(rabbitstream.Observation{
		Kind: rabbitstream.ObservationPublishError, Count: 1,
		Category: rabbitstream.ErrorCategory("secret-customer-value"),
	})

	var data metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &data); err != nil {
		t.Fatalf("Collect() error = %v", err)
	}
	observed := metricStrings(data)
	for _, name := range []string{
		"rabbitstream.reconnects", "rabbitstream.publish.messages",
		"rabbitstream.publish.bytes", "rabbitstream.errors",
	} {
		if !strings.Contains(observed, name) {
			t.Fatalf("metrics missing %q in %q", name, observed)
		}
	}
	if strings.Contains(observed, "secret-customer-value") {
		t.Fatalf("metrics retained unrecognized category: %q", observed)
	}
}

func mustTraceState(t *testing.T, value string) trace.TraceState {
	t.Helper()
	state, err := trace.ParseTraceState(value)
	if err != nil {
		t.Fatalf("ParseTraceState() error = %v", err)
	}
	return state
}

func metricStrings(data metricdata.ResourceMetrics) string {
	var values []string
	for _, scope := range data.ScopeMetrics {
		for _, metric := range scope.Metrics {
			values = append(values, metric.Name, metric.Description, metric.Unit)
			values = append(values, fmt.Sprintf("%v", metric.Data))
		}
	}
	return strings.Join(values, "\n")
}
