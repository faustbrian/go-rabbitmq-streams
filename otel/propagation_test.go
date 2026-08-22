package rabbitstreamotel_test

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/faustbrian/golib/pkg/rabbitstream"
	rabbitstreamotel "github.com/faustbrian/golib/pkg/rabbitstream/otel"
	"go.opentelemetry.io/otel/baggage"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/trace"
)

func TestInjectReplacesStaleTraceFieldsAndExcludesBaggage(t *testing.T) {
	t.Parallel()

	adapter := newAdapter(t, rabbitstream.DefaultLimits())
	member, err := baggage.NewMember("tenant", "secret-tenant")
	if err != nil {
		t.Fatalf("NewMember() error = %v", err)
	}
	bag, err := baggage.New(member)
	if err != nil {
		t.Fatalf("New() baggage error = %v", err)
	}
	message := rabbitstream.Message{
		Stream: "tracking.events",
		Headers: []rabbitstream.MetadataEntry{
			{Key: "TraceParent", Value: []byte("stale")},
			{Key: "tracestate", Value: []byte("stale=value")},
			{Key: "application", Value: []byte("preserved")},
		},
	}
	original := message.Retain()

	injected, err := adapter.Inject(baggage.ContextWithBaggage(context.Background(), bag), message)
	if err != nil {
		t.Fatalf("Inject() error = %v", err)
	}
	if !reflect.DeepEqual(message, original) {
		t.Fatalf("Inject() mutated message = %#v", message)
	}
	if len(injected.Headers) != 1 || injected.Headers[0].Key != "application" ||
		string(injected.Headers[0].Value) != "preserved" {
		t.Fatalf("Inject() headers = %#v", injected.Headers)
	}
}

func TestExtractRejectsAmbiguousTraceFieldsWithoutReplacingContext(t *testing.T) {
	t.Parallel()

	adapter := newAdapter(t, rabbitstream.DefaultLimits())
	existing := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID: trace.TraceID{16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1},
		SpanID:  trace.SpanID{24, 23, 22, 21, 20, 19, 18, 17},
	})
	ctx := trace.ContextWithSpanContext(context.Background(), existing)
	traceParent := []byte("00-0102030405060708090a0b0c0d0e0f10-1112131415161718-01")

	for _, headers := range [][]rabbitstream.MetadataEntry{
		{
			{Key: "traceparent", Value: traceParent},
			{Key: "TraceParent", Value: append([]byte(nil), traceParent...)},
		},
		{
			{Key: "traceparent", Value: traceParent},
			{Key: "tracestate", Value: []byte("vendor=one")},
			{Key: "TraceState", Value: []byte("vendor=two")},
		},
	} {
		extracted, err := adapter.Extract(ctx, rabbitstream.Message{
			Stream: "tracking.events", Headers: headers,
		})
		if err != nil {
			t.Fatalf("Extract() error = %v", err)
		}
		if got := trace.SpanContextFromContext(extracted); !got.Equal(existing) {
			t.Fatalf("Extract() span context = %#v", got)
		}
	}
}

func TestExtractAcceptsAValidatedSuperStreamDelivery(t *testing.T) {
	t.Parallel()

	adapter := newAdapter(t, rabbitstream.DefaultLimits())
	traceParent := []byte("00-0102030405060708090a0b0c0d0e0f10-1112131415161718-01")
	extracted, err := adapter.Extract(context.Background(), rabbitstream.Message{
		Stream: "tracking-0", SuperStream: "tracking", Partition: "tracking-0",
		Offset: 42, HasOffset: true,
		Headers: []rabbitstream.MetadataEntry{{Key: "traceparent", Value: traceParent}},
	})
	if err != nil {
		t.Fatalf("Extract(Super Stream delivery) error = %v", err)
	}
	spanContext := trace.SpanContextFromContext(extracted)
	if !spanContext.IsValid() || !spanContext.IsRemote() {
		t.Fatalf("Extract(Super Stream delivery) span context = %#v", spanContext)
	}
}

func TestPropagationValidatesInputAndUsesOperationSpecificErrors(t *testing.T) {
	t.Parallel()

	limits := rabbitstream.DefaultLimits()
	limits.MaxMetadataEntries = 1
	adapter := newAdapter(t, limits)
	message := rabbitstream.Message{
		Stream: "tracking.events",
		Headers: []rabbitstream.MetadataEntry{
			{Key: "traceparent", Value: []byte("stale")},
			{Key: "tracestate", Value: []byte("stale=value")},
		},
	}

	if _, err := adapter.Inject(context.Background(), message); !errors.Is(err, rabbitstream.ErrValidation) {
		t.Fatalf("Inject() error = %v", err)
	} else {
		assertOperation(t, err, rabbitstream.OperationPublish)
	}
	if _, err := adapter.Extract(context.Background(), message); !errors.Is(err, rabbitstream.ErrValidation) {
		t.Fatalf("Extract() error = %v", err)
	} else {
		assertOperation(t, err, rabbitstream.OperationConsume)
	}
	for name, malformed := range map[string]rabbitstream.Message{
		"partition only":   {Stream: "tracking.events", Partition: "tracking.events"},
		"offset flag only": {Stream: "tracking.events", HasOffset: true},
		"offset only":      {Stream: "tracking.events", Offset: 1},
	} {
		if _, err := adapter.Extract(context.Background(), malformed); !errors.Is(err, rabbitstream.ErrValidation) {
			t.Fatalf("Extract(%s) error = %v", name, err)
		}
	}
}

func newAdapter(t *testing.T, limits rabbitstream.Limits) *rabbitstreamotel.Adapter {
	t.Helper()
	adapter, err := rabbitstreamotel.New(rabbitstreamotel.Config{
		MeterProvider: sdkmetric.NewMeterProvider(), Limits: limits,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	return adapter
}

func assertOperation(t *testing.T, err error, operation rabbitstream.Operation) {
	t.Helper()
	var operationError *rabbitstream.OperationError
	if !errors.As(err, &operationError) {
		t.Fatalf("error = %T/%v; want OperationError", err, err)
	}
	if operationError.Operation != operation {
		t.Fatalf("error operation = %q, want %q", operationError.Operation, operation)
	}
}
