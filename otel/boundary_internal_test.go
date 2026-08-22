package rabbitstreamotel

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/faustbrian/golib/pkg/rabbitstream"
	"go.opentelemetry.io/otel/metric"
	metricnoop "go.opentelemetry.io/otel/metric/noop"
)

func TestNewRejectsIncompleteConfigurationAndContainsProviderPanic(t *testing.T) {
	t.Parallel()

	var typedNil *typedNilMeterProvider
	for _, config := range []Config{
		{},
		{MeterProvider: typedNil, Limits: rabbitstream.DefaultLimits()},
		{MeterProvider: metricnoop.NewMeterProvider()},
		{MeterProvider: panicsDuringMeterCreation{}, Limits: rabbitstream.DefaultLimits()},
	} {
		adapter, err := New(config)
		if adapter != nil || !errors.Is(err, rabbitstream.ErrInvalidConfiguration) {
			t.Fatalf("New(%#v) = %#v, %v", config, adapter, err)
		}
	}
}

func TestPropagationRejectsNilReceiversAndContexts(t *testing.T) {
	t.Parallel()

	message := rabbitstream.Message{Stream: "tracking.events"}
	var nilAdapter *Adapter
	var nilContext context.Context
	for _, test := range []struct {
		name      string
		operation rabbitstream.Operation
		call      func() error
	}{
		{name: "nil adapter inject", operation: rabbitstream.OperationPublish, call: func() error {
			_, err := nilAdapter.Inject(context.Background(), message)
			return err
		}},
		{name: "nil context inject", operation: rabbitstream.OperationPublish, call: func() error {
			adapter, err := New(Config{MeterProvider: metricnoop.NewMeterProvider(), Limits: rabbitstream.DefaultLimits()})
			if err != nil {
				return err
			}
			_, err = adapter.Inject(nilContext, message)
			return err
		}},
		{name: "nil adapter extract", operation: rabbitstream.OperationConsume, call: func() error {
			_, err := nilAdapter.Extract(context.Background(), message)
			return err
		}},
		{name: "nil context extract", operation: rabbitstream.OperationConsume, call: func() error {
			adapter, err := New(Config{MeterProvider: metricnoop.NewMeterProvider(), Limits: rabbitstream.DefaultLimits()})
			if err != nil {
				return err
			}
			_, err = adapter.Extract(nilContext, message)
			return err
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := test.call()
			if !errors.Is(err, rabbitstream.ErrValidation) {
				t.Fatalf("error = %v", err)
			}
			var operationError *rabbitstream.OperationError
			if !errors.As(err, &operationError) || operationError.Operation != test.operation {
				t.Fatalf("error = %#v, want operation %q", err, test.operation)
			}
		})
	}
}

func TestMetadataCarrierFulfillsTextMapContract(t *testing.T) {
	t.Parallel()

	carrier := metadataCarrier{entries: []rabbitstream.MetadataEntry{
		{Key: "TraceParent", Value: []byte("old")},
		{Key: "application", Value: []byte("value")},
		{Key: "TRACEPARENT", Value: []byte("ambiguous")},
	}}
	if got := carrier.Get("traceparent"); got != "" {
		t.Fatalf("Get(duplicate) = %q", got)
	}
	carrier.Set("traceparent", "new")
	if got := carrier.Get("TRACEPARENT"); got != "new" {
		t.Fatalf("Get(traceparent) = %q", got)
	}
	if got := carrier.Get("missing"); got != "" {
		t.Fatalf("Get(missing) = %q", got)
	}
	if got := carrier.Keys(); !reflect.DeepEqual(got, []string{"application", "traceparent"}) {
		t.Fatalf("Keys() = %#v", got)
	}
}

func TestASCIIHeaderMatchingAndNilDetectionBoundaries(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		left  string
		right string
		want  bool
	}{
		{left: "A", right: "a", want: true},
		{left: "Z", right: "z", want: true},
		{left: "a", right: "A", want: true},
		{left: "z", right: "Z", want: true},
		{left: "@", right: "`", want: false},
		{left: "[", right: "{", want: false},
		{left: "traceparent", right: "traceparen", want: false},
		{left: "ſ", right: "S", want: false},
	} {
		if got := equalASCIIFold(test.left, test.right); got != test.want {
			t.Fatalf("equalASCIIFold(%q, %q) = %t", test.left, test.right, got)
		}
	}
	var pointer *int
	var nilMap map[string]string
	if !isNil(nil) || !isNil(pointer) || !isNil(nilMap) || isNil(42) || isNil(new(int)) {
		t.Fatal("isNil() boundary mismatch")
	}
}

func TestEveryMessageLimitMustBePositive(t *testing.T) {
	t.Parallel()

	tests := map[string]func(*rabbitstream.Limits){
		"stream name":        func(limits *rabbitstream.Limits) { limits.MaxStreamNameBytes = 0 },
		"routing key":        func(limits *rabbitstream.Limits) { limits.MaxRoutingKeyBytes = 0 },
		"payload":            func(limits *rabbitstream.Limits) { limits.MaxPayloadBytes = 0 },
		"metadata entries":   func(limits *rabbitstream.Limits) { limits.MaxMetadataEntries = 0 },
		"metadata key":       func(limits *rabbitstream.Limits) { limits.MaxMetadataKeyBytes = 0 },
		"metadata value":     func(limits *rabbitstream.Limits) { limits.MaxMetadataValueBytes = 0 },
		"metadata aggregate": func(limits *rabbitstream.Limits) { limits.MaxMetadataBytes = 0 },
		"batch messages":     func(limits *rabbitstream.Limits) { limits.MaxBatchMessages = 0 },
		"batch bytes":        func(limits *rabbitstream.Limits) { limits.MaxBatchBytes = 0 },
		"buffered messages":  func(limits *rabbitstream.Limits) { limits.MaxBufferedMessages = 0 },
	}
	for name, invalidate := range tests {
		t.Run(name, func(t *testing.T) {
			limits := rabbitstream.DefaultLimits()
			invalidate(&limits)
			if validLimits(limits) {
				t.Fatalf("validLimits(%#v) = true", limits)
			}
		})
	}
	if !validLimits(rabbitstream.DefaultLimits()) {
		t.Fatal("validLimits(DefaultLimits()) = false")
	}
}

type typedNilMeterProvider struct{ metric.MeterProvider }

type panicsDuringMeterCreation struct{ metric.MeterProvider }

func (panicsDuringMeterCreation) Meter(string, ...metric.MeterOption) metric.Meter {
	panic("credential=secret")
}
