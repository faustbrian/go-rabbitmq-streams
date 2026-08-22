package rabbitstreamotel

import (
	"context"
	"fmt"
	"math"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/faustbrian/golib/pkg/rabbitstream"
	"go.opentelemetry.io/otel/metric"
	metricnoop "go.opentelemetry.io/otel/metric/noop"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func TestObserveMapsEveryStableObservationWithoutSensitiveDimensions(t *testing.T) {
	t.Parallel()

	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	adapter, err := New(Config{
		MeterProvider: provider, Limits: rabbitstream.DefaultLimits(),
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	observations := []rabbitstream.Observation{
		{Kind: rabbitstream.ObservationConnectionConnecting, Count: 1},
		{Kind: rabbitstream.ObservationConnectionReady, Count: 1},
		{Kind: rabbitstream.ObservationConnectionLost, Count: 1, Category: rabbitstream.ErrorCategory("customer-secret")},
		{Kind: rabbitstream.ObservationReconnectAttempt, Count: 2},
		{Kind: rabbitstream.ObservationAuthenticationError, Count: 1},
		{Kind: rabbitstream.ObservationPublishAttempt, Count: 3, Bytes: 128},
		{Kind: rabbitstream.ObservationPublishConfirmed, Count: 2, Duration: time.Second},
		{Kind: rabbitstream.ObservationPublishRejected, Count: 1, Duration: 2 * time.Second, Category: rabbitstream.CategoryAuthorization},
		{Kind: rabbitstream.ObservationPublishAttempt, Count: 1, Bytes: 1},
		{Kind: rabbitstream.ObservationPublishAmbiguous, Count: 1, Duration: 3 * time.Second, Category: rabbitstream.CategoryPublishAmbiguous},
		{Kind: rabbitstream.ObservationPublishAttempt, Count: 1, Bytes: 1},
		{Kind: rabbitstream.ObservationPublishError, Count: 1, Duration: 4 * time.Second},
		{Kind: rabbitstream.ObservationConsumerMessage, Count: 4, Bytes: 1024},
		{Kind: rabbitstream.ObservationHandlerSuccess, Count: 4, Duration: 2 * time.Second},
		{Kind: rabbitstream.ObservationHandlerError, Count: 4, Duration: -time.Second, Category: rabbitstream.CategoryHandler},
		{Kind: rabbitstream.ObservationHandlerRetry, Count: 3},
		{Kind: rabbitstream.ObservationRetryStreamPublished, Count: 2},
		{Kind: rabbitstream.ObservationDeadLetterPublished, Count: 1},
		{Kind: rabbitstream.ObservationFailurePublishError, Count: 1, Category: rabbitstream.CategoryAuthorization},
		{Kind: rabbitstream.ObservationOffsetStoreAccepted, Count: 1, Value: 77},
		{Kind: rabbitstream.ObservationStreamEndOffset, Count: 1, Value: 99},
		{Kind: rabbitstream.ObservationConsumerLag, Count: 1, Value: 22},
		{Kind: rabbitstream.ObservationReplayProgress, Count: 5},
		{Kind: rabbitstream.ObservationProducerShutdown, Count: 1, Duration: time.Second},
		{Kind: rabbitstream.ObservationConsumerShutdown, Count: 1, Duration: 2 * time.Second},
		{Kind: rabbitstream.ObservationKind("ignored"), Count: math.MaxUint64},
	}
	for _, observation := range observations {
		adapter.Observe(observation)
	}

	data := collectMetrics(t, reader)
	assertInt64Point(t, data, "rabbitstream.connection.state", 0)
	assertInt64Point(t, data, "rabbitstream.reconnects", 2)
	assertInt64Point(t, data, "rabbitstream.publish.messages", 2)
	assertInt64Point(t, data, "rabbitstream.publish.bytes", 130)
	assertInt64Point(t, data, "rabbitstream.publish.unconfirmed", 0)
	assertInt64Point(t, data, "rabbitstream.consumer.messages", 4)
	assertInt64Point(t, data, "rabbitstream.consumer.bytes", 1024)
	assertInt64Point(t, data, "rabbitstream.consumer.handler.retries", 3)
	assertInt64Point(t, data, "rabbitstream.consumer.retry_stream.messages", 2)
	assertInt64Point(t, data, "rabbitstream.consumer.dead_letter.messages", 1)
	assertInt64Point(t, data, "rabbitstream.consumer.failure_publish.errors", 1)
	assertInt64Point(t, data, "rabbitstream.consumer.offset", 77)
	assertInt64Point(t, data, "rabbitstream.stream.end_offset", 99)
	assertInt64Point(t, data, "rabbitstream.consumer.lag", 22)
	assertInt64Point(t, data, "rabbitstream.replay.messages", 5)
	assertFloat64Histogram(t, data, "rabbitstream.publish.confirmation.duration", 4, 10)
	assertFloat64Histogram(t, data, "rabbitstream.consumer.handler.duration", 2, 2)
	assertFloat64Histogram(t, data, "rabbitstream.producer.shutdown.duration", 1, 1)
	assertFloat64Histogram(t, data, "rabbitstream.consumer.shutdown.duration", 1, 2)
	formatted := metricDataString(data)
	if strings.Contains(formatted, "customer-secret") {
		t.Fatalf("metrics exposed an unrecognized category: %s", formatted)
	}
	for _, category := range []string{"unknown", "authentication", "authorization", "publish_ambiguous", "handler"} {
		if !strings.Contains(formatted, category) {
			t.Fatalf("metrics missing closed category %q: %s", category, formatted)
		}
	}
}

func TestObserveContainsProviderPanicsAndIsConcurrent(t *testing.T) {
	t.Parallel()

	base := metricnoop.NewMeterProvider().Meter("test")
	adapter, err := New(Config{
		MeterProvider: panickingMeterProvider{
			MeterProvider: metricnoop.NewMeterProvider(),
			meter:         panickingMeter{Meter: base},
		},
		Limits: rabbitstream.DefaultLimits(),
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	kinds := []rabbitstream.ObservationKind{
		rabbitstream.ObservationConnectionConnecting,
		rabbitstream.ObservationReconnectAttempt,
		rabbitstream.ObservationPublishAttempt,
		rabbitstream.ObservationPublishConfirmed,
		rabbitstream.ObservationConsumerMessage,
		rabbitstream.ObservationHandlerSuccess,
		rabbitstream.ObservationHandlerRetry,
		rabbitstream.ObservationRetryStreamPublished,
		rabbitstream.ObservationDeadLetterPublished,
		rabbitstream.ObservationFailurePublishError,
		rabbitstream.ObservationOffsetStoreAccepted,
		rabbitstream.ObservationReplayProgress,
		rabbitstream.ObservationProducerShutdown,
		rabbitstream.ObservationConsumerShutdown,
	}
	var group sync.WaitGroup
	for range 64 {
		for _, kind := range kinds {
			kind := kind
			group.Add(1)
			go func() {
				defer group.Done()
				adapter.Observe(rabbitstream.Observation{Kind: kind, Count: 1, Bytes: 1})
			}()
		}
	}
	group.Wait()

	var nilAdapter *Adapter
	nilAdapter.Observe(rabbitstream.Observation{Kind: rabbitstream.ObservationConnectionReady})
}

func TestUnconfirmedGaugeSaturatesWithoutSignedOverflow(t *testing.T) {
	t.Parallel()

	reader := sdkmetric.NewManualReader()
	adapter, err := New(Config{
		MeterProvider: sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)),
		Limits:        rabbitstream.DefaultLimits(),
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	adapter.Observe(rabbitstream.Observation{
		Kind: rabbitstream.ObservationPublishAttempt, Count: math.MaxUint64,
	})
	adapter.Observe(rabbitstream.Observation{
		Kind: rabbitstream.ObservationPublishAttempt, Count: math.MaxUint64,
	})
	assertInt64Point(t, collectMetrics(t, reader), "rabbitstream.publish.unconfirmed", math.MaxInt64)

	adapter.Observe(rabbitstream.Observation{
		Kind: rabbitstream.ObservationPublishConfirmed, Count: math.MaxUint64,
	})
	assertInt64Point(t, collectMetrics(t, reader), "rabbitstream.publish.unconfirmed", 0)
}

func TestObservationNormalizationHelpersAreClosedAndBounded(t *testing.T) {
	t.Parallel()

	if got := boundedInt64(math.MaxUint64); got != math.MaxInt64 {
		t.Fatalf("boundedInt64(MaxUint64) = %d", got)
	}
	if got := boundedInt64(math.MaxInt64); got != math.MaxInt64 {
		t.Fatalf("boundedInt64(MaxInt64) = %d", got)
	}
	if got := boundedInt64(42); got != 42 {
		t.Fatalf("boundedInt64(42) = %d", got)
	}
	if got := nonnegativeSeconds(-time.Second); got != 0 {
		t.Fatalf("nonnegativeSeconds(-1s) = %f", got)
	}
	if got := nonnegativeSeconds(0); got != 0 {
		t.Fatalf("nonnegativeSeconds(0) = %f", got)
	}
	if got := nonnegativeSeconds(1500 * time.Millisecond); got != 1.5 {
		t.Fatalf("nonnegativeSeconds(1.5s) = %f", got)
	}
	closed := []rabbitstream.ErrorCategory{
		rabbitstream.CategoryInvalidConfiguration, rabbitstream.CategoryValidation,
		rabbitstream.CategoryClosed, rabbitstream.CategoryCanceled, rabbitstream.CategoryTimeout,
		rabbitstream.CategoryAuthentication, rabbitstream.CategoryAuthorization,
		rabbitstream.CategoryConnection, rabbitstream.CategoryStreamUnavailable,
		rabbitstream.CategoryPartitionUnavailable, rabbitstream.CategoryBrokerRejected,
		rabbitstream.CategoryMessageTooLarge, rabbitstream.CategoryPublishAmbiguous,
		rabbitstream.CategoryConfirmation, rabbitstream.CategoryRetentionGap,
		rabbitstream.CategoryReplayRange, rabbitstream.CategoryOffset,
		rabbitstream.CategoryHandler, rabbitstream.CategoryFatal,
	}
	for _, category := range closed {
		if got := closedCategory(category); got != string(category) {
			t.Fatalf("closedCategory(%q) = %q", category, got)
		}
	}
	if got := closedCategory(rabbitstream.ErrorCategory("tenant-secret")); got != "unknown" {
		t.Fatalf("closedCategory(unrecognized) = %q", got)
	}
}

type panickingMeterProvider struct {
	metric.MeterProvider
	meter metric.Meter
}

func (provider panickingMeterProvider) Meter(string, ...metric.MeterOption) metric.Meter {
	return provider.meter
}

type panickingMeter struct{ metric.Meter }

func (meter panickingMeter) Int64Counter(name string, options ...metric.Int64CounterOption) (metric.Int64Counter, error) {
	instrument, err := meter.Meter.Int64Counter(name, options...)
	return panickingInt64Counter{Int64Counter: instrument}, err
}

func (meter panickingMeter) Int64Gauge(name string, options ...metric.Int64GaugeOption) (metric.Int64Gauge, error) {
	instrument, err := meter.Meter.Int64Gauge(name, options...)
	return panickingInt64Gauge{Int64Gauge: instrument}, err
}

func (meter panickingMeter) Float64Histogram(name string, options ...metric.Float64HistogramOption) (metric.Float64Histogram, error) {
	instrument, err := meter.Meter.Float64Histogram(name, options...)
	return panickingFloat64Histogram{Float64Histogram: instrument}, err
}

type panickingInt64Counter struct{ metric.Int64Counter }

func (panickingInt64Counter) Add(context.Context, int64, ...metric.AddOption) {
	panic("counter secret")
}

type panickingInt64Gauge struct{ metric.Int64Gauge }

func (panickingInt64Gauge) Record(context.Context, int64, ...metric.RecordOption) {
	panic("gauge secret")
}

type panickingFloat64Histogram struct{ metric.Float64Histogram }

func (panickingFloat64Histogram) Record(context.Context, float64, ...metric.RecordOption) {
	panic("histogram secret")
}

func collectMetrics(t *testing.T, reader *sdkmetric.ManualReader) metricdata.ResourceMetrics {
	t.Helper()
	var data metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &data); err != nil {
		t.Fatalf("Collect() error = %v", err)
	}
	return data
}

func assertInt64Point(t *testing.T, data metricdata.ResourceMetrics, name string, want int64) {
	t.Helper()
	for _, scope := range data.ScopeMetrics {
		for _, measured := range scope.Metrics {
			if measured.Name != name {
				continue
			}
			switch points := measured.Data.(type) {
			case metricdata.Sum[int64]:
				if len(points.DataPoints) == 1 && points.DataPoints[0].Value == want {
					return
				}
			case metricdata.Gauge[int64]:
				if len(points.DataPoints) == 1 && points.DataPoints[0].Value == want {
					return
				}
			}
			t.Fatalf("metric %q = %#v, want %d", name, measured.Data, want)
		}
	}
	t.Fatalf("metric %q not found", name)
}

func assertFloat64Histogram(t *testing.T, data metricdata.ResourceMetrics, name string, count uint64, sum float64) {
	t.Helper()
	for _, scope := range data.ScopeMetrics {
		for _, measured := range scope.Metrics {
			if measured.Name != name {
				continue
			}
			points, ok := measured.Data.(metricdata.Histogram[float64])
			if !ok || len(points.DataPoints) != 1 || points.DataPoints[0].Count != count || points.DataPoints[0].Sum != sum {
				t.Fatalf("metric %q = %#v, want count=%d sum=%f", name, measured.Data, count, sum)
			}
			return
		}
	}
	t.Fatalf("metric %q not found", name)
}

func metricDataString(data metricdata.ResourceMetrics) string {
	var values []string
	for _, scope := range data.ScopeMetrics {
		for _, measured := range scope.Metrics {
			values = append(values, measured.Name, fmt.Sprintf("%v", measured.Data))
		}
	}
	return strings.Join(values, "\n")
}
