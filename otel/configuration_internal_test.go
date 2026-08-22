package rabbitstreamotel

import (
	"errors"
	"strings"
	"testing"

	"github.com/faustbrian/golib/pkg/rabbitstream"
	"go.opentelemetry.io/otel/metric"
	metricnoop "go.opentelemetry.io/otel/metric/noop"
)

func TestNewPreservesInstrumentCreationCauseWithoutRenderingIt(t *testing.T) {
	t.Parallel()

	names := []string{
		"rabbitstream.connection.state",
		"rabbitstream.reconnects",
		"rabbitstream.publish.messages",
		"rabbitstream.publish.bytes",
		"rabbitstream.publish.confirmation.duration",
		"rabbitstream.publish.unconfirmed",
		"rabbitstream.consumer.messages",
		"rabbitstream.consumer.bytes",
		"rabbitstream.consumer.handler.duration",
		"rabbitstream.consumer.handler.retries",
		"rabbitstream.consumer.retry_stream.messages",
		"rabbitstream.consumer.dead_letter.messages",
		"rabbitstream.consumer.failure_publish.errors",
		"rabbitstream.consumer.offset",
		"rabbitstream.stream.end_offset",
		"rabbitstream.consumer.lag",
		"rabbitstream.replay.messages",
		"rabbitstream.producer.shutdown.duration",
		"rabbitstream.consumer.shutdown.duration",
		"rabbitstream.errors",
	}
	cause := errors.New("credential=secret")
	base := metricnoop.NewMeterProvider().Meter("test")
	for _, name := range names {
		name := name
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			adapter, err := New(Config{
				MeterProvider: failingMeterProvider{
					MeterProvider: metricnoop.NewMeterProvider(),
					meter:         failingMeter{Meter: base, failName: name, cause: cause},
				},
				Limits: rabbitstream.DefaultLimits(),
			})
			if adapter != nil || !errors.Is(err, rabbitstream.ErrInvalidConfiguration) ||
				!errors.Is(err, cause) {
				t.Fatalf("New() = %#v, %v", adapter, err)
			}
			if strings.Contains(err.Error(), "secret") {
				t.Fatalf("New() rendered unsafe cause = %q", err)
			}
		})
	}
}

func TestNewRejectsNilInstruments(t *testing.T) {
	t.Parallel()

	names := []string{
		"rabbitstream.connection.state",
		"rabbitstream.reconnects",
		"rabbitstream.publish.messages",
		"rabbitstream.publish.bytes",
		"rabbitstream.publish.confirmation.duration",
		"rabbitstream.publish.unconfirmed",
		"rabbitstream.consumer.messages",
		"rabbitstream.consumer.bytes",
		"rabbitstream.consumer.handler.duration",
		"rabbitstream.consumer.handler.retries",
		"rabbitstream.consumer.retry_stream.messages",
		"rabbitstream.consumer.dead_letter.messages",
		"rabbitstream.consumer.failure_publish.errors",
		"rabbitstream.consumer.offset",
		"rabbitstream.stream.end_offset",
		"rabbitstream.consumer.lag",
		"rabbitstream.replay.messages",
		"rabbitstream.producer.shutdown.duration",
		"rabbitstream.consumer.shutdown.duration",
		"rabbitstream.errors",
	}
	base := metricnoop.NewMeterProvider().Meter("test")
	for _, name := range names {
		name := name
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			adapter, err := New(Config{
				MeterProvider: failingMeterProvider{
					MeterProvider: metricnoop.NewMeterProvider(),
					meter:         failingMeter{Meter: base, failName: name},
				},
				Limits: rabbitstream.DefaultLimits(),
			})
			if adapter != nil || !errors.Is(err, rabbitstream.ErrInvalidConfiguration) {
				t.Fatalf("New() = %#v, %v", adapter, err)
			}
		})
	}
}

type failingMeterProvider struct {
	metric.MeterProvider
	meter metric.Meter
}

func (provider failingMeterProvider) Meter(string, ...metric.MeterOption) metric.Meter {
	return provider.meter
}

type failingMeter struct {
	metric.Meter
	failName string
	cause    error
}

func (meter failingMeter) Int64Counter(
	name string,
	options ...metric.Int64CounterOption,
) (metric.Int64Counter, error) {
	if name == meter.failName {
		return nil, meter.cause
	}
	return meter.Meter.Int64Counter(name, options...)
}

func (meter failingMeter) Int64Gauge(
	name string,
	options ...metric.Int64GaugeOption,
) (metric.Int64Gauge, error) {
	if name == meter.failName {
		return nil, meter.cause
	}
	return meter.Meter.Int64Gauge(name, options...)
}

func (meter failingMeter) Float64Histogram(
	name string,
	options ...metric.Float64HistogramOption,
) (metric.Float64Histogram, error) {
	if name == meter.failName {
		return nil, meter.cause
	}
	return meter.Meter.Float64Histogram(name, options...)
}
