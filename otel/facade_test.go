//nolint:staticcheck // This test intentionally verifies the deprecated compatibility facade.
package rabbitstreamotel_test

import (
	"context"
	"errors"
	"reflect"
	"testing"

	rabbitstream "github.com/faustbrian/go-rabbitmq-streams"
	successor "github.com/faustbrian/go-rabbitmq-streams/adapters/otel"
	legacy "github.com/faustbrian/go-rabbitmq-streams/otel"
	"go.opentelemetry.io/otel/metric"
	metricnoop "go.opentelemetry.io/otel/metric/noop"
)

func TestFacadePreservesTypeIdentityScopeAndBehavior(t *testing.T) {
	t.Parallel()

	if got := reflect.TypeOf(legacy.Config{}); got.PkgPath() != "github.com/faustbrian/go-rabbitmq-streams/otel" || got.Name() != "Config" {
		t.Fatalf("Config reflection identity = %s.%s", got.PkgPath(), got.Name())
	}
	adapterType := reflect.TypeOf((*legacy.Adapter)(nil)).Elem()
	if adapterType.PkgPath() != "github.com/faustbrian/go-rabbitmq-streams/otel" || adapterType.Name() != "Adapter" {
		t.Fatalf("Adapter reflection identity = %s.%s", adapterType.PkgPath(), adapterType.Name())
	}

	scopes := make(chan string, 1)
	provider := scopeMeterProvider{MeterProvider: metricnoop.NewMeterProvider(), scopes: scopes}
	adapter, err := legacy.New(legacy.Config{MeterProvider: provider, Limits: rabbitstream.DefaultLimits()})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if got := <-scopes; got != "github.com/faustbrian/go-rabbitmq-streams/otel" {
		t.Fatalf("instrumentation scope = %q", got)
	}

	message := rabbitstream.Message{Stream: "events"}
	legacyResult, legacyErr := adapter.Inject(context.Background(), message)
	canonical, err := successor.New(successor.Config{
		MeterProvider: metricnoop.NewMeterProvider(), Limits: rabbitstream.DefaultLimits(),
	})
	if err != nil {
		t.Fatalf("successor.New() error = %v", err)
	}
	canonicalResult, canonicalErr := canonical.Inject(context.Background(), message)
	if !reflect.DeepEqual(legacyResult, canonicalResult) || !errors.Is(legacyErr, canonicalErr) {
		t.Fatalf("Inject() = %#v, %v; successor = %#v, %v", legacyResult, legacyErr, canonicalResult, canonicalErr)
	}
}

func TestNilFacadePreservesValidationFailures(t *testing.T) {
	t.Parallel()

	var adapter *legacy.Adapter
	adapter.Observe(rabbitstream.Observation{})
	if _, err := adapter.Inject(context.Background(), rabbitstream.Message{}); !errors.Is(err, rabbitstream.ErrValidation) {
		t.Fatalf("Inject() error = %v", err)
	}
	if _, err := adapter.Extract(context.Background(), rabbitstream.Message{}); !errors.Is(err, rabbitstream.ErrValidation) {
		t.Fatalf("Extract() error = %v", err)
	}
}

func TestFacadePreservesNilProviderValidationCause(t *testing.T) {
	t.Parallel()

	var typedNil *typedNilMeterProvider
	for _, provider := range []metric.MeterProvider{nil, typedNil} {
		adapter, err := legacy.New(legacy.Config{
			MeterProvider: provider,
			Limits:        rabbitstream.DefaultLimits(),
		})
		if adapter != nil || !errors.Is(err, rabbitstream.ErrInvalidConfiguration) {
			t.Fatalf("New() = %#v, %v", adapter, err)
		}
		cause := errors.Unwrap(err)
		if cause == nil || cause.Error() != "OpenTelemetry adapter configuration is invalid" {
			t.Fatalf("New() cause = %v", cause)
		}
	}
}

type typedNilMeterProvider struct{ metric.MeterProvider }

type scopeMeterProvider struct {
	metric.MeterProvider
	scopes chan<- string
}

func (provider scopeMeterProvider) Meter(name string, options ...metric.MeterOption) metric.Meter {
	provider.scopes <- name
	return provider.MeterProvider.Meter(name, options...)
}
