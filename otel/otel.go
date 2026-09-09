// Package rabbitstreamotel preserves the released OpenTelemetry adapter import
// path while delegating behavior to the target-oriented successor.
//
// Deprecated: use github.com/faustbrian/go-rabbitmq-streams/adapters/otel.
package rabbitstreamotel

import (
	"context"
	"errors"
	"reflect"

	rabbitstream "github.com/faustbrian/go-rabbitmq-streams"
	successor "github.com/faustbrian/go-rabbitmq-streams/adapters/otel"
	"go.opentelemetry.io/otel/metric"
)

const instrumentationScope = "github.com/faustbrian/go-rabbitmq-streams/otel"

// Config preserves the released caller-owned provider and message-boundary
// configuration.
//
// Deprecated: use successor.Config.
type Config struct {
	// MeterProvider owns meter creation and exporter lifecycle outside this adapter.
	MeterProvider metric.MeterProvider
	// Limits bounds trace propagation metadata using the producer's message policy.
	Limits rabbitstream.Limits
}

// Adapter preserves the released adapter identity while delegating all runtime
// behavior to the target-oriented successor.
//
// Deprecated: use successor.Adapter.
type Adapter struct {
	delegate *successor.Adapter
}

// New preserves the released construction and error contract.
//
// Deprecated: use successor.New.
func New(config Config) (*Adapter, error) {
	if isNilMeterProvider(config.MeterProvider) {
		return nil, &rabbitstream.OperationError{
			Operation: rabbitstream.OperationConnect,
			Category:  rabbitstream.CategoryInvalidConfiguration,
			Cause:     errors.New("OpenTelemetry adapter configuration is invalid"),
		}
	}
	delegate, err := successor.New(successor.Config{
		MeterProvider: legacyScopeProvider{MeterProvider: config.MeterProvider},
		Limits:        config.Limits,
	})
	if err != nil {
		return nil, err
	}
	return &Adapter{delegate: delegate}, nil
}

func isNilMeterProvider(provider metric.MeterProvider) bool {
	if provider == nil {
		return true
	}
	value := reflect.ValueOf(provider)
	return value.Kind() == reflect.Pointer && value.IsNil()
}

type legacyScopeProvider struct {
	metric.MeterProvider
}

func (provider legacyScopeProvider) Meter(_ string, options ...metric.MeterOption) metric.Meter {
	return provider.MeterProvider.Meter(instrumentationScope, options...)
}

// Observe implements rabbitstream.Observer.
func (adapter *Adapter) Observe(observation rabbitstream.Observation) {
	adapter.successor().Observe(observation)
}

// Inject preserves the released W3C Trace Context injection contract.
func (adapter *Adapter) Inject(
	ctx context.Context,
	message rabbitstream.Message,
) (rabbitstream.Message, error) {
	return adapter.successor().Inject(ctx, message)
}

// Extract preserves the released W3C Trace Context extraction contract.
func (adapter *Adapter) Extract(
	ctx context.Context,
	message rabbitstream.Message,
) (context.Context, error) {
	return adapter.successor().Extract(ctx, message)
}

func (adapter *Adapter) successor() *successor.Adapter {
	if adapter == nil {
		return nil
	}
	return adapter.delegate
}

var _ rabbitstream.Observer = (*Adapter)(nil)
