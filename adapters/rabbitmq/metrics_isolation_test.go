package rabbitmq

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/faustbrian/go-rabbitmq-streams/adapters/rabbitmq/v2/internal/rabbitmqstream/stream"
	rabbitstream "github.com/faustbrian/go-rabbitmq-streams/v2"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
)

func TestNativeEnvironmentDoesNotUseProcessMeterProvider(t *testing.T) {
	previous := otel.GetMeterProvider()
	provider := &recordingProcessMeter{MeterProvider: noop.NewMeterProvider()}
	otel.SetMeterProvider(provider)
	t.Cleanup(func() { otel.SetMeterProvider(previous) })
	// Installing the first provider delegates meters created by earlier tests.
	// Only registrations made by the native operation below belong to this check.
	installedCalls := provider.calls.Load()
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Exercise native metric initialization without opening a socket.
	options := environmentOptions(
		rabbitstream.ConnectionConfig{
			Heartbeat: 5 * time.Second, Security: rabbitstream.DevelopmentPlaintextSecurity(),
		},
		rabbitstream.Endpoint{Host: "localhost", Port: 5552},
		rabbitstream.Credentials{Username: "local-fixture", Password: []byte("fixture-only")},
		time.Second,
	)
	environment, _ := stream.NewEnvironmentContext(ctx, options)
	if environment != nil {
		_ = environment.Close()
		_ = environment.WaitContext(context.Background())
	}
	if calls := provider.calls.Load() - installedCalls; calls != 0 {
		t.Fatalf("native adapter registered %d meter(s) with the process provider", calls)
	}
}

type recordingProcessMeter struct {
	metric.MeterProvider
	calls atomic.Int32
}

func (provider *recordingProcessMeter) Meter(name string, options ...metric.MeterOption) metric.Meter {
	provider.calls.Add(1)
	return provider.MeterProvider.Meter(name, options...)
}
