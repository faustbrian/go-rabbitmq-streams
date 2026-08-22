package rabbitstreamotel_test

import (
	"context"
	"fmt"

	"github.com/faustbrian/golib/pkg/rabbitstream"
	rabbitstreamotel "github.com/faustbrian/golib/pkg/rabbitstream/otel"
	metricnoop "go.opentelemetry.io/otel/metric/noop"
)

func Example() {
	adapter, err := rabbitstreamotel.New(rabbitstreamotel.Config{
		MeterProvider: metricnoop.NewMeterProvider(),
		Limits:        rabbitstream.DefaultLimits(),
	})
	if err != nil {
		panic(err)
	}

	outbound, err := adapter.Inject(context.Background(), rabbitstream.Message{
		Stream: "tracking.events", Payload: []byte("record"),
	})
	if err != nil {
		panic(err)
	}
	adapter.Observe(rabbitstream.Observation{
		Kind: rabbitstream.ObservationPublishAttempt, Count: 1, Bytes: uint64(len(outbound.Payload)),
	})
	fmt.Println(outbound.Stream)
	// Output: tracking.events
}
