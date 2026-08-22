package rabbitstream_test

import (
	"context"
	"fmt"

	"github.com/faustbrian/golib/pkg/rabbitstream"
)

func ExampleNewProducer() {
	producer, err := rabbitstream.NewProducer(rabbitstream.ProducerConfig{
		Stream: "tracking.events",
	}, confirmedTransport{})
	if err != nil {
		panic(err)
	}
	defer func() { _ = producer.Close(context.Background()) }()

	result, err := producer.Publish(context.Background(), rabbitstream.Message{
		Stream:    "tracking.events",
		MessageID: "event-123",
		Payload:   []byte("opaque event bytes"),
	})
	if err != nil {
		panic(err)
	}
	fmt.Println(result.State == rabbitstream.DeliveryConfirmed)
	// Output: true
}

func ExampleNewConsumer() {
	transport := &singleDeliveryTransport{message: rabbitstream.Message{
		Stream:    "tracking.events",
		Partition: "tracking.events",
		Offset:    42,
		HasOffset: true,
		Payload:   []byte("opaque event bytes"),
	}}
	consumer, err := rabbitstream.NewConsumer(rabbitstream.ConsumerConfig{
		Stream:       "tracking.events",
		ConsumerName: "tracking-projector-v1",
	}, transport)
	if err != nil {
		panic(err)
	}
	defer func() { _ = consumer.Close(context.Background()) }()

	ctx, cancel := context.WithCancel(context.Background())
	err = consumer.Run(ctx, func(_ context.Context, message rabbitstream.Message) error {
		fmt.Println(message.Stream, message.Offset)
		cancel()
		return nil
	})
	if err == nil {
		panic("consumer unexpectedly returned nil")
	}
	// Output: tracking.events 42
}

type confirmedTransport struct{}

func (confirmedTransport) Send(
	_ context.Context,
	message rabbitstream.Message,
	confirm func(rabbitstream.TransportConfirmation),
) error {
	confirm(rabbitstream.TransportConfirmation{
		Confirmed:    true,
		PublishingID: message.PublishingID,
		Partition:    message.Stream,
	})
	return nil
}

func (confirmedTransport) Close() error { return nil }

type singleDeliveryTransport struct {
	message   rabbitstream.Message
	delivered bool
}

func (transport *singleDeliveryTransport) Next(ctx context.Context) (rabbitstream.Message, error) {
	if !transport.delivered {
		transport.delivered = true
		return transport.message.Retain(), nil
	}
	<-ctx.Done()
	return rabbitstream.Message{}, ctx.Err()
}

func (*singleDeliveryTransport) StoreOffset(context.Context, string, uint64) error { return nil }
func (*singleDeliveryTransport) Close() error                                      { return nil }
