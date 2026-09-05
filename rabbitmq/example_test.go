package rabbitmq_test

import (
	"context"
	"crypto/tls"
	"errors"
	"os"
	"time"

	"github.com/faustbrian/go-rabbitmq-streams"
	"github.com/faustbrian/go-rabbitmq-streams/rabbitmq"
)

func ExampleOpenProducer() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	connection := rabbitstream.ConnectionConfig{
		Endpoints: []rabbitstream.Endpoint{{
			Host: "rabbitmq.internal",
			Port: 5551,
		}},
		VirtualHost: "/",
		Credentials: rabbitstream.StaticCredentials(
			os.Getenv("RABBITMQ_STREAM_USER"),
			[]byte(os.Getenv("RABBITMQ_STREAM_PASSWORD")),
		),
		Security: rabbitstream.SecurityConfig{
			TLS: &tls.Config{ServerName: "rabbitmq.internal", MinVersion: tls.VersionTLS12},
		},
	}

	producer, err := rabbitmq.OpenProducer(ctx, connection, rabbitstream.ProducerConfig{
		Stream: "tracking.events",
	})
	if err != nil {
		panic(err)
	}
	defer func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer closeCancel()
		_ = producer.Close(closeCtx)
	}()

	result, err := producer.Publish(ctx, rabbitstream.Message{
		Stream:    "tracking.events",
		MessageID: "event-123",
		Payload:   []byte("opaque event bytes"),
	})
	if err != nil {
		panic(err)
	}
	if result.State != rabbitstream.DeliveryConfirmed {
		panic("publication was not confirmed")
	}
}

func ExampleOpenConsumer() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	connection := rabbitstream.ConnectionConfig{
		Endpoints: []rabbitstream.Endpoint{{
			Host: "rabbitmq.internal",
			Port: 5551,
		}},
		VirtualHost: "/",
		Credentials: rabbitstream.StaticCredentials(
			os.Getenv("RABBITMQ_STREAM_USER"),
			[]byte(os.Getenv("RABBITMQ_STREAM_PASSWORD")),
		),
		Security: rabbitstream.SecurityConfig{
			TLS: &tls.Config{ServerName: "rabbitmq.internal", MinVersion: tls.VersionTLS12},
		},
	}

	consumer, err := rabbitmq.OpenConsumer(ctx, connection, rabbitstream.ConsumerConfig{
		Stream:       "tracking.events",
		ConsumerName: "tracking-projector-v1",
		Start: rabbitstream.StartPosition{
			Kind: rabbitstream.OffsetStartStored,
		},
	})
	if err != nil {
		panic(err)
	}
	defer func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer closeCancel()
		_ = consumer.Close(closeCtx)
	}()

	err = consumer.Run(ctx, func(_ context.Context, message rabbitstream.Message) error {
		_ = message.Payload // Decode and apply the application event here.
		return nil
	})
	if err != nil && !errors.Is(err, context.DeadlineExceeded) {
		panic(err)
	}
}
