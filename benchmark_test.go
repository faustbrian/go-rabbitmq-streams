package rabbitstream

import (
	"context"
	"fmt"
	"testing"
)

func BenchmarkMessagePolicy(b *testing.B) {
	limits := DefaultLimits()
	for _, payloadBytes := range []int{128, 1 << 10, 64 << 10} {
		message := benchmarkMessage(payloadBytes)
		b.Run(fmt.Sprintf("validate/%dB", payloadBytes), func(b *testing.B) {
			b.SetBytes(int64(payloadBytes))
			b.ReportAllocs()
			for b.Loop() {
				if err := message.Validate(limits); err != nil {
					b.Fatal(err)
				}
			}
		})
		b.Run(fmt.Sprintf("retain/%dB", payloadBytes), func(b *testing.B) {
			b.SetBytes(int64(payloadBytes))
			b.ReportAllocs()
			for b.Loop() {
				retained := message.Retain()
				if len(retained.Payload) != payloadBytes {
					b.Fatalf("retained payload bytes = %d", len(retained.Payload))
				}
			}
		})
	}
}

func BenchmarkProducerPolicy(b *testing.B) {
	ctx := context.Background()
	for _, payloadBytes := range []int{128, 1 << 10, 64 << 10} {
		message := benchmarkMessage(payloadBytes)
		b.Run(fmt.Sprintf("synchronous/%dB", payloadBytes), func(b *testing.B) {
			producer := benchmarkProducer(b)
			b.SetBytes(int64(payloadBytes))
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				result, err := producer.Publish(ctx, message)
				if err != nil || result.State != DeliveryConfirmed {
					b.Fatalf("Publish() = %#v, %v", result, err)
				}
			}
		})
	}

	for _, messages := range []int{10, 100} {
		batch := make([]Message, messages)
		for index := range batch {
			batch[index] = benchmarkMessage(1 << 10)
		}
		b.Run(fmt.Sprintf("batch/%d-messages/1024B", messages), func(b *testing.B) {
			producer := benchmarkProducer(b)
			b.SetBytes(int64(messages << 10))
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				results, err := producer.PublishBatch(ctx, batch)
				if err != nil || len(results) != messages || results[messages-1].State != DeliveryConfirmed {
					b.Fatalf("PublishBatch() = %d results, %v", len(results), err)
				}
			}
		})
	}

	b.Run("asynchronous/1024B", func(b *testing.B) {
		producer := benchmarkProducer(b)
		message := benchmarkMessage(1 << 10)
		b.SetBytes(1 << 10)
		b.ReportAllocs()
		b.ResetTimer()
		for b.Loop() {
			outcomes, err := producer.PublishAsync(ctx, message)
			if err != nil {
				b.Fatal(err)
			}
			outcome := <-outcomes
			if outcome.Err != nil || outcome.Result.State != DeliveryConfirmed {
				b.Fatalf("PublishAsync() = %#v", outcome)
			}
		}
	})
}

func BenchmarkConsumerPolicy(b *testing.B) {
	ctx := context.Background()
	message := benchmarkMessage(1 << 10)
	message.Partition = message.Stream
	message.Offset = 1
	message.HasOffset = true
	consumer := benchmarkConsumer(b)

	b.Run("single-with-offset-store/1024B", func(b *testing.B) {
		b.SetBytes(1 << 10)
		b.ReportAllocs()
		for b.Loop() {
			if err := consumer.process(ctx, benchmarkMessageHandler, message, true); err != nil {
				b.Fatal(err)
			}
		}
	})

	for _, messages := range []int{10, 100} {
		batch := make([]Message, messages)
		for index := range batch {
			batch[index] = message
			batch[index].Offset = uint64(index + 1)
		}
		b.Run(fmt.Sprintf("batch/%d-messages/1024B", messages), func(b *testing.B) {
			b.SetBytes(int64(messages << 10))
			b.ReportAllocs()
			for b.Loop() {
				if err := consumer.processBatch(ctx, benchmarkBatchHandler, batch); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkReplayPolicy(b *testing.B) {
	const messages = 100
	replayMessages := make([]Message, messages)
	for index := range replayMessages {
		replayMessages[index] = benchmarkMessage(1 << 10)
		replayMessages[index].Partition = replayMessages[index].Stream
		replayMessages[index].Offset = uint64(index)
		replayMessages[index].HasOffset = true
	}
	source := benchmarkReplaySource{messages: replayMessages}
	replayer, err := NewReplayer(DefaultLimits(), source, nil)
	if err != nil {
		b.Fatal(err)
	}
	request := ReplayRequest{Stream: "tracking.events", Start: StartPosition{Kind: OffsetStartBeginning}}

	b.SetBytes(messages << 10)
	b.ReportAllocs()
	for b.Loop() {
		if err := replayer.Run(context.Background(), request, benchmarkReplayHandler); err != nil {
			b.Fatal(err)
		}
	}
}

func benchmarkMessage(payloadBytes int) Message {
	return Message{
		Stream: "tracking.events", RoutingKey: "tracked-item-1",
		ContentType: "application/octet-stream", MessageID: "message-1",
		CorrelationID: "correlation-1", Payload: make([]byte, payloadBytes),
		Headers:    []MetadataEntry{{Key: "traceparent", Value: []byte("00-00000000000000000000000000000001-0000000000000001-01")}},
		Properties: []MetadataEntry{{Key: "schema-version", Value: []byte("1")}},
	}
}

func benchmarkProducer(b *testing.B) *Producer {
	b.Helper()
	producer, err := NewProducer(ProducerConfig{Stream: "tracking.events"}, benchmarkProducerTransport{})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		if err := producer.Close(context.Background()); err != nil {
			b.Error(err)
		}
	})
	return producer
}

type benchmarkProducerTransport struct{}

func (benchmarkProducerTransport) Send(
	_ context.Context,
	message Message,
	confirm func(TransportConfirmation),
) error {
	confirm(TransportConfirmation{Confirmed: true, PublishingID: message.PublishingID, Partition: message.Stream})
	return nil
}

func (benchmarkProducerTransport) Close() error { return nil }

func benchmarkConsumer(b *testing.B) *Consumer {
	b.Helper()
	consumer, err := NewConsumer(
		ConsumerConfig{Stream: "tracking.events", ConsumerName: "benchmark-consumer"},
		benchmarkConsumerTransport{},
	)
	if err != nil {
		b.Fatal(err)
	}
	return consumer
}

type benchmarkConsumerTransport struct{}

func (benchmarkConsumerTransport) Next(context.Context) (Message, error) {
	return Message{}, context.Canceled
}

func (benchmarkConsumerTransport) StoreOffset(context.Context, string, uint64) error { return nil }
func (benchmarkConsumerTransport) Close() error                                      { return nil }

func benchmarkMessageHandler(context.Context, Message) error       { return nil }
func benchmarkBatchHandler(context.Context, []Message) error       { return nil }
func benchmarkReplayHandler(context.Context, ReplayDelivery) error { return nil }

type benchmarkReplaySource struct{ messages []Message }

func (source benchmarkReplaySource) RetainedRange(context.Context, ReplayRequest) (RetainedRange, error) {
	return RetainedRange{FirstOffset: 0, LastOffset: uint64(len(source.messages) - 1)}, nil
}

func (source benchmarkReplaySource) Open(context.Context, ReplayRequest) (ReplayCursor, error) {
	return &benchmarkReplayCursor{messages: source.messages}, nil
}

type benchmarkReplayCursor struct {
	messages []Message
	index    int
}

func (cursor *benchmarkReplayCursor) Next(context.Context) (Message, error) {
	message := cursor.messages[cursor.index]
	cursor.index++
	return message, nil
}

func (*benchmarkReplayCursor) Close() error { return nil }
