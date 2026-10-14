//go:build integration

package rabbitmq

import (
	"context"
	"testing"
	"time"

	"github.com/faustbrian/go-rabbitmq-streams/adapters/rabbitmq/v2/internal/rabbitmqstream/amqp"
	"github.com/faustbrian/go-rabbitmq-streams/adapters/rabbitmq/v2/internal/rabbitmqstream/message"
	"github.com/faustbrian/go-rabbitmq-streams/adapters/rabbitmq/v2/internal/rabbitmqstream/stream"
	rabbitstream "github.com/faustbrian/go-rabbitmq-streams/v2"
)

// TestCompressedSubentryInspectionAndReplay observes physical and logical
// counts independently against the selected broker, not a synthetic decoder.
func TestCompressedSubentryInspectionAndReplay(t *testing.T) {
	connection, environment := integrationBroker(t)
	t.Cleanup(func() {
		closeCtx, cancelClose := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancelClose()
		if err := environment.CloseContext(closeCtx); err != nil {
			t.Error(err)
		}
	})
	name := integrationName("r31-gzip")
	if err := environment.DeclareStream(name, stream.NewStreamOptions()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := environment.DeleteStream(name); err != nil {
			t.Error(err)
		}
	})
	producer, err := environment.NewProducer(name, stream.NewProducerOptions().SetSubEntrySize(2).SetCompression(stream.Compression{}.Gzip()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := producer.Close(); err != nil {
			t.Error(err)
		}
	})
	confirmations := producer.NotifyPublishConfirmation()
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	if err := producer.BatchSendContext(ctx, []message.StreamMessage{amqp.NewMessage([]byte("first")), amqp.NewMessage([]byte("second"))}); err != nil {
		t.Fatal(err)
	}
	confirmed := map[string]bool{}
	for len(confirmed) < 2 {
		select {
		case batch := <-confirmations:
			for _, status := range batch {
				statuses := append([]*stream.ConfirmationStatus{status}, status.LinkedMessages()...)
				for _, linked := range statuses {
					if !linked.IsConfirmed() {
						t.Fatalf("unconfirmed: %v", linked.GetError())
					}
					confirmed[string(linked.GetMessage().GetData()[0])] = true
				}
			}
		case <-ctx.Done():
			t.Fatal("two logical confirmations not received", ctx.Err())
		}
	}
	if !confirmed["first"] || !confirmed["second"] {
		t.Fatalf("confirmed payloads = %v", confirmed)
	}
	type observed struct {
		offset   int64
		physical uint16
		payload  string
	}
	deliveries := make(chan observed, 2)
	consumer, err := environment.NewConsumer(name, func(cc stream.ConsumerContext, m *amqp.Message) {
		deliveries <- observed{cc.Consumer.GetOffset(), cc.GetEntriesCount(), string(m.GetData())}
	}, stream.NewConsumerOptions().SetManualCommit().SetOffset(stream.OffsetSpecification{}.First()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := consumer.Close(); err != nil {
			t.Error(err)
		}
	})
	for i, want := range []string{"first", "second"} {
		select {
		case got := <-deliveries:
			if got.offset != int64(i) || got.physical != 1 || got.payload != want {
				t.Fatalf("native physical/logical delivery = %+v; want offset%d physical1 payload%s", got, i, want)
			}
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	inspector, err := NewInspector(connection, rabbitstream.DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	facts, err := inspector.Inspect(ctx, rabbitstream.InspectionRequest{Stream: name})
	if err != nil {
		t.Fatal(err)
	}
	if len(facts.Partitions) != 1 || facts.Partitions[0].LastOffset == nil || *facts.Partitions[0].LastOffset != 1 {
		t.Errorf("compressed public inspection last offset = %+v; want1", facts)
	}
	replayer, err := NewReplayer(connection, rabbitstream.DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	end := uint64(1)
	request := rabbitstream.ReplayRequest{Stream: name, Start: rabbitstream.StartPosition{Kind: rabbitstream.OffsetStartBeginning}, EndOffset: &end}
	retained, err := replayer.Inspect(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if retained.Empty || retained.FirstOffset != 0 || retained.LastOffset != 1 {
		t.Errorf("compressed retained range = %+v; want0..1", retained)
	}
	var replayed []rabbitstream.Message
	if err := replayer.Run(ctx, request, func(_ context.Context, d rabbitstream.ReplayDelivery) error {
		replayed = append(replayed, d.Message.Retain())
		return nil
	}); err != nil {
		t.Errorf("compressed replay: %v", err)
	}
	if len(replayed) != 2 {
		t.Fatalf("compressed replay delivered%d logical messages; want2", len(replayed))
	}
	for i, want := range []string{"first", "second"} {
		if !replayed[i].HasOffset || replayed[i].Offset != uint64(i) || string(replayed[i].Payload) != want {
			t.Fatalf("replayed[%d] = %+v", i, replayed[i])
		}
	}
}
