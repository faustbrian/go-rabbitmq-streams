package rabbitstream

import (
	"context"
	"errors"
	"testing"
)

func TestDeliveryLimitsApplyAtEveryRootIngress(t *testing.T) {
	limits := DefaultLimits()
	limits.MaxPayloadBytes = 2
	limits.MaxMetadataEntries = 2
	limits.MaxMetadataKeyBytes = 2
	limits.MaxMetadataValueBytes = 2
	limits.MaxMetadataBytes = 3
	limits.MaxRoutingKeyBytes = 2
	limits.MaxStreamNameBytes = 2
	for name, change := range map[string]func(*Message){
		"payload":        func(m *Message) { m.Payload = []byte("abc") },
		"header count":   func(m *Message) { m.Headers = []MetadataEntry{{Key: "a"}, {Key: "a"}, {Key: "a"}} },
		"property count": func(m *Message) { m.Properties = []MetadataEntry{{Key: "a"}, {Key: "a"}, {Key: "a"}} },
		"broker count":   func(m *Message) { m.BrokerMetadata = []MetadataEntry{{Key: "a"}, {Key: "a"}, {Key: "a"}} },
		"combined count": func(m *Message) {
			m.Headers = []MetadataEntry{{Key: "a"}}
			m.Properties = []MetadataEntry{{Key: "a"}}
			m.BrokerMetadata = []MetadataEntry{{Key: "a"}}
		},
		"metadata key":   func(m *Message) { m.Headers = []MetadataEntry{{Key: "abc"}} },
		"metadata value": func(m *Message) { m.Headers = []MetadataEntry{{Key: "a", Value: []byte("abc")}} },
		"metadata aggregate": func(m *Message) {
			m.Headers = []MetadataEntry{{Key: "a", Value: []byte("b")}, {Key: "c", Value: []byte("d")}}
		},
		"content type":   func(m *Message) { m.ContentType = "abc" },
		"message ID":     func(m *Message) { m.MessageID = "abc" },
		"correlation ID": func(m *Message) { m.CorrelationID = "abc" },
		"stream":         func(m *Message) { m.Stream, m.Partition = "abc", "abc" },
		"super stream":   func(m *Message) { m.SuperStream = "abc" },
		"routing key":    func(m *Message) { m.RoutingKey = "abc" },
		"partition":      func(m *Message) { m.Partition = "other" },
		"missing offset": func(m *Message) { m.HasOffset = false },
	} {
		for _, ingress := range []string{"live", "batch", "replay"} {
			t.Run(name+"/"+ingress, func(t *testing.T) {
				message := Message{Stream: "s", Partition: "s", Offset: 1, HasOffset: true}
				change(&message)
				called := false
				var err error
				if ingress == "replay" {
					source := &fakeReplaySource{retained: RetainedRange{FirstOffset: 1, LastOffset: 1}, messages: []Message{message}}
					replayer, newErr := NewReplayer(limits, source, nil)
					if newErr != nil {
						t.Fatal(newErr)
					}
					err = replayer.Run(boundedTestContext(), ReplayRequest{Stream: "s", Start: StartPosition{Kind: OffsetStartBeginning}}, func(context.Context, ReplayDelivery) error { called = true; return ErrFatal })
					if !source.closed {
						t.Fatal("replay cursor not released")
					}
				} else {
					transport := newFakeConsumerTransport(message)
					consumer, newErr := NewConsumer(ConsumerConfig{Stream: "s", ConsumerName: "c", Limits: limits}, transport)
					if newErr != nil {
						t.Fatal(newErr)
					}
					if ingress == "live" {
						err = consumer.Run(boundedTestContext(), func(context.Context, Message) error { called = true; return ErrFatal })
					} else {
						err = consumer.RunBatch(boundedTestContext(), BatchPolicy{MaxMessages: 1}, func(context.Context, []Message) error { called = true; return ErrFatal })
					}
					if len(transport.stored) != 0 {
						t.Fatal("invalid delivery advanced offset")
					}
				}
				if !errors.Is(err, ErrValidation) || called {
					t.Fatalf("invalid delivery: error=%v handler=%t", err, called)
				}
			})
		}
	}
}

func TestRootIngressPreservesValidDeliveryAndOffsetProgress(t *testing.T) {
	for _, superStream := range []string{"", "super"} {
		for _, ingress := range []string{"live", "batch", "replay"} {
			t.Run(superStream+"/"+ingress, func(t *testing.T) {
				message := Message{Stream: "stream", Partition: "stream", SuperStream: superStream, HasOffset: true,
					Payload: []byte("ab"), Headers: []MetadataEntry{{Key: "a", Value: []byte("b")}}}
				limits := DefaultLimits()
				limits.MaxPayloadBytes, limits.MaxMetadataEntries = 2, 1
				limits.MaxMetadataKeyBytes, limits.MaxMetadataValueBytes, limits.MaxMetadataBytes = 1, 1, 2
				calls := 0
				check := func(got Message) {
					calls++
					if got.Stream != "stream" || got.Partition != "stream" || got.SuperStream != superStream || !got.HasOffset || got.Offset != 0 || string(got.Payload) != "ab" {
						t.Error("admitted delivery identity or payload changed")
					}
				}
				if ingress == "replay" {
					source := &fakeReplaySource{retained: RetainedRange{}, messages: []Message{message}}
					replayer, err := NewReplayer(limits, source, nil)
					if err != nil {
						t.Fatal(err)
					}
					request := ReplayRequest{Stream: "stream", Start: StartPosition{Kind: OffsetStartBeginning}}
					if superStream != "" {
						request.Stream, request.SuperStream, request.Partition = "", superStream, "stream"
						request.ExpectedPartitions = []string{"stream"}
					}
					if err := replayer.Run(boundedTestContext(), request, func(_ context.Context, delivery ReplayDelivery) error { check(delivery.Message); return nil }); err != nil {
						t.Fatal(err)
					}
					if !source.closed {
						t.Fatal("cursor not released")
					}
				} else {
					transport := newFakeConsumerTransport(message)
					config := ConsumerConfig{Stream: "stream", ConsumerName: "consumer", Limits: limits}
					if superStream != "" {
						config.Stream, config.SuperStream = "", superStream
					}
					consumer, err := NewConsumer(config, transport)
					if err != nil {
						t.Fatal(err)
					}
					ctx, cancel := context.WithCancel(boundedTestContext())
					defer cancel()
					transport.storeHook = cancel
					if ingress == "live" {
						err = consumer.Run(ctx, func(_ context.Context, got Message) error { check(got); return nil })
					} else {
						err = consumer.RunBatch(ctx, BatchPolicy{MaxMessages: 1}, func(_ context.Context, got []Message) error { check(got[0]); return nil })
					}
					if !errors.Is(err, ErrCanceled) {
						t.Fatalf("terminal error: %v", err)
					}
					if len(transport.stored) != 1 {
						t.Fatalf("offset stores: %d", len(transport.stored))
					}
					stored := <-transport.stored
					if stored.partition != "stream" || stored.offset != 0 {
						t.Fatalf("stored offset: %#v", stored)
					}
				}
				if calls != 1 {
					t.Fatalf("handler calls: %d", calls)
				}
			})
		}
	}
}

type canceledAdmissionTransport struct {
	*fakeConsumerTransport
	cancel  context.CancelFunc
	message Message
}

func (transport *canceledAdmissionTransport) Next(context.Context) (Message, error) {
	transport.cancel()
	return transport.message, nil
}

func TestRootIngressCancellationWinsAfterSuccessfulTransportRead(t *testing.T) {
	for _, ingress := range []string{"live", "batch", "replay"} {
		t.Run(ingress, func(t *testing.T) {
			ctx, cancel := context.WithCancel(boundedTestContext())
			defer cancel()
			message := Message{Stream: "stream", Partition: "stream", HasOffset: true}
			called := false
			observer := &recordingObserver{}
			var err error
			if ingress == "replay" {
				source := &fakeReplaySource{retained: RetainedRange{}, messages: []Message{message}, nextHook: cancel}
				replayer, newErr := NewReplayer(DefaultLimits(), source, observer)
				if newErr != nil {
					t.Fatal(newErr)
				}
				err = replayer.Run(ctx, ReplayRequest{Stream: "stream", Start: StartPosition{Kind: OffsetStartBeginning}}, func(context.Context, ReplayDelivery) error { called = true; return nil })
				if !source.closed {
					t.Fatal("canceled cursor not released")
				}
			} else {
				transport := &canceledAdmissionTransport{fakeConsumerTransport: newFakeConsumerTransport(), cancel: cancel, message: message}
				consumer, newErr := NewConsumer(ConsumerConfig{Stream: "stream", ConsumerName: "consumer", Observer: observer}, transport)
				if newErr != nil {
					t.Fatal(newErr)
				}
				if ingress == "live" {
					err = consumer.Run(ctx, func(context.Context, Message) error { called = true; return nil })
				} else {
					err = consumer.RunBatch(ctx, BatchPolicy{MaxMessages: 1}, func(context.Context, []Message) error { called = true; return nil })
				}
				if len(transport.stored) != 0 {
					t.Fatal("canceled delivery advanced offset")
				}
			}
			if !errors.Is(err, ErrCanceled) || called || len(observer.observations) != 0 {
				t.Fatalf("canceled admission: error=%v handler=%t observations=%d", err, called, len(observer.observations))
			}
		})
	}
}

func TestInvalidRootDeliveryDoesNotReachFailurePublication(t *testing.T) {
	for _, ingress := range []string{"live", "batch"} {
		t.Run(ingress, func(t *testing.T) {
			transport := newFakeConsumerTransport(Message{Stream: "stream", Partition: "stream", HasOffset: true, Payload: []byte("ab")})
			publisher := &fakeFailurePublisher{result: DeliveryResult{State: DeliveryConfirmed}, messages: make(chan Message, 1)}
			limits := DefaultLimits()
			limits.MaxPayloadBytes = 1
			consumer, err := NewConsumer(ConsumerConfig{Stream: "stream", ConsumerName: "consumer", Limits: limits,
				Policy: ConsumerPolicy{FailureStrategy: FailureDeadLetter}, FailurePublisher: publisher, DeadLetterStream: "dead"}, transport)
			if err != nil {
				t.Fatal(err)
			}
			called := false
			if ingress == "live" {
				err = consumer.Run(boundedTestContext(), func(context.Context, Message) error { called = true; return ErrFatal })
			} else {
				err = consumer.RunBatch(boundedTestContext(), BatchPolicy{MaxMessages: 1}, func(context.Context, []Message) error { called = true; return ErrFatal })
			}
			if !errors.Is(err, ErrValidation) || called || len(transport.stored) != 0 || len(publisher.messages) != 0 {
				t.Fatalf("invalid admission: error=%v handler=%t stores=%d publications=%d", err, called, len(transport.stored), len(publisher.messages))
			}
		})
	}
}
