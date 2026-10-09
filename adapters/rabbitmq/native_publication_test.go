package rabbitmq

import (
	"context"
	"errors"
	"testing"

	"github.com/faustbrian/go-rabbitmq-streams/adapters/rabbitmq/v2/internal/rabbitmqstream/message"
	privatestream "github.com/faustbrian/go-rabbitmq-streams/adapters/rabbitmq/v2/internal/rabbitmqstream/stream"
	"github.com/faustbrian/go-rabbitmq-streams/v2"
)

func TestNativeWriteFailureRemainsAmbiguousAtAdapterBoundary(t *testing.T) {
	for _, cause := range []error{context.Canceled, context.DeadlineExceeded} {
		t.Run(cause.Error(), func(t *testing.T) {
			session := newRabbitProducerSessionForTest(func(message.StreamMessage) error {
				return &privatestream.WriteAdmittedError{Err: cause}
			})
			var result rabbitstream.TransportConfirmation
			calls := 0
			err := session.Send(context.Background(), rabbitstream.Message{
				Stream: "tracking.events", Payload: []byte("event"),
				HasPublishingID: true, PublishingID: 42,
			}, func(confirmation rabbitstream.TransportConfirmation) {
				calls++
				result = confirmation
			})
			if err != nil {
				t.Fatal("an attempted native write was reported as pre-admission failure")
			}
			if calls != 1 || !result.Ambiguous || result.BrokerRejected ||
				result.Confirmed || result.PublishingID != 42 ||
				!errors.Is(result.Cause, cause) || result.Partition != "tracking.events" {
				t.Fatalf("native write outcome was not preserved: calls=%d, result=%+v", calls, result)
			}
			session.Abort(cause)
			if calls != 1 || len(session.pending) != 0 {
				t.Fatal("terminal session failure repeated the publication callback")
			}
		})
	}
}

func TestNativeWriteFailureDoesNotRepeatConcurrentAbort(t *testing.T) {
	var session *rabbitProducerSession
	session = newRabbitProducerSessionForTest(func(message.StreamMessage) error {
		session.Abort(context.Canceled)
		return &privatestream.WriteAdmittedError{Err: context.Canceled}
	})
	calls := 0
	err := session.Send(context.Background(), rabbitstream.Message{Stream: "tracking.events"}, func(result rabbitstream.TransportConfirmation) {
		calls++
		if !result.Ambiguous {
			t.Fatal("abort did not preserve publication ambiguity")
		}
	})
	if err != nil || calls != 1 || len(session.pending) != 0 {
		t.Fatalf("abort/write race: err=%v callbacks=%d pending=%d", err, calls, len(session.pending))
	}
}

func TestProducerSessionPassesOperationContextToNativeSend(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	session := newRabbitProducerSessionForTest(func(message.StreamMessage) error { return nil })
	called := false
	session.send = func(operation context.Context, _ message.StreamMessage) error {
		called = true
		if operation != ctx {
			t.Fatal("native publication did not receive the caller's operation context")
		}
		return nil
	}
	if err := session.Send(ctx, rabbitstream.Message{Stream: "tracking.events"}, func(rabbitstream.TransportConfirmation) {}); err != nil || !called {
		t.Fatalf("native send: err=%v called=%t", err, called)
	}
	session.Abort(context.Canceled)
}

func TestAbortPreservesExplicitPublishingID(t *testing.T) {
	session := newRabbitProducerSessionForTest(func(message.StreamMessage) error { return nil })
	var result rabbitstream.TransportConfirmation
	if err := session.Send(context.Background(), rabbitstream.Message{
		Payload: []byte("bounded"), PublishingID: 41, HasPublishingID: true,
	}, func(confirmation rabbitstream.TransportConfirmation) { result = confirmation }); err != nil {
		t.Fatalf("Send: %v", err)
	}
	session.Abort(rabbitstream.ErrClosed)
	if !result.Ambiguous || result.PublishingID != 41 {
		t.Fatalf("aborted result = %#v, want ambiguous publishing ID 41", result)
	}
}

func TestProducerSessionPreservesPreAdmissionCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	session := newRabbitProducerSessionForTest(func(message.StreamMessage) error { return nil })
	session.send = func(operation context.Context, _ message.StreamMessage) error {
		cancel()
		return operation.Err()
	}
	calls := 0
	err := session.Send(ctx, rabbitstream.Message{Stream: "tracking.events"}, func(rabbitstream.TransportConfirmation) { calls++ })
	if !errors.Is(err, context.Canceled) || calls != 0 || len(session.pending) != 0 {
		t.Fatalf("pre-admission cancellation: err=%v callbacks=%d pending=%d", err, calls, len(session.pending))
	}
}
