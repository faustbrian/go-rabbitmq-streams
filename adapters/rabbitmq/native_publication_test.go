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

func TestProducerTransportReconnectsAfterNativeLifetimeCancellation(t *testing.T) {
	first := newRabbitProducerSessionForTest(func(message.StreamMessage) error {
		// A native lifetime can stop without canceling its caller's operation.
		return context.Canceled
	})
	second := newFakeProducerSession()
	opens := 0
	transport, err := newReconnectingProducerTransport(t.Context(), func(context.Context) (producerSession, error) {
		opens++
		if opens == 1 {
			return first, nil
		}
		return second, nil
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = transport.Close() })
	calls := 0
	err = transport.Send(t.Context(), rabbitstream.Message{Stream: "tracking.events"}, func(result rabbitstream.TransportConfirmation) {
		calls++
		if !result.Confirmed || result.Ambiguous {
			t.Errorf("recovered publication = %+v", result)
		}
	})
	if err != nil || opens != 2 || calls != 0 || !first.aborted || len(first.pending) != 0 {
		t.Fatalf("native lifetime recovery: err=%v opens=%d callbacks=%d aborted=%t pending=%d", err, opens, calls, first.aborted, len(first.pending))
	}
	second.mutex.Lock()
	confirm := second.confirm
	second.mutex.Unlock()
	if confirm == nil {
		t.Fatal("replacement session did not admit the publication")
	}
	confirm(rabbitstream.TransportConfirmation{Confirmed: true})
	if calls != 1 {
		t.Fatalf("recovered confirmation callbacks = %d", calls)
	}
}

func TestProducerTransportDoesNotRetryNativeCancellationClaimedByAbort(t *testing.T) {
	var first *rabbitProducerSession
	first = newRabbitProducerSessionForTest(func(message.StreamMessage) error {
		first.Abort(context.Canceled)
		return context.Canceled
	})
	opens := 0
	transport, err := newReconnectingProducerTransport(t.Context(), func(context.Context) (producerSession, error) {
		opens++
		if opens != 1 {
			t.Error("ambiguous publication opened a replacement session")
			return nil, rabbitstream.ErrConnection
		}
		return first, nil
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = transport.Close() })
	calls := 0
	err = transport.Send(t.Context(), rabbitstream.Message{
		Stream: "tracking.events", HasPublishingID: true, PublishingID: 42,
	}, func(result rabbitstream.TransportConfirmation) {
		calls++
		if !result.Ambiguous || result.Confirmed || result.PublishingID != 42 {
			t.Errorf("retirement outcome = %+v", result)
		}
	})
	if err != nil || opens != 1 || calls != 1 || len(first.pending) != 0 {
		t.Fatalf("claimed cancellation: err=%v opens=%d callbacks=%d pending=%d", err, opens, calls, len(first.pending))
	}
}
