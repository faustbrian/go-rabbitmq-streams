package rabbitmq

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/faustbrian/go-rabbitmq-streams/adapters/rabbitmq/v2/internal/rabbitmqstream/message"
	privatestream "github.com/faustbrian/go-rabbitmq-streams/adapters/rabbitmq/v2/internal/rabbitmqstream/stream"
	rabbitstream "github.com/faustbrian/go-rabbitmq-streams/v2"
)

func TestTransportNativeAdmissionRefusalPreservesHealthySession(t *testing.T) {
	for _, refusal := range []error{privatestream.ErrPendingPublishingID, privatestream.ErrUnconfirmedCapacity} {
		t.Run(refusal.Error(), func(t *testing.T) {
			var original message.StreamMessage
			writes, attempts := 0, 0
			session := newRabbitProducerSessionForTest(func(wire message.StreamMessage) error {
				attempts++
				if original != nil {
					return refusal
				}
				original = wire
				writes++
				return nil
			})
			var opens atomic.Int32
			transport, err := newReconnectingProducerTransport(t.Context(), func(context.Context) (producerSession, error) {
				if opens.Add(1) != 1 {
					return nil, rabbitstream.ErrConnection
				}
				return session, nil
			}, nil)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = transport.Close() })
			outbound := rabbitstream.Message{Stream: "events", Payload: []byte("event"), HasPublishingID: true, PublishingID: 41}
			if err = outbound.Validate(rabbitstream.DefaultLimits()); err != nil {
				t.Fatal(err)
			}
			confirmations := make(chan rabbitstream.TransportConfirmation, 2)
			if err = transport.Send(t.Context(), outbound, func(result rabbitstream.TransportConfirmation) { confirmations <- result }); err != nil {
				t.Fatal(err)
			}
			owner := session.pending[original]
			if owner == nil || !owner.admitted {
				t.Fatal("first publication was not admitted")
			}
			err = transport.Send(t.Context(), outbound, func(rabbitstream.TransportConfirmation) { t.Error("refused publication received a callback") })
			if !errors.Is(err, rabbitstream.ErrValidation) {
				t.Errorf("deterministic native refusal = %v, want caller validation", err)
			}
			transport.mutex.Lock()
			current := transport.current
			transport.mutex.Unlock()
			if opens.Load() != 1 || current != session || attempts != 2 || writes != 1 {
				t.Errorf("refusal disturbed session: opens=%d current=%t attempts=%d writes=%d", opens.Load(), current == session, attempts, writes)
			}
			if len(session.pending) != 1 || session.pending[original] != owner || session.aborted {
				t.Error("original pending confirmation ownership was not retained")
			}
			select {
			case <-confirmations:
				t.Error("pre-write refusal changed the original publication outcome")
			default:
			}
		})
	}
}

type retiringProducerSession struct {
	*fakeProducerSession
	started   chan struct{}
	release   chan struct{}
	finished  chan struct{}
	closeOnce sync.Once
	closes    atomic.Int32
}

func (session *retiringProducerSession) Close() error {
	session.closes.Add(1)
	session.closeOnce.Do(func() {
		close(session.started)
		<-session.release
		close(session.finished)
	})
	return rabbitstream.ErrAuthorization
}

func TestTransportCloseJoinsRetiredProducerSession(t *testing.T) {
	for _, origin := range []string{"failure watcher", "send refusal"} {
		t.Run(origin, func(t *testing.T) {
			session := &retiringProducerSession{
				fakeProducerSession: newFakeProducerSession(),
				started:             make(chan struct{}), release: make(chan struct{}), finished: make(chan struct{}),
			}
			session.sendErr = errProducerSessionClosed
			transport, err := newReconnectingProducerTransport(t.Context(), func(context.Context) (producerSession, error) { return session, nil }, nil)
			if err != nil {
				t.Fatal(err)
			}
			var releaseOnce sync.Once
			release := func() { releaseOnce.Do(func() { close(session.release) }) }
			closeFinished := make(chan struct{})
			var closeErr error
			var sendFinished chan struct{}
			t.Cleanup(func() {
				release()
				_ = transport.Close()
				if sendFinished != nil {
					select {
					case <-sendFinished:
					case <-time.After(time.Second):
						t.Error("retiring send did not terminate")
					}
				}
			})
			if origin == "failure watcher" {
				session.failures <- rabbitstream.ErrConnection
			} else {
				sendFinished = make(chan struct{})
				go func() {
					_ = transport.Send(t.Context(), rabbitstream.Message{Stream: "events"}, func(rabbitstream.TransportConfirmation) {})
					close(sendFinished)
				}()
			}
			select {
			case <-session.started:
			case <-time.After(time.Second):
				t.Fatal("retirement did not reach its cleanup barrier")
			}
			go func() { closeErr = transport.Close(); close(closeFinished) }()
			<-transport.done
			select {
			case <-closeFinished:
				t.Error("Close completed while retired session cleanup remained blocked")
			case <-time.After(20 * time.Millisecond):
			}
			release()
			select {
			case <-closeFinished:
			case <-time.After(time.Second):
				t.Fatal("Close did not join released retirement")
			}
			if !errors.Is(closeErr, rabbitstream.ErrAuthorization) || session.closes.Load() != 1 {
				t.Errorf("retired cleanup result=%v calls=%d, want original error and one close", closeErr, session.closes.Load())
			}
			select {
			case <-session.finished:
			default:
				t.Error("retired cleanup did not finish before Close returned")
			}
		})
	}
}
