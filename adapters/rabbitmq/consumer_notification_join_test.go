package rabbitmq

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/faustbrian/go-rabbitmq-streams/adapters/rabbitmq/v2/internal/rabbitmqstream/stream"
	rabbitstream "github.com/faustbrian/go-rabbitmq-streams/v2"
)

type notificationJoinEnvironment struct {
	*fakeRabbitEnvironment
	closed chan struct{}
}

func (environment *notificationJoinEnvironment) Close() error {
	close(environment.closed)
	return nil
}

func TestConsumerAndReplayCloseJoinNotificationWork(t *testing.T) {
	for _, replay := range []bool{false, true} {
		name := "consumer"
		if replay {
			name = "replay"
		}
		t.Run(name, func(t *testing.T) {
			consumer := newFakeRabbitConsumer()
			consumer.closed = make(chan stream.Event)
			// The native supplier has already entered terminal notification.
			consumer.closeOnce.Do(func() {})
			environment := &notificationJoinEnvironment{
				fakeRabbitEnvironment: &fakeRabbitEnvironment{consumers: []rabbitConsumer{consumer}},
				closed:                make(chan struct{}),
			}
			var failureOwner *sync.Once
			var closeResource func() error
			if replay {
				end := uint64(1)
				source := &replaySource{
					limits:          rabbitstream.DefaultLimits(),
					openEnvironment: func(context.Context) (rabbitEnvironment, error) { return environment, nil },
				}
				cursor, err := source.Open(t.Context(), rabbitstream.ReplayRequest{Stream: "events", EndOffset: &end})
				if err != nil {
					t.Fatal(err)
				}
				owned := cursor.(*replayCursor)
				failureOwner, closeResource = &owned.failureOnce, owned.Close
			} else {
				owned := newRabbitConsumerSession(environment, rabbitstream.ConsumerConfig{Limits: rabbitstream.DefaultLimits()}, []string{"events"})
				if err := owned.open(t.Context(), []string{"events"}); err != nil {
					t.Fatal(err)
				}
				failureOwner, closeResource = &owned.failureOnce, owned.Close
			}
			entered, release, released := make(chan struct{}), make(chan struct{}), make(chan struct{})
			go func() {
				defer close(released)
				failureOwner.Do(func() { close(entered); <-release })
			}()
			<-entered
			consumer.closed <- stream.Event{Err: errors.New("terminal notification")}
			closed := make(chan error, 1)
			go func() { closed <- closeResource() }()
			select {
			case <-environment.closed:
			case <-time.After(time.Second):
				close(release)
				t.Fatal("Close did not release the environment")
			}
			returnedEarly := false
			select {
			case <-closed:
				returnedEarly = true
			case <-time.After(50 * time.Millisecond):
			}
			close(release)
			<-released
			if returnedEarly {
				t.Fatal("Close returned while owned terminal notification work remained blocked")
			}
			select {
			case err := <-closed:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(time.Second):
				t.Fatal("Close did not join released notification work")
			}
		})
	}
}
