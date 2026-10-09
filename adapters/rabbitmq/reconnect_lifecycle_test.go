package rabbitmq

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	rabbitstream "github.com/faustbrian/go-rabbitmq-streams/v2"
)

func TestTransportCloseCancelsAndJoinsReconnectOpening(t *testing.T) {
	for _, kind := range []string{"producer", "consumer"} {
		t.Run(kind, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			started := make(chan struct{})
			canceled := make(chan struct{})
			release := make(chan struct{})
			var releaseOnce sync.Once
			releaseOpen := func() { releaseOnce.Do(func() { close(release) }) }
			opened := make(chan error, 1)
			closed := make(chan error, 1)
			open := func(openCtx context.Context) {
				close(started)
				<-openCtx.Done()
				close(canceled)
				<-release
			}
			var connect func() error
			var shutdown func() error
			var closeCalls func() int
			if kind == "producer" {
				late := newFakeProducerSession()
				transport := &producerTransport{
					opener: func(openCtx context.Context) (producerSession, error) {
						open(openCtx)
						return late, nil
					},
					done: make(chan struct{}),
				}
				connect = func() error { _, err := transport.session(ctx); return err }
				shutdown = transport.Close
				closeCalls = func() int { _, count := late.calls(); return count }
			} else {
				late := &fakeConsumerSession{}
				transport := &consumerTransport{
					opener: func(openCtx context.Context, _ bool) (consumerSession, error) {
						open(openCtx)
						return late, nil
					},
					done: make(chan struct{}),
				}
				connect = func() error { _, err := transport.session(ctx); return err }
				shutdown, closeCalls = transport.Close, late.CloseCalls
			}
			go func() { opened <- connect() }()
			<-started
			t.Cleanup(func() {
				cancel()
				releaseOpen()
				select {
				case <-opened:
				case <-time.After(time.Second):
					t.Error("reconnect did not terminate during cleanup")
				}
			})
			go func() { closed <- shutdown() }()
			select {
			case <-canceled:
			case <-closed:
				t.Fatal("Close returned without canceling reconnect opening")
			case <-time.After(time.Second):
				t.Fatal("Close did not cancel reconnect opening")
			}
			select {
			case <-closed:
				t.Fatal("Close returned before reconnect opening joined")
			case <-time.After(10 * time.Millisecond):
			}
			releaseOpen()
			select {
			case err := <-closed:
				if err != nil {
					t.Fatalf("Close: %v", err)
				}
			case <-time.After(time.Second):
				t.Fatal("Close did not join finished reconnect opening")
			}
			if closeCalls() != 1 {
				t.Fatalf("late resource close calls = %d, want 1 before Close returns", closeCalls())
			}
			select {
			case err := <-opened:
				opened <- err // Preserve completion for cleanup.
				if !errors.Is(err, rabbitstream.ErrClosed) {
					t.Fatalf("reconnect result = %v, want closed", err)
				}
			case <-time.After(time.Second):
				t.Fatal("reconnect caller did not finish")
			}
		})
	}
}
