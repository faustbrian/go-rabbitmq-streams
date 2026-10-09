package rabbitmq

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/faustbrian/go-rabbitmq-streams/adapters/rabbitmq/v2/internal/rabbitmqstream/stream"
	"github.com/faustbrian/go-rabbitmq-streams/v2"
)

func TestFreshEnvironmentClosesPartialFailuresBeforeRetry(t *testing.T) {
	connection := rabbitstream.ConnectionConfig{
		Endpoints:      []rabbitstream.Endpoint{{Host: "localhost", Port: 5552}},
		Credentials:    rabbitstream.StaticCredentials("test", []byte("test-only")),
		Security:       rabbitstream.DevelopmentPlaintextSecurity(),
		ConnectTimeout: time.Second, RPCTimeout: time.Second,
		MaxReconnectAttempts: 2, InitialReconnectDelay: time.Nanosecond, MaxReconnectBackoff: time.Nanosecond,
	}
	partial := []*fakeRabbitEnvironment{{}, {}}
	attempts := 0
	resource, err := openFreshEnvironmentWith(context.Background(), connection, func(_ context.Context, _ *stream.EnvironmentOptions) (producerEnvironment, error) {
		if attempts > 0 && partial[attempts-1].closeCalls != 1 {
			t.Error("retry started without closing prior partial environment")
		}
		current := partial[attempts]
		attempts++
		return current, stream.AuthenticationFailure
	})
	if resource != nil || !errors.Is(err, rabbitstream.ErrAuthentication) || attempts != 2 {
		t.Fatalf("failed open returned resource=%v err=%v attempts=%d", resource, err, attempts)
	}
	for _, environment := range partial {
		if environment.closeCalls != 1 {
			t.Fatalf("partial environment closes=%d, want exactly one", environment.closeCalls)
		}
	}
	opened := &fakeRabbitEnvironment{}
	resource, err = openFreshEnvironmentWith(context.Background(), connection, func(_ context.Context, _ *stream.EnvironmentOptions) (producerEnvironment, error) { return opened, nil })
	if err != nil || resource != opened || opened.closeCalls != 0 {
		t.Fatalf("successful ownership changed: resource=%v err=%v closes=%d", resource, err, opened.closeCalls)
	}
	if err := resource.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestOpenedSessionFailureDiscardsAndClosesPartialResource(t *testing.T) {
	partial := newFakeProducerSession()
	cause := errors.New("session open failed")
	resource, err := openResourceWithinContext(context.Background(), func() (producerSession, error) { return partial, cause })
	if resource != nil || !errors.Is(err, cause) {
		t.Fatalf("partial session returned: %v, %v", resource, err)
	}
	select {
	case <-partial.closed:
	default:
		t.Fatal("partial session not closed before returning failure")
	}
}

type partialOpenCloser struct {
	close func()
}

func (resource *partialOpenCloser) Close() error { resource.close(); return nil }

func TestPartialOpenCleanupPreservesCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	closes := 0
	partial := &partialOpenCloser{close: func() { closes++; cancel() }}
	resource, err := openResourceWithinContext(ctx, func() (*partialOpenCloser, error) {
		return partial, errors.New("open failed")
	})
	if resource != nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("cleanup cancellation lost priority: %v, %v", resource, err)
	}
	if closes != 1 {
		t.Fatalf("partial resource closes=%d, want exactly one", closes)
	}
}

func TestContextAwareResourceOpeningJoinsBeforeReturning(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	started := make(chan struct{})
	release := make(chan struct{})
	returned := make(chan error, 1)
	closed := make(chan struct{})
	closes := 0
	partial := &partialOpenCloser{close: func() { closes++; close(closed) }}
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
		select {
		case <-closed:
		case <-time.After(time.Second):
			t.Error("partial opening resource did not finish cleanup")
		}
	})
	go func() {
		_, err := openResourceWithinContext(ctx, func() (*partialOpenCloser, error) {
			close(started)
			<-release
			return partial, nil
		})
		returned <- err
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		close(release)
		t.Fatal("opening did not start")
	}
	cancel()
	select {
	case <-returned:
		close(release)
		t.Fatal("resource opening returned with supplier work still active")
	case <-time.After(25 * time.Millisecond):
	}
	close(release)
	select {
	case err := <-returned:
		if !errors.Is(err, context.Canceled) || closes != 1 {
			t.Fatalf("joined cancellation: err=%v partial closes=%d", err, closes)
		}
	case <-time.After(time.Second):
		t.Fatal("completed opening did not release its caller")
	}
}
