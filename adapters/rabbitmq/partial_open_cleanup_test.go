package rabbitmq

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/faustbrian/go-rabbitmq-streams"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"
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
	resource, err := openFreshEnvironmentWith(context.Background(), connection, func(*stream.EnvironmentOptions) (producerEnvironment, error) {
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
	resource, err = openFreshEnvironmentWith(context.Background(), connection, func(*stream.EnvironmentOptions) (producerEnvironment, error) { return opened, nil })
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
