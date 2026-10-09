package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"sync"
	"testing"
	"time"

	rabbitstream "github.com/faustbrian/go-rabbitmq-streams"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"
)

func TestReplayRejectsUnrepresentableStartBeforeAcquisition(t *testing.T) {
	for _, offset := range []uint64{math.MaxInt64, math.MaxInt64 + 1, math.MaxUint64} {
		t.Run(fmt.Sprint(offset), func(t *testing.T) {
			acquisition := errors.New("environment acquisition reached")
			opened := false
			source := &replaySource{openEnvironment: func(context.Context) (rabbitEnvironment, error) {
				opened = true
				return nil, acquisition
			}}
			end := uint64(math.MaxUint64)
			cursor, err := source.Open(t.Context(), rabbitstream.ReplayRequest{
				Start:     rabbitstream.StartPosition{Kind: rabbitstream.OffsetStartExplicit, Offset: offset},
				EndOffset: &end,
			})
			if offset <= math.MaxInt64 {
				if cursor != nil || !errors.Is(err, acquisition) || !opened {
					t.Fatal("representable start did not reach acquisition")
				}
			} else if cursor != nil || !errors.Is(err, rabbitstream.ErrReplayRange) || opened {
				t.Fatal("unrepresentable replay start reached environment acquisition")
			}
		})
	}
}

func TestSessionOpeningPreservesPermanentAndExhaustedFailures(t *testing.T) {
	t.Parallel()
	for _, permanent := range []error{rabbitstream.ErrAuthorization, rabbitstream.ErrInvalidConfiguration} {
		t.Run(permanent.Error(), func(t *testing.T) {
			connection := retryContractConnection()
			calls := 0
			laterSuccess := newFakeProducerSession()
			session, err := openSessionWithRetries(t.Context(), connection,
				func(context.Context, rabbitstream.ConnectionConfig) (producerSession, error) {
					calls++
					if calls == 1 {
						return nil, permanent
					}
					return laterSuccess, nil
				})
			if session != nil || !errors.Is(err, permanent) || calls != 1 {
				t.Fatalf("permanent failure became %#v, %v after %d attempts", session, err, calls)
			}
		})
	}
	t.Run("exhaustion preserves final cause", func(t *testing.T) {
		first := errors.New("first transport failure")
		last := errors.New("final transport failure")
		calls := 0
		session, err := openSessionWithRetries(t.Context(), retryContractConnection(),
			func(context.Context, rabbitstream.ConnectionConfig) (producerSession, error) {
				calls++
				if calls == 1 {
					return nil, first
				}
				return nil, last
			})
		if session != nil || !errors.Is(err, last) || errors.Is(err, first) || calls != 2 {
			t.Fatalf("exhausted failure became %#v, %v after %d attempts", session, err, calls)
		}
	})
}

func retryContractConnection() rabbitstream.ConnectionConfig {
	return rabbitstream.ConnectionConfig{
		Endpoints:      []rabbitstream.Endpoint{{Host: "rabbit", Port: 5552}},
		ConnectTimeout: time.Second, RPCTimeout: time.Second,
		MaxReconnectAttempts: 2, InitialReconnectDelay: time.Microsecond,
		MaxReconnectBackoff: time.Microsecond,
	}
}

func TestReplayCursorDeliversAfterNextStartsWaiting(t *testing.T) {
	t.Parallel()
	cursor := newReplayCursorForTest()
	cursor.target, cursor.end, cursor.limits = "tracking.events", 7, rabbitstream.DefaultLimits()
	parent, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	ctx := &replayWaitingContext{Context: parent, entered: make(chan struct{})}
	type result struct {
		message rabbitstream.Message
		err     error
	}
	results := make(chan result, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		message, err := cursor.Next(ctx)
		results <- result{message, err}
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Error("waiting Next did not terminate")
		}
	})
	select {
	case <-ctx.entered:
	case <-parent.Done():
		t.Fatal("Next did not begin waiting on the empty cursor")
	}
	cursor.accept(5, &amqp.Message{Data: [][]byte{[]byte("later delivery")}})
	got := <-results
	if got.err != nil || got.message.Offset != 5 || !got.message.HasOffset ||
		got.message.Stream != "tracking.events" || got.message.Partition != "tracking.events" ||
		string(got.message.Payload) != "later delivery" {
		t.Fatalf("waiting Next returned %#v, %v", got.message, got.err)
	}
	cursor.complete()
	if _, err := cursor.Next(parent); !errors.Is(err, io.EOF) {
		t.Fatalf("drained completed cursor returned %v", err)
	}
}

type replayWaitingContext struct {
	context.Context
	entered chan struct{}
	once    sync.Once
}

func (ctx *replayWaitingContext) Done() <-chan struct{} {
	ctx.once.Do(func() { close(ctx.entered) })
	return ctx.Context.Done()
}
