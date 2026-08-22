package rabbitmq

import (
	"context"
	"errors"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/faustbrian/golib/pkg/rabbitstream"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"
)

func TestStoredStartResolvesTheBrokerOffsetBeforeOpeningTheConsumer(t *testing.T) {
	t.Parallel()

	environment := &fakeRabbitEnvironment{
		queryOffset: 41,
		consumers:   []rabbitConsumer{newFakeRabbitConsumer()},
	}
	config, err := (rabbitstream.ConsumerConfig{
		Stream: "tracking.events", ConsumerName: "tracking-indexer",
	}).Normalized()
	if err != nil {
		t.Fatalf("normalize consumer config: %v", err)
	}
	config.Start = rabbitstream.StartPosition{Kind: rabbitstream.OffsetStartStored}
	session := newRabbitConsumerSession(environment, config, []string{config.Stream})
	t.Cleanup(func() { _ = session.Close() })

	if err := session.open([]string{config.Stream}); err != nil {
		t.Fatalf("open stored-offset consumer: %v", err)
	}
	if environment.queryOffsetCalls != 1 {
		t.Fatalf("broker offset queries = %d, want 1", environment.queryOffsetCalls)
	}
	if got := environment.consumerOptions[0].Offset.String(); got != "offset, value: 41" {
		t.Fatalf("consumer offset = %q, want explicit stored offset", got)
	}
}

func TestStoredStartHandlesMissingAndFailedBrokerOffsets(t *testing.T) {
	t.Parallel()

	config, err := (rabbitstream.ConsumerConfig{
		Stream: "tracking.events", ConsumerName: "tracking-indexer",
	}).Normalized()
	if err != nil {
		t.Fatalf("normalize consumer config: %v", err)
	}
	config.Start = rabbitstream.StartPosition{Kind: rabbitstream.OffsetStartStored}

	missing := newRabbitConsumerSession(
		&fakeRabbitEnvironment{queryOffsetErr: stream.OffsetNotFoundError},
		config,
		[]string{config.Stream},
	)
	offset, err := missing.startOffset(config.Stream)
	if err != nil || offset.String() != "first" {
		t.Fatalf("missing stored offset = %q, %v; want first", offset.String(), err)
	}

	want := errors.New("query stored offset")
	failed := newRabbitConsumerSession(
		&fakeRabbitEnvironment{queryOffsetErr: want},
		config,
		[]string{config.Stream},
	)
	if _, err := failed.startOffset(config.Stream); !errors.Is(err, want) {
		t.Fatalf("failed stored offset query = %v, want %v", err, want)
	}
}

func TestConsumerTransportCoalescesConcurrentReconnect(t *testing.T) {
	t.Parallel()

	first := &fakeConsumerSession{
		nextErr:     rabbitstream.ErrConnection,
		nextCalled:  make(chan struct{}, 2),
		nextRelease: make(chan struct{}),
	}
	second := &fakeConsumerSession{messages: make(chan rabbitstream.Message, 2)}
	second.messages <- rabbitstream.Message{Stream: "tracking.events", Offset: 1}
	second.messages <- rabbitstream.Message{Stream: "tracking.events", Offset: 2}
	reconnectStarted := make(chan struct{})
	releaseReconnect := make(chan struct{})
	observations := make(observationChannel, 3)
	var mutex sync.Mutex
	openCalls := 0
	transport, err := newReconnectingConsumerTransport(
		context.Background(),
		func(context.Context, bool) (consumerSession, error) {
			mutex.Lock()
			openCalls++
			call := openCalls
			mutex.Unlock()
			if call == 1 {
				return first, nil
			}
			close(reconnectStarted)
			<-releaseReconnect
			return second, nil
		},
		observations,
	)
	if err != nil {
		t.Fatalf("newReconnectingConsumerTransport() error = %v", err)
	}

	results := make(chan rabbitstream.Message, 2)
	errorsChannel := make(chan error, 2)
	for range 2 {
		go func() {
			message, nextErr := transport.Next(context.Background())
			results <- message
			errorsChannel <- nextErr
		}()
	}
	<-first.nextCalled
	<-first.nextCalled
	close(first.nextRelease)
	<-reconnectStarted
	close(releaseReconnect)
	for range 2 {
		if err := <-errorsChannel; err != nil {
			t.Fatalf("Next() error = %v", err)
		}
	}
	firstResult, secondResult := <-results, <-results
	if firstResult.Offset == secondResult.Offset {
		t.Fatalf("reconnected results = %#v and %#v", firstResult, secondResult)
	}
	mutex.Lock()
	calls := openCalls
	mutex.Unlock()
	if calls != 2 || first.CloseCalls() != 1 {
		t.Fatalf("open calls = %d, first close calls = %d", calls, first.CloseCalls())
	}
	transport.mutex.Lock()
	current, lastErr, retryAfter := transport.current, transport.lastErr, transport.retryAfter
	transport.mutex.Unlock()
	if current != second || lastErr != nil || !retryAfter.IsZero() {
		t.Fatalf("reconnected transport current = %#v, last error = %v, retry after = %v", current, lastErr, retryAfter)
	}
	for _, expected := range []rabbitstream.ObservationKind{
		rabbitstream.ObservationConnectionLost,
		rabbitstream.ObservationReconnectAttempt,
		rabbitstream.ObservationConnectionReady,
	} {
		select {
		case observation := <-observations:
			if observation.Kind != expected {
				t.Fatalf("reconnect observation = %q, want %q", observation.Kind, expected)
			}
		case <-time.After(time.Second):
			t.Fatalf("timed out waiting for reconnect observation %q", expected)
		}
	}
}

func TestConsumerTransportRetriesTransientReconnectOpenFailuresWithinBudget(t *testing.T) {
	t.Parallel()

	first := &fakeConsumerSession{nextErr: rabbitstream.ErrConnection}
	recovered := &fakeConsumerSession{messages: make(chan rabbitstream.Message, 1)}
	recovered.messages <- rabbitstream.Message{Stream: "tracking.events", Offset: 41}
	openCalls := 0
	transport, err := newReconnectingConsumerTransport(
		context.Background(),
		func(context.Context, bool) (consumerSession, error) {
			openCalls++
			switch openCalls {
			case 1:
				return first, nil
			case 2, 3:
				return nil, rabbitstream.ErrConnection
			default:
				return recovered, nil
			}
		},
		nil,
	)
	if err != nil {
		t.Fatalf("newReconnectingConsumerTransport() error = %v", err)
	}
	transport.retryDelay = 0

	message, err := transport.Next(context.Background())
	if err != nil || message.Offset != 41 {
		t.Fatalf("Next() after transient reconnect failures = %#v, %v", message, err)
	}
	if openCalls != 4 || first.CloseCalls() != 1 {
		t.Fatalf("open calls = %d, initial session closes = %d", openCalls, first.CloseCalls())
	}
}

func TestConsumerTransportBoundsAndCancelsReconnectOpenRetries(t *testing.T) {
	t.Parallel()

	initial := &fakeConsumerSession{nextErr: rabbitstream.ErrConnection}
	openCalls := 0
	transport, err := newReconnectingConsumerTransport(
		context.Background(),
		func(context.Context, bool) (consumerSession, error) {
			openCalls++
			if openCalls == 1 {
				return initial, nil
			}
			return nil, rabbitstream.ErrConnection
		},
		nil,
	)
	if err != nil {
		t.Fatalf("newReconnectingConsumerTransport() error = %v", err)
	}
	transport.maxReconnectAttempts = 2
	transport.retryDelay = 0
	if _, err := transport.Next(context.Background()); !errors.Is(err, rabbitstream.ErrConnection) {
		t.Fatalf("bounded Next() error = %v", err)
	}
	if openCalls != 3 {
		t.Fatalf("bounded open calls = %d, want one initial and two reconnect attempts", openCalls)
	}

	initial = &fakeConsumerSession{nextErr: rabbitstream.ErrConnection}
	firstFailure := make(chan struct{})
	releaseFailure := make(chan struct{})
	openCalls = 0
	transport, err = newReconnectingConsumerTransport(
		context.Background(),
		func(context.Context, bool) (consumerSession, error) {
			openCalls++
			if openCalls == 1 {
				return initial, nil
			}
			close(firstFailure)
			<-releaseFailure
			return nil, rabbitstream.ErrConnection
		},
		nil,
	)
	if err != nil {
		t.Fatalf("newReconnectingConsumerTransport() error = %v", err)
	}
	transport.retryDelay = 0
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, nextErr := transport.Next(ctx)
		done <- nextErr
	}()
	<-firstFailure
	transport.mutex.Lock()
	transport.retryDelay = time.Hour
	transport.mutex.Unlock()
	close(releaseFailure)
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled reconnect Next() error = %v", err)
	}
	if openCalls != 2 {
		t.Fatalf("canceled open calls = %d", openCalls)
	}
}

func TestConsumerTransportDoesNotRetryTerminalReconnectOpenFailure(t *testing.T) {
	t.Parallel()

	initial := &fakeConsumerSession{nextErr: rabbitstream.ErrConnection}
	openCalls := 0
	transport, err := newReconnectingConsumerTransport(
		context.Background(),
		func(context.Context, bool) (consumerSession, error) {
			openCalls++
			if openCalls == 1 {
				return initial, nil
			}
			return nil, rabbitstream.ErrAuthentication
		},
		nil,
	)
	if err != nil {
		t.Fatalf("newReconnectingConsumerTransport() error = %v", err)
	}
	transport.retryDelay = 0
	if _, err := transport.Next(context.Background()); !errors.Is(err, rabbitstream.ErrAuthentication) {
		t.Fatalf("terminal reconnect Next() error = %v", err)
	}
	if openCalls != 2 {
		t.Fatalf("terminal open calls = %d", openCalls)
	}
}

func TestConsumerTransportReconnectWaitersHonorCancellation(t *testing.T) {
	t.Parallel()

	first := &fakeConsumerSession{
		nextErr:     rabbitstream.ErrConnection,
		nextCalled:  make(chan struct{}, 4),
		nextRelease: make(chan struct{}),
	}
	reconnectStarted := make(chan struct{})
	var reconnectOnce sync.Once
	var mutex sync.Mutex
	openCalls := 0
	transport, err := newReconnectingConsumerTransport(
		context.Background(),
		func(ctx context.Context, _ bool) (consumerSession, error) {
			mutex.Lock()
			openCalls++
			call := openCalls
			mutex.Unlock()
			if call == 1 {
				return first, nil
			}
			reconnectOnce.Do(func() { close(reconnectStarted) })
			<-ctx.Done()
			return nil, ctx.Err()
		},
		nil,
	)
	if err != nil {
		t.Fatalf("newReconnectingConsumerTransport() error = %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 4)
	for range 4 {
		go func() {
			_, nextErr := transport.Next(ctx)
			done <- nextErr
		}()
	}
	for range 4 {
		<-first.nextCalled
	}
	close(first.nextRelease)
	<-reconnectStarted
	cancel()
	for range 4 {
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Fatalf("Next() error = %v", err)
		}
	}
	mutex.Lock()
	calls := openCalls
	mutex.Unlock()
	if calls != 2 {
		t.Fatalf("open calls = %d, want one initial and one coalesced reconnect", calls)
	}
}

func TestConsumerTransportRejectsInvalidConstructionAndOpenFailure(t *testing.T) {
	t.Parallel()

	validOpener := func(context.Context, bool) (consumerSession, error) { return &fakeConsumerSession{}, nil }
	var nilContext context.Context
	if transport, err := newReconnectingConsumerTransport(nilContext, validOpener, nil); transport != nil || !errors.Is(err, rabbitstream.ErrInvalidConfiguration) {
		t.Fatalf("nil-context construction = %#v, %v", transport, err)
	}
	if transport, err := newReconnectingConsumerTransport(context.Background(), nil, nil); transport != nil || !errors.Is(err, rabbitstream.ErrInvalidConfiguration) {
		t.Fatalf("nil-opener construction = %#v, %v", transport, err)
	}
	want := errors.New("open consumer")
	transport, err := newReconnectingConsumerTransport(
		context.Background(),
		func(context.Context, bool) (consumerSession, error) { return nil, want },
		nil,
	)
	if transport != nil || !errors.Is(err, want) {
		t.Fatalf("failed construction = %#v, %v", transport, err)
	}
}

func TestFinishOpenConsumerClosesTransportWhenCorePolicyRejectsConfig(t *testing.T) {
	t.Parallel()

	session := &fakeConsumerSession{}
	transport, err := newReconnectingConsumerTransport(
		context.Background(),
		func(context.Context, bool) (consumerSession, error) { return session, nil },
		nil,
	)
	if err != nil {
		t.Fatalf("newReconnectingConsumerTransport() error = %v", err)
	}
	if consumer, err := finishOpenConsumer(rabbitstream.ConsumerConfig{}, transport); consumer != nil || !errors.Is(err, rabbitstream.ErrInvalidConfiguration) {
		t.Fatalf("finishOpenConsumer() = %#v, %v", consumer, err)
	}
	if session.CloseCalls() != 1 {
		t.Fatalf("transport close calls = %d", session.CloseCalls())
	}
}

func TestConsumerTransportKeepsTerminalNextErrorsOnTheCurrentSession(t *testing.T) {
	for _, terminal := range []error{rabbitstream.ErrClosed, context.Canceled} {
		session := &fakeConsumerSession{nextErr: terminal}
		transport, err := newReconnectingConsumerTransport(
			context.Background(),
			func(context.Context, bool) (consumerSession, error) { return session, nil },
			nil,
		)
		if err != nil {
			t.Fatalf("newReconnectingConsumerTransport() error = %v", err)
		}
		ctx := context.Background()
		if errors.Is(terminal, context.Canceled) {
			var cancel context.CancelFunc
			ctx, cancel = context.WithCancel(ctx)
			cancel()
		}
		if _, err := transport.Next(ctx); !errors.Is(err, terminal) {
			t.Fatalf("Next() error = %v, want %v", err, terminal)
		}
		if session.CloseCalls() != 0 {
			t.Fatalf("terminal Next() closed session %d times", session.CloseCalls())
		}
		_ = transport.Close()
	}
}

func TestConsumerTransportReturnsSuccessfulNextWithoutReconnect(t *testing.T) {
	session := &fakeConsumerSession{messages: make(chan rabbitstream.Message, 1)}
	session.messages <- rabbitstream.Message{Stream: "tracking.events", Offset: 41}
	openCalls := 0
	transport, err := newReconnectingConsumerTransport(
		context.Background(),
		func(context.Context, bool) (consumerSession, error) {
			openCalls++
			return session, nil
		},
		nil,
	)
	if err != nil {
		t.Fatalf("newReconnectingConsumerTransport() error = %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()
	message, err := transport.Next(ctx)
	if err != nil || message.Offset != 41 {
		t.Fatalf("Next() = %#v, %v", message, err)
	}
	if openCalls != 1 || session.CloseCalls() != 0 {
		t.Fatalf("successful Next() opened %d sessions and closed %d", openCalls, session.CloseCalls())
	}
	_ = transport.Close()
}

func TestConsumerTransportStoreOffsetRetriesOnlyConnectionFailures(t *testing.T) {
	first := &fakeConsumerSession{storeErr: rabbitstream.ErrConnection}
	second := &fakeConsumerSession{}
	sessions := make(chan consumerSession, 2)
	sessions <- first
	sessions <- second
	transport, err := newReconnectingConsumerTransport(
		context.Background(),
		func(context.Context, bool) (consumerSession, error) { return <-sessions, nil },
		nil,
	)
	if err != nil {
		t.Fatalf("newReconnectingConsumerTransport() error = %v", err)
	}
	if err := transport.StoreOffset(context.Background(), "tracking.events", 41); err != nil {
		t.Fatalf("StoreOffset() error = %v", err)
	}
	if first.CloseCalls() != 1 || second.StoreCalls() != 1 {
		t.Fatalf("first closes = %d, second stores = %d", first.CloseCalls(), second.StoreCalls())
	}
	_ = transport.Close()

	for _, terminal := range []error{rabbitstream.ErrOffset, rabbitstream.ErrPartitionUnavailable} {
		session := &fakeConsumerSession{storeErr: terminal}
		transport, err := newReconnectingConsumerTransport(
			context.Background(),
			func(context.Context, bool) (consumerSession, error) { return session, nil },
			nil,
		)
		if err != nil {
			t.Fatalf("newReconnectingConsumerTransport() error = %v", err)
		}
		if err := transport.StoreOffset(context.Background(), "tracking.events", 41); !errors.Is(err, terminal) {
			t.Fatalf("StoreOffset() error = %v, want %v", err, terminal)
		}
		if session.CloseCalls() != 0 {
			t.Fatalf("terminal StoreOffset() closed session %d times", session.CloseCalls())
		}
		_ = transport.Close()
	}
}

func TestConsumerTransportBoundsConsecutiveOffsetReconnects(t *testing.T) {
	first := &fakeConsumerSession{storeErr: rabbitstream.ErrConnection}
	second := &fakeConsumerSession{storeErr: rabbitstream.ErrConnection}
	third := &fakeConsumerSession{}
	sessions := make(chan consumerSession, 3)
	sessions <- first
	sessions <- second
	sessions <- third
	openCalls := 0
	transport, err := newReconnectingConsumerTransport(
		context.Background(),
		func(context.Context, bool) (consumerSession, error) {
			openCalls++
			return <-sessions, nil
		},
		nil,
	)
	if err != nil {
		t.Fatalf("newReconnectingConsumerTransport() error = %v", err)
	}
	if err := transport.StoreOffset(context.Background(), "tracking.events", 41); !errors.Is(err, rabbitstream.ErrConnection) {
		t.Fatalf("StoreOffset() error = %v", err)
	}
	if first.CloseCalls() != 1 || second.CloseCalls() != 1 {
		t.Fatalf("closed sessions = %d, %d", first.CloseCalls(), second.CloseCalls())
	}
	if openCalls != 2 {
		t.Fatalf("consumer session opens = %d, want 2", openCalls)
	}
}

func TestConsumerTransportCloseIsIdempotentAndPreservesCloseError(t *testing.T) {
	t.Parallel()

	session := &fakeConsumerSession{closeErr: errors.New("close consumer")}
	transport, err := newReconnectingConsumerTransport(
		context.Background(),
		func(context.Context, bool) (consumerSession, error) { return session, nil },
		nil,
	)
	if err != nil {
		t.Fatalf("newReconnectingConsumerTransport() error = %v", err)
	}
	if err := transport.Close(); !errors.Is(err, session.closeErr) {
		t.Fatalf("first Close() error = %v", err)
	}
	if err := transport.Close(); !errors.Is(err, session.closeErr) {
		t.Fatalf("second Close() error = %v", err)
	}
	if _, err := transport.Next(context.Background()); !errors.Is(err, rabbitstream.ErrClosed) {
		t.Fatalf("closed Next() error = %v", err)
	}
	if err := transport.StoreOffset(context.Background(), "tracking.events", 1); !errors.Is(err, rabbitstream.ErrClosed) {
		t.Fatalf("closed StoreOffset() error = %v", err)
	}
}

func TestConsumerTransportReconnectWaitAndCloseTransitions(t *testing.T) {
	t.Parallel()

	first := &fakeConsumerSession{}
	reconnectStarted := make(chan struct{})
	releaseReconnect := make(chan struct{})
	second := &fakeConsumerSession{}
	openCalls := 0
	transport, err := newReconnectingConsumerTransport(
		context.Background(),
		func(context.Context, bool) (consumerSession, error) {
			openCalls++
			if openCalls == 1 {
				return first, nil
			}
			close(reconnectStarted)
			<-releaseReconnect
			return second, nil
		},
		nil,
	)
	if err != nil {
		t.Fatalf("newReconnectingConsumerTransport() error = %v", err)
	}
	transport.retryDelay = 0
	transport.invalidate(first, rabbitstream.ErrConnection)
	if first.CloseCalls() != 1 {
		t.Fatalf("invalidated closes = %d", first.CloseCalls())
	}
	reconnected := make(chan error, 1)
	go func() {
		_, sessionErr := transport.session(context.Background())
		reconnected <- sessionErr
	}()
	<-reconnectStarted
	if err := transport.Close(); err != nil {
		t.Fatalf("Close() during reconnect error = %v", err)
	}
	close(releaseReconnect)
	if err := <-reconnected; !errors.Is(err, rabbitstream.ErrClosed) {
		t.Fatalf("reconnect-after-close error = %v", err)
	}
	if second.CloseCalls() != 1 {
		t.Fatalf("post-close session closes = %d", second.CloseCalls())
	}
	transport.mutex.Lock()
	closedCurrent := transport.current
	transport.mutex.Unlock()
	if closedCurrent != nil {
		t.Fatalf("closed reconnect retained current session = %#v", closedCurrent)
	}

	waiting := &consumerTransport{reconnecting: make(chan struct{}), done: make(chan struct{})}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := waiting.session(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("reconnect waiter error = %v", err)
	}

	done := make(chan struct{})
	close(done)
	woke := &consumerTransport{reconnecting: done, closed: true, done: make(chan struct{})}
	if _, err := woke.session(context.Background()); !errors.Is(err, rabbitstream.ErrClosed) {
		t.Fatalf("completed reconnect waiter error = %v", err)
	}
	backoff := &consumerTransport{
		lastErr: rabbitstream.ErrConnection, retryAfter: time.Now().Add(time.Millisecond),
		opener: func(context.Context, bool) (consumerSession, error) { return nil, rabbitstream.ErrConnection },
		done:   make(chan struct{}),
	}
	if _, err := backoff.session(context.Background()); !errors.Is(err, rabbitstream.ErrConnection) {
		t.Fatalf("completed backoff error = %v", err)
	}
}

func TestConsumerTransportCurrentSessionInvalidationChangesStateBeforeCleanup(t *testing.T) {
	t.Parallel()

	current := &fakeConsumerSession{}
	transport := &consumerTransport{current: current, retryDelay: time.Millisecond, done: make(chan struct{})}
	transport.invalidate(current, rabbitstream.ErrConnection)
	transport.mutex.Lock()
	retained, lastErr, retryAfter := transport.current, transport.lastErr, transport.retryAfter
	transport.mutex.Unlock()
	if retained != nil || !errors.Is(lastErr, rabbitstream.ErrConnection) || retryAfter.IsZero() {
		t.Fatalf("invalidated state = %#v, %v, %v", retained, lastErr, retryAfter)
	}
}

func TestConsumerTransportReconnectFailurePreservesRetryState(t *testing.T) {
	t.Parallel()

	current := &fakeConsumerSession{}
	want := errors.New("reconnect failed")
	openCalls := 0
	transport, err := newReconnectingConsumerTransport(
		context.Background(),
		func(context.Context, bool) (consumerSession, error) {
			openCalls++
			if openCalls == 1 {
				return current, nil
			}
			return nil, want
		},
		nil,
	)
	if err != nil {
		t.Fatalf("newReconnectingConsumerTransport() error = %v", err)
	}
	transport.maxReconnectAttempts = 1
	transport.retryDelay = 0
	transport.invalidate(current, rabbitstream.ErrConnection)
	if _, err := transport.session(context.Background()); !errors.Is(err, want) {
		t.Fatalf("reconnect session error = %v", err)
	}
	transport.mutex.Lock()
	storedErr, retryAfter := transport.lastErr, transport.retryAfter
	transport.mutex.Unlock()
	if !errors.Is(storedErr, want) || retryAfter.IsZero() {
		t.Fatalf("failed reconnect state = %v, %v", storedErr, retryAfter)
	}
}

func TestConsumerTransportWaitersObserveReconnectCompletionAndCancellation(t *testing.T) {
	t.Parallel()

	current := &fakeConsumerSession{}
	reconnectDone := make(chan struct{})
	waiting := &consumerTransport{reconnecting: reconnectDone, done: make(chan struct{})}
	ctx := newObservedContext()
	result := make(chan error, 1)
	go func() {
		_, err := waiting.session(ctx)
		result <- err
	}()
	<-ctx.observed
	close(ctx.done)
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled reconnect waiter error = %v", err)
	}

	reconnectDone = make(chan struct{})
	waiting = &consumerTransport{reconnecting: reconnectDone, done: make(chan struct{})}
	ctx = newObservedContext()
	sessionResult := make(chan consumerSession, 1)
	go func() {
		session, _ := waiting.session(ctx)
		sessionResult <- session
	}()
	<-ctx.observed
	waiting.mutex.Lock()
	waiting.reconnecting = nil
	waiting.current = current
	waiting.mutex.Unlock()
	close(reconnectDone)
	if session := <-sessionResult; session != current {
		t.Fatalf("completed reconnect session = %#v", session)
	}

	backoff := &consumerTransport{
		lastErr: rabbitstream.ErrConnection, retryAfter: time.Now().Add(time.Hour), done: make(chan struct{}),
	}
	ctx = newObservedContext()
	go func() {
		_, err := backoff.session(ctx)
		result <- err
	}()
	<-ctx.observed
	close(ctx.done)
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled backoff error = %v", err)
	}
}

func TestRabbitConsumerSessionValidatesDeliveryIdentityAndBounds(t *testing.T) {
	limits := rabbitstream.DefaultLimits()
	valid := rabbitstream.Message{Stream: "tracking.events", Partition: "tracking.events", Offset: 1, HasOffset: true}
	if err := validateDelivery(valid, limits); err != nil {
		t.Fatalf("valid delivery error = %v", err)
	}
	for name, delivery := range map[string]rabbitstream.Message{
		"missing partition":   {Stream: "tracking.events", HasOffset: true},
		"different partition": {Stream: "tracking.events", Partition: "tracking-0", HasOffset: true},
		"missing offset":      {Stream: "tracking.events", Partition: "tracking.events"},
	} {
		if err := validateDelivery(delivery, limits); !errors.Is(err, rabbitstream.ErrValidation) {
			t.Fatalf("%s delivery error = %v", name, err)
		}
	}
	invalidPayload := valid
	invalidPayload.Payload = make([]byte, limits.MaxPayloadBytes+1)
	if err := validateDelivery(invalidPayload, limits); !errors.Is(err, rabbitstream.ErrValidation) {
		t.Fatalf("oversized delivery error = %v", err)
	}
}

func TestRabbitConsumerSessionReportsEveryTerminalState(t *testing.T) {
	queued := newRabbitConsumerSessionForTest()
	queued.messages <- rabbitstream.Message{Stream: "tracking.events", Offset: 4}
	if message, err := queued.Next(context.Background()); err != nil || message.Offset != 4 {
		t.Fatalf("queued Next() = %#v, %v", message, err)
	}

	failed := newRabbitConsumerSessionForTest()
	failed.reportFailure(nil)
	failed.reportFailure(rabbitstream.ErrReplayRange)
	if _, err := failed.Next(context.Background()); err == nil || err.Error() != "consumer transport closed unexpectedly" {
		t.Fatalf("failed Next() error = %v", err)
	}

	closed := newRabbitConsumerSessionForTest()
	close(closed.done)
	if _, err := closed.Next(context.Background()); !errors.Is(err, rabbitstream.ErrClosed) {
		t.Fatalf("closed Next() error = %v", err)
	}

	canceled := newRabbitConsumerSessionForTest()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := canceled.Next(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled Next() error = %v", err)
	}
}

func TestRabbitConsumerSessionAcceptsOnlyValidActiveDeliveries(t *testing.T) {
	valid := newRabbitConsumerSessionForTest()
	valid.accept("tracking.events", 4, &amqp.Message{Data: [][]byte{[]byte("event")}})
	if message := <-valid.messages; message.Offset != 4 || message.Partition != "tracking.events" || string(message.Payload) != "event" {
		t.Fatalf("accepted delivery = %#v", message)
	}

	negative := newRabbitConsumerSessionForTest()
	negative.accept("tracking.events", -1, &amqp.Message{})
	if _, err := negative.Next(context.Background()); err == nil || err.Error() != "consumer returned a negative offset" {
		t.Fatalf("negative-offset error = %v", err)
	}

	invalid := newRabbitConsumerSessionForTest()
	invalid.accept("tracking.events", 1, &amqp.Message{Data: [][]byte{[]byte("one"), []byte("two")}})
	if _, err := invalid.Next(context.Background()); !errors.Is(err, rabbitstream.ErrValidation) {
		t.Fatalf("invalid-wire error = %v", err)
	}

	oversized := newRabbitConsumerSessionForTest()
	oversized.accept("tracking.events", 1, &amqp.Message{
		Data: [][]byte{make([]byte, oversized.config.Limits.MaxPayloadBytes+1)},
	})
	if _, err := oversized.Next(context.Background()); !errors.Is(err, rabbitstream.ErrMessageTooLarge) {
		t.Fatalf("oversized-delivery error = %v", err)
	}

	failed := newRabbitConsumerSessionForTest()
	failed.reportFailure(rabbitstream.ErrConnection)
	failed.accept("tracking.events", 1, &amqp.Message{})
	closed := newRabbitConsumerSessionForTest()
	close(closed.done)
	closed.accept("tracking.events", 1, &amqp.Message{})
}

func TestDeliveryAdmissionStopsOnEitherTerminalState(t *testing.T) {
	t.Parallel()

	failed := make(chan struct{})
	done := make(chan struct{})
	close(failed)
	if deliverUntilTerminal(nil, failed, done, rabbitstream.Message{}) {
		t.Fatal("delivery was admitted after failure")
	}
	failed = make(chan struct{})
	close(done)
	if deliverUntilTerminal(nil, failed, done, rabbitstream.Message{}) {
		t.Fatal("delivery was admitted after closure")
	}
}

func TestRabbitConsumerSessionStoresOnlyOwnedBoundedOffsets(t *testing.T) {
	session := newRabbitConsumerSessionForTest()
	stored := int64(-1)
	session.partitions["tracking.events"] = struct{}{}
	session.stores["tracking.events"] = func(offset int64) error { stored = offset; return nil }
	if err := session.StoreOffset(context.Background(), "tracking.events", 41); err != nil || stored != 41 {
		t.Fatalf("StoreOffset() = %v, stored %d", err, stored)
	}
	if err := session.StoreOffset(context.Background(), "tracking.events", math.MaxInt64); err != nil || stored != math.MaxInt64 {
		t.Fatalf("maximum StoreOffset() = %v, stored %d", err, stored)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := session.StoreOffset(ctx, "tracking.events", 42); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled StoreOffset() error = %v", err)
	}
	if err := session.StoreOffset(context.Background(), "unknown", 1); !errors.Is(err, rabbitstream.ErrOffset) {
		t.Fatalf("unknown StoreOffset() error = %v", err)
	}
	if err := session.StoreOffset(context.Background(), "tracking.events", uint64(math.MaxInt64)+1); !errors.Is(err, rabbitstream.ErrOffset) {
		t.Fatalf("overflow StoreOffset() error = %v", err)
	}
	delete(session.stores, "tracking.events")
	if err := session.StoreOffset(context.Background(), "tracking.events", 1); !errors.Is(err, rabbitstream.ErrPartitionUnavailable) {
		t.Fatalf("unavailable StoreOffset() error = %v", err)
	}
	session.stores["tracking.events"] = func(int64) error { return rabbitstream.ErrOffset }
	if err := session.StoreOffset(context.Background(), "tracking.events", 1); !errors.Is(err, rabbitstream.ErrOffset) {
		t.Fatalf("broker StoreOffset() error = %v", err)
	}
}

func TestRabbitConsumerSessionCloseNormalizesOwnedResourceFailures(t *testing.T) {
	consumerFailure := newRabbitConsumerSessionForTest()
	consumerFailure.closers = []func() error{
		func() error { return errors.New("first close") },
		func() error { return errors.New("second close") },
	}
	consumerFailure.closeEnv = func() error { return errors.New("environment close") }
	if err := consumerFailure.Close(); err == nil || err.Error() != "first close" {
		t.Fatalf("consumer Close() error = %v", err)
	}
	if err := consumerFailure.Close(); err == nil || err.Error() != "first close" {
		t.Fatalf("idempotent consumer Close() error = %v", err)
	}

	environmentFailure := newRabbitConsumerSessionForTest()
	environmentFailure.closers = []func() error{func() error { return stream.AlreadyClosed }}
	environmentFailure.closeEnv = func() error { return errors.New("environment close") }
	if err := environmentFailure.Close(); err == nil || err.Error() != "environment close" {
		t.Fatalf("environment Close() error = %v", err)
	}
}

func newRabbitConsumerSessionForTest() *rabbitConsumerSession {
	return &rabbitConsumerSession{
		config:     rabbitstream.ConsumerConfig{Limits: rabbitstream.DefaultLimits()},
		partitions: make(map[string]struct{}),
		stores:     make(map[string]func(int64) error),
		closeEnv:   func() error { return nil },
		messages:   make(chan rabbitstream.Message, 1),
		done:       make(chan struct{}),
		failed:     make(chan struct{}),
	}
}

type fakeConsumerSession struct {
	messages    chan rabbitstream.Message
	nextErr     error
	nextCalled  chan struct{}
	nextRelease chan struct{}
	storeErr    error
	closeErr    error

	mutex      sync.Mutex
	storeCalls int
	closeCalls int
}

func (session *fakeConsumerSession) Next(ctx context.Context) (rabbitstream.Message, error) {
	if session.nextCalled != nil {
		session.nextCalled <- struct{}{}
	}
	if session.nextRelease != nil {
		select {
		case <-session.nextRelease:
		case <-ctx.Done():
			return rabbitstream.Message{}, ctx.Err()
		}
	}
	if session.nextErr != nil {
		return rabbitstream.Message{}, session.nextErr
	}
	select {
	case message := <-session.messages:
		return message, nil
	case <-ctx.Done():
		return rabbitstream.Message{}, ctx.Err()
	}
}

func (session *fakeConsumerSession) StoreOffset(context.Context, string, uint64) error {
	session.mutex.Lock()
	defer session.mutex.Unlock()
	session.storeCalls++
	return session.storeErr
}

func (session *fakeConsumerSession) Close() error {
	session.mutex.Lock()
	defer session.mutex.Unlock()
	session.closeCalls++
	return session.closeErr
}

func (session *fakeConsumerSession) CloseCalls() int {
	session.mutex.Lock()
	defer session.mutex.Unlock()
	return session.closeCalls
}

func (session *fakeConsumerSession) StoreCalls() int {
	session.mutex.Lock()
	defer session.mutex.Unlock()
	return session.storeCalls
}
