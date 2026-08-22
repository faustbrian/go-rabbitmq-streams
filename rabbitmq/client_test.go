package rabbitmq

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/faustbrian/golib/pkg/rabbitstream"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/message"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"
)

func TestOpenProducerHonorsCancellationBeforeCredentialResolution(t *testing.T) {
	t.Parallel()

	provider := &countingCredentialProvider{}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	producer, err := OpenProducer(
		ctx,
		rabbitstream.ConnectionConfig{
			Endpoints:   []rabbitstream.Endpoint{{Host: "rabbitmq.internal", Port: 5551}},
			Credentials: provider,
		},
		rabbitstream.ProducerConfig{Stream: "tracking.events"},
	)
	if producer != nil || !errors.Is(err, rabbitstream.ErrCanceled) || provider.calls != 0 {
		t.Fatalf("OpenProducer() = %#v, %v; credential calls = %d", producer, err, provider.calls)
	}
}

func TestOpenProducerRejectsNilContextAndCredentialFailure(t *testing.T) {
	t.Parallel()

	connection := rabbitstream.ConnectionConfig{
		Endpoints: []rabbitstream.Endpoint{{Host: "rabbitmq.internal", Port: 5551}},
		Credentials: credentialProviderFunc(func(context.Context) (rabbitstream.Credentials, error) {
			return rabbitstream.Credentials{}, errors.New("credential backend unavailable")
		}),
		Security: rabbitstream.DevelopmentPlaintextSecurity(),
	}
	config := rabbitstream.ProducerConfig{Stream: "tracking.events"}
	var nilContext context.Context
	if producer, err := OpenProducer(nilContext, connection, config); producer != nil || !errors.Is(err, rabbitstream.ErrInvalidConfiguration) {
		t.Fatalf("nil-context OpenProducer() = %#v, %v", producer, err)
	}
	if producer, err := OpenProducer(context.Background(), connection, config); producer != nil || !errors.Is(err, rabbitstream.ErrAuthentication) {
		t.Fatalf("credential-failure OpenProducer() = %#v, %v", producer, err)
	}
}

func TestOpenProducerMapsCredentialDeadlineToTimeout(t *testing.T) {
	t.Parallel()

	connection := rabbitstream.ConnectionConfig{
		Endpoints: []rabbitstream.Endpoint{{Host: "rabbitmq.internal", Port: 5551}},
		Credentials: credentialProviderFunc(func(ctx context.Context) (rabbitstream.Credentials, error) {
			<-ctx.Done()
			return rabbitstream.Credentials{}, ctx.Err()
		}),
		Security:       rabbitstream.DevelopmentPlaintextSecurity(),
		ConnectTimeout: time.Millisecond,
	}
	producer, err := OpenProducer(context.Background(), connection, rabbitstream.ProducerConfig{Stream: "tracking.events"})
	if producer != nil || !errors.Is(err, rabbitstream.ErrTimeout) {
		t.Fatalf("OpenProducer() = %#v, %v", producer, err)
	}
}

func TestOpenProducerValidatesPolicyBeforeCredentialResolution(t *testing.T) {
	t.Parallel()

	provider := &countingCredentialProvider{}
	producer, err := OpenProducer(
		context.Background(),
		rabbitstream.ConnectionConfig{
			Endpoints:   []rabbitstream.Endpoint{{Host: "rabbitmq.internal", Port: 5551}},
			Credentials: provider,
		},
		rabbitstream.ProducerConfig{},
	)
	if producer != nil || !errors.Is(err, rabbitstream.ErrInvalidConfiguration) ||
		provider.calls != 0 {
		t.Fatalf("OpenProducer() = %#v, %v; credential calls = %d", producer, err, provider.calls)
	}
}

func TestOpenProducerRejectsConnectionPolicyBeforeProducerSetup(t *testing.T) {
	t.Parallel()

	producer, err := OpenProducer(
		context.Background(),
		rabbitstream.ConnectionConfig{},
		rabbitstream.ProducerConfig{Stream: "tracking.events"},
	)
	if producer != nil || !errors.Is(err, rabbitstream.ErrInvalidConfiguration) {
		t.Fatalf("OpenProducer() = %#v, %v", producer, err)
	}
}

func TestFinishOpenProducerClosesTransportWhenCorePolicyRejectsConfig(t *testing.T) {
	t.Parallel()

	session := newFakeProducerSession()
	transport, err := newReconnectingProducerTransport(
		context.Background(),
		func(context.Context) (producerSession, error) { return session, nil },
		nil,
	)
	if err != nil {
		t.Fatalf("newReconnectingProducerTransport() error = %v", err)
	}
	if producer, err := finishOpenProducer(rabbitstream.ProducerConfig{}, transport); producer != nil || !errors.Is(err, rabbitstream.ErrInvalidConfiguration) {
		t.Fatalf("finishOpenProducer() = %#v, %v", producer, err)
	}
	if _, closeCalls := session.calls(); closeCalls != 1 {
		t.Fatalf("transport close calls = %d", closeCalls)
	}
}

func TestProducerTransportReconnectsAfterLossAndMarksAcceptedPublishAmbiguous(t *testing.T) {
	t.Parallel()

	first := newFakeProducerSession()
	second := newFakeProducerSession()
	observations := make(observationChannel, 3)
	sessions := make(chan producerSession, 2)
	sessions <- first
	sessions <- second
	transport, err := newReconnectingProducerTransport(
		context.Background(),
		func(context.Context) (producerSession, error) { return <-sessions, nil },
		observations,
	)
	if err != nil {
		t.Fatalf("newReconnectingProducerTransport() error = %v", err)
	}

	confirmation := make(chan rabbitstream.TransportConfirmation, 1)
	if err := transport.Send(context.Background(), rabbitstream.Message{Stream: "tracking.events"}, func(result rabbitstream.TransportConfirmation) {
		confirmation <- result
	}); err != nil {
		t.Fatalf("first Send() error = %v", err)
	}
	<-first.sent
	first.failures <- rabbitstream.ErrConnection
	result := <-confirmation
	if !result.Ambiguous || !errors.Is(result.Cause, rabbitstream.ErrConnection) {
		t.Fatalf("lost confirmation = %#v", result)
	}
	<-first.closed

	if err := transport.Send(context.Background(), rabbitstream.Message{Stream: "tracking.events"}, func(rabbitstream.TransportConfirmation) {}); err != nil {
		t.Fatalf("reconnected Send() error = %v", err)
	}
	<-second.sent
	transport.mutex.Lock()
	current, lastErr, retryAfter := transport.current, transport.lastErr, transport.retryAfter
	transport.mutex.Unlock()
	if current != second || lastErr != nil || !retryAfter.IsZero() {
		t.Fatalf("reconnected transport current = %#v, last error = %v, retry after = %v", current, lastErr, retryAfter)
	}
	abortCalls, closeCalls := first.calls()
	if abortCalls != 1 || closeCalls != 1 {
		t.Fatalf("failed session aborts = %d, closes = %d", abortCalls, closeCalls)
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

func TestProducerTransportReconnectsWhenSendReportsConnectionLossBeforeFailureSignal(t *testing.T) {
	t.Parallel()

	first := newFakeProducerSession()
	first.sendErr = rabbitstream.ErrConnection
	second := newFakeProducerSession()
	sessions := make(chan producerSession, 2)
	sessions <- first
	sessions <- second
	transport, err := newReconnectingProducerTransport(
		context.Background(),
		func(context.Context) (producerSession, error) { return <-sessions, nil },
		nil,
	)
	if err != nil {
		t.Fatalf("newReconnectingProducerTransport() error = %v", err)
	}

	if err := transport.Send(context.Background(), rabbitstream.Message{Stream: "tracking.events"}, func(rabbitstream.TransportConfirmation) {}); err != nil {
		t.Fatalf("Send() error = %v", err)
	}
	<-second.sent
	<-first.aborted
	<-first.closed
	if abortCalls, closeCalls := first.calls(); abortCalls != 1 || closeCalls != 1 {
		t.Fatalf("failed session aborts = %d, closes = %d", abortCalls, closeCalls)
	}
}

func TestProducerTransportReconnectAdmissionHonorsCancellation(t *testing.T) {
	t.Parallel()

	first := newFakeProducerSession()
	reconnectStarted := make(chan struct{})
	initial := true
	transport, err := newReconnectingProducerTransport(
		context.Background(),
		func(ctx context.Context) (producerSession, error) {
			if initial {
				initial = false
				return first, nil
			}
			close(reconnectStarted)
			<-ctx.Done()
			return nil, ctx.Err()
		},
		nil,
	)
	if err != nil {
		t.Fatalf("newReconnectingProducerTransport() error = %v", err)
	}
	first.failures <- rabbitstream.ErrConnection
	<-first.aborted
	<-first.closed

	ctx, cancel := context.WithCancel(context.Background())
	sendDone := make(chan error, 1)
	go func() {
		sendDone <- transport.Send(ctx, rabbitstream.Message{Stream: "tracking.events"}, func(rabbitstream.TransportConfirmation) {})
	}()
	<-reconnectStarted
	cancel()
	if err := <-sendDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("Send() error = %v", err)
	}
}

func TestProducerTransportRejectsInvalidConstructionAndOpenFailure(t *testing.T) {
	t.Parallel()

	validOpener := func(context.Context) (producerSession, error) { return newFakeProducerSession(), nil }
	var nilContext context.Context
	if transport, err := newReconnectingProducerTransport(nilContext, validOpener, nil); transport != nil || !errors.Is(err, rabbitstream.ErrInvalidConfiguration) {
		t.Fatalf("nil-context construction = %#v, %v", transport, err)
	}
	if transport, err := newReconnectingProducerTransport(context.Background(), nil, nil); transport != nil || !errors.Is(err, rabbitstream.ErrInvalidConfiguration) {
		t.Fatalf("nil-opener construction = %#v, %v", transport, err)
	}
	want := errors.New("open producer")
	transport, err := newReconnectingProducerTransport(
		context.Background(),
		func(context.Context) (producerSession, error) { return nil, want },
		nil,
	)
	if transport != nil || !errors.Is(err, want) {
		t.Fatalf("failed construction = %#v, %v", transport, err)
	}
}

func TestSessionOpeningRetriesWholeSessionAcrossEndpoints(t *testing.T) {
	t.Parallel()

	connection := rabbitstream.ConnectionConfig{
		Endpoints: []rabbitstream.Endpoint{
			{Host: "rabbit1", Port: 5552},
			{Host: "rabbit2", Port: 5552},
		},
		ConnectTimeout:        time.Second,
		RPCTimeout:            time.Second,
		MaxReconnectAttempts:  2,
		InitialReconnectDelay: time.Microsecond,
		MaxReconnectBackoff:   time.Microsecond,
	}
	want := newFakeProducerSession()
	var endpoints []string
	session, err := openSessionWithRetries(
		context.Background(),
		connection,
		func(_ context.Context, attempt rabbitstream.ConnectionConfig) (producerSession, error) {
			endpoints = append(endpoints, attempt.Endpoints[0].Host)
			if attempt.MaxReconnectAttempts != 1 {
				t.Fatalf("nested reconnect attempts = %d", attempt.MaxReconnectAttempts)
			}
			if len(endpoints) == 1 {
				return nil, rabbitstream.ErrConnection
			}
			return want, nil
		},
	)
	if err != nil || session != want {
		t.Fatalf("openSessionWithRetries() = %#v, %v", session, err)
	}
	if len(endpoints) != 2 || endpoints[0] != "rabbit1" || endpoints[1] != "rabbit2" {
		t.Fatalf("attempt endpoints = %#v", endpoints)
	}
}

func TestSessionOpeningRetriesBrokerAuthenticationButNotPermanentFailure(t *testing.T) {
	t.Parallel()

	connection := rabbitstream.ConnectionConfig{
		Endpoints:             []rabbitstream.Endpoint{{Host: "rabbit1", Port: 5552}},
		ConnectTimeout:        time.Second,
		RPCTimeout:            time.Second,
		MaxReconnectAttempts:  2,
		InitialReconnectDelay: time.Microsecond,
		MaxReconnectBackoff:   time.Microsecond,
	}
	calls := 0
	session, err := openSessionWithRetries(
		context.Background(),
		connection,
		func(context.Context, rabbitstream.ConnectionConfig) (producerSession, error) {
			calls++
			return nil, rabbitstream.ErrAuthentication
		},
	)
	if session != nil || !errors.Is(err, rabbitstream.ErrAuthentication) || calls != 1 {
		t.Fatalf("permanent session open = %#v, %v after %d calls", session, err, calls)
	}

	want := newFakeProducerSession()
	calls = 0
	session, err = openSessionWithRetries(
		context.Background(),
		connection,
		func(context.Context, rabbitstream.ConnectionConfig) (producerSession, error) {
			calls++
			if calls == 1 {
				return nil, stream.AuthenticationFailure
			}
			return want, nil
		},
	)
	if session != want || err != nil || calls != 2 {
		t.Fatalf("rotated-credential session open = %#v, %v after %d calls", session, err, calls)
	}
	calls = 0
	session, err = openSessionWithRetries(
		context.Background(),
		connection,
		func(context.Context, rabbitstream.ConnectionConfig) (producerSession, error) {
			calls++
			if calls == 1 {
				return nil, stream.AuthenticationFailureLoopbackError
			}
			return want, nil
		},
	)
	if session != want || err != nil || calls != 2 {
		t.Fatalf("loopback-credential session open = %#v, %v after %d calls", session, err, calls)
	}

	connection.MaxReconnectAttempts = 0
	if session, err := openSessionWithRetries(
		context.Background(), connection,
		func(context.Context, rabbitstream.ConnectionConfig) (producerSession, error) {
			t.Fatal("zero-attempt opener called")
			return nil, nil
		},
	); session != nil || !errors.Is(err, rabbitstream.ErrConnection) {
		t.Fatalf("zero-attempt session open = %#v, %v", session, err)
	}
}

func TestSessionOpeningRejectsExhaustedAttemptBudget(t *testing.T) {
	t.Parallel()

	connection := rabbitstream.ConnectionConfig{
		Endpoints:             []rabbitstream.Endpoint{{Host: "rabbit1", Port: 5552}},
		ConnectTimeout:        time.Second,
		RPCTimeout:            0,
		MaxReconnectAttempts:  1,
		InitialReconnectDelay: time.Microsecond,
		MaxReconnectBackoff:   time.Microsecond,
	}
	opener := func(context.Context, rabbitstream.ConnectionConfig) (producerSession, error) {
		t.Fatal("opener called without an attempt budget")
		return nil, nil
	}
	if session, err := openSessionWithRetries(context.Background(), connection, opener); session != nil ||
		!errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("invalid attempt budget = %#v, %v", session, err)
	}

	expired, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	connection.RPCTimeout = time.Second
	if session, err := openSessionWithRetries(expired, connection, opener); session != nil ||
		!errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expired attempt budget = %#v, %v", session, err)
	}
}

func TestProducerTransportClosedFailureChannelTriggersReconnect(t *testing.T) {
	t.Parallel()

	first := newFakeProducerSession()
	second := newFakeProducerSession()
	sessions := make(chan producerSession, 2)
	sessions <- first
	sessions <- second
	transport, err := newReconnectingProducerTransport(
		context.Background(),
		func(context.Context) (producerSession, error) { return <-sessions, nil },
		nil,
	)
	if err != nil {
		t.Fatalf("newReconnectingProducerTransport() error = %v", err)
	}
	close(first.failures)
	<-first.aborted
	<-first.closed
	if err := transport.Send(context.Background(), rabbitstream.Message{Stream: "tracking.events"}, func(rabbitstream.TransportConfirmation) {}); err != nil {
		t.Fatalf("Send() error = %v", err)
	}
	<-second.sent
	if err := transport.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
}

func TestProducerTransportTreatsNilFailureAsConnectionLoss(t *testing.T) {
	t.Parallel()

	first := newFakeProducerSession()
	second := newFakeProducerSession()
	sessions := make(chan producerSession, 2)
	sessions <- first
	sessions <- second
	transport, err := newReconnectingProducerTransport(
		context.Background(),
		func(context.Context) (producerSession, error) { return <-sessions, nil },
		nil,
	)
	if err != nil {
		t.Fatalf("newReconnectingProducerTransport() error = %v", err)
	}
	first.failures <- nil
	<-first.aborted
	<-first.closed
	if err := transport.Send(context.Background(), rabbitstream.Message{Stream: "tracking.events"}, func(rabbitstream.TransportConfirmation) {}); err != nil {
		t.Fatalf("Send() after nil failure error = %v", err)
	}
	<-second.sent
}

func TestProducerTransportIgnoresStaleSessionInvalidation(t *testing.T) {
	t.Parallel()

	current := newFakeProducerSession()
	stale := newFakeProducerSession()
	transport := &producerTransport{current: current, done: make(chan struct{})}
	transport.invalidate(stale, rabbitstream.ErrConnection)
	if transport.current != current || transport.lastErr != nil {
		t.Fatalf("stale invalidation changed transport = %#v", transport)
	}
	if aborts, closes := stale.calls(); aborts != 0 || closes != 0 {
		t.Fatalf("stale session aborts = %d, closes = %d", aborts, closes)
	}
}

func TestProducerTransportReturnsDefiniteSendErrorsWithoutReconnect(t *testing.T) {
	t.Parallel()

	session := newFakeProducerSession()
	session.sendErr = rabbitstream.ErrMessageTooLarge
	transport, err := newReconnectingProducerTransport(
		context.Background(),
		func(context.Context) (producerSession, error) { return session, nil },
		nil,
	)
	if err != nil {
		t.Fatalf("newReconnectingProducerTransport() error = %v", err)
	}
	if err := transport.Send(context.Background(), rabbitstream.Message{Stream: "tracking.events"}, func(rabbitstream.TransportConfirmation) {}); !errors.Is(err, rabbitstream.ErrMessageTooLarge) {
		t.Fatalf("Send() error = %v", err)
	}
	if abortCalls, closeCalls := session.calls(); abortCalls != 0 || closeCalls != 0 {
		t.Fatalf("definite failure aborts = %d, closes = %d", abortCalls, closeCalls)
	}
	_ = transport.Close()
}

func TestProducerTransportBoundsConsecutivePreAdmissionReconnects(t *testing.T) {
	t.Parallel()

	first := newFakeProducerSession()
	first.sendErr = rabbitstream.ErrConnection
	second := newFakeProducerSession()
	second.sendErr = rabbitstream.ErrConnection
	third := newFakeProducerSession()
	sessions := make(chan producerSession, 3)
	sessions <- first
	sessions <- second
	sessions <- third
	openCalls := 0
	transport, err := newReconnectingProducerTransport(
		context.Background(),
		func(context.Context) (producerSession, error) {
			openCalls++
			return <-sessions, nil
		},
		nil,
	)
	if err != nil {
		t.Fatalf("newReconnectingProducerTransport() error = %v", err)
	}
	if err := transport.Send(context.Background(), rabbitstream.Message{Stream: "tracking.events"}, func(rabbitstream.TransportConfirmation) {}); !errors.Is(err, rabbitstream.ErrConnection) {
		t.Fatalf("Send() error = %v", err)
	}
	<-first.aborted
	<-first.closed
	<-second.aborted
	<-second.closed
	if openCalls != 2 {
		t.Fatalf("producer session opens = %d, want 2", openCalls)
	}
}

func TestProducerTransportReconnectFailureAndBackoffHonorCallerContext(t *testing.T) {
	t.Parallel()

	first := newFakeProducerSession()
	want := errors.New("reconnect failed")
	openCalls := 0
	transport, err := newReconnectingProducerTransport(
		context.Background(),
		func(context.Context) (producerSession, error) {
			openCalls++
			if openCalls == 1 {
				return first, nil
			}
			return nil, want
		},
		nil,
	)
	if err != nil {
		t.Fatalf("newReconnectingProducerTransport() error = %v", err)
	}
	transport.invalidate(first, rabbitstream.ErrConnection)
	<-first.aborted
	<-first.closed
	transport.retryDelay = 0
	transport.retryAfter = time.Time{}
	if _, err := transport.session(context.Background()); !errors.Is(err, want) {
		t.Fatalf("reconnect session error = %v", err)
	}
	transport.mutex.Lock()
	storedErr, retryAfter := transport.lastErr, transport.retryAfter
	transport.mutex.Unlock()
	if !errors.Is(storedErr, want) || retryAfter.IsZero() {
		t.Fatalf("failed reconnect state = %v, %v", storedErr, retryAfter)
	}

	transport.mutex.Lock()
	transport.retryAfter = time.Now().Add(time.Hour)
	transport.lastErr = rabbitstream.ErrConnection
	transport.mutex.Unlock()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := transport.session(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("backoff session error = %v", err)
	}
}

func TestProducerTransportCloseIsIdempotentAndPreservesCloseError(t *testing.T) {
	t.Parallel()

	session := newFakeProducerSession()
	session.closeErr = errors.New("close producer")
	transport, err := newReconnectingProducerTransport(
		context.Background(),
		func(context.Context) (producerSession, error) { return session, nil },
		nil,
	)
	if err != nil {
		t.Fatalf("newReconnectingProducerTransport() error = %v", err)
	}
	if err := transport.Close(); !errors.Is(err, session.closeErr) {
		t.Fatalf("first Close() error = %v", err)
	}
	if err := transport.Close(); !errors.Is(err, session.closeErr) {
		t.Fatalf("second Close() error = %v", err)
	}
	if _, err := transport.session(context.Background()); !errors.Is(err, rabbitstream.ErrClosed) {
		t.Fatalf("closed session error = %v", err)
	}
}

func TestProducerTransportReconnectWaitAndCloseTransitions(t *testing.T) {
	t.Parallel()

	first := newFakeProducerSession()
	reconnectStarted := make(chan struct{})
	releaseReconnect := make(chan struct{})
	second := newFakeProducerSession()
	openCalls := 0
	transport, err := newReconnectingProducerTransport(
		context.Background(),
		func(context.Context) (producerSession, error) {
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
		t.Fatalf("newReconnectingProducerTransport() error = %v", err)
	}
	transport.retryDelay = 0
	transport.invalidate(first, rabbitstream.ErrConnection)
	<-first.aborted
	<-first.closed
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
	<-second.closed
	transport.mutex.Lock()
	closedCurrent := transport.current
	transport.mutex.Unlock()
	if closedCurrent != nil {
		t.Fatalf("closed reconnect retained current session = %#v", closedCurrent)
	}

	waiting := &producerTransport{reconnecting: make(chan struct{}), done: make(chan struct{})}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := waiting.session(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("reconnect waiter error = %v", err)
	}

	done := make(chan struct{})
	close(done)
	woke := &producerTransport{reconnecting: done, closed: true, done: make(chan struct{})}
	if _, err := woke.session(context.Background()); !errors.Is(err, rabbitstream.ErrClosed) {
		t.Fatalf("completed reconnect waiter error = %v", err)
	}
	backoff := &producerTransport{
		lastErr: rabbitstream.ErrConnection, retryAfter: time.Now().Add(time.Millisecond),
		opener: func(context.Context) (producerSession, error) { return nil, rabbitstream.ErrConnection },
		done:   make(chan struct{}),
	}
	if _, err := backoff.session(context.Background()); !errors.Is(err, rabbitstream.ErrConnection) {
		t.Fatalf("completed backoff error = %v", err)
	}

	stale := newFakeProducerSession()
	current := newFakeProducerSession()
	staleTransport := &producerTransport{current: current, done: make(chan struct{})}
	staleTransport.invalidate(stale, rabbitstream.ErrConnection)
	if staleTransport.current != current {
		t.Fatal("stale invalidation replaced the current producer session")
	}
}

func TestProducerTransportCurrentSessionInvalidationChangesStateBeforeCleanup(t *testing.T) {
	t.Parallel()

	current := newFakeProducerSession()
	transport := &producerTransport{current: current, retryDelay: time.Millisecond, done: make(chan struct{})}
	transport.invalidate(current, rabbitstream.ErrConnection)
	transport.mutex.Lock()
	retained, lastErr, retryAfter := transport.current, transport.lastErr, transport.retryAfter
	transport.mutex.Unlock()
	if retained != nil || !errors.Is(lastErr, rabbitstream.ErrConnection) || retryAfter.IsZero() {
		t.Fatalf("invalidated state = %#v, %v, %v", retained, lastErr, retryAfter)
	}
}

func TestProducerRetryWaitPredicateKeepsExactBoundaries(t *testing.T) {
	t.Parallel()

	if shouldWaitForRetry(nil, time.Second) || shouldWaitForRetry(rabbitstream.ErrConnection, 0) ||
		shouldWaitForRetry(rabbitstream.ErrConnection, -time.Nanosecond) ||
		!shouldWaitForRetry(rabbitstream.ErrConnection, time.Nanosecond) {
		t.Fatal("producer retry wait predicate changed an exact boundary")
	}
}

func TestProducerTransportWaitersObserveReconnectCompletionAndCancellation(t *testing.T) {
	t.Parallel()

	current := newFakeProducerSession()
	reconnectDone := make(chan struct{})
	waiting := &producerTransport{reconnecting: reconnectDone, done: make(chan struct{})}
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
	waiting = &producerTransport{reconnecting: reconnectDone, done: make(chan struct{})}
	ctx = newObservedContext()
	sessionResult := make(chan producerSession, 1)
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

	backoff := &producerTransport{
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

func TestOpenConsumerHonorsCancellationBeforeCredentialResolution(t *testing.T) {
	t.Parallel()

	provider := &countingCredentialProvider{}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	consumer, err := OpenConsumer(
		ctx,
		rabbitstream.ConnectionConfig{
			Endpoints:   []rabbitstream.Endpoint{{Host: "rabbitmq.internal", Port: 5551}},
			Credentials: provider,
		},
		rabbitstream.ConsumerConfig{Stream: "tracking.events", ConsumerName: "tracking-indexer"},
	)
	if consumer != nil || !errors.Is(err, rabbitstream.ErrCanceled) || provider.calls != 0 {
		t.Fatalf("OpenConsumer() = %#v, %v; credential calls = %d", consumer, err, provider.calls)
	}
}

func TestOpenConsumerRejectsNilContextCredentialFailureAndOffsetOverflow(t *testing.T) {
	connection := rabbitstream.ConnectionConfig{
		Endpoints: []rabbitstream.Endpoint{{Host: "rabbitmq.internal", Port: 5551}},
		Credentials: credentialProviderFunc(func(context.Context) (rabbitstream.Credentials, error) {
			return rabbitstream.Credentials{}, errors.New("credential backend unavailable")
		}),
		Security: rabbitstream.DevelopmentPlaintextSecurity(),
	}
	config := rabbitstream.ConsumerConfig{Stream: "tracking.events", ConsumerName: "tracking-indexer"}
	var nilContext context.Context
	if consumer, err := OpenConsumer(nilContext, connection, config); consumer != nil || !errors.Is(err, rabbitstream.ErrInvalidConfiguration) {
		t.Fatalf("nil-context OpenConsumer() = %#v, %v", consumer, err)
	}
	if consumer, err := OpenConsumer(context.Background(), connection, config); consumer != nil || !errors.Is(err, rabbitstream.ErrAuthentication) {
		t.Fatalf("credential-failure OpenConsumer() = %#v, %v", consumer, err)
	}
	overflowConnection := connection
	overflowConnection.Credentials = rabbitstream.StaticCredentials("user", []byte("credential"))
	overflow := config
	overflow.Start = rabbitstream.StartPosition{Kind: rabbitstream.OffsetStartExplicit, Offset: uint64(^uint64(0))}
	if consumer, err := OpenConsumer(context.Background(), overflowConnection, overflow); consumer != nil || !errors.Is(err, rabbitstream.ErrInvalidConfiguration) {
		t.Fatalf("overflow OpenConsumer() = %#v, %v", consumer, err)
	}
}

func TestOpenConsumerValidatesPolicyBeforeCredentialResolution(t *testing.T) {
	t.Parallel()

	provider := &countingCredentialProvider{}
	consumer, err := OpenConsumer(
		context.Background(),
		rabbitstream.ConnectionConfig{
			Endpoints:   []rabbitstream.Endpoint{{Host: "rabbitmq.internal", Port: 5551}},
			Credentials: provider,
		},
		rabbitstream.ConsumerConfig{},
	)
	if consumer != nil || !errors.Is(err, rabbitstream.ErrInvalidConfiguration) || provider.calls != 0 {
		t.Fatalf("OpenConsumer() = %#v, %v; credential calls = %d", consumer, err, provider.calls)
	}
}

func TestOpenConsumerRejectsConnectionPolicyAndMapsCredentialDeadline(t *testing.T) {
	t.Parallel()

	config := rabbitstream.ConsumerConfig{Stream: "tracking.events", ConsumerName: "tracking-indexer"}
	if consumer, err := OpenConsumer(context.Background(), rabbitstream.ConnectionConfig{}, config); consumer != nil || !errors.Is(err, rabbitstream.ErrInvalidConfiguration) {
		t.Fatalf("invalid-connection OpenConsumer() = %#v, %v", consumer, err)
	}
	connection := rabbitstream.ConnectionConfig{
		Endpoints: []rabbitstream.Endpoint{{Host: "rabbitmq.internal", Port: 5551}},
		Credentials: credentialProviderFunc(func(ctx context.Context) (rabbitstream.Credentials, error) {
			<-ctx.Done()
			return rabbitstream.Credentials{}, ctx.Err()
		}),
		Security:       rabbitstream.DevelopmentPlaintextSecurity(),
		ConnectTimeout: time.Millisecond,
	}
	if consumer, err := OpenConsumer(context.Background(), connection, config); consumer != nil || !errors.Is(err, rabbitstream.ErrTimeout) {
		t.Fatalf("deadline OpenConsumer() = %#v, %v", consumer, err)
	}
}

func TestConfirmationClassificationPreservesAmbiguity(t *testing.T) {
	timedOut := classifyConfirmation(false, stream.ConfirmationTimoutError, 41)
	if timedOut.Confirmed || timedOut.BrokerRejected || !timedOut.Ambiguous || timedOut.PublishingID != 41 ||
		!errors.Is(timedOut.Cause, stream.ConfirmationTimoutError) {
		t.Fatalf("timeout classification = %#v", timedOut)
	}

	rejected := classifyConfirmation(false, stream.CodeAccessRefused, 42)
	if rejected.Confirmed || !rejected.BrokerRejected || rejected.PublishingID != 42 {
		t.Fatalf("rejection classification = %#v", rejected)
	}

	confirmed := classifyConfirmation(true, nil, 43)
	if !confirmed.Confirmed || confirmed.BrokerRejected || confirmed.PublishingID != 43 {
		t.Fatalf("confirmed classification = %#v", confirmed)
	}

	confirmedAfterTimeout := classifyConfirmation(true, stream.ConfirmationTimoutError, 44)
	if !confirmedAfterTimeout.Confirmed || confirmedAfterTimeout.BrokerRejected ||
		confirmedAfterTimeout.Ambiguous || confirmedAfterTimeout.PublishingID != 44 ||
		!errors.Is(confirmedAfterTimeout.Cause, stream.ConfirmationTimoutError) {
		t.Fatalf("confirmed-after-timeout classification = %#v", confirmedAfterTimeout)
	}
}

func TestRabbitProducerSessionClassifiesPreAdmissionOutcomes(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		sendErr error
		want    error
	}{
		"accepted":        {sendErr: nil, want: nil},
		"frame too large": {sendErr: stream.FrameTooLarge, want: rabbitstream.ErrMessageTooLarge},
		"closed upstream": {sendErr: errors.New("producer closed"), want: errProducerSessionClosed},
	}
	for name, test := range tests {
		test := test
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			var sent message.StreamMessage
			session := newRabbitProducerSessionForTest(func(wireMessage message.StreamMessage) error {
				sent = wireMessage
				return test.sendErr
			})
			err := session.Send(rabbitstream.Message{Stream: "tracking.events", Payload: []byte("event")}, func(rabbitstream.TransportConfirmation) {})
			if !errors.Is(err, test.want) || (test.want == nil && err != nil) {
				t.Fatalf("Send() error = %v, want %v", err, test.want)
			}
			if sent == nil {
				t.Fatal("Send() did not pass an owned wire message to the client boundary")
			}
			if test.sendErr == nil && len(session.pending) != 1 {
				t.Fatalf("accepted pending confirmations = %d", len(session.pending))
			}
			if test.sendErr != nil && len(session.pending) != 0 {
				t.Fatalf("failed pending confirmations = %d", len(session.pending))
			}
		})
	}

	aborted := newRabbitProducerSessionForTest(func(message.StreamMessage) error { return nil })
	aborted.Abort(rabbitstream.ErrConnection)
	if err := aborted.Send(rabbitstream.Message{Stream: "tracking.events"}, func(rabbitstream.TransportConfirmation) {}); !errors.Is(err, errProducerSessionClosed) {
		t.Fatalf("aborted Send() error = %v", err)
	}
}

func TestRabbitSuperProducerSessionRoutesAndRejectsUnavailablePartitions(t *testing.T) {
	t.Parallel()

	selected := ""
	session := newRabbitProducerSessionForTest(nil)
	session.send = nil
	session.partitions = []string{"tracking-0", "tracking-1"}
	session.partitionSenders = map[string]func(message.StreamMessage) error{
		"tracking-0": func(message.StreamMessage) error { selected = "tracking-0"; return nil },
		"tracking-1": func(message.StreamMessage) error { selected = "tracking-1"; return nil },
	}
	if err := session.Send(rabbitstream.Message{SuperStream: "tracking", RoutingKey: "tracking-123"}, func(rabbitstream.TransportConfirmation) {}); err != nil {
		t.Fatalf("routed Send() error = %v", err)
	}
	if selected == "" {
		t.Fatal("routed Send() did not select a backing stream")
	}

	session.partitions = nil
	if err := session.Send(rabbitstream.Message{SuperStream: "tracking", RoutingKey: "tracking-123"}, func(rabbitstream.TransportConfirmation) {}); err == nil {
		t.Fatal("Send() accepted an empty Super Stream topology")
	}
	session.partitions = []string{"tracking-missing"}
	if err := session.Send(rabbitstream.Message{SuperStream: "tracking", RoutingKey: "tracking-123"}, func(rabbitstream.TransportConfirmation) {}); !errors.Is(err, rabbitstream.ErrPartitionUnavailable) {
		t.Fatalf("unavailable partition Send() error = %v", err)
	}
}

func TestRabbitProducerSessionConfirmationAdmissionOrdering(t *testing.T) {
	var wireMessage message.StreamMessage
	status := &stream.ConfirmationStatus{}

	admittedResult := make(chan rabbitstream.TransportConfirmation, 1)
	admitted := newRabbitProducerSessionForTest(func(message.StreamMessage) error { return nil })
	admitted.pending[wireMessage] = &pendingConfirmation{partition: "tracking-0", admitted: true, confirm: func(result rabbitstream.TransportConfirmation) {
		admittedResult <- result
	}}
	admittedConfirmations := make(chan []*stream.ConfirmationStatus, 1)
	admittedConfirmations <- []*stream.ConfirmationStatus{status}
	close(admittedConfirmations)
	admitted.handleConfirmations(admittedConfirmations, "tracking-0")
	select {
	case result := <-admittedResult:
		if !result.BrokerRejected || result.Partition != "tracking-0" {
			t.Fatalf("admitted confirmation = %#v", result)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for admitted confirmation")
	}
	if len(admitted.pending) != 0 {
		t.Fatalf("admitted pending confirmations = %d", len(admitted.pending))
	}

	early := newRabbitProducerSessionForTest(func(message.StreamMessage) error { return nil })
	early.pending[wireMessage] = &pendingConfirmation{partition: "tracking-0"}
	earlyConfirmations := make(chan []*stream.ConfirmationStatus, 1)
	earlyConfirmations <- []*stream.ConfirmationStatus{status}
	close(earlyConfirmations)
	early.handleConfirmations(earlyConfirmations, "tracking-0")
	if early.pending[wireMessage].result == nil || !early.pending[wireMessage].result.BrokerRejected {
		t.Fatalf("early confirmation = %#v", early.pending[wireMessage].result)
	}

	unknown := newRabbitProducerSessionForTest(func(message.StreamMessage) error { return nil })
	unknownConfirmations := make(chan []*stream.ConfirmationStatus, 1)
	unknownConfirmations <- []*stream.ConfirmationStatus{status}
	close(unknownConfirmations)
	unknown.handleConfirmations(unknownConfirmations, "tracking-0")
}

func TestRabbitProducerSessionDeliversSynchronousConfirmationAfterAdmission(t *testing.T) {
	t.Parallel()

	result := make(chan rabbitstream.TransportConfirmation, 1)
	session := newRabbitProducerSessionForTest(nil)
	session.send = func(wireMessage message.StreamMessage) error {
		session.mutex.Lock()
		session.pending[wireMessage].result = &rabbitstream.TransportConfirmation{
			Confirmed: true, Partition: "tracking.events", PublishingID: 41,
		}
		session.mutex.Unlock()
		return nil
	}
	if err := session.Send(rabbitstream.Message{Stream: "tracking.events"}, func(confirmation rabbitstream.TransportConfirmation) {
		result <- confirmation
	}); err != nil {
		t.Fatalf("Send() error = %v", err)
	}
	confirmation := <-result
	if !confirmation.Confirmed || confirmation.PublishingID != 41 || len(session.pending) != 0 {
		t.Fatalf("synchronous confirmation = %#v, pending %d", confirmation, len(session.pending))
	}
}

func TestRabbitProducerSessionFailureAndAbortAreSingleTerminalTransitions(t *testing.T) {
	t.Parallel()

	session := newRabbitProducerSessionForTest(func(message.StreamMessage) error { return nil })
	result := make(chan rabbitstream.TransportConfirmation, 1)
	var wireMessage message.StreamMessage
	session.pending[wireMessage] = &pendingConfirmation{partition: "tracking-0", confirm: func(confirmation rabbitstream.TransportConfirmation) {
		result <- confirmation
	}}
	session.Abort(rabbitstream.ErrConnection)
	session.Abort(rabbitstream.ErrReplayRange)
	confirmation := <-result
	if !confirmation.Ambiguous || confirmation.Partition != "tracking-0" || !errors.Is(confirmation.Cause, rabbitstream.ErrConnection) {
		t.Fatalf("aborted confirmation = %#v", confirmation)
	}
	if len(session.pending) != 0 {
		t.Fatalf("aborted pending confirmations = %d", len(session.pending))
	}

	nilFailure := newRabbitProducerSessionForTest(func(message.StreamMessage) error { return nil })
	nilFailure.signalFailure(nil)
	nilFailure.signalFailure(rabbitstream.ErrReplayRange)
	if err := <-nilFailure.Failures(); !errors.Is(err, rabbitstream.ErrConnection) {
		t.Fatalf("nil failure signal = %v", err)
	}
}

func TestRabbitProducerSessionWatchesClosedAndFailedProducers(t *testing.T) {
	t.Parallel()

	closedSession := newRabbitProducerSessionForTest(func(message.StreamMessage) error { return nil })
	closed := make(chan stream.Event)
	close(closed)
	closedSession.watchProducer(closed)
	if err := <-closedSession.Failures(); !errors.Is(err, rabbitstream.ErrConnection) {
		t.Fatalf("closed producer failure = %v", err)
	}

	failedSession := newRabbitProducerSessionForTest(func(message.StreamMessage) error { return nil })
	failed := make(chan stream.Event, 1)
	failed <- stream.Event{Err: rabbitstream.ErrAuthorization}
	failedSession.watchProducer(failed)
	if err := <-failedSession.Failures(); !errors.Is(err, rabbitstream.ErrAuthorization) {
		t.Fatalf("producer failure = %v", err)
	}
}

func TestRabbitProducerSessionCloseNormalizesOwnedResourceFailures(t *testing.T) {
	t.Parallel()

	producerFailure := newRabbitProducerSessionForTest(func(message.StreamMessage) error { return nil })
	producerFailure.producerClosers = []func() error{
		func() error { return errors.New("first close") },
		func() error { return errors.New("second close") },
	}
	if err := producerFailure.Close(); err == nil || err.Error() != "close RabbitMQ Streams producer: first close" {
		t.Fatalf("producer Close() error = %v", err)
	}
	if err := producerFailure.Close(); err == nil || err.Error() != "close RabbitMQ Streams producer: first close" {
		t.Fatalf("idempotent producer Close() error = %v", err)
	}

	environmentFailure := newRabbitProducerSessionForTest(func(message.StreamMessage) error { return nil })
	environmentFailure.producerClosers = []func() error{func() error { return stream.AlreadyClosed }}
	environmentFailure.environmentClose = func() error { return errors.New("environment close") }
	if err := environmentFailure.Close(); err == nil || err.Error() != "close RabbitMQ Streams environment: environment close" {
		t.Fatalf("environment Close() error = %v", err)
	}
}

func newRabbitProducerSessionForTest(send func(message.StreamMessage) error) *rabbitProducerSession {
	return &rabbitProducerSession{
		send:             send,
		producerClosers:  []func() error{func() error { return nil }},
		environmentClose: func() error { return nil },
		pending:          make(map[message.StreamMessage]*pendingConfirmation),
		failures:         make(chan error, 1),
	}
}

func TestWireMessagePreservesLanguageNeutralEventMetadata(t *testing.T) {
	timestamp := time.Unix(1_700_000_000, 0).UTC()
	wire := toWireMessage(rabbitstream.Message{
		Stream:          "tracking.events",
		RoutingKey:      "tracking-123",
		PublishingID:    41,
		HasPublishingID: true,
		Timestamp:       timestamp,
		ContentType:     "application/octet-stream",
		MessageID:       "event-123",
		CorrelationID:   "tracking-123",
		Payload:         []byte("payload"),
		Headers:         []rabbitstream.MetadataEntry{{Key: "traceparent", Value: []byte("trace")}},
		Properties:      []rabbitstream.MetadataEntry{{Key: "schema", Value: []byte("tracking.v1")}},
	})

	if got := string(wire.GetData()[0]); got != "payload" {
		t.Fatalf("payload = %q", got)
	}
	if !wire.HasPublishingId() || wire.GetPublishingId() != 41 {
		t.Fatalf("publishing ID = %d, present %t", wire.GetPublishingId(), wire.HasPublishingId())
	}
	properties := wire.GetMessageProperties()
	if properties == nil || properties.ContentType != "application/octet-stream" ||
		properties.MessageID != "event-123" || properties.CorrelationID != "tracking-123" ||
		!properties.CreationTime.Equal(timestamp) {
		t.Fatalf("message properties = %#v", properties)
	}
	if got := string(wire.GetMessageAnnotations()["traceparent"].([]byte)); got != "trace" {
		t.Fatalf("trace annotation = %q", got)
	}
	if got := wire.GetMessageAnnotations()[routingKeyAnnotation]; got != "tracking-123" {
		t.Fatalf("routing annotation = %#v", got)
	}
	if got := string(wire.GetApplicationProperties()["schema"].([]byte)); got != "tracking.v1" {
		t.Fatalf("schema property = %q", got)
	}
}

func TestWireDeliveryCopiesAndNormalizesLanguageNeutralMetadata(t *testing.T) {
	timestamp := time.Unix(1_700_000_000, 0).UTC()
	wire := toWireMessage(rabbitstream.Message{
		Stream: "tracking.events", RoutingKey: "tracking-123", Timestamp: timestamp,
		ContentType: "application/octet-stream", MessageID: "event-123",
		CorrelationID: "tracking-123", Payload: []byte("payload"),
		Headers:    []rabbitstream.MetadataEntry{{Key: "traceparent", Value: []byte("trace")}},
		Properties: []rabbitstream.MetadataEntry{{Key: "schema", Value: []byte("tracking.v1")}},
	})

	delivery, err := fromWireMessage("tracking", "tracking-0", 41, wire)
	if err != nil {
		t.Fatalf("fromWireMessage() error = %v", err)
	}
	wire.GetData()[0][0] = 'X'
	if delivery.SuperStream != "tracking" || delivery.Stream != "tracking-0" ||
		delivery.Partition != "tracking-0" || delivery.Offset != 41 || !delivery.HasOffset ||
		delivery.RoutingKey != "tracking-123" || delivery.ContentType != "application/octet-stream" ||
		delivery.MessageID != "event-123" || delivery.CorrelationID != "tracking-123" ||
		!delivery.Timestamp.Equal(timestamp) || string(delivery.Payload) != "payload" {
		t.Fatalf("delivery = %#v", delivery)
	}
	if len(delivery.Headers) != 1 || delivery.Headers[0].Key != "traceparent" ||
		string(delivery.Headers[0].Value) != "trace" {
		t.Fatalf("delivery headers = %#v", delivery.Headers)
	}
	if len(delivery.Properties) != 1 || delivery.Properties[0].Key != "schema" ||
		string(delivery.Properties[0].Value) != "tracking.v1" {
		t.Fatalf("delivery properties = %#v", delivery.Properties)
	}
}

func TestConsumerStartPositionMapsWithoutSemanticSubstitution(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		start rabbitstream.StartPosition
		want  string
	}{
		"beginning": {start: rabbitstream.StartPosition{Kind: rabbitstream.OffsetStartBeginning}, want: "first"},
		"end":       {start: rabbitstream.StartPosition{Kind: rabbitstream.OffsetStartEnd}, want: "next"},
		"offset":    {start: rabbitstream.StartPosition{Kind: rabbitstream.OffsetStartExplicit, Offset: 41}, want: "offset, value: 41"},
		"timestamp": {
			start: rabbitstream.StartPosition{Kind: rabbitstream.OffsetStartTimestamp, Timestamp: time.UnixMilli(1_700_000_000_000)},
			want:  "time-stamp, value: 1700000000000",
		},
	}
	if got := toOffsetSpecification(rabbitstream.StartPosition{Kind: rabbitstream.OffsetStartStored}).String(); got != "" {
		t.Fatalf("stored offset must be resolved through the broker, got %q", got)
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			if got := toOffsetSpecification(test.start).String(); got != test.want {
				t.Fatalf("offset specification = %q, want %q", got, test.want)
			}
		})
	}
	if got := toOffsetSpecification(rabbitstream.StartPosition{Kind: 255}).String(); got != "" {
		t.Fatalf("invalid offset specification = %q", got)
	}
}

func TestHashRoutingIsStableForAnOrderedPartitionTopology(t *testing.T) {
	t.Parallel()

	partitions := []string{"tracking-0", "tracking-1", "tracking-2", "tracking-3"}
	first, err := hashPartition("tracking-123", partitions)
	if err != nil {
		t.Fatalf("hashPartition() error = %v", err)
	}
	second, err := hashPartition("tracking-123", append([]string(nil), partitions...))
	if err != nil {
		t.Fatalf("hashPartition() second error = %v", err)
	}
	if first != second || first == "" {
		t.Fatalf("stable partition = %q then %q", first, second)
	}
	if _, err := hashPartition("tracking-123", nil); err == nil {
		t.Fatal("hashPartition() accepted empty topology")
	}
	want := errors.New("route")
	if partition, err := routedPartition(nil, want); partition != "" || !errors.Is(err, want) {
		t.Fatalf("failed routedPartition() = %q, %v", partition, err)
	}
	if partition, err := routedPartition([]string{"one", "two"}, nil); partition != "" || err == nil {
		t.Fatalf("ambiguous routedPartition() = %q, %v", partition, err)
	}
}

func TestChunkRangeProducesAnExactLastMessageOffset(t *testing.T) {
	last, err := chunkLastOffset(40, 3)
	if err != nil || last != 42 {
		t.Fatalf("chunkLastOffset() = %d, %v", last, err)
	}
	if _, err := chunkLastOffset(40, 0); err == nil {
		t.Fatal("chunkLastOffset() accepted an empty delivered chunk")
	}
	if _, err := chunkLastOffset(^uint64(0), 2); err == nil {
		t.Fatal("chunkLastOffset() accepted overflow")
	}
}

type fakeProducerSession struct {
	mutex    sync.Mutex
	failures chan error
	sent     chan struct{}
	aborted  chan struct{}
	closed   chan struct{}

	confirm    func(rabbitstream.TransportConfirmation)
	sendErr    error
	closeErr   error
	abortCalls int
	closeCalls int
	abortErr   error
}

func newFakeProducerSession() *fakeProducerSession {
	return &fakeProducerSession{
		failures: make(chan error, 1),
		sent:     make(chan struct{}, 1),
		aborted:  make(chan struct{}, 1),
		closed:   make(chan struct{}, 1),
	}
}

func (session *fakeProducerSession) Send(
	_ rabbitstream.Message,
	confirm func(rabbitstream.TransportConfirmation),
) error {
	session.mutex.Lock()
	session.confirm = confirm
	err := session.sendErr
	session.mutex.Unlock()
	if err != nil {
		return err
	}
	session.sent <- struct{}{}
	return nil
}

func (session *fakeProducerSession) Failures() <-chan error { return session.failures }

func (session *fakeProducerSession) Abort(err error) {
	session.mutex.Lock()
	session.abortCalls++
	session.abortErr = err
	confirm := session.confirm
	session.confirm = nil
	session.mutex.Unlock()
	if confirm != nil {
		confirm(rabbitstream.TransportConfirmation{Ambiguous: true, Cause: err})
	}
	session.aborted <- struct{}{}
}

func (session *fakeProducerSession) AbortError() error {
	session.mutex.Lock()
	defer session.mutex.Unlock()
	return session.abortErr
}

func (session *fakeProducerSession) Close() error {
	session.mutex.Lock()
	session.closeCalls++
	session.mutex.Unlock()
	session.closed <- struct{}{}
	return session.closeErr
}

func (session *fakeProducerSession) calls() (int, int) {
	session.mutex.Lock()
	defer session.mutex.Unlock()
	return session.abortCalls, session.closeCalls
}

type countingCredentialProvider struct {
	calls int
}

type credentialProviderFunc func(context.Context) (rabbitstream.Credentials, error)

type observationChannel chan rabbitstream.Observation

func (observer observationChannel) Observe(observation rabbitstream.Observation) {
	observer <- observation
}

func (provider credentialProviderFunc) Credentials(ctx context.Context) (rabbitstream.Credentials, error) {
	return provider(ctx)
}

func (provider *countingCredentialProvider) Credentials(context.Context) (rabbitstream.Credentials, error) {
	provider.calls++
	return rabbitstream.Credentials{Username: "track", Password: []byte("secret")}, nil
}
