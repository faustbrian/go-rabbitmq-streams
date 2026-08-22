package rabbitmq

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/faustbrian/golib/pkg/rabbitstream"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/message"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"
)

func TestAdapterMutationBoundariesFailFast(t *testing.T) {
	await := func(name string, done <-chan struct{}) {
		t.Helper()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatalf("timed out waiting for %s", name)
		}
	}

	emptyWire := toWireMessage(rabbitstream.Message{})
	if emptyWire.Properties != nil || emptyWire.Annotations != nil || emptyWire.ApplicationProperties != nil {
		t.Fatalf(
			"empty wire metadata = %#v, %#v, %#v",
			emptyWire.Properties, emptyWire.Annotations, emptyWire.ApplicationProperties,
		)
	}
	for name, outbound := range map[string]rabbitstream.Message{
		"content type":   {ContentType: "application/octet-stream"},
		"message ID":     {MessageID: "event-1"},
		"correlation ID": {CorrelationID: "correlation-1"},
		"timestamp":      {Timestamp: time.Unix(1, 0)},
		"header":         {Headers: []rabbitstream.MetadataEntry{{Key: "header", Value: []byte("value")}}},
		"routing key":    {RoutingKey: "carrier.ups"},
		"property":       {Properties: []rabbitstream.MetadataEntry{{Key: "property", Value: []byte("value")}}},
	} {
		wire := toWireMessage(outbound)
		switch name {
		case "content type", "message ID", "correlation ID", "timestamp":
			if wire.Properties == nil {
				t.Fatalf("%s did not allocate message properties", name)
			}
		case "header", "routing key":
			if len(wire.Annotations) != 1 {
				t.Fatalf("%s annotations = %#v", name, wire.Annotations)
			}
		case "property":
			if len(wire.ApplicationProperties) != 1 {
				t.Fatalf("property metadata = %#v", wire.ApplicationProperties)
			}
		}
	}

	delivery, err := fromWireMessage("tracking", "tracking-0", 1, &amqp.Message{
		Data: [][]byte{[]byte("payload")},
		Annotations: amqp.Annotations{
			routingKeyAnnotation: "carrier.ups",
			"z-header":           "value",
		},
	})
	if err != nil {
		t.Fatalf("decode single-section delivery: %v", err)
	}
	if string(delivery.Payload) != "payload" || delivery.RoutingKey != "carrier.ups" ||
		len(delivery.Headers) != 1 || delivery.Headers[0].Key != "z-header" {
		t.Fatalf("single-section delivery = %#v", delivery)
	}

	successMessages := make(chan rabbitstream.Message, 1)
	successMessages <- rabbitstream.Message{Stream: "tracking.events", Offset: 7}
	successConsumerSession := &fakeConsumerSession{messages: successMessages}
	successConsumerTransport := &consumerTransport{
		current: successConsumerSession, done: make(chan struct{}),
	}
	consumerNextCtx, cancelConsumerNext := context.WithTimeout(context.Background(), time.Second)
	defer cancelConsumerNext()
	if message, err := successConsumerTransport.Next(consumerNextCtx); err != nil || message.Offset != 7 {
		t.Fatalf("successful consumer Next() = %#v, %v", message, err)
	}
	_ = successConsumerTransport.Close()

	closedNextSession := &fakeConsumerSession{nextErr: rabbitstream.ErrClosed}
	closedNextTransport := &consumerTransport{current: closedNextSession, done: make(chan struct{})}
	if _, err := closedNextTransport.Next(context.Background()); !errors.Is(err, rabbitstream.ErrClosed) {
		t.Fatalf("closed consumer Next() error = %v", err)
	}
	if closedNextSession.CloseCalls() != 0 {
		t.Fatalf("closed consumer Next() invalidated the session")
	}
	_ = closedNextTransport.Close()

	canceledNextSession := &fakeConsumerSession{}
	canceledNextTransport := &consumerTransport{current: canceledNextSession, done: make(chan struct{})}
	canceledNextCtx, cancelCanceledNext := context.WithCancel(context.Background())
	cancelCanceledNext()
	if _, err := canceledNextTransport.Next(canceledNextCtx); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled consumer Next() error = %v", err)
	}
	_ = canceledNextTransport.Close()

	validConsumerDelivery := newRabbitConsumerSessionForTest()
	validConsumerDelivery.accept("tracking.events", 0, &amqp.Message{Data: [][]byte{[]byte("event")}})
	select {
	case message := <-validConsumerDelivery.messages:
		if message.Offset != 0 || !message.HasOffset || message.Partition != "tracking.events" {
			t.Fatalf("accepted consumer delivery = %#v", message)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for accepted consumer delivery")
	}

	negativeConsumerDelivery := newRabbitConsumerSessionForTest()
	negativeConsumerDelivery.accept("tracking.events", -1, &amqp.Message{})
	if _, err := negativeConsumerDelivery.Next(context.Background()); err == nil ||
		err.Error() != "consumer returned a negative offset" {
		t.Fatalf("negative consumer delivery error = %v", err)
	}

	invalidConsumerDelivery := newRabbitConsumerSessionForTest()
	invalidConsumerDelivery.accept("tracking.events", 1, &amqp.Message{
		Data: [][]byte{[]byte("one"), []byte("two")},
	})
	if _, err := invalidConsumerDelivery.Next(context.Background()); !errors.Is(err, rabbitstream.ErrValidation) {
		t.Fatalf("invalid consumer delivery error = %v", err)
	}

	validDelivery := rabbitstream.Message{
		Stream: "tracking.events", Partition: "tracking.events", Offset: 1, HasOffset: true,
	}
	if err := validateDelivery(validDelivery, rabbitstream.DefaultLimits()); err != nil {
		t.Fatalf("valid delivery error = %v", err)
	}
	for name, invalidDelivery := range map[string]rabbitstream.Message{
		"missing partition":   {Stream: "tracking.events", HasOffset: true},
		"different partition": {Stream: "tracking.events", Partition: "tracking-0", HasOffset: true},
		"missing offset":      {Stream: "tracking.events", Partition: "tracking.events"},
	} {
		if err := validateDelivery(invalidDelivery, rabbitstream.DefaultLimits()); !errors.Is(err, rabbitstream.ErrValidation) {
			t.Fatalf("%s delivery error = %v", name, err)
		}
	}

	producerOpenCause := errors.New("open producer transport")
	producerOnFailure := newFakeProducerSession()
	producerOnFailureTransport, err := newReconnectingProducerTransport(
		context.Background(),
		func(context.Context) (producerSession, error) { return producerOnFailure, producerOpenCause },
		nil,
	)
	if producerOnFailureTransport != nil || !errors.Is(err, producerOpenCause) {
		t.Fatalf("failed producer transport = %#v, %v", producerOnFailureTransport, err)
	}

	consumerOpenCause := errors.New("open consumer transport")
	consumerOnFailure := &fakeConsumerSession{}
	consumerOnFailureTransport, err := newReconnectingConsumerTransport(
		context.Background(),
		func(context.Context, bool) (consumerSession, error) { return consumerOnFailure, consumerOpenCause },
		nil,
	)
	if consumerOnFailureTransport != nil || !errors.Is(err, consumerOpenCause) {
		t.Fatalf("failed consumer transport = %#v, %v", consumerOnFailureTransport, err)
	}

	if shouldWaitForRetry(nil, time.Second) || shouldWaitForRetry(rabbitstream.ErrConnection, 0) ||
		shouldWaitForRetry(rabbitstream.ErrConnection, -time.Nanosecond) ||
		!shouldWaitForRetry(rabbitstream.ErrConnection, time.Nanosecond) {
		t.Fatal("retry wait predicate changed an exact boundary")
	}
	if !consumerStartOffsetFits(rabbitstream.StartPosition{
		Kind: rabbitstream.OffsetStartExplicit, Offset: math.MaxInt64,
	}) || consumerStartOffsetFits(rabbitstream.StartPosition{
		Kind: rabbitstream.OffsetStartExplicit, Offset: uint64(math.MaxInt64) + 1,
	}) || !consumerStartOffsetFits(rabbitstream.StartPosition{Kind: rabbitstream.OffsetStartBeginning}) {
		t.Fatal("consumer start offset changed an exact boundary")
	}
	if validSuperStreamPartitionCount(0) || !validSuperStreamPartitionCount(1) ||
		!validSuperStreamPartitionCount(rabbitstream.MaxSuperStreamPartitions) ||
		validSuperStreamPartitionCount(rabbitstream.MaxSuperStreamPartitions+1) {
		t.Fatal("inspection partition count changed an exact boundary")
	}
	if nonNegativeBrokerOffset(-1) || !nonNegativeBrokerOffset(0) || !nonNegativeBrokerOffset(1) {
		t.Fatal("broker offset sign changed an exact boundary")
	}
	if last, err := chunkLastOffset(40, 3); err != nil || last != 42 {
		t.Fatalf("chunk last offset = %d, %v", last, err)
	}
	if last, err := chunkLastOffset(math.MaxUint64-1, 2); err != nil || last != math.MaxUint64 {
		t.Fatalf("maximum chunk last offset = %d, %v", last, err)
	}
	if _, err := chunkLastOffset(40, 0); !errors.Is(err, rabbitstream.ErrReplayRange) {
		t.Fatalf("empty chunk error = %v", err)
	}
	if _, err := chunkLastOffset(math.MaxUint64, 2); !errors.Is(err, rabbitstream.ErrReplayRange) {
		t.Fatalf("overflowing chunk error = %v", err)
	}

	currentSession := newFakeProducerSession()
	currentTransport := &producerTransport{
		current: currentSession, retryDelay: time.Millisecond, done: make(chan struct{}),
	}
	currentTransport.invalidate(currentSession, rabbitstream.ErrConnection)
	if currentTransport.current != nil || !errors.Is(currentTransport.lastErr, rabbitstream.ErrConnection) {
		t.Fatalf("producer invalidation state = %#v, %v", currentTransport.current, currentTransport.lastErr)
	}

	reconnectedSession := newFakeProducerSession()
	zeroReconnect := &producerTransport{
		opener: func(context.Context) (producerSession, error) { return reconnectedSession, nil },
		done:   make(chan struct{}),
	}
	reconnectCtx, cancelReconnect := context.WithTimeout(context.Background(), time.Second)
	defer cancelReconnect()
	if session, err := zeroReconnect.session(reconnectCtx); err != nil || session != reconnectedSession {
		t.Fatalf("zero-state producer reconnect = %#v, %v", session, err)
	}
	_ = zeroReconnect.Close()

	producerReconnectCause := errors.New("producer reconnect failure")
	producerReconnectFailure := newFakeProducerSession()
	failedReconnect := &producerTransport{
		opener: func(context.Context) (producerSession, error) {
			return producerReconnectFailure, producerReconnectCause
		},
		done: make(chan struct{}),
	}
	if session, err := failedReconnect.session(context.Background()); session != nil || !errors.Is(err, producerReconnectCause) {
		t.Fatalf("failed producer reconnect = %#v, %v", session, err)
	}
	if failedReconnect.current != nil || !errors.Is(failedReconnect.lastErr, producerReconnectCause) || failedReconnect.retryAfter.IsZero() {
		t.Fatalf(
			"failed producer reconnect state = %#v, %v, %v",
			failedReconnect.current, failedReconnect.lastErr, failedReconnect.retryAfter,
		)
	}
	_ = failedReconnect.Close()

	closedReconnectSession := newFakeProducerSession()
	producerReconnectStarted := make(chan struct{})
	releaseProducerReconnect := make(chan struct{})
	closedReconnect := &producerTransport{
		opener: func(context.Context) (producerSession, error) {
			close(producerReconnectStarted)
			<-releaseProducerReconnect
			return closedReconnectSession, nil
		},
		done: make(chan struct{}),
	}
	closedProducerResult := make(chan error, 1)
	go func() {
		_, sessionErr := closedReconnect.session(context.Background())
		closedProducerResult <- sessionErr
	}()
	await("producer reconnect start", producerReconnectStarted)
	if err := closedReconnect.Close(); err != nil {
		t.Fatalf("close reconnecting producer: %v", err)
	}
	close(releaseProducerReconnect)
	select {
	case err := <-closedProducerResult:
		if !errors.Is(err, rabbitstream.ErrClosed) {
			t.Fatalf("closed producer reconnect error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for closed producer reconnect")
	}
	if closedReconnect.current != nil {
		t.Fatalf("closed producer retained current session = %#v", closedReconnect.current)
	}

	zeroLimitConsumerSession := &fakeConsumerSession{}
	zeroLimitConsumer := &consumerTransport{
		opener: func(context.Context, bool) (consumerSession, error) {
			return zeroLimitConsumerSession, nil
		},
		maxReconnectAttempts: 0,
		done:                 make(chan struct{}),
	}
	zeroLimitCtx, cancelZeroLimit := context.WithTimeout(context.Background(), time.Second)
	defer cancelZeroLimit()
	if session, err := zeroLimitConsumer.session(zeroLimitCtx); err != nil || session != zeroLimitConsumerSession {
		t.Fatalf("zero-limit consumer reconnect = %#v, %v", session, err)
	}
	_ = zeroLimitConsumer.Close()

	consumerReconnectCause := errors.New("consumer reconnect failure")
	consumerReconnectFailure := &fakeConsumerSession{}
	failedConsumerReconnect := &consumerTransport{
		opener: func(context.Context, bool) (consumerSession, error) {
			return consumerReconnectFailure, consumerReconnectCause
		},
		maxReconnectAttempts: 1,
		done:                 make(chan struct{}),
	}
	consumerReconnectCtx, cancelConsumerReconnect := context.WithTimeout(context.Background(), time.Second)
	defer cancelConsumerReconnect()
	if session, err := failedConsumerReconnect.session(consumerReconnectCtx); session != nil || !errors.Is(err, consumerReconnectCause) {
		t.Fatalf("failed consumer reconnect = %#v, %v", session, err)
	}
	if failedConsumerReconnect.current != nil || !errors.Is(failedConsumerReconnect.lastErr, consumerReconnectCause) ||
		failedConsumerReconnect.retryAfter.IsZero() || failedConsumerReconnect.reconnectFailures != 1 {
		t.Fatalf(
			"failed consumer reconnect state = %#v, %v, %v, %d",
			failedConsumerReconnect.current, failedConsumerReconnect.lastErr,
			failedConsumerReconnect.retryAfter, failedConsumerReconnect.reconnectFailures,
		)
	}
	_ = failedConsumerReconnect.Close()

	closedConsumerSession := &fakeConsumerSession{}
	consumerReconnectStarted := make(chan struct{})
	releaseConsumerReconnect := make(chan struct{})
	closedConsumerReconnect := &consumerTransport{
		opener: func(context.Context, bool) (consumerSession, error) {
			close(consumerReconnectStarted)
			<-releaseConsumerReconnect
			return closedConsumerSession, nil
		},
		maxReconnectAttempts: 1,
		done:                 make(chan struct{}),
	}
	closedConsumerResult := make(chan error, 1)
	go func() {
		_, sessionErr := closedConsumerReconnect.session(context.Background())
		closedConsumerResult <- sessionErr
	}()
	await("consumer reconnect start", consumerReconnectStarted)
	if err := closedConsumerReconnect.Close(); err != nil {
		t.Fatalf("close reconnecting consumer: %v", err)
	}
	close(releaseConsumerReconnect)
	select {
	case err := <-closedConsumerResult:
		if !errors.Is(err, rabbitstream.ErrClosed) {
			t.Fatalf("closed consumer reconnect error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for closed consumer reconnect")
	}
	if closedConsumerReconnect.current != nil || closedConsumerSession.CloseCalls() != 1 {
		t.Fatalf(
			"closed consumer reconnect state = %#v, closes %d",
			closedConsumerReconnect.current, closedConsumerSession.CloseCalls(),
		)
	}

	plainConfig, err := (rabbitstream.ProducerConfig{Stream: "tracking.events"}).Normalized()
	if err != nil {
		t.Fatalf("normalize plain producer: %v", err)
	}
	plainEnvironment := &fakeRabbitEnvironment{
		producers: []rabbitProducer{newFakeRabbitProducer("tracking.events")},
	}
	plainSession, err := openProducerSessionWithEnvironment(
		context.Background(), plainConfig,
		func(context.Context) (producerEnvironment, error) { return plainEnvironment, nil },
	)
	if err != nil {
		t.Fatalf("open plain producer session: %v", err)
	}
	if len(plainEnvironment.producerOptions) != 1 || plainEnvironment.producerOptions[0].Name != "" {
		t.Fatalf("plain producer options = %#v", plainEnvironment.producerOptions)
	}
	if err := plainSession.Close(); err != nil {
		t.Fatalf("close plain producer session: %v", err)
	}

	deduplicatedConfig, err := (rabbitstream.ProducerConfig{
		Stream: "tracking.events",
		Policy: rabbitstream.ProducerPolicy{
			Deduplication: rabbitstream.DeduplicationPublishingID,
			ProducerName:  "tracking-publisher",
		},
	}).Normalized()
	if err != nil {
		t.Fatalf("normalize deduplicated producer: %v", err)
	}
	deduplicatedEnvironment := &fakeRabbitEnvironment{
		producers: []rabbitProducer{newFakeRabbitProducer("tracking.events")},
	}
	deduplicatedSession, err := openProducerSessionWithEnvironment(
		context.Background(), deduplicatedConfig,
		func(context.Context) (producerEnvironment, error) { return deduplicatedEnvironment, nil },
	)
	if err != nil {
		t.Fatalf("open deduplicated producer session: %v", err)
	}
	if len(deduplicatedEnvironment.producerOptions) != 1 ||
		deduplicatedEnvironment.producerOptions[0].Name != "tracking-publisher" {
		t.Fatalf("deduplicated producer options = %#v", deduplicatedEnvironment.producerOptions)
	}
	if err := deduplicatedSession.Close(); err != nil {
		t.Fatalf("close deduplicated producer session: %v", err)
	}

	superConfig, err := (rabbitstream.ProducerConfig{
		SuperStream: "tracking", ExpectedPartitions: 2,
	}).Normalized()
	if err != nil {
		t.Fatalf("normalize Super Stream producer: %v", err)
	}
	superEnvironment := &fakeRabbitEnvironment{
		partitions: []string{"tracking-0", "tracking-1"},
		producers: []rabbitProducer{
			newFakeRabbitProducer("tracking-0"),
			newFakeRabbitProducer("tracking-1"),
		},
	}
	superSession, err := openProducerSessionWithEnvironment(
		context.Background(), superConfig,
		func(context.Context) (producerEnvironment, error) { return superEnvironment, nil },
	)
	if err != nil || superSession == nil {
		t.Fatalf("open matching Super Stream producer = %#v, %v", superSession, err)
	}
	if err := superSession.Close(); err != nil {
		t.Fatalf("close matching Super Stream producer: %v", err)
	}

	partitionQueryCause := errors.New("query Super Stream partitions")
	queryFailureEnvironment := &fakeRabbitEnvironment{
		partitions:         []string{"tracking-0", "tracking-1"},
		queryPartitionsErr: partitionQueryCause,
	}
	queryFailureSession, err := openProducerSessionWithEnvironment(
		context.Background(), superConfig,
		func(context.Context) (producerEnvironment, error) { return queryFailureEnvironment, nil },
	)
	if queryFailureSession != nil || !errors.Is(err, partitionQueryCause) || queryFailureEnvironment.closeCalls != 1 {
		t.Fatalf(
			"failed Super Stream query = %#v, %v, closes %d",
			queryFailureSession, err, queryFailureEnvironment.closeCalls,
		)
	}

	nilFailure := newFakeProducerSession()
	nilFailureTransport, err := newReconnectingProducerTransport(
		context.Background(),
		func(context.Context) (producerSession, error) { return nilFailure, nil },
		nil,
	)
	if err != nil {
		t.Fatalf("open nil-failure producer transport: %v", err)
	}
	nilFailure.failures <- nil
	await("nil producer failure abort", nilFailure.aborted)
	if !errors.Is(nilFailure.AbortError(), rabbitstream.ErrConnection) {
		t.Fatalf("nil producer failure cause = %v", nilFailure.AbortError())
	}
	_ = nilFailureTransport.Close()

	cause := errors.New("producer failure cause")
	failed := newFakeProducerSession()
	recovered := newFakeProducerSession()
	sessions := make(chan producerSession, 2)
	sessions <- failed
	sessions <- recovered
	transport, err := newReconnectingProducerTransport(
		context.Background(),
		func(context.Context) (producerSession, error) { return <-sessions, nil },
		nil,
	)
	if err != nil {
		t.Fatalf("open producer transport: %v", err)
	}
	failed.failures <- cause
	await("producer failure abort", failed.aborted)
	if !errors.Is(failed.AbortError(), cause) {
		t.Fatalf("producer failure cause = %v", failed.AbortError())
	}
	transport.retryDelay = 0
	transport.retryAfter = time.Time{}
	session, err := transport.session(context.Background())
	if err != nil || session != recovered {
		t.Fatalf("reconnect producer session = %#v, %v", session, err)
	}
	transport.mutex.Lock()
	current, lastErr, retryAfter := transport.current, transport.lastErr, transport.retryAfter
	transport.mutex.Unlock()
	if current != recovered || lastErr != nil || !retryAfter.IsZero() {
		t.Fatalf("reconnected producer state = %#v, %v, %v", current, lastErr, retryAfter)
	}

	firstSend := newFakeProducerSession()
	firstSend.sendErr = rabbitstream.ErrConnection
	secondSend := newFakeProducerSession()
	secondSend.sendErr = rabbitstream.ErrConnection
	thirdSend := newFakeProducerSession()
	sendSessions := make(chan producerSession, 3)
	sendSessions <- firstSend
	sendSessions <- secondSend
	sendSessions <- thirdSend
	openCalls := 0
	sendTransport, err := newReconnectingProducerTransport(
		context.Background(),
		func(context.Context) (producerSession, error) {
			openCalls++
			return <-sendSessions, nil
		},
		nil,
	)
	if err != nil {
		t.Fatalf("open bounded producer transport: %v", err)
	}
	sendTransport.retryDelay = 0
	if err := sendTransport.Send(context.Background(), rabbitstream.Message{}, func(rabbitstream.TransportConfirmation) {}); !errors.Is(err, rabbitstream.ErrConnection) {
		t.Fatalf("bounded producer Send() error = %v", err)
	}
	if openCalls != 2 {
		t.Fatalf("bounded producer opens = %d", openCalls)
	}
	_ = sendTransport.Close()

	initialConsumer := &fakeConsumerSession{}
	recoveredConsumer := &fakeConsumerSession{}
	consumerSessions := make(chan consumerSession, 2)
	consumerSessions <- initialConsumer
	consumerSessions <- recoveredConsumer
	reconnectingConsumer, err := newReconnectingConsumerTransport(
		context.Background(),
		func(context.Context, bool) (consumerSession, error) { return <-consumerSessions, nil },
		nil,
	)
	if err != nil {
		t.Fatalf("open consumer transport: %v", err)
	}
	reconnectingConsumer.retryDelay = 0
	reconnectingConsumer.invalidate(initialConsumer, rabbitstream.ErrConnection)
	currentConsumerSession, err := reconnectingConsumer.session(context.Background())
	if err != nil || currentConsumerSession != recoveredConsumer {
		t.Fatalf("reconnect consumer session = %#v, %v", currentConsumerSession, err)
	}
	reconnectingConsumer.mutex.Lock()
	consumerCurrent, consumerLastErr, consumerRetryAfter, consumerFailures :=
		reconnectingConsumer.current, reconnectingConsumer.lastErr, reconnectingConsumer.retryAfter, reconnectingConsumer.reconnectFailures
	reconnectingConsumer.mutex.Unlock()
	if consumerCurrent != recoveredConsumer || consumerLastErr != nil || !consumerRetryAfter.IsZero() || consumerFailures != 0 {
		t.Fatalf(
			"reconnected consumer state = %#v, %v, %v, %d",
			consumerCurrent, consumerLastErr, consumerRetryAfter, consumerFailures,
		)
	}
	_ = reconnectingConsumer.Close()

	boundedInitial := &fakeConsumerSession{storeErr: rabbitstream.ErrConnection}
	boundedSecond := &fakeConsumerSession{storeErr: rabbitstream.ErrConnection}
	boundedThird := &fakeConsumerSession{}
	boundedSessions := make(chan consumerSession, 3)
	boundedSessions <- boundedInitial
	boundedSessions <- boundedSecond
	boundedSessions <- boundedThird
	consumerOpenCalls := 0
	boundedConsumer, err := newReconnectingConsumerTransport(
		context.Background(),
		func(context.Context, bool) (consumerSession, error) {
			consumerOpenCalls++
			return <-boundedSessions, nil
		},
		nil,
	)
	if err != nil {
		t.Fatalf("open bounded consumer transport: %v", err)
	}
	boundedConsumer.retryDelay = 0
	if err := boundedConsumer.StoreOffset(context.Background(), "tracking.events", 1); !errors.Is(err, rabbitstream.ErrConnection) {
		t.Fatalf("bounded consumer StoreOffset() error = %v", err)
	}
	if consumerOpenCalls != 2 {
		t.Fatalf("bounded consumer opens = %d", consumerOpenCalls)
	}
	_ = boundedConsumer.Close()

	reconnectFailure := errors.New("consumer reconnect failure")
	reconnectCalls := 0
	boundedReconnect := &consumerTransport{
		opener: func(context.Context, bool) (consumerSession, error) {
			reconnectCalls++
			return nil, rabbitstream.ErrConnection
		},
		lastErr:              reconnectFailure,
		maxReconnectAttempts: 1,
		reconnectFailures:    1,
		done:                 make(chan struct{}),
	}
	if _, err := boundedReconnect.session(context.Background()); !errors.Is(err, reconnectFailure) {
		t.Fatalf("bounded consumer reconnect error = %v", err)
	}
	if reconnectCalls != 0 {
		t.Fatalf("bounded consumer reconnect calls = %d", reconnectCalls)
	}
}

func TestWireDeliveryRejectsUnsupportedMetadataWithoutLeakingIt(t *testing.T) {
	tests := map[string]any{
		"wire type":              struct{}{},
		"multiple data sections": &amqp.Message{Data: [][]byte{[]byte("one"), []byte("two")}},
		"message ID":             &amqp.Message{Properties: &amqp.MessageProperties{MessageID: 41}},
		"correlation ID":         &amqp.Message{Properties: &amqp.MessageProperties{CorrelationID: true}},
		"annotation key":         &amqp.Message{Annotations: amqp.Annotations{uint64(1): "value"}},
		"annotation value":       &amqp.Message{Annotations: amqp.Annotations{"key": uint64(1)}},
		"application property":   &amqp.Message{ApplicationProperties: map[string]any{"key": uint64(1)}},
	}
	for name, wire := range tests {
		wire := wire
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			_, err := fromWireMessage("", "tracking.events", 1, wire)
			if !errors.Is(err, rabbitstream.ErrValidation) || err.Error() != "rabbitstream consume failed: validation" {
				t.Fatalf("fromWireMessage() error = %v", err)
			}
		})
	}
}

func TestWireDeliveryAcceptsStringAndByteMetadataRepresentations(t *testing.T) {
	t.Parallel()

	delivery, err := fromWireMessage("", "tracking.events", 1, &amqp.Message{
		Properties:            &amqp.MessageProperties{MessageID: []byte("event"), CorrelationID: "correlation"},
		Annotations:           amqp.Annotations{"bytes": []byte("header"), "string": "header"},
		ApplicationProperties: map[string]any{"bytes": []byte("property"), "string": "property"},
	})
	if err != nil {
		t.Fatalf("fromWireMessage() error = %v", err)
	}
	if delivery.MessageID != "event" || delivery.CorrelationID != "correlation" ||
		len(delivery.Headers) != 2 || len(delivery.Properties) != 2 {
		t.Fatalf("delivery = %#v", delivery)
	}
}

func TestBrokerErrorsMapToStablePolicyCategories(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		err  error
		want rabbitstream.ErrorCategory
	}{
		"authentication policy":   {rabbitstream.ErrAuthentication, rabbitstream.CategoryAuthentication},
		"authentication broker":   {stream.AuthenticationFailure, rabbitstream.CategoryAuthentication},
		"authentication loopback": {stream.AuthenticationFailureLoopbackError, rabbitstream.CategoryAuthentication},
		"authorization policy":    {rabbitstream.ErrAuthorization, rabbitstream.CategoryAuthorization},
		"virtual host":            {stream.VirtualHostAccessFailure, rabbitstream.CategoryAuthorization},
		"access refused":          {stream.CodeAccessRefused, rabbitstream.CategoryAuthorization},
		"configuration":           {rabbitstream.ErrInvalidConfiguration, rabbitstream.CategoryInvalidConfiguration},
		"stream policy":           {rabbitstream.ErrStreamUnavailable, rabbitstream.CategoryStreamUnavailable},
		"stream missing":          {stream.StreamDoesNotExist, rabbitstream.CategoryStreamUnavailable},
		"stream unavailable":      {stream.StreamNotAvailable, rabbitstream.CategoryStreamUnavailable},
		"size policy":             {rabbitstream.ErrMessageTooLarge, rabbitstream.CategoryMessageTooLarge},
		"frame size":              {stream.FrameTooLarge, rabbitstream.CategoryMessageTooLarge},
		"canceled":                {context.Canceled, rabbitstream.CategoryCanceled},
		"timeout":                 {context.DeadlineExceeded, rabbitstream.CategoryTimeout},
		"connection":              {errors.New("socket unavailable"), rabbitstream.CategoryConnection},
	}
	for name, test := range tests {
		test := test
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			if got := brokerErrorCategory(test.err); got != test.want {
				t.Fatalf("brokerErrorCategory() = %q, want %q", got, test.want)
			}
		})
	}
}

func TestEnvironmentOptionsPreserveConnectionSecurityPolicy(t *testing.T) {
	t.Parallel()

	tlsConfig := &tls.Config{MinVersion: tls.VersionTLS13}
	connection := rabbitstream.ConnectionConfig{
		VirtualHost: "/tracking",
		Heartbeat:   9 * time.Second,
		Security:    rabbitstream.SecurityConfig{TLS: tlsConfig},
	}
	options := environmentOptions(
		connection,
		rabbitstream.Endpoint{Host: "rabbitmq.internal", Port: 5551},
		rabbitstream.Credentials{Username: "publisher", Password: []byte("credential")},
		7*time.Second,
	)
	if len(options.ConnectionParameters) != 1 {
		t.Fatalf("connection parameters = %d", len(options.ConnectionParameters))
	}
	broker := options.ConnectionParameters[0]
	if broker.Host != "rabbitmq.internal" || broker.Port != "5551" || broker.User != "publisher" ||
		broker.Vhost != "/tracking" || broker.Scheme != "rabbitmq-stream+tls" ||
		options.TCPParameters.RequestedHeartbeat != 9*time.Second || options.RPCTimeout != 7*time.Second {
		t.Fatalf("environment options = %#v, broker = %#v", options, broker)
	}
	if tlsConfig.ServerName != "" {
		t.Fatal("environmentOptions() mutated caller-owned TLS configuration")
	}
}

func TestOperationDeadlineUsesContextOrBoundedFallback(t *testing.T) {
	t.Parallel()

	deadline := time.Now().Add(time.Minute)
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()
	if got := operationDeadline(ctx, time.Second); !got.Equal(deadline) {
		t.Fatalf("context deadline = %v, want %v", got, deadline)
	}
	before := time.Now().Add(time.Second)
	got := operationDeadline(context.Background(), time.Second)
	after := time.Now().Add(time.Second)
	if got.Before(before) || got.After(after) {
		t.Fatalf("fallback deadline = %v, want within [%v, %v]", got, before, after)
	}
}

func TestFreshEnvironmentOpeningCoversBoundedLifecycleOutcomes(t *testing.T) {
	connection := rabbitstream.ConnectionConfig{
		Endpoints:             []rabbitstream.Endpoint{{Host: "localhost", Port: 5552}},
		Credentials:           rabbitstream.StaticCredentials("test", []byte("not-a-real-secret")),
		Security:              rabbitstream.DevelopmentPlaintextSecurity(),
		ConnectTimeout:        time.Second,
		RPCTimeout:            time.Second,
		MaxReconnectAttempts:  1,
		InitialReconnectDelay: time.Millisecond,
		MaxReconnectBackoff:   time.Millisecond,
	}
	opened := &fakeRabbitEnvironment{}
	if environment, err := openFreshEnvironmentWith(
		context.Background(), connection,
		func(*stream.EnvironmentOptions) (producerEnvironment, error) { return opened, nil },
	); err != nil || environment != opened {
		t.Fatalf("successful open = %#v, %v", environment, err)
	}

	invalidCredentials := connection
	invalidCredentials.Credentials = credentialProviderFunc(func(context.Context) (rabbitstream.Credentials, error) {
		return rabbitstream.Credentials{}, nil
	})
	if environment, err := openFreshEnvironmentWith(
		context.Background(), invalidCredentials,
		func(*stream.EnvironmentOptions) (producerEnvironment, error) { return opened, nil },
	); environment != nil || !errors.Is(err, rabbitstream.ErrInvalidConfiguration) {
		t.Fatalf("invalid-credential open = %#v, %v", environment, err)
	}

	credentialFailure := connection
	credentialFailure.Credentials = credentialProviderFunc(func(context.Context) (rabbitstream.Credentials, error) {
		return rabbitstream.Credentials{}, errors.New("credential backend")
	})
	if environment, err := openFreshEnvironmentWith(
		context.Background(), credentialFailure,
		func(*stream.EnvironmentOptions) (producerEnvironment, error) { return opened, nil },
	); environment != nil || !errors.Is(err, rabbitstream.ErrAuthentication) {
		t.Fatalf("credential-failure open = %#v, %v", environment, err)
	}

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if environment, err := openFreshEnvironmentWith(
		canceled, connection,
		func(*stream.EnvironmentOptions) (producerEnvironment, error) { return opened, nil },
	); environment != nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled credential open = %#v, %v", environment, err)
	}
	ignoresCancellation := connection
	ignoresCancellation.Credentials = credentialProviderFunc(func(context.Context) (rabbitstream.Credentials, error) {
		return rabbitstream.Credentials{Username: "test", Password: []byte("not-a-real-secret")}, nil
	})
	if environment, err := openFreshEnvironmentWith(
		canceled, ignoresCancellation,
		func(*stream.EnvironmentOptions) (producerEnvironment, error) { return opened, nil },
	); environment != nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled attempt-budget open = %#v, %v", environment, err)
	}
	expired, cancelExpired := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancelExpired()
	if environment, err := openFreshEnvironmentWith(
		expired, ignoresCancellation,
		func(*stream.EnvironmentOptions) (producerEnvironment, error) { return opened, nil },
	); environment != nil || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expired attempt-budget open = %#v, %v", environment, err)
	}

	invalidTimeout := connection
	invalidTimeout.RPCTimeout = -time.Second
	if environment, err := openFreshEnvironmentWith(
		context.Background(), invalidTimeout,
		func(*stream.EnvironmentOptions) (producerEnvironment, error) { return opened, nil },
	); environment != nil || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("invalid attempt timeout open = %#v, %v", environment, err)
	}

	authentication := connection
	if environment, err := openFreshEnvironmentWith(
		context.Background(), authentication,
		func(*stream.EnvironmentOptions) (producerEnvironment, error) {
			return nil, stream.AuthenticationFailure
		},
	); environment != nil || !errors.Is(err, rabbitstream.ErrAuthentication) {
		t.Fatalf("authentication open = %#v, %v", environment, err)
	}

	noAttempts := connection
	noAttempts.MaxReconnectAttempts = 0
	if environment, err := openFreshEnvironmentWith(
		context.Background(), noAttempts,
		func(*stream.EnvironmentOptions) (producerEnvironment, error) { return opened, nil },
	); environment != nil || !errors.Is(err, rabbitstream.ErrConnection) {
		t.Fatalf("zero-attempt open = %#v, %v", environment, err)
	}

	retrying := connection
	retrying.MaxReconnectAttempts = 2
	openCalls := 0
	if environment, err := openFreshEnvironmentWith(
		context.Background(), retrying,
		func(*stream.EnvironmentOptions) (producerEnvironment, error) {
			openCalls++
			if openCalls == 1 {
				return nil, rabbitstream.ErrConnection
			}
			return opened, nil
		},
	); err != nil || environment != opened || openCalls != 2 {
		t.Fatalf("retried open = %#v, %v after %d calls", environment, err, openCalls)
	}

	started := make(chan struct{})
	release := make(chan struct{})
	late := &fakeRabbitEnvironment{closeCalled: make(chan struct{})}
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		_, err := openFreshEnvironmentWith(ctx, connection, func(*stream.EnvironmentOptions) (producerEnvironment, error) {
			close(started)
			<-release
			return late, nil
		})
		result <- err
	}()
	<-started
	cancel()
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled in-flight open error = %v", err)
	}
	close(release)
	<-late.closeCalled

	backoffStarted := make(chan struct{})
	cancelBackoff := retrying
	cancelBackoff.InitialReconnectDelay = time.Hour
	cancelBackoff.MaxReconnectBackoff = time.Hour
	ctx, cancel = context.WithCancel(context.Background())
	go func() {
		_, err := openFreshEnvironmentWith(ctx, cancelBackoff, func(*stream.EnvironmentOptions) (producerEnvironment, error) {
			close(backoffStarted)
			return nil, rabbitstream.ErrConnection
		})
		result <- err
	}()
	<-backoffStarted
	cancel()
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled backoff open error = %v", err)
	}

	wantWrap := errors.New("open environment")
	if environment, err := wrapStreamEnvironment(nil, wantWrap); environment != nil || !errors.Is(err, wantWrap) {
		t.Fatalf("nil wrapped environment = %#v, %v", environment, err)
	}
	if environment, err := wrapStreamEnvironment(&stream.Environment{}, nil); environment == nil || err != nil {
		t.Fatalf("wrapped environment = %#v, %v", environment, err)
	}
	timer := time.NewTimer(time.Hour)
	stopAndDrainTimer(timer)
	drainStoppedTimer(false, make(chan time.Time))
	fired := make(chan time.Time, 1)
	fired <- time.Now()
	drainStoppedTimer(false, fired)
}

func TestObserverFailuresCannotAffectDeliveryPolicy(t *testing.T) {
	t.Parallel()

	safeObserve(nil, rabbitstream.Observation{Kind: rabbitstream.ObservationConnectionReady})
	safeObserve(panickingObserver{}, rabbitstream.Observation{Kind: rabbitstream.ObservationConnectionReady})
	observer := &recordingObserver{}
	safeObserve(observer, rabbitstream.Observation{Kind: rabbitstream.ObservationConnectionReady})
	if observer.calls != 1 {
		t.Fatalf("observer calls = %d", observer.calls)
	}
}

func TestAdapterConstructorsRejectInvalidPoliciesWithoutConnecting(t *testing.T) {
	t.Parallel()

	if inspector, err := NewInspector(rabbitstream.ConnectionConfig{}, rabbitstream.DefaultLimits()); inspector != nil || !errors.Is(err, rabbitstream.ErrInvalidConfiguration) {
		t.Fatalf("NewInspector() = %#v, %v", inspector, err)
	}
	if replayer, err := NewReplayer(rabbitstream.ConnectionConfig{}, rabbitstream.DefaultLimits()); replayer != nil || !errors.Is(err, rabbitstream.ErrInvalidConfiguration) {
		t.Fatalf("NewReplayer() = %#v, %v", replayer, err)
	}
	connection := rabbitstream.ConnectionConfig{
		Endpoints:   []rabbitstream.Endpoint{{Host: "rabbitmq.internal", Port: 5551}},
		Credentials: rabbitstream.StaticCredentials("user", []byte("credential")),
		Security:    rabbitstream.DevelopmentPlaintextSecurity(),
	}
	if inspector, err := NewInspector(connection, rabbitstream.Limits{}); inspector == nil || err != nil {
		t.Fatalf("default-limits NewInspector() = %#v, %v", inspector, err)
	}
	if replayer, err := NewReplayer(connection, rabbitstream.Limits{}); replayer == nil || err != nil {
		t.Fatalf("default-limits NewReplayer() = %#v, %v", replayer, err)
	}
	invalidLimits := rabbitstream.DefaultLimits()
	invalidLimits.MaxPayloadBytes = -1
	if inspector, err := NewInspector(connection, invalidLimits); inspector != nil || !errors.Is(err, rabbitstream.ErrValidation) {
		t.Fatalf("invalid-limits NewInspector() = %#v, %v", inspector, err)
	}
	if replayer, err := NewReplayer(connection, invalidLimits); replayer != nil || !errors.Is(err, rabbitstream.ErrValidation) {
		t.Fatalf("invalid-limits NewReplayer() = %#v, %v", replayer, err)
	}
}

func TestInspectionErrorsExposeOnlyStableCategories(t *testing.T) {
	t.Parallel()

	err := inspectError(stream.CodeAccessRefused)
	if !errors.Is(err, rabbitstream.ErrAuthorization) || err.Error() != "rabbitstream inspect failed: authorization" {
		t.Fatalf("inspectError() = %v", err)
	}
}

func TestStoredOffsetInspectionDistinguishesMissingInvalidAndLaggingOffsets(t *testing.T) {
	inspection := rabbitstream.StreamInspection{}
	if err := applyStoredOffset(&inspection, 0, stream.OffsetNotFoundError); err != nil || inspection.StoredOffset != nil {
		t.Fatalf("missing stored offset = %#v, %v", inspection, err)
	}
	if err := applyStoredOffset(&inspection, 0, errors.New("offset query")); err == nil {
		t.Fatal("failed stored-offset query was accepted")
	}
	if err := applyStoredOffset(&inspection, -1, nil); err != nil || inspection.StoredOffset != nil {
		t.Fatalf("negative stored offset = %#v, %v", inspection, err)
	}
	last := uint64(9)
	inspection.LastOffset = &last
	if err := applyStoredOffset(&inspection, 4, nil); err != nil || inspection.StoredOffset == nil || *inspection.StoredOffset != 4 || inspection.Lag == nil || *inspection.Lag != 5 {
		t.Fatalf("lagging stored offset = %#v, %v", inspection, err)
	}
	ahead := rabbitstream.StreamInspection{LastOffset: &last}
	if err := applyStoredOffset(&ahead, 10, nil); err != nil || ahead.StoredOffset == nil || ahead.Lag != nil {
		t.Fatalf("ahead stored offset = %#v, %v", ahead, err)
	}
}

func TestInspectorReadsStoredOffsetWithoutSnapshottingStreamRange(t *testing.T) {
	t.Parallel()

	stored := int64(41)
	environment := &fakeRabbitEnvironment{queryOffset: stored}
	inspector := &Inspector{
		limits: rabbitstream.DefaultLimits(),
		openEnvironment: func(context.Context) (rabbitEnvironment, error) {
			return environment, nil
		},
	}
	offset, err := inspector.StoredOffset(
		context.Background(), "tracking.events", "delivery-planner",
	)
	if err != nil || offset == nil || *offset != uint64(stored) {
		t.Fatalf("StoredOffset() = %#v, %v", offset, err)
	}
	if environment.streamStatsCalls != 0 {
		t.Fatalf("StoredOffset() snapshotted stream range %d times", environment.streamStatsCalls)
	}
	if environment.closeCalls != 1 {
		t.Fatalf("StoredOffset() closed environment %d times", environment.closeCalls)
	}
}

func TestInspectorStoredOffsetRejectsInvalidInputsAndClassifiesBrokerResults(t *testing.T) {
	t.Parallel()

	inspector := &Inspector{limits: rabbitstream.DefaultLimits()}
	if offset, err := inspector.StoredOffset(nil, "tracking.events", "consumer"); offset != nil || !errors.Is(err, rabbitstream.ErrInvalidConfiguration) {
		t.Fatalf("nil-context StoredOffset() = %#v, %v", offset, err)
	}
	if offset, err := inspector.StoredOffset(context.Background(), "", "consumer"); offset != nil || !errors.Is(err, rabbitstream.ErrValidation) {
		t.Fatalf("invalid-request StoredOffset() = %#v, %v", offset, err)
	}
	openFailure := errors.New("open environment")
	inspector.openEnvironment = func(context.Context) (rabbitEnvironment, error) {
		return nil, openFailure
	}
	if offset, err := inspector.StoredOffset(
		context.Background(), "tracking.events", "consumer",
	); offset != nil || !errors.Is(err, rabbitstream.ErrConnection) {
		t.Fatalf("open-failure StoredOffset() = %#v, %v", offset, err)
	}
	for name, test := range map[string]struct {
		stored int64
		err    error
		want   error
	}{
		"missing":  {err: stream.OffsetNotFoundError},
		"negative": {stored: -1, want: rabbitstream.ErrOffset},
		"failure":  {err: errors.New("query offset"), want: rabbitstream.ErrConnection},
	} {
		t.Run(name, func(t *testing.T) {
			environment := &fakeRabbitEnvironment{
				queryOffset: test.stored, queryOffsetErr: test.err,
			}
			inspector.openEnvironment = func(context.Context) (rabbitEnvironment, error) {
				return environment, nil
			}
			offset, err := inspector.StoredOffset(
				context.Background(), "tracking.events", "consumer",
			)
			if offset != nil || !errors.Is(err, test.want) {
				t.Fatalf("StoredOffset() = %#v, %v", offset, err)
			}
			if environment.closeCalls != 1 {
				t.Fatalf("StoredOffset() closed environment %d times", environment.closeCalls)
			}
		})
	}
}

func TestStreamStatisticsInspectionIncludesOnlyAvailableNonNegativeChunkIDs(t *testing.T) {
	base := rabbitstream.StreamInspection{Stream: "tracking.events"}
	for name, test := range map[string]struct {
		stats         fakeStreamStatistics
		want          uint64
		wantAvailable bool
	}{
		"available": {
			stats:         fakeStreamStatistics{firstErr: errors.New("first offset unavailable"), committed: 7},
			want:          7,
			wantAvailable: true,
		},
		"query failure": {
			stats: fakeStreamStatistics{
				firstErr: errors.New("first offset unavailable"), committed: 7,
				committedErr: errors.New("committed chunk unavailable"),
			},
		},
		"negative": {
			stats: fakeStreamStatistics{firstErr: errors.New("first offset unavailable"), committed: -1},
		},
	} {
		t.Run(name, func(t *testing.T) {
			inspection, err := inspectStreamStats(
				context.Background(), &fakeRabbitEnvironment{}, base, "", test.stats,
			)
			if err != nil {
				t.Fatalf("inspectStreamStats() error = %v", err)
			}
			if !test.wantAvailable {
				if inspection.CommittedChunkID != nil {
					t.Fatalf("committed chunk ID = %d, want unavailable", *inspection.CommittedChunkID)
				}
				return
			}
			if inspection.CommittedChunkID == nil || *inspection.CommittedChunkID != test.want {
				t.Fatalf("committed chunk ID = %#v, want %d", inspection.CommittedChunkID, test.want)
			}
		})
	}
}

func TestRetainedStartDistinguishesEmptyInvalidAndAvailableHistory(t *testing.T) {
	t.Parallel()

	if first, empty, err := retainedStart(0, errors.New("empty stream")); err != nil || !empty || first != 0 {
		t.Fatalf("empty retained start = %d, %t, %v", first, empty, err)
	}
	if _, empty, err := retainedStart(-1, nil); !errors.Is(err, rabbitstream.ErrReplayRange) || empty {
		t.Fatalf("negative retained start = empty %t, error %v", empty, err)
	}
	if first, empty, err := retainedStart(4, nil); err != nil || empty || first != 4 {
		t.Fatalf("available retained start = %d, %t, %v", first, empty, err)
	}
	if retained, err := retainedRangeFromStats(
		context.Background(), &fakeRabbitEnvironment{}, "tracking.events",
		fakeStreamStatistics{firstErr: errors.New("empty")},
	); err != nil || !retained.Empty {
		t.Fatalf("empty retained range = %#v, %v", retained, err)
	}
	if _, err := retainedRangeFromStats(
		context.Background(), &fakeRabbitEnvironment{}, "tracking.events",
		fakeStreamStatistics{first: -1},
	); !errors.Is(err, rabbitstream.ErrReplayRange) {
		t.Fatalf("negative retained range error = %v", err)
	}
}

func TestReplayTargetPrefersAnExplicitBackingPartition(t *testing.T) {
	t.Parallel()

	if got := replayTarget(rabbitstream.ReplayRequest{Stream: "tracking.events"}); got != "tracking.events" {
		t.Fatalf("stream replay target = %q", got)
	}
	if got := replayTarget(rabbitstream.ReplayRequest{Stream: "tracking", Partition: "tracking-1"}); got != "tracking-1" {
		t.Fatalf("partition replay target = %q", got)
	}
}

func TestInspectorHandlesTopologyAndDependencyBoundaries(t *testing.T) {
	limits := rabbitstream.DefaultLimits()
	openCalls := 0
	inspector := &Inspector{
		limits: limits,
		openEnvironment: func(context.Context) (rabbitEnvironment, error) {
			openCalls++
			return nil, rabbitstream.ErrConnection
		},
	}
	var nilContext context.Context
	if _, err := inspector.Inspect(nilContext, rabbitstream.InspectionRequest{Stream: "tracking.events"}); !errors.Is(err, rabbitstream.ErrInvalidConfiguration) || openCalls != 0 {
		t.Fatalf("nil-context Inspect() error = %v, opens = %d", err, openCalls)
	}
	if health := inspector.Health(nilContext); health.State != rabbitstream.DependencyUnavailable || health.Category != rabbitstream.CategoryInvalidConfiguration || openCalls != 0 {
		t.Fatalf("nil-context Health() = %#v, opens = %d", health, openCalls)
	}
	if _, err := inspector.Inspect(context.Background(), rabbitstream.InspectionRequest{}); !errors.Is(err, rabbitstream.ErrValidation) || openCalls != 0 {
		t.Fatalf("invalid Inspect() error = %v, opens = %d", err, openCalls)
	}
	if _, err := inspector.Inspect(context.Background(), rabbitstream.InspectionRequest{Stream: "tracking.events"}); !errors.Is(err, rabbitstream.ErrConnection) {
		t.Fatalf("open-failure Inspect() error = %v", err)
	}
	if health := inspector.Health(context.Background()); health.State != rabbitstream.DependencyUnavailable || health.Category != rabbitstream.CategoryConnection {
		t.Fatalf("unavailable Health() = %#v", health)
	}

	healthEnvironment := &fakeRabbitEnvironment{}
	inspector.openEnvironment = func(context.Context) (rabbitEnvironment, error) { return healthEnvironment, nil }
	if health := inspector.Health(context.Background()); health.State != rabbitstream.DependencyHealthy || healthEnvironment.closeCalls != 1 {
		t.Fatalf("healthy Health() = %#v, closes = %d", health, healthEnvironment.closeCalls)
	}

	queryFailure := &fakeRabbitEnvironment{queryPartitionsErr: stream.StreamNotAvailable}
	inspector.openEnvironment = func(context.Context) (rabbitEnvironment, error) { return queryFailure, nil }
	if _, err := inspector.Inspect(context.Background(), rabbitstream.InspectionRequest{SuperStream: "tracking"}); !errors.Is(err, rabbitstream.ErrStreamUnavailable) {
		t.Fatalf("topology-failure Inspect() error = %v", err)
	}

	for name, partitions := range map[string][]string{
		"empty":     nil,
		"oversized": make([]string, rabbitstream.MaxSuperStreamPartitions+1),
	} {
		environment := &fakeRabbitEnvironment{partitions: partitions}
		inspector.openEnvironment = func(context.Context) (rabbitEnvironment, error) { return environment, nil }
		if _, err := inspector.Inspect(context.Background(), rabbitstream.InspectionRequest{SuperStream: "tracking"}); !errors.Is(err, rabbitstream.ErrPartitionUnavailable) {
			t.Fatalf("%s topology Inspect() error = %v", name, err)
		}
	}

	missing := &fakeRabbitEnvironment{exists: false}
	inspector.openEnvironment = func(context.Context) (rabbitEnvironment, error) { return missing, nil }
	result, err := inspector.Inspect(context.Background(), rabbitstream.InspectionRequest{Stream: "tracking.events"})
	if err != nil || len(result.Partitions) != 1 || result.Partitions[0].Exists {
		t.Fatalf("missing-stream Inspect() = %#v, %v", result, err)
	}

	existsFailure := &fakeRabbitEnvironment{existsErr: errors.New("existence lookup")}
	inspector.openEnvironment = func(context.Context) (rabbitEnvironment, error) { return existsFailure, nil }
	if _, err := inspector.Inspect(context.Background(), rabbitstream.InspectionRequest{Stream: "tracking.events"}); err == nil {
		t.Fatal("Inspect() accepted a failed existence lookup")
	}

	statsFailure := &fakeRabbitEnvironment{exists: true, statsErr: errors.New("stats lookup")}
	inspector.openEnvironment = func(context.Context) (rabbitEnvironment, error) { return statsFailure, nil }
	if _, err := inspector.Inspect(context.Background(), rabbitstream.InspectionRequest{Stream: "tracking.events"}); err == nil {
		t.Fatal("Inspect() accepted a failed stats lookup")
	}

	snapshotFailure := &fakeRabbitEnvironment{exists: true, stats: &stream.StreamStats{}, newConsumerErr: errors.New("snapshot consumer")}
	inspector.openEnvironment = func(context.Context) (rabbitEnvironment, error) { return snapshotFailure, nil }
	if _, err := inspector.Inspect(context.Background(), rabbitstream.InspectionRequest{Stream: "tracking.events"}); err == nil {
		t.Fatal("Inspect() accepted a failed retained-range snapshot")
	}

	offsetFailure := errors.New("stored offset lookup")
	if _, err := inspectStreamStats(
		context.Background(),
		&fakeRabbitEnvironment{queryOffsetErr: offsetFailure},
		rabbitstream.StreamInspection{Stream: "tracking.events", Exists: true},
		"tracking-indexer",
		fakeStreamStatistics{firstErr: errors.New("empty"), committedErr: errors.New("empty")},
	); !errors.Is(err, offsetFailure) {
		t.Fatalf("stored-offset inspection error = %v", err)
	}
}

func TestReplaySourceRejectsUnavailableAndChangedHistoryBoundaries(t *testing.T) {
	request := rabbitstream.ReplayRequest{Stream: "tracking.events"}
	source := &replaySource{limits: rabbitstream.DefaultLimits()}
	source.openEnvironment = func(context.Context) (rabbitEnvironment, error) { return nil, rabbitstream.ErrConnection }
	if _, err := source.RetainedRange(context.Background(), request); !errors.Is(err, rabbitstream.ErrConnection) {
		t.Fatalf("open-failure RetainedRange() error = %v", err)
	}

	missing := &fakeRabbitEnvironment{exists: false}
	source.openEnvironment = func(context.Context) (rabbitEnvironment, error) { return missing, nil }
	if _, err := source.RetainedRange(context.Background(), request); !errors.Is(err, rabbitstream.ErrStreamUnavailable) {
		t.Fatalf("missing RetainedRange() error = %v", err)
	}

	existsFailure := &fakeRabbitEnvironment{existsErr: errors.New("existence lookup")}
	source.openEnvironment = func(context.Context) (rabbitEnvironment, error) { return existsFailure, nil }
	if _, err := source.RetainedRange(context.Background(), request); err == nil {
		t.Fatal("RetainedRange() accepted a failed existence lookup")
	}

	statsFailure := &fakeRabbitEnvironment{exists: true, statsErr: errors.New("stats lookup")}
	source.openEnvironment = func(context.Context) (rabbitEnvironment, error) { return statsFailure, nil }
	if _, err := source.RetainedRange(context.Background(), request); err == nil {
		t.Fatal("RetainedRange() accepted a failed stats lookup")
	}

	snapshotFailure := &fakeRabbitEnvironment{exists: true, stats: &stream.StreamStats{}, newConsumerErr: errors.New("snapshot consumer")}
	source.openEnvironment = func(context.Context) (rabbitEnvironment, error) { return snapshotFailure, nil }
	if _, err := source.RetainedRange(context.Background(), request); err == nil {
		t.Fatal("RetainedRange() accepted a failed last-offset snapshot")
	}

	if cursor, err := source.Open(context.Background(), request); cursor != nil || !errors.Is(err, rabbitstream.ErrReplayRange) {
		t.Fatalf("unbounded Open() = %#v, %v", cursor, err)
	}
	end := uint64(4)
	request.EndOffset = &end
	source.openEnvironment = func(context.Context) (rabbitEnvironment, error) { return nil, rabbitstream.ErrConnection }
	if cursor, err := source.Open(context.Background(), request); cursor != nil || !errors.Is(err, rabbitstream.ErrConnection) {
		t.Fatalf("open-failure Open() = %#v, %v", cursor, err)
	}
	source.openEnvironment = func(context.Context) (rabbitEnvironment, error) { return snapshotFailure, nil }
	if cursor, err := source.Open(context.Background(), request); cursor != nil || err == nil {
		t.Fatalf("consumer-failure Open() = %#v, %v", cursor, err)
	}
}

func TestReplayTopologyMustRemainExactAndOrdered(t *testing.T) {
	if err := ensureReplayTopology(&fakeRabbitEnvironment{}, rabbitstream.ReplayRequest{Stream: "tracking.events"}); err != nil {
		t.Fatalf("single-stream topology error = %v", err)
	}
	request := rabbitstream.ReplayRequest{SuperStream: "tracking", ExpectedPartitions: []string{"tracking-0", "tracking-1"}}
	if err := ensureReplayTopology(&fakeRabbitEnvironment{queryPartitionsErr: stream.StreamNotAvailable}, request); !errors.Is(err, rabbitstream.ErrStreamUnavailable) {
		t.Fatalf("query topology error = %v", err)
	}
	for name, partitions := range map[string][]string{
		"empty": nil,
		"count": {"tracking-0"},
		"order": {"tracking-1", "tracking-0"},
	} {
		if err := ensureReplayTopology(&fakeRabbitEnvironment{partitions: partitions}, request); !errors.Is(err, rabbitstream.ErrPartitionUnavailable) {
			t.Fatalf("%s topology error = %v", name, err)
		}
	}
	if err := ensureReplayTopology(&fakeRabbitEnvironment{partitions: append([]string(nil), request.ExpectedPartitions...)}, request); err != nil {
		t.Fatalf("exact topology error = %v", err)
	}
}

func TestReplayCursorClosePreservesEnvironmentFailure(t *testing.T) {
	environment := &fakeRabbitEnvironment{closeErr: errors.New("close replay environment")}
	cursor := newReplayCursorForTest()
	cursor.environment = environment
	if err := cursor.Close(); !errors.Is(err, environment.closeErr) {
		t.Fatalf("first Close() error = %v", err)
	}
	if err := cursor.Close(); !errors.Is(err, environment.closeErr) {
		t.Fatalf("second Close() error = %v", err)
	}
	consumer := newFakeRabbitConsumer()
	consumer.closeErr = errors.New("close replay consumer")
	consumerCursor := newReplayCursorForTest()
	consumerCursor.environment = &fakeRabbitEnvironment{}
	consumerCursor.consumer = consumer
	if err := consumerCursor.Close(); !errors.Is(err, consumer.closeErr) {
		t.Fatalf("consumer Close() error = %v", err)
	}
}

func TestReplayChunkSnapshotRejectsNegativeAndInvalidRanges(t *testing.T) {
	if _, err := snapshotChunkLastOffset(-1, 1); !errors.Is(err, rabbitstream.ErrReplayRange) {
		t.Fatalf("negative snapshot error = %v", err)
	}
	if _, err := snapshotChunkLastOffset(4, 0); !errors.Is(err, rabbitstream.ErrReplayRange) {
		t.Fatalf("empty snapshot error = %v", err)
	}
	if last, err := snapshotChunkLastOffset(4, 3); err != nil || last != 6 {
		t.Fatalf("snapshot last offset = %d, %v", last, err)
	}
}

func TestReplayCursorAcceptsOnlyTheRequestedActiveRange(t *testing.T) {
	valid := newReplayCursorForTest()
	valid.target = "tracking.events"
	valid.end = 5
	valid.limits = rabbitstream.DefaultLimits()
	valid.accept(4, &amqp.Message{Data: [][]byte{[]byte("event")}})
	if message := <-valid.messages; message.Offset != 4 || string(message.Payload) != "event" {
		t.Fatalf("accepted replay = %#v", message)
	}

	terminal := newReplayCursorForTest()
	terminal.target = "tracking.events"
	terminal.end = 5
	terminal.limits = rabbitstream.DefaultLimits()
	terminal.accept(5, &amqp.Message{Data: [][]byte{[]byte("last")}})
	if message := <-terminal.messages; message.Offset != 5 {
		t.Fatalf("terminal replay = %#v", message)
	}
	if _, err := terminal.Next(context.Background()); !errors.Is(err, io.EOF) {
		t.Fatalf("terminal replay Next() error = %v", err)
	}

	beyond := newReplayCursorForTest()
	beyond.end = 5
	beyond.accept(6, &amqp.Message{})
	if _, err := beyond.Next(context.Background()); !errors.Is(err, io.EOF) {
		t.Fatalf("beyond-end Next() error = %v", err)
	}

	negative := newReplayCursorForTest()
	negative.accept(-1, &amqp.Message{})
	if _, err := negative.Next(context.Background()); !errors.Is(err, rabbitstream.ErrReplayRange) {
		t.Fatalf("negative replay error = %v", err)
	}

	invalid := newReplayCursorForTest()
	invalid.target = "tracking.events"
	invalid.end = 5
	invalid.limits = rabbitstream.DefaultLimits()
	invalid.accept(1, &amqp.Message{Data: [][]byte{[]byte("one"), []byte("two")}})
	if _, err := invalid.Next(context.Background()); !errors.Is(err, rabbitstream.ErrValidation) {
		t.Fatalf("invalid replay error = %v", err)
	}

	closed := newReplayCursorForTest()
	close(closed.done)
	closed.accept(1, &amqp.Message{})
	completed := newReplayCursorForTest()
	completed.complete()
	completed.accept(1, &amqp.Message{})
	failed := newReplayCursorForTest()
	failed.reportFailure(rabbitstream.ErrConnection)
	failed.accept(1, &amqp.Message{})
}

func TestSnapshotAndReplayConsumerClosurePreserveConnectionOutcomes(t *testing.T) {
	brokerFailure := errors.New("consumer closed")
	failedConsumer := newFakeRabbitConsumer()
	failedConsumer.closed <- stream.Event{Err: brokerFailure}
	failedEnvironment := &fakeRabbitEnvironment{consumers: []rabbitConsumer{failedConsumer}}
	if _, err := snapshotLastOffset(context.Background(), failedEnvironment, "tracking.events"); !errors.Is(err, brokerFailure) {
		t.Fatalf("failed snapshot error = %v", err)
	}

	closedConsumer := newFakeRabbitConsumer()
	closedConsumer.closed <- stream.Event{}
	closedEnvironment := &fakeRabbitEnvironment{consumers: []rabbitConsumer{closedConsumer}}
	if _, err := snapshotLastOffset(context.Background(), closedEnvironment, "tracking.events"); !errors.Is(err, rabbitstream.ErrConnection) {
		t.Fatalf("closed snapshot error = %v", err)
	}

	canceledConsumer := newFakeRabbitConsumer()
	canceledEnvironment := &fakeRabbitEnvironment{consumers: []rabbitConsumer{canceledConsumer}}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := snapshotLastOffset(ctx, canceledEnvironment, "tracking.events"); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled snapshot error = %v", err)
	}
}

func TestReplayCursorOpenWatchesFailureCloseAndCompletion(t *testing.T) {
	newSource := func(consumer *fakeRabbitConsumer) *replaySource {
		environment := &fakeRabbitEnvironment{consumers: []rabbitConsumer{consumer}}
		return &replaySource{
			limits: rabbitstream.DefaultLimits(),
			openEnvironment: func(context.Context) (rabbitEnvironment, error) {
				return environment, nil
			},
		}
	}
	end := uint64(4)
	request := rabbitstream.ReplayRequest{
		Stream: "tracking.events", Start: rabbitstream.StartPosition{Kind: rabbitstream.OffsetStartBeginning}, EndOffset: &end,
	}

	failedConsumer := newFakeRabbitConsumer()
	cursorValue, err := newSource(failedConsumer).Open(context.Background(), request)
	if err != nil {
		t.Fatalf("failed-cursor Open() error = %v", err)
	}
	failedCursor := cursorValue.(*replayCursor)
	failedConsumer.closed <- stream.Event{Err: rabbitstream.ErrConnection}
	if _, err := failedCursor.Next(context.Background()); !errors.Is(err, rabbitstream.ErrConnection) {
		t.Fatalf("failed cursor Next() error = %v", err)
	}
	_ = failedCursor.Close()

	closedConsumer := newFakeRabbitConsumer()
	cursorValue, err = newSource(closedConsumer).Open(context.Background(), request)
	if err != nil {
		t.Fatalf("closed-cursor Open() error = %v", err)
	}
	closedCursor := cursorValue.(*replayCursor)
	if err := closedCursor.Close(); err != nil {
		t.Fatalf("closed cursor Close() error = %v", err)
	}

	completedConsumer := newFakeRabbitConsumer()
	cursorValue, err = newSource(completedConsumer).Open(context.Background(), request)
	if err != nil {
		t.Fatalf("completed-cursor Open() error = %v", err)
	}
	completedCursor := cursorValue.(*replayCursor)
	completedCursor.complete()
	if err := completedCursor.Close(); err != nil {
		t.Fatalf("completed cursor Close() error = %v", err)
	}

	topologyEnvironment := &fakeRabbitEnvironment{partitions: []string{"tracking-1"}}
	topologySource := &replaySource{
		limits: rabbitstream.DefaultLimits(),
		openEnvironment: func(context.Context) (rabbitEnvironment, error) {
			return topologyEnvironment, nil
		},
	}
	topologyRequest := request
	topologyRequest.Stream = ""
	topologyRequest.SuperStream = "tracking"
	topologyRequest.Partition = "tracking-0"
	topologyRequest.ExpectedPartitions = []string{"tracking-0"}
	if cursor, err := topologySource.Open(context.Background(), topologyRequest); cursor != nil || !errors.Is(err, rabbitstream.ErrPartitionUnavailable) || topologyEnvironment.closeCalls != 1 {
		t.Fatalf("changed-topology Open() = %#v, %v, closes %d", cursor, err, topologyEnvironment.closeCalls)
	}
}

func TestReplayCompletedDrainReturnsBufferedMessageThenEOF(t *testing.T) {
	cursor := newReplayCursorForTest()
	cursor.messages <- rabbitstream.Message{Offset: 4}
	if message, err := cursor.nextCompleted(); err != nil || message.Offset != 4 {
		t.Fatalf("buffered completed message = %#v, %v", message, err)
	}
	if _, err := cursor.nextCompleted(); !errors.Is(err, io.EOF) {
		t.Fatalf("drained completed error = %v", err)
	}
}

func TestProducerSessionOpeningRejectsEnvironmentAndTopologyFailures(t *testing.T) {
	t.Parallel()

	streamConfig, err := (rabbitstream.ProducerConfig{Stream: "tracking.events"}).Normalized()
	if err != nil {
		t.Fatalf("normalize stream producer: %v", err)
	}
	wantOpen := errors.New("open environment")
	if session, err := openProducerSessionWithEnvironment(context.Background(), streamConfig, func(context.Context) (producerEnvironment, error) {
		return nil, wantOpen
	}); session != nil || !errors.Is(err, wantOpen) {
		t.Fatalf("environment-failure producer session = %#v, %v", session, err)
	}
	producerFailure := &fakeRabbitEnvironment{newProducerErr: errors.New("open producer")}
	if session, err := openProducerSessionWithEnvironment(context.Background(), streamConfig, func(context.Context) (producerEnvironment, error) {
		return producerFailure, nil
	}); session != nil || !errors.Is(err, producerFailure.newProducerErr) || producerFailure.closeCalls != 1 {
		t.Fatalf("producer-failure session = %#v, %v, closes %d", session, err, producerFailure.closeCalls)
	}

	superConfig, err := (rabbitstream.ProducerConfig{SuperStream: "tracking", ExpectedPartitions: 2}).Normalized()
	if err != nil {
		t.Fatalf("normalize Super Stream producer: %v", err)
	}
	for name, environment := range map[string]*fakeRabbitEnvironment{
		"query":    {queryPartitionsErr: stream.StreamNotAvailable},
		"empty":    {partitions: nil},
		"count":    {partitions: []string{"tracking-0"}},
		"producer": {partitions: []string{"tracking-0", "tracking-1"}, newProducerErr: errors.New("open partition producer")},
	} {
		session, err := openProducerSessionWithEnvironment(context.Background(), superConfig, func(context.Context) (producerEnvironment, error) {
			return environment, nil
		})
		if session != nil || err == nil || environment.closeCalls != 1 {
			t.Fatalf("%s producer session = %#v, %v, closes %d", name, session, err, environment.closeCalls)
		}
	}
	uncountedSuperConfig, err := (rabbitstream.ProducerConfig{SuperStream: "tracking"}).Normalized()
	if err != nil {
		t.Fatalf("normalize uncounted Super Stream producer: %v", err)
	}
	for name, partitions := range map[string][]string{
		"oversized": make([]string, rabbitstream.MaxSuperStreamPartitions+1),
		"duplicate": {"tracking-0", "tracking-0"},
		"invalid":   {"tracking-0", " bad"},
	} {
		environment := &fakeRabbitEnvironment{
			partitions: partitions, newProducerErr: errors.New("producer fan-out must not start"),
		}
		if session, err := openProducerSessionWithEnvironment(context.Background(), uncountedSuperConfig, func(context.Context) (producerEnvironment, error) {
			return environment, nil
		}); session != nil || !errors.Is(err, rabbitstream.ErrPartitionUnavailable) || environment.producerCalls != 0 || environment.closeCalls != 1 {
			t.Fatalf("%s producer topology = %#v, %v, opens %d, closes %d", name, session, err, environment.producerCalls, environment.closeCalls)
		}
	}
}

func TestConsumerSessionOpeningRejectsEnvironmentAndTopologyFailures(t *testing.T) {
	t.Parallel()

	streamConfig, err := (rabbitstream.ConsumerConfig{Stream: "tracking.events", ConsumerName: "tracking-indexer"}).Normalized()
	if err != nil {
		t.Fatalf("normalize stream consumer: %v", err)
	}
	wantOpen := errors.New("open environment")
	if session, err := openConsumerSessionWithEnvironment(context.Background(), streamConfig, func(context.Context) (rabbitEnvironment, error) {
		return nil, wantOpen
	}); session != nil || !errors.Is(err, wantOpen) {
		t.Fatalf("environment-failure consumer session = %#v, %v", session, err)
	}
	offsetFailure := &fakeRabbitEnvironment{queryOffsetErr: errors.New("query stored offset")}
	if session, err := openConsumerSessionWithEnvironment(context.Background(), streamConfig, func(context.Context) (rabbitEnvironment, error) {
		return offsetFailure, nil
	}); session != nil || !errors.Is(err, offsetFailure.queryOffsetErr) || offsetFailure.closeCalls != 1 {
		t.Fatalf("offset-failure session = %#v, %v, closes %d", session, err, offsetFailure.closeCalls)
	}
	consumerFailure := &fakeRabbitEnvironment{newConsumerErr: errors.New("open consumer")}
	if session, err := openConsumerSessionWithEnvironment(context.Background(), streamConfig, func(context.Context) (rabbitEnvironment, error) {
		return consumerFailure, nil
	}); session != nil || !errors.Is(err, consumerFailure.newConsumerErr) || consumerFailure.closeCalls != 1 {
		t.Fatalf("consumer-failure session = %#v, %v, closes %d", session, err, consumerFailure.closeCalls)
	}

	superConfig, err := (rabbitstream.ConsumerConfig{SuperStream: "tracking", ConsumerName: "tracking-indexer"}).Normalized()
	if err != nil {
		t.Fatalf("normalize Super Stream consumer: %v", err)
	}
	for name, environment := range map[string]*fakeRabbitEnvironment{
		"query":    {queryPartitionsErr: stream.StreamNotAvailable},
		"empty":    {partitions: nil},
		"consumer": {partitions: []string{"tracking-0", "tracking-1"}, newConsumerErr: errors.New("open partition consumer")},
	} {
		session, err := openConsumerSessionWithEnvironment(context.Background(), superConfig, func(context.Context) (rabbitEnvironment, error) {
			return environment, nil
		})
		if session != nil || err == nil || environment.closeCalls != 1 {
			t.Fatalf("%s consumer session = %#v, %v, closes %d", name, session, err, environment.closeCalls)
		}
	}
	for name, partitions := range map[string][]string{
		"oversized": make([]string, rabbitstream.MaxSuperStreamPartitions+1),
		"duplicate": {"tracking-0", "tracking-0"},
		"invalid":   {"tracking-0", " bad"},
	} {
		environment := &fakeRabbitEnvironment{
			partitions: partitions, newConsumerErr: errors.New("consumer fan-out must not start"),
		}
		if session, err := openConsumerSessionWithEnvironment(context.Background(), superConfig, func(context.Context) (rabbitEnvironment, error) {
			return environment, nil
		}); session != nil || !errors.Is(err, rabbitstream.ErrPartitionUnavailable) || environment.queryOffsetCalls != 0 || environment.consumerCalls != 0 || environment.closeCalls != 1 {
			t.Fatalf(
				"%s consumer topology = %#v, %v, offset queries %d, opens %d, closes %d",
				name, session, err, environment.queryOffsetCalls, environment.consumerCalls, environment.closeCalls,
			)
		}
	}
}

func TestProducerAndConsumerSessionsOwnEveryOpenedResource(t *testing.T) {
	t.Parallel()

	streamProducerConfig, err := (rabbitstream.ProducerConfig{Stream: "tracking.events"}).Normalized()
	if err != nil {
		t.Fatalf("normalize producer: %v", err)
	}
	producer := newFakeRabbitProducer("tracking.events")
	producerFixture := &fakeRabbitEnvironment{producers: []rabbitProducer{producer}}
	producerSession, err := openProducerSessionWithEnvironment(context.Background(), streamProducerConfig, func(context.Context) (producerEnvironment, error) {
		return producerFixture, nil
	})
	if err != nil {
		t.Fatalf("open producer session: %v", err)
	}
	if err := producerSession.Close(); err != nil || producer.closeCalls != 1 || producerFixture.closeCalls != 1 {
		t.Fatalf("producer close = %v, producer calls %d, environment calls %d", err, producer.closeCalls, producerFixture.closeCalls)
	}
	if len(producerFixture.producerOptions) != 1 || producerFixture.producerOptions[0].Name != "" {
		t.Fatalf("unnamed producer options = %#v", producerFixture.producerOptions)
	}

	deduplicatedConfig, err := (rabbitstream.ProducerConfig{
		Stream: "tracking.events",
		Policy: rabbitstream.ProducerPolicy{
			Deduplication: rabbitstream.DeduplicationPublishingID,
			ProducerName:  "tracking-publisher",
		},
	}).Normalized()
	if err != nil {
		t.Fatalf("normalize deduplicated producer: %v", err)
	}
	deduplicatedEnvironment := &fakeRabbitEnvironment{
		producers: []rabbitProducer{newFakeRabbitProducer("tracking.events")},
	}
	deduplicatedSession, err := openProducerSessionWithEnvironment(
		context.Background(), deduplicatedConfig,
		func(context.Context) (producerEnvironment, error) { return deduplicatedEnvironment, nil },
	)
	if err != nil {
		t.Fatalf("open deduplicated producer session: %v", err)
	}
	if len(deduplicatedEnvironment.producerOptions) != 1 ||
		deduplicatedEnvironment.producerOptions[0].Name != "tracking-publisher" {
		t.Fatalf("deduplicated producer options = %#v", deduplicatedEnvironment.producerOptions)
	}
	if err := deduplicatedSession.Close(); err != nil {
		t.Fatalf("close deduplicated producer session: %v", err)
	}

	superProducerConfig, err := (rabbitstream.ProducerConfig{SuperStream: "tracking", ExpectedPartitions: 2}).Normalized()
	if err != nil {
		t.Fatalf("normalize Super Stream producer: %v", err)
	}
	firstProducer := newFakeRabbitProducer("tracking-0")
	secondFailure := errors.New("second producer")
	partialProducerEnvironment := &fakeRabbitEnvironment{
		partitions:     []string{"tracking-0", "tracking-1"},
		producers:      []rabbitProducer{firstProducer},
		newProducerErr: secondFailure,
		producerErrAt:  2,
	}
	if session, err := openProducerSessionWithEnvironment(context.Background(), superProducerConfig, func(context.Context) (producerEnvironment, error) {
		return partialProducerEnvironment, nil
	}); session != nil || !errors.Is(err, secondFailure) || firstProducer.closeCalls != 1 || partialProducerEnvironment.closeCalls != 1 {
		t.Fatalf("partial producer session = %#v, %v, producer closes %d, environment closes %d", session, err, firstProducer.closeCalls, partialProducerEnvironment.closeCalls)
	}

	consumerConfig, err := (rabbitstream.ConsumerConfig{Stream: "tracking.events", ConsumerName: "tracking-indexer"}).Normalized()
	if err != nil {
		t.Fatalf("normalize consumer: %v", err)
	}
	consumer := newFakeRabbitConsumer()
	consumerEnvironment := &fakeRabbitEnvironment{consumers: []rabbitConsumer{consumer}}
	consumerSession, err := openConsumerSessionWithEnvironment(context.Background(), consumerConfig, func(context.Context) (rabbitEnvironment, error) {
		return consumerEnvironment, nil
	})
	if err != nil {
		t.Fatalf("open consumer session: %v", err)
	}
	if err := consumerSession.StoreOffset(context.Background(), "tracking.events", 9); err != nil || consumer.stored != 9 {
		t.Fatalf("consumer StoreOffset() = %v, stored %d", err, consumer.stored)
	}
	if err := consumerSession.Close(); err != nil || consumer.closeCalls != 1 || consumerEnvironment.closeCalls != 1 {
		t.Fatalf("consumer close = %v, consumer calls %d, environment calls %d", err, consumer.closeCalls, consumerEnvironment.closeCalls)
	}

	firstConsumer := newFakeRabbitConsumer()
	secondConsumerFailure := errors.New("second consumer")
	partialConsumerEnvironment := &fakeRabbitEnvironment{
		partitions:     []string{"tracking-0", "tracking-1"},
		consumers:      []rabbitConsumer{firstConsumer},
		newConsumerErr: secondConsumerFailure,
		consumerErrAt:  2,
	}
	superConsumerConfig, err := (rabbitstream.ConsumerConfig{SuperStream: "tracking", ConsumerName: "tracking-indexer"}).Normalized()
	if err != nil {
		t.Fatalf("normalize Super Stream consumer: %v", err)
	}
	if session, err := openConsumerSessionWithEnvironment(context.Background(), superConsumerConfig, func(context.Context) (rabbitEnvironment, error) {
		return partialConsumerEnvironment, nil
	}); session != nil || !errors.Is(err, secondConsumerFailure) || firstConsumer.closeCalls != 1 || partialConsumerEnvironment.closeCalls != 1 {
		t.Fatalf("partial consumer session = %#v, %v, consumer closes %d, environment closes %d", session, err, firstConsumer.closeCalls, partialConsumerEnvironment.closeCalls)
	}
}

func TestReplayCursorReportsEveryTerminalState(t *testing.T) {
	queued := newReplayCursorForTest()
	queued.messages <- rabbitstream.Message{Stream: "tracking.events", Offset: 4}
	if message, err := queued.Next(context.Background()); err != nil || message.Offset != 4 {
		t.Fatalf("queued Next() = %#v, %v", message, err)
	}

	completed := newReplayCursorForTest()
	completed.complete()
	completed.complete()
	if _, err := completed.Next(context.Background()); !errors.Is(err, io.EOF) {
		t.Fatalf("completed Next() error = %v", err)
	}

	failed := newReplayCursorForTest()
	failed.reportFailure(nil)
	failed.reportFailure(rabbitstream.ErrReplayRange)
	if _, err := failed.Next(context.Background()); !errors.Is(err, rabbitstream.ErrConnection) {
		t.Fatalf("failed Next() error = %v", err)
	}

	closed := newReplayCursorForTest()
	close(closed.done)
	if _, err := closed.Next(context.Background()); !errors.Is(err, rabbitstream.ErrClosed) {
		t.Fatalf("closed Next() error = %v", err)
	}

	canceled := newReplayCursorForTest()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := canceled.Next(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled Next() error = %v", err)
	}
}

func newReplayCursorForTest() *replayCursor {
	return &replayCursor{
		messages:  make(chan rabbitstream.Message, 1),
		done:      make(chan struct{}),
		completed: make(chan struct{}),
		failed:    make(chan struct{}),
	}
}

type panickingObserver struct{}

func (panickingObserver) Observe(rabbitstream.Observation) { panic("observer failure") }

type recordingObserver struct{ calls int }

func (observer *recordingObserver) Observe(rabbitstream.Observation) { observer.calls++ }

type fakeStreamStatistics struct {
	first        int64
	firstErr     error
	committed    int64
	committedErr error
}

func (stats fakeStreamStatistics) FirstOffset() (int64, error) {
	return stats.first, stats.firstErr
}

func (stats fakeStreamStatistics) CommittedChunkId() (int64, error) {
	return stats.committed, stats.committedErr
}

type observedContext struct {
	done     chan struct{}
	observed chan struct{}
	once     sync.Once
}

func newObservedContext() *observedContext {
	return &observedContext{done: make(chan struct{}), observed: make(chan struct{})}
}

func (*observedContext) Deadline() (time.Time, bool) { return time.Time{}, false }

func (ctx *observedContext) Done() <-chan struct{} {
	ctx.once.Do(func() { close(ctx.observed) })
	return ctx.done
}

func (ctx *observedContext) Err() error {
	select {
	case <-ctx.done:
		return context.Canceled
	default:
		return nil
	}
}

func (*observedContext) Value(any) any { return nil }

type fakeRabbitEnvironment struct {
	partitions         []string
	queryPartitionsErr error
	exists             bool
	existsErr          error
	stats              *stream.StreamStats
	statsErr           error
	streamStatsCalls   int
	queryOffset        int64
	queryOffsetErr     error
	newConsumerErr     error
	newProducerErr     error
	consumerErrAt      int
	producerErrAt      int
	producerOptions    []*stream.ProducerOptions
	consumers          []rabbitConsumer
	producers          []rabbitProducer
	consumerCalls      int
	consumerOptions    []*stream.ConsumerOptions
	queryOffsetCalls   int
	producerCalls      int
	closeErr           error
	closeCalls         int
	closeCalled        chan struct{}
	closeOnce          sync.Once
}

func (environment *fakeRabbitEnvironment) QueryPartitions(string) ([]string, error) {
	return append([]string(nil), environment.partitions...), environment.queryPartitionsErr
}

func (environment *fakeRabbitEnvironment) StreamExists(string) (bool, error) {
	return environment.exists, environment.existsErr
}

func (environment *fakeRabbitEnvironment) StreamStats(string) (*stream.StreamStats, error) {
	environment.streamStatsCalls++
	return environment.stats, environment.statsErr
}

func (environment *fakeRabbitEnvironment) QueryOffset(string, string) (int64, error) {
	environment.queryOffsetCalls++
	return environment.queryOffset, environment.queryOffsetErr
}

func (environment *fakeRabbitEnvironment) NewConsumer(
	_ string,
	_ stream.MessagesHandler,
	options *stream.ConsumerOptions,
) (rabbitConsumer, error) {
	environment.consumerCalls++
	environment.consumerOptions = append(environment.consumerOptions, options)
	if environment.newConsumerErr != nil && (environment.consumerErrAt == 0 || environment.consumerCalls == environment.consumerErrAt) {
		return nil, environment.newConsumerErr
	}
	consumer := environment.consumers[0]
	environment.consumers = environment.consumers[1:]
	return consumer, nil
}

func (environment *fakeRabbitEnvironment) NewProducer(_ string, options *stream.ProducerOptions) (rabbitProducer, error) {
	environment.producerCalls++
	environment.producerOptions = append(environment.producerOptions, options)
	if environment.newProducerErr != nil && (environment.producerErrAt == 0 || environment.producerCalls == environment.producerErrAt) {
		return nil, environment.newProducerErr
	}
	producer := environment.producers[0]
	environment.producers = environment.producers[1:]
	return producer, nil
}

func (environment *fakeRabbitEnvironment) Close() error {
	environment.closeCalls++
	if environment.closeCalled != nil {
		environment.closeOnce.Do(func() { close(environment.closeCalled) })
	}
	return environment.closeErr
}

type fakeRabbitProducer struct {
	streamName    string
	confirmations chan []*stream.ConfirmationStatus
	closed        chan stream.Event
	closeOnce     sync.Once
	closeCalls    int
}

func newFakeRabbitProducer(streamName string) *fakeRabbitProducer {
	return &fakeRabbitProducer{
		streamName:    streamName,
		confirmations: make(chan []*stream.ConfirmationStatus),
		closed:        make(chan stream.Event, 1),
	}
}

func (*fakeRabbitProducer) Send(message.StreamMessage) error { return nil }

func (producer *fakeRabbitProducer) NotifyPublishConfirmation() stream.ChannelPublishConfirm {
	return producer.confirmations
}

func (producer *fakeRabbitProducer) NotifyClose() stream.ChannelClose { return producer.closed }

func (producer *fakeRabbitProducer) GetStreamName() string { return producer.streamName }

func (producer *fakeRabbitProducer) Close() error {
	producer.closeCalls++
	producer.closeOnce.Do(func() {
		close(producer.confirmations)
		producer.closed <- stream.Event{}
		close(producer.closed)
	})
	return nil
}

type fakeRabbitConsumer struct {
	closed     chan stream.Event
	closeOnce  sync.Once
	stored     int64
	closeErr   error
	closeCalls int
}

func newFakeRabbitConsumer() *fakeRabbitConsumer {
	return &fakeRabbitConsumer{closed: make(chan stream.Event, 1)}
}

func (consumer *fakeRabbitConsumer) StoreCustomOffset(offset int64) error {
	consumer.stored = offset
	return consumer.closeErr
}

func (consumer *fakeRabbitConsumer) NotifyClose() stream.ChannelClose { return consumer.closed }

func (consumer *fakeRabbitConsumer) Close() error {
	consumer.closeCalls++
	consumer.closeOnce.Do(func() {
		consumer.closed <- stream.Event{}
		close(consumer.closed)
	})
	return consumer.closeErr
}
