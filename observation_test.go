package rabbitstream

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestObserverPanicCannotAffectConfirmedDelivery(t *testing.T) {
	t.Parallel()

	transport := newFakeProducerTransport()
	producer, err := newProducer(ProducerConfig{
		Stream: "tracking.events", Observer: panicObserver{},
	}, transport)
	if err != nil {
		t.Fatalf("newProducer() error = %v", err)
	}
	resultChannel := make(chan DeliveryResult, 1)
	errorChannel := make(chan error, 1)
	go func() {
		result, publishErr := producer.Publish(
			boundedTestContext(), Message{Stream: "tracking.events", Payload: []byte("payload")},
		)
		resultChannel <- result
		errorChannel <- publishErr
	}()

	outbound := receiveTest(t, transport.sent)
	outbound.confirm(TransportConfirmation{Confirmed: true})
	if err := receiveTest(t, errorChannel); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	if result := receiveTest(t, resultChannel); result.State != DeliveryConfirmed {
		t.Fatalf("Publish() result = %#v", result)
	}
}

func TestObserverPanicCannotAffectConsumerSettlement(t *testing.T) {
	t.Parallel()

	transport := newFakeConsumerTransport(Message{
		Stream: "tracking.events", Partition: "tracking.events", Offset: 41, HasOffset: true,
	})
	consumer, err := NewConsumer(ConsumerConfig{
		Stream: "tracking.events", ConsumerName: "tracking-indexer", Observer: panicObserver{},
	}, transport)
	if err != nil {
		t.Fatalf("NewConsumer() error = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan error, 1)
	go func() {
		runDone <- consumer.Run(ctx, func(context.Context, Message) error { return nil })
	}()
	receiveTest(t, transport.stored)
	cancel()
	if err := receiveTest(t, runDone); err == nil {
		t.Fatal("Run() returned nil after cancellation")
	}
}

func TestConsumerObservesRetriesFailurePublicationAndShutdown(t *testing.T) {
	t.Parallel()

	observer := &recordingObserver{}
	retrying, err := NewConsumer(ConsumerConfig{
		Stream: "tracking.events", ConsumerName: "tracking-indexer", Observer: observer,
		Policy: ConsumerPolicy{
			FailureStrategy: FailureRetry,
			Retry:           RetryPolicy{MaxAttempts: 2, InitialBackoff: time.Nanosecond, MaxBackoff: time.Nanosecond},
		},
	}, newFakeConsumerTransport())
	if err != nil {
		t.Fatalf("NewConsumer(retry) error = %v", err)
	}
	attempts := 0
	if err := retrying.handle(context.Background(), func(context.Context, Message) error {
		attempts++
		if attempts == 1 {
			return ErrFatal
		}
		return nil
	}, Message{Stream: "tracking.events"}); err != nil {
		t.Fatalf("handle(retry) error = %v", err)
	}
	assertObservation(t, observer.observations, Observation{Kind: ObservationHandlerRetry, Count: 1, Value: 2})
	batchObserver := &recordingObserver{}
	batchRetrying, err := NewConsumer(ConsumerConfig{
		Stream: "tracking.events", ConsumerName: "tracking-batch-indexer", Observer: batchObserver,
		Policy: ConsumerPolicy{
			FailureStrategy: FailureRetry,
			Retry:           RetryPolicy{MaxAttempts: 2, InitialBackoff: time.Nanosecond, MaxBackoff: time.Nanosecond},
		},
	}, newFakeConsumerTransport())
	if err != nil {
		t.Fatalf("NewConsumer(batch retry) error = %v", err)
	}
	batchAttempts := 0
	if err := batchRetrying.handleBatch(context.Background(), func(context.Context, []Message) error {
		batchAttempts++
		if batchAttempts == 1 {
			return ErrFatal
		}
		return nil
	}, []Message{{Stream: "tracking.events"}}); err != nil {
		t.Fatalf("handleBatch(retry) error = %v", err)
	}
	if len(batchObserver.observations) != 1 || batchObserver.observations[0] != (Observation{
		Kind: ObservationHandlerRetry, Count: 1, Value: 2,
	}) {
		t.Fatalf("batch retry observations = %#v", batchObserver.observations)
	}

	message := Message{
		Stream: "tracking.events", Partition: "tracking.events", Offset: 7, HasOffset: true,
	}
	retryPublisher := &fakeFailurePublisher{result: DeliveryResult{State: DeliveryConfirmed}}
	retryStream, err := NewConsumer(ConsumerConfig{
		Stream: "tracking.events", ConsumerName: "tracking-indexer", Observer: observer,
		Policy:           ConsumerPolicy{FailureStrategy: FailureRetryStream},
		FailurePublisher: retryPublisher, RetryStream: "tracking.retry",
	}, newFakeConsumerTransport())
	if err != nil {
		t.Fatalf("NewConsumer(retry stream) error = %v", err)
	}
	if err := retryStream.publishFailure(context.Background(), message, 1); err != nil {
		t.Fatalf("publishFailure(retry stream) error = %v", err)
	}
	assertObservation(t, observer.observations, Observation{Kind: ObservationRetryStreamPublished, Count: 1})

	deadLetterPublisher := &fakeFailurePublisher{result: DeliveryResult{State: DeliveryConfirmed}}
	deadLetter, err := NewConsumer(ConsumerConfig{
		Stream: "tracking.events", ConsumerName: "tracking-indexer", Observer: observer,
		Policy:           ConsumerPolicy{FailureStrategy: FailureDeadLetter},
		FailurePublisher: deadLetterPublisher, DeadLetterStream: "tracking.dead",
	}, newFakeConsumerTransport())
	if err != nil {
		t.Fatalf("NewConsumer(dead letter) error = %v", err)
	}
	if err := deadLetter.publishFailure(context.Background(), message, 1); err != nil {
		t.Fatalf("publishFailure(dead letter) error = %v", err)
	}
	assertObservation(t, observer.observations, Observation{Kind: ObservationDeadLetterPublished, Count: 1})

	failedPublisher := &fakeFailurePublisher{err: ErrAuthorization}
	failed, err := NewConsumer(ConsumerConfig{
		Stream: "tracking.events", ConsumerName: "tracking-indexer", Observer: observer,
		Policy:           ConsumerPolicy{FailureStrategy: FailureDeadLetter},
		FailurePublisher: failedPublisher, DeadLetterStream: "tracking.dead",
	}, newFakeConsumerTransport())
	if err != nil {
		t.Fatalf("NewConsumer(failed dead letter) error = %v", err)
	}
	if err := failed.publishFailure(context.Background(), message, 1); !errors.Is(err, ErrAuthorization) {
		t.Fatalf("publishFailure(failed dead letter) error = %v", err)
	}
	assertObservation(t, observer.observations, Observation{
		Kind: ObservationFailurePublishError, Count: 1, Category: CategoryAuthorization,
	})

	if err := deadLetter.Close(context.Background()); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	assertObservationKind(t, observer.observations, ObservationConsumerShutdown)
}

func TestProducerObservesShutdownDuration(t *testing.T) {
	t.Parallel()

	observer := &recordingObserver{}
	producer, err := NewProducer(ProducerConfig{
		Stream: "tracking.events", Observer: observer,
	}, newFakeProducerTransport())
	if err != nil {
		t.Fatalf("NewProducer() error = %v", err)
	}
	if err := producer.Close(context.Background()); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	assertObservationKind(t, observer.observations, ObservationProducerShutdown)
}

func assertObservation(t *testing.T, observations []Observation, want Observation) {
	t.Helper()
	for _, observation := range observations {
		if observation == want {
			return
		}
	}
	t.Fatalf("observations %#v do not contain %#v", observations, want)
}

func assertObservationKind(t *testing.T, observations []Observation, want ObservationKind) {
	t.Helper()
	for _, observation := range observations {
		if observation.Kind == want && observation.Count == 1 && observation.Duration >= 0 {
			return
		}
	}
	t.Fatalf("observations %#v do not contain %q", observations, want)
}

type panicObserver struct{}

func (panicObserver) Observe(Observation) {
	panic("telemetry failure")
}
