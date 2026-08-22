package rabbitstream

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"testing"
	"time"
)

func TestConsumerStoresOffsetOnlyAfterSuccessfulHandler(t *testing.T) {
	t.Parallel()

	transport := newFakeConsumerTransport(Message{
		Stream: "tracking.events", Partition: "tracking.events-0", Offset: 41,
		Payload: []byte("payload"),
	})
	consumer, err := NewConsumer(ConsumerConfig{
		Stream: "tracking.events", ConsumerName: "tracking-indexer",
	}, transport)
	if err != nil {
		t.Fatalf("NewConsumer() error = %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	handlerErr := make(chan error, 1)
	runDone := make(chan error, 1)
	go func() {
		runDone <- consumer.Run(ctx, func(_ context.Context, message Message) error {
			var checkErr error
			select {
			case <-transport.stored:
				checkErr = errors.New("offset stored before handler success")
			default:
			}
			if string(message.Payload) != "payload" || message.Offset != 41 {
				checkErr = errors.New("handler received an unexpected message")
			}
			handlerErr <- checkErr
			return nil
		})
	}()

	if err := receiveTest(t, handlerErr); err != nil {
		t.Fatal(err)
	}
	stored := receiveTest(t, transport.stored)
	if stored.partition != "tracking.events-0" || stored.offset != 41 {
		t.Fatalf("stored offset = %#v", stored)
	}
	cancel()
	if err := receiveTest(t, runDone); !errors.Is(err, ErrCanceled) {
		t.Fatalf("Run() error = %v", err)
	}
}

func TestConsumerOffsetStoreFrequencyBoundsCrashWindow(t *testing.T) {
	t.Parallel()

	transport := newFakeConsumerTransport(
		Message{Stream: "tracking.events", Partition: "tracking.events", Offset: 40, HasOffset: true},
		Message{Stream: "tracking.events", Partition: "tracking.events", Offset: 41, HasOffset: true},
		Message{Stream: "tracking.events", Partition: "tracking.events", Offset: 42, HasOffset: true},
	)
	consumer, err := NewConsumer(ConsumerConfig{
		Stream: "tracking.events", ConsumerName: "tracking-indexer",
		Policy: ConsumerPolicy{OffsetStoreEveryMessages: 3},
	}, transport)
	if err != nil {
		t.Fatalf("NewConsumer() error = %v", err)
	}
	runCtx, cancelRun := context.WithCancel(context.Background())
	runDone := make(chan error, 1)
	handled := make(chan uint64, 3)
	thirdStarted := make(chan struct{})
	releaseThird := make(chan struct{})
	go func() {
		runDone <- consumer.Run(runCtx, func(_ context.Context, message Message) error {
			if message.Offset == 42 {
				close(thirdStarted)
				<-releaseThird
			}
			handled <- message.Offset
			return nil
		})
	}()
	receiveTest(t, handled)
	receiveTest(t, handled)
	receiveTest(t, thirdStarted)
	select {
	case stored := <-transport.stored:
		t.Fatalf("offset stored before configured frequency: %#v", stored)
	default:
	}
	close(releaseThird)
	receiveTest(t, handled)
	stored := receiveTest(t, transport.stored)
	if stored.partition != "tracking.events" || stored.offset != 42 {
		t.Fatalf("stored offset = %#v", stored)
	}
	cancelRun()
	if err := receiveTest(t, runDone); !errors.Is(err, ErrCanceled) {
		t.Fatalf("Run() error = %v", err)
	}
}

func TestConsumerPauseBeforeRunStopsTransportReadsUntilResume(t *testing.T) {
	t.Parallel()

	transport := newFakeConsumerTransport(Message{
		Stream: "tracking.events", Partition: "tracking.events", Offset: 41, HasOffset: true,
	})
	consumer, err := NewConsumer(ConsumerConfig{
		Stream: "tracking.events", ConsumerName: "tracking-indexer",
	}, transport)
	if err != nil {
		t.Fatalf("NewConsumer() error = %v", err)
	}
	if err := consumer.Pause(); err != nil {
		t.Fatalf("Pause() error = %v", err)
	}
	runCtx, cancelRun := context.WithCancel(context.Background())
	runDone := make(chan error, 1)
	go func() {
		runDone <- consumer.Run(runCtx, func(context.Context, Message) error { return nil })
	}()
	waitForConsumerRunning(t, consumer)
	select {
	case <-transport.nextCalled:
		t.Fatal("paused consumer read from transport")
	default:
	}
	if err := consumer.Resume(); err != nil {
		t.Fatalf("Resume() error = %v", err)
	}
	receiveTest(t, transport.stored)
	cancelRun()
	if err := receiveTest(t, runDone); !errors.Is(err, ErrCanceled) {
		t.Fatalf("Run() error = %v", err)
	}
}

func TestConsumerBatchStoresOnlyLastOffsetAfterWholeBatchSuccess(t *testing.T) {
	t.Parallel()

	transport := newFakeConsumerTransport(
		Message{Stream: "tracking.events", Partition: "tracking.events", Offset: 40, HasOffset: true},
		Message{Stream: "tracking.events", Partition: "tracking.events", Offset: 41, HasOffset: true},
		Message{Stream: "tracking.events", Partition: "tracking.events", Offset: 42, HasOffset: true},
	)
	consumer, err := NewConsumer(ConsumerConfig{
		Stream: "tracking.events", ConsumerName: "tracking-indexer",
	}, transport)
	if err != nil {
		t.Fatalf("NewConsumer() error = %v", err)
	}
	runCtx, cancelRun := context.WithCancel(context.Background())
	runDone := make(chan error, 1)
	batchErr := make(chan error, 1)
	go func() {
		runDone <- consumer.RunBatch(runCtx, BatchPolicy{MaxMessages: 3, MaxWait: time.Second}, func(_ context.Context, messages []Message) error {
			if len(messages) != 3 || messages[0].Offset != 40 || messages[2].Offset != 42 {
				batchErr <- errors.New("handler received an unexpected batch")
			} else {
				batchErr <- nil
			}
			return nil
		})
	}()
	if err := receiveTest(t, batchErr); err != nil {
		t.Fatal(err)
	}
	stored := receiveTest(t, transport.stored)
	if stored.partition != "tracking.events" || stored.offset != 42 {
		t.Fatalf("stored offset = %#v", stored)
	}
	select {
	case extra := <-transport.stored:
		t.Fatalf("batch stored more than its terminal offset: %#v", extra)
	default:
	}
	cancelRun()
	if err := receiveTest(t, runDone); !errors.Is(err, ErrCanceled) {
		t.Fatalf("RunBatch() error = %v", err)
	}
}

func TestConsumerBatchNeverMixesPartitions(t *testing.T) {
	t.Parallel()

	transport := newFakeConsumerTransport(
		Message{Stream: "tracking-0", Partition: "tracking-0", Offset: 1, HasOffset: true},
		Message{Stream: "tracking-1", Partition: "tracking-1", Offset: 10, HasOffset: true},
		Message{Stream: "tracking-0", Partition: "tracking-0", Offset: 2, HasOffset: true},
		Message{Stream: "tracking-1", Partition: "tracking-1", Offset: 11, HasOffset: true},
	)
	consumer, err := NewConsumer(ConsumerConfig{
		SuperStream: "tracking", ConsumerName: "tracking-indexer",
	}, transport)
	if err != nil {
		t.Fatalf("NewConsumer() error = %v", err)
	}
	runCtx, cancelRun := context.WithCancel(context.Background())
	runDone := make(chan error, 1)
	handled := make(chan string, 2)
	go func() {
		runDone <- consumer.RunBatch(runCtx, BatchPolicy{MaxMessages: 2, MaxWait: time.Second}, func(_ context.Context, messages []Message) error {
			if len(messages) != 2 || messages[0].Partition != messages[1].Partition {
				return errors.New("mixed partition batch")
			}
			handled <- messages[0].Partition
			return nil
		})
	}()
	first, second := receiveTest(t, handled), receiveTest(t, handled)
	if first == second {
		t.Fatalf("handled partitions = %q and %q", first, second)
	}
	receiveTest(t, transport.stored)
	receiveTest(t, transport.stored)
	cancelRun()
	if err := receiveTest(t, runDone); !errors.Is(err, ErrCanceled) {
		t.Fatalf("RunBatch() error = %v", err)
	}
}

func TestConsumerBatchCancellationLeavesPartialBatchUnstored(t *testing.T) {
	t.Parallel()

	transport := newFakeConsumerTransport(Message{
		Stream: "tracking.events", Partition: "tracking.events", Offset: 41, HasOffset: true,
	})
	consumer, err := NewConsumer(ConsumerConfig{
		Stream: "tracking.events", ConsumerName: "tracking-indexer",
	}, transport)
	if err != nil {
		t.Fatalf("NewConsumer() error = %v", err)
	}
	runCtx, cancelRun := context.WithCancel(context.Background())
	runDone := make(chan error, 1)
	called := make(chan struct{}, 1)
	go func() {
		runDone <- consumer.RunBatch(runCtx, BatchPolicy{MaxMessages: 2, MaxWait: time.Minute}, func(context.Context, []Message) error {
			called <- struct{}{}
			return nil
		})
	}()
	receiveTest(t, transport.nextCalled)
	cancelRun()
	if err := receiveTest(t, runDone); !errors.Is(err, ErrCanceled) {
		t.Fatalf("RunBatch() error = %v", err)
	}
	select {
	case <-called:
		t.Fatal("partial batch was handled during cancellation")
	case stored := <-transport.stored:
		t.Fatalf("partial batch stored offset: %#v", stored)
	default:
	}
}

func waitForConsumerRunning(t *testing.T, consumer *Consumer) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for {
		consumer.stateMutex.Lock()
		running := consumer.running
		consumer.stateMutex.Unlock()
		if running {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("consumer did not enter running state")
		}
		runtime.Gosched()
	}
}

func TestConsumerHandlerFailureDoesNotAdvanceOffset(t *testing.T) {
	t.Parallel()

	transport := newFakeConsumerTransport(Message{
		Stream: "tracking.events", Partition: "tracking.events-0", Offset: 41,
	})
	consumer, err := NewConsumer(ConsumerConfig{
		Stream: "tracking.events", ConsumerName: "tracking-indexer",
	}, transport)
	if err != nil {
		t.Fatalf("NewConsumer() error = %v", err)
	}

	handlerErr := errors.New("classified handler failure")
	err = consumer.Run(boundedTestContext(), func(context.Context, Message) error {
		return handlerErr
	})
	if !errors.Is(err, ErrHandler) || !errors.Is(err, handlerErr) {
		t.Fatalf("Run() error = %v", err)
	}
	select {
	case stored := <-transport.stored:
		t.Fatalf("failed message advanced offset: %#v", stored)
	default:
	}
}

func TestConsumerHandlerDeadlineBoundsCooperativeHandlerWithoutAdvancingOffset(t *testing.T) {
	t.Parallel()

	transport := newFakeConsumerTransport(Message{
		Stream: "tracking.events", Partition: "tracking.events", Offset: 41, HasOffset: true,
	})
	consumer, err := NewConsumer(ConsumerConfig{
		Stream: "tracking.events", ConsumerName: "tracking-indexer",
		Policy: ConsumerPolicy{HandlerTimeout: time.Millisecond},
	}, transport)
	if err != nil {
		t.Fatalf("NewConsumer() error = %v", err)
	}
	err = consumer.Run(boundedTestContext(), func(ctx context.Context, _ Message) error {
		<-ctx.Done()
		return ctx.Err()
	})
	var operationError *OperationError
	if !errors.Is(err, context.DeadlineExceeded) || !errors.As(err, &operationError) ||
		operationError.Category != CategoryHandler {
		t.Fatalf("Run() error = %v", err)
	}
	select {
	case stored := <-transport.stored:
		t.Fatalf("timed-out handler advanced offset: %#v", stored)
	default:
	}
}

func TestConsumerPreservesStableTransportAuthorizationCategory(t *testing.T) {
	t.Parallel()

	transport := newFakeConsumerTransport()
	transport.nextErr = ErrAuthorization
	consumer, err := NewConsumer(ConsumerConfig{
		Stream: "tracking.events", ConsumerName: "tracking-indexer",
	}, transport)
	if err != nil {
		t.Fatalf("NewConsumer() error = %v", err)
	}
	err = consumer.Run(boundedTestContext(), func(context.Context, Message) error { return nil })
	var operationError *OperationError
	if !errors.Is(err, ErrAuthorization) || !errors.As(err, &operationError) ||
		operationError.Category != CategoryAuthorization {
		t.Fatalf("Run() error = %v", err)
	}
}

func TestConsumerContainsHandlerPanicAndRemainsClosable(t *testing.T) {
	t.Parallel()

	transport := newFakeConsumerTransport(Message{Stream: "tracking.events", Offset: 1})
	consumer, err := NewConsumer(ConsumerConfig{
		Stream: "tracking.events", ConsumerName: "tracking-indexer",
	}, transport)
	if err != nil {
		t.Fatalf("NewConsumer() error = %v", err)
	}

	err = consumer.Run(boundedTestContext(), func(context.Context, Message) error {
		panic("sensitive caller value")
	})
	if !errors.Is(err, ErrHandler) || err.Error() == "sensitive caller value" {
		t.Fatalf("Run() panic error = %v", err)
	}
	if err := consumer.Close(boundedTestContext()); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
}

func TestConsumerRetriesWithinFinitePolicyBeforeStoringOffset(t *testing.T) {
	t.Parallel()

	transport := newFakeConsumerTransport(Message{Stream: "tracking.events", Offset: 7})
	consumer, err := NewConsumer(ConsumerConfig{
		Stream:       "tracking.events",
		ConsumerName: "tracking-indexer",
		Policy: ConsumerPolicy{
			FailureStrategy: FailureRetry,
			Retry:           RetryPolicy{MaxAttempts: 3, InitialBackoff: time.Nanosecond, MaxBackoff: time.Nanosecond},
		},
	}, transport)
	if err != nil {
		t.Fatalf("NewConsumer() error = %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var mutex sync.Mutex
	attempts := 0
	runDone := make(chan error, 1)
	go func() {
		runDone <- consumer.Run(ctx, func(context.Context, Message) error {
			mutex.Lock()
			defer mutex.Unlock()
			attempts++
			if attempts < 3 {
				return errors.New("retry")
			}
			return nil
		})
	}()

	receiveTest(t, transport.stored)
	mutex.Lock()
	gotAttempts := attempts
	mutex.Unlock()
	if gotAttempts != 3 {
		t.Fatalf("handler attempts = %d", gotAttempts)
	}
	cancel()
	if err := receiveTest(t, runDone); !errors.Is(err, ErrCanceled) {
		t.Fatalf("Run() error = %v", err)
	}
}

func TestConsumerPublishesDeadLetterBeforeAdvancingSourceOffset(t *testing.T) {
	t.Parallel()

	transport := newFakeConsumerTransport(Message{
		Stream: "tracking.events", Partition: "tracking.events", Offset: 41, HasOffset: true,
		RoutingKey: "tracking-123", Payload: []byte("payload"),
		Properties: []MetadataEntry{{Key: "schema", Value: []byte("tracking.v1")}},
	})
	publisher := &fakeFailurePublisher{result: DeliveryResult{State: DeliveryConfirmed}}
	consumer, err := NewConsumer(ConsumerConfig{
		Stream: "tracking.events", ConsumerName: "tracking-indexer",
		Policy:           ConsumerPolicy{FailureStrategy: FailureDeadLetter},
		FailurePublisher: publisher,
		DeadLetterStream: "tracking.dead-letter",
	}, transport)
	if err != nil {
		t.Fatalf("NewConsumer() error = %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan error, 1)
	go func() {
		runDone <- consumer.Run(ctx, func(context.Context, Message) error {
			return errors.New("unprocessable")
		})
	}()

	stored := receiveTest(t, transport.stored)
	if stored.offset != 41 {
		t.Fatalf("stored offset = %#v", stored)
	}
	published := receiveTest(t, publisher.messages)
	if published.Stream != "tracking.dead-letter" || published.Partition != "" ||
		published.HasOffset || published.RoutingKey != "tracking-123" ||
		string(published.Payload) != "payload" || len(published.Properties) != 6 {
		t.Fatalf("dead-letter message = %#v", published)
	}
	cancel()
	if err := receiveTest(t, runDone); !errors.Is(err, ErrCanceled) {
		t.Fatalf("Run() error = %v", err)
	}
}

func TestConsumerDoesNotAdvanceWhenFailurePublicationIsAmbiguous(t *testing.T) {
	t.Parallel()

	transport := newFakeConsumerTransport(Message{
		Stream: "tracking.events", Partition: "tracking.events", Offset: 41, HasOffset: true,
	})
	publisher := &fakeFailurePublisher{
		result: DeliveryResult{State: DeliveryAmbiguous}, err: ErrPublishAmbiguous,
	}
	consumer, err := NewConsumer(ConsumerConfig{
		Stream: "tracking.events", ConsumerName: "tracking-indexer",
		Policy:           ConsumerPolicy{FailureStrategy: FailureRetryStream},
		FailurePublisher: publisher,
		RetryStream:      "tracking.retry",
	}, transport)
	if err != nil {
		t.Fatalf("NewConsumer() error = %v", err)
	}

	err = consumer.Run(boundedTestContext(), func(context.Context, Message) error {
		return errors.New("retry later")
	})
	if !errors.Is(err, ErrPublishAmbiguous) {
		t.Fatalf("Run() error = %v", err)
	}
	select {
	case stored := <-transport.stored:
		t.Fatalf("ambiguous failure publication advanced offset: %#v", stored)
	default:
	}
}

func TestConsumerRunsIndependentPartitionsWithinBoundedConcurrency(t *testing.T) {
	t.Parallel()

	transport := newFakeConsumerTransport(
		Message{Stream: "tracking-0", SuperStream: "tracking", Partition: "tracking-0", Offset: 1, HasOffset: true},
		Message{Stream: "tracking-1", SuperStream: "tracking", Partition: "tracking-1", Offset: 1, HasOffset: true},
	)
	consumer, err := NewConsumer(ConsumerConfig{
		SuperStream: "tracking", ConsumerName: "tracking-indexer",
		Policy: ConsumerPolicy{MaxConcurrency: 2},
	}, transport)
	if err != nil {
		t.Fatalf("NewConsumer() error = %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	started := make(chan string, 2)
	release := make(chan struct{})
	runDone := make(chan error, 1)
	go func() {
		runDone <- consumer.Run(ctx, func(_ context.Context, message Message) error {
			started <- message.Partition
			<-release
			return nil
		})
	}()

	first := receiveTest(t, started)
	second := receiveTest(t, started)
	if first == second {
		t.Fatalf("concurrent partitions = %q, %q", first, second)
	}
	close(release)
	receiveTest(t, transport.stored)
	receiveTest(t, transport.stored)
	cancel()
	if err := receiveTest(t, runDone); !errors.Is(err, ErrCanceled) {
		t.Fatalf("Run() error = %v", err)
	}
}

type fakeConsumerTransport struct {
	messages   chan Message
	stored     chan storedOffset
	nextCalled chan struct{}

	mutex      sync.Mutex
	closeCount int
	nextCount  int
	nextErr    error
	storeErr   error
	storeHook  func()
	closeErr   error
	closeBlock <-chan struct{}
}

func newFakeConsumerTransport(messages ...Message) *fakeConsumerTransport {
	transport := &fakeConsumerTransport{
		messages:   make(chan Message, len(messages)),
		stored:     make(chan storedOffset, max(len(messages)+1, 1024)),
		nextCalled: make(chan struct{}, 1024),
	}
	for _, message := range messages {
		transport.messages <- message
	}
	return transport
}

func (transport *fakeConsumerTransport) Next(ctx context.Context) (Message, error) {
	transport.mutex.Lock()
	transport.nextCount++
	nextCount := transport.nextCount
	transport.mutex.Unlock()
	if nextCount > 256 {
		panic("fake consumer transport exceeded bounded reads")
	}
	select {
	case transport.nextCalled <- struct{}{}:
	default:
	}
	if transport.nextErr != nil {
		return Message{}, transport.nextErr
	}
	select {
	case message := <-transport.messages:
		return message, nil
	case <-ctx.Done():
		return Message{}, ctx.Err()
	}
}

func (transport *fakeConsumerTransport) StoreOffset(
	_ context.Context,
	partition string,
	offset uint64,
) error {
	if transport.storeHook != nil {
		transport.storeHook()
	}
	if transport.storeErr != nil {
		return transport.storeErr
	}
	transport.stored <- storedOffset{partition: partition, offset: offset}
	return nil
}

func (transport *fakeConsumerTransport) Close() error {
	if transport.closeBlock != nil {
		<-transport.closeBlock
	}
	transport.mutex.Lock()
	defer transport.mutex.Unlock()
	transport.closeCount++
	return transport.closeErr
}

type storedOffset struct {
	partition string
	offset    uint64
}

type fakeFailurePublisher struct {
	result   DeliveryResult
	err      error
	messages chan Message
}

func (publisher *fakeFailurePublisher) Publish(_ context.Context, message Message) (DeliveryResult, error) {
	if publisher.messages == nil {
		publisher.messages = make(chan Message, 1024)
	}
	publisher.messages <- message
	return publisher.result, publisher.err
}
