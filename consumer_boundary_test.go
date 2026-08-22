package rabbitstream

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestBatchPolicyAndConsumerConfigurationBoundEveryPolicyDimension(t *testing.T) {
	t.Parallel()

	limits := DefaultLimits()
	if normalized, err := (BatchPolicy{}).normalized(limits); err != nil || normalized.MaxMessages == 0 || normalized.MaxWait == 0 {
		t.Fatalf("BatchPolicy.normalized() = %#v, %v", normalized, err)
	}
	for _, policy := range []BatchPolicy{{MaxMessages: -1}, {MaxWait: -1}, {MaxMessages: limits.MaxBatchMessages + 1}, {MaxWait: maximumBatchWait + 1}} {
		if _, err := policy.normalized(limits); err == nil {
			t.Fatalf("BatchPolicy.normalized(%#v) succeeded", policy)
		}
	}

	badLimits := limits
	badLimits.MaxPayloadBytes = 0
	invalid := []ConsumerConfig{
		{},
		{Stream: "stream", ConsumerName: "consumer", Limits: badLimits},
		{Stream: " bad", ConsumerName: "consumer"},
		{SuperStream: " bad", ConsumerName: "consumer"},
		{Stream: "stream"},
		{Stream: "stream", ConsumerName: "consumer", Start: StartPosition{Kind: OffsetStartKind(255)}},
		{Stream: "stream", ConsumerName: "consumer", Policy: ConsumerPolicy{HandlerTimeout: -1}},
		{Stream: "stream", ConsumerName: "consumer", Policy: ConsumerPolicy{MaxConcurrency: maximumConsumerWorkers + 1}},
		{Stream: "stream", ConsumerName: "consumer", Policy: ConsumerPolicy{FailureStrategy: FailureRetry, Retry: RetryPolicy{MaxAttempts: maximumRetryAttempts + 1}}},
		{Stream: "stream", ConsumerName: "consumer", Policy: ConsumerPolicy{Retry: RetryPolicy{MaxAttempts: 1}}},
		{Stream: "stream", ConsumerName: "consumer", Policy: ConsumerPolicy{FailureStrategy: FailureRetryStream}},
		{Stream: "stream", ConsumerName: "consumer", Policy: ConsumerPolicy{FailureStrategy: FailureDeadLetter}},
		{Stream: "stream", ConsumerName: "consumer", FailurePublisher: &fakeFailurePublisher{}},
	}
	for _, config := range invalid {
		if _, err := config.Normalized(); !errors.Is(err, ErrInvalidConfiguration) {
			t.Fatalf("ConsumerConfig.Normalized(%#v) error = %v", config, err)
		}
	}
	retry, err := (ConsumerConfig{
		Stream: "stream", ConsumerName: "consumer", Policy: ConsumerPolicy{FailureStrategy: FailureRetry},
	}).Normalized()
	if err != nil || retry.Policy.Retry.MaxAttempts == 0 || retry.Policy.Retry.InitialBackoff == 0 || retry.Policy.Retry.MaxBackoff == 0 {
		t.Fatalf("retry defaults = %#v, %v", retry.Policy.Retry, err)
	}
}

func TestConsumerNormalizationUsesExactDefaultsAndAcceptsExactBounds(t *testing.T) {
	t.Parallel()

	normalized, err := (ConsumerConfig{Stream: "stream", ConsumerName: "consumer"}).Normalized()
	if err != nil {
		t.Fatalf("Normalized() error = %v", err)
	}
	if normalized.Policy.HandlerTimeout != 30*time.Second || normalized.Policy.CloseTimeout != 30*time.Second ||
		normalized.Policy.MaxConcurrency != 1 || normalized.Policy.OffsetStoreEveryMessages != 1 {
		t.Fatalf("consumer defaults = %#v", normalized.Policy)
	}
	retry, err := (ConsumerConfig{
		Stream: "stream", ConsumerName: "consumer", Policy: ConsumerPolicy{FailureStrategy: FailureRetry},
	}).Normalized()
	if err != nil {
		t.Fatalf("Normalized(retry) error = %v", err)
	}
	if retry.Policy.Retry != (RetryPolicy{MaxAttempts: 3, InitialBackoff: 100 * time.Millisecond, MaxBackoff: time.Second}) {
		t.Fatalf("retry defaults = %#v", retry.Policy.Retry)
	}

	limits := DefaultLimits()
	limits.MaxBufferedMessages = maximumConsumerWorkers
	limits.MaxBatchMessages = maximumConsumerWorkers
	maximum := ConsumerConfig{
		Stream: "stream", ConsumerName: "consumer", Limits: limits,
		Start: StartPosition{Kind: OffsetStartTimestamp, Timestamp: time.Unix(1, 0)},
		Policy: ConsumerPolicy{
			HandlerTimeout: maximumHandlerTimeout, CloseTimeout: maximumConsumerClose,
			MaxConcurrency: maximumConsumerWorkers, OffsetStoreEveryMessages: maximumConsumerWorkers,
			FailureStrategy: FailureRetry,
			Retry:           RetryPolicy{MaxAttempts: maximumRetryAttempts, InitialBackoff: time.Second, MaxBackoff: time.Second},
		},
	}
	if _, err := maximum.Normalized(); err != nil {
		t.Fatalf("Normalized(exact maxima) error = %v", err)
	}
	for name, exceed := range map[string]func(*ConsumerConfig){
		"handler timeout": func(config *ConsumerConfig) { config.Policy.HandlerTimeout++ },
		"close timeout":   func(config *ConsumerConfig) { config.Policy.CloseTimeout++ },
		"workers":         func(config *ConsumerConfig) { config.Policy.MaxConcurrency++ },
		"store interval":  func(config *ConsumerConfig) { config.Policy.OffsetStoreEveryMessages++ },
		"retry attempts":  func(config *ConsumerConfig) { config.Policy.Retry.MaxAttempts++ },
		"retry ordering":  func(config *ConsumerConfig) { config.Policy.Retry.InitialBackoff++ },
	} {
		t.Run(name, func(t *testing.T) {
			config := maximum
			exceed(&config)
			if _, err := config.Normalized(); !errors.Is(err, ErrInvalidConfiguration) {
				t.Fatalf("Normalized() error = %v", err)
			}
		})
	}
}

func TestBatchPolicyUsesExactDefaultsAndBounds(t *testing.T) {
	t.Parallel()

	limits := DefaultLimits()
	limits.MaxBatchMessages = 50
	normalized, err := (BatchPolicy{}).normalized(limits)
	if err != nil || normalized != (BatchPolicy{MaxMessages: 50, MaxWait: 100 * time.Millisecond}) {
		t.Fatalf("normalized() = %#v, %v", normalized, err)
	}
	if _, err := (BatchPolicy{MaxMessages: 50, MaxWait: maximumBatchWait}).normalized(limits); err != nil {
		t.Fatalf("normalized(exact bounds) error = %v", err)
	}
	if _, err := (BatchPolicy{MaxMessages: 51, MaxWait: maximumBatchWait}).normalized(limits); !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("normalized(excess messages) error = %v", err)
	}
	if _, err := (BatchPolicy{MaxMessages: 50, MaxWait: maximumBatchWait + 1}).normalized(limits); !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("normalized(excess wait) error = %v", err)
	}
}

func TestConsumerStartPositionsKeepOffsetAndTimestampSemanticsDistinct(t *testing.T) {
	t.Parallel()

	valid := []StartPosition{
		{Kind: OffsetStartStored}, {Kind: OffsetStartBeginning}, {Kind: OffsetStartEnd},
		{Kind: OffsetStartExplicit, Offset: 1}, {Kind: OffsetStartTimestamp, Timestamp: time.Unix(1, 0)},
	}
	for _, start := range valid {
		if _, err := (ConsumerConfig{Stream: "stream", ConsumerName: "consumer", Start: start}).Normalized(); err != nil {
			t.Fatalf("Normalized(%#v) error = %v", start, err)
		}
	}
	invalid := []StartPosition{
		{Kind: OffsetStartKind(OffsetStartTimestamp + 1)},
		{Kind: OffsetStartTimestamp},
		{Kind: OffsetStartBeginning, Timestamp: time.Unix(1, 0)},
		{Kind: OffsetStartBeginning, Offset: 1},
	}
	for _, start := range invalid {
		if _, err := (ConsumerConfig{Stream: "stream", ConsumerName: "consumer", Start: start}).Normalized(); !errors.Is(err, ErrInvalidConfiguration) {
			t.Fatalf("Normalized(%#v) error = %v", start, err)
		}
	}
}

func TestConsumerConstructionPauseResumeAndRunStateBoundaries(t *testing.T) {
	t.Parallel()

	transport := newFakeConsumerTransport()
	if _, err := NewConsumer(ConsumerConfig{}, transport); err == nil {
		t.Fatal("NewConsumer() accepted invalid configuration")
	}
	if _, err := NewConsumer(ConsumerConfig{Stream: "stream", ConsumerName: "consumer"}, nil); err == nil {
		t.Fatal("NewConsumer() accepted nil transport")
	}
	consumer, err := NewConsumer(ConsumerConfig{Stream: "stream", ConsumerName: "consumer"}, transport)
	if err != nil {
		t.Fatalf("NewConsumer() error = %v", err)
	}
	if err := consumer.Pause(); err != nil {
		t.Fatalf("Pause() error = %v", err)
	}
	alreadyResumed := make(chan struct{})
	close(alreadyResumed)
	consumer.pauseMutex.Lock()
	consumer.paused = true
	consumer.resume = alreadyResumed
	consumer.pauseMutex.Unlock()
	if err := consumer.waitWhilePaused(context.Background()); err != nil {
		t.Fatalf("waitWhilePaused() resumed error = %v", err)
	}
	consumer.pauseMutex.Lock()
	consumer.paused = false
	consumer.resume = nil
	consumer.pauseMutex.Unlock()
	waitDone := make(chan error, 1)
	go func() { waitDone <- consumer.waitWhilePaused(context.Background()) }()
	if err := consumer.Resume(); err != nil {
		t.Fatalf("Resume() error = %v", err)
	}
	if err := receiveTest(t, waitDone); err != nil {
		t.Fatalf("waitWhilePaused() error = %v", err)
	}
	if err := consumer.Resume(); err != nil {
		t.Fatalf("Resume() idempotent error = %v", err)
	}
	if err := consumer.waitWhilePaused(context.Background()); err != nil {
		t.Fatalf("waitWhilePaused() unpaused error = %v", err)
	}
	if err := consumer.Pause(); err != nil {
		t.Fatalf("Pause() second error = %v", err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := consumer.waitWhilePaused(canceled); !errors.Is(err, context.Canceled) {
		t.Fatalf("waitWhilePaused() canceled error = %v", err)
	}
	if err := consumer.Close(boundedTestContext()); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if err := consumer.Pause(); !errors.Is(err, ErrClosed) {
		t.Fatalf("Pause() closed error = %v", err)
	}
	if err := consumer.Resume(); !errors.Is(err, ErrClosed) {
		t.Fatalf("Resume() closed error = %v", err)
	}
	if err := consumer.Run(absentContext, func(context.Context, Message) error { return nil }); !errors.Is(err, ErrValidation) {
		t.Fatalf("Run(nil) error = %v", err)
	}
	if err := consumer.Run(boundedTestContext(), func(context.Context, Message) error { return nil }); !errors.Is(err, ErrClosed) {
		t.Fatalf("Run(closed) error = %v", err)
	}
	if err := consumer.RunBatch(absentContext, BatchPolicy{}, func(context.Context, []Message) error { return nil }); !errors.Is(err, ErrValidation) {
		t.Fatalf("RunBatch(nil) error = %v", err)
	}
	if err := consumer.RunBatch(boundedTestContext(), BatchPolicy{MaxMessages: -1}, func(context.Context, []Message) error { return nil }); err == nil {
		t.Fatal("RunBatch() accepted invalid policy")
	}
	if err := consumer.RunBatch(boundedTestContext(), BatchPolicy{}, func(context.Context, []Message) error { return nil }); !errors.Is(err, ErrClosed) {
		t.Fatalf("RunBatch(closed) error = %v", err)
	}
}

func TestConsumerRejectsConcurrentSingleAndBatchRuns(t *testing.T) {
	t.Parallel()

	workerTransport := newFakeConsumerTransport()
	consumer, _ := NewConsumer(ConsumerConfig{Stream: "stream", ConsumerName: "consumer"}, workerTransport)
	consumer.stateMutex.Lock()
	consumer.running = true
	consumer.stateMutex.Unlock()
	if err := consumer.Run(boundedTestContext(), func(context.Context, Message) error { return nil }); !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("Run(concurrent) error = %v", err)
	}
	if err := consumer.RunBatch(boundedTestContext(), BatchPolicy{}, func(context.Context, []Message) error { return nil }); !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("RunBatch(concurrent) error = %v", err)
	}
}

func TestConsumerDirectProcessingCoversBatchOffsetAndFailureBoundaries(t *testing.T) {
	t.Parallel()

	message := Message{Stream: "stream", Partition: "stream", Offset: 1, HasOffset: true}
	transport := newFakeConsumerTransport()
	consumer, _ := NewConsumer(ConsumerConfig{Stream: "stream", ConsumerName: "consumer"}, transport)
	if err := consumer.processBatch(boundedTestContext(), func(context.Context, []Message) error { return nil }, nil); !errors.Is(err, ErrValidation) {
		t.Fatalf("processBatch(empty) error = %v", err)
	}
	if err := consumer.processBatch(boundedTestContext(), func(context.Context, []Message) error { return nil }, []Message{message, Message{Stream: "other", Partition: "other", Offset: 0}}); !errors.Is(err, ErrValidation) {
		t.Fatalf("processBatch(mixed) error = %v", err)
	}
	if err := consumer.processBatch(boundedTestContext(), func(context.Context, []Message) error { return ErrFatal }, []Message{message}); !errors.Is(err, ErrHandler) {
		t.Fatalf("processBatch(handler) error = %v", err)
	}
	if err := consumer.processBatch(boundedTestContext(), func(context.Context, []Message) error { panic("secret") }, []Message{message}); !errors.Is(err, ErrHandler) {
		t.Fatalf("processBatch(panic) error = %v", err)
	}

	transport.storeErr = ErrConnection
	if err := consumer.processBatch(boundedTestContext(), func(context.Context, []Message) error { return nil }, []Message{message}); !errors.Is(err, ErrOffset) {
		t.Fatalf("processBatch(store) error = %v", err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	transport.storeHook = cancel
	if err := consumer.processBatch(canceled, func(context.Context, []Message) error { return nil }, []Message{message}); !errors.Is(err, ErrCanceled) {
		t.Fatalf("processBatch(canceled store) error = %v", err)
	}

	singleTransport := newFakeConsumerTransport()
	singleTransport.storeErr = ErrConnection
	single, _ := NewConsumer(ConsumerConfig{Stream: "stream", ConsumerName: "consumer"}, singleTransport)
	if err := single.process(boundedTestContext(), func(context.Context, Message) error { return nil }, message, false); err != nil {
		t.Fatalf("process(no store) error = %v", err)
	}
	if err := single.process(boundedTestContext(), func(context.Context, Message) error { return nil }, message, true); !errors.Is(err, ErrOffset) {
		t.Fatalf("process(store) error = %v", err)
	}
	singleCanceled, cancelSingle := context.WithCancel(context.Background())
	singleTransport.storeHook = cancelSingle
	if err := single.process(singleCanceled, func(context.Context, Message) error { return nil }, message, true); !errors.Is(err, ErrCanceled) {
		t.Fatalf("process(canceled store) error = %v", err)
	}
}

func TestConsumerBatchRequiresOnePartitionAndNondecreasingOffsets(t *testing.T) {
	t.Parallel()

	consumer, _ := NewConsumer(ConsumerConfig{Stream: "stream", ConsumerName: "consumer"}, newFakeConsumerTransport())
	handler := func(context.Context, []Message) error { return nil }
	base := Message{Stream: "stream", Partition: "stream", Offset: 2, HasOffset: true}
	if err := consumer.processBatch(boundedTestContext(), handler, []Message{base, base}); err != nil {
		t.Fatalf("processBatch(equal offsets) error = %v", err)
	}
	differentPartition := base
	differentPartition.Partition = "other"
	if err := consumer.processBatch(boundedTestContext(), handler, []Message{base, differentPartition}); !errors.Is(err, ErrValidation) {
		t.Fatalf("processBatch(partition) error = %v", err)
	}
	lowerOffset := base
	lowerOffset.Offset--
	if err := consumer.processBatch(boundedTestContext(), handler, []Message{base, lowerOffset}); !errors.Is(err, ErrValidation) {
		t.Fatalf("processBatch(offset) error = %v", err)
	}
}

func TestConsumerWorkerHashIsStable(t *testing.T) {
	t.Parallel()

	for partition, want := range map[string]int{
		"": 1, "a": 1, "stream": 2, "tracking-0": 0, "tracking-1": 1,
	} {
		if got := consumerWorker(partition, 3); got != want {
			t.Fatalf("consumerWorker(%q, 3) = %d, want %d", partition, got, want)
		}
	}
}

func TestConsumerWorkerCapacitiesAndBackoffArithmeticAreExact(t *testing.T) {
	t.Parallel()

	if got := []int{
		consumerWorkerQueueCapacity(8, 3, 0),
		consumerWorkerQueueCapacity(8, 3, 1),
		consumerWorkerQueueCapacity(8, 3, 2),
	}; got[0] != 3 || got[1] != 3 || got[2] != 2 {
		t.Fatalf("worker capacities = %#v", got)
	}
	if got := nextConsumerBackoff(10*time.Millisecond, 100*time.Millisecond); got != 20*time.Millisecond {
		t.Fatalf("nextConsumerBackoff(growth) = %s", got)
	}
	if got := nextConsumerBackoff(60*time.Millisecond, 100*time.Millisecond); got != 100*time.Millisecond {
		t.Fatalf("nextConsumerBackoff(cap) = %s", got)
	}
}

func TestConsumerFailureStreamConfigurationRequiresBothOwnedParts(t *testing.T) {
	t.Parallel()

	publisher := &fakeFailurePublisher{}
	valid := []ConsumerConfig{
		{Stream: "stream", ConsumerName: "consumer", Policy: ConsumerPolicy{FailureStrategy: FailureRetryStream}, FailurePublisher: publisher, RetryStream: "retry"},
		{Stream: "stream", ConsumerName: "consumer", Policy: ConsumerPolicy{FailureStrategy: FailureDeadLetter}, FailurePublisher: publisher, DeadLetterStream: "dead"},
	}
	for _, config := range valid {
		if _, err := config.Normalized(); err != nil {
			t.Fatalf("Normalized(%#v) error = %v", config, err)
		}
	}
	invalid := []ConsumerConfig{
		{Stream: "stream", ConsumerName: "consumer", Policy: ConsumerPolicy{FailureStrategy: FailureRetryStream}, RetryStream: "retry"},
		{Stream: "stream", ConsumerName: "consumer", Policy: ConsumerPolicy{FailureStrategy: FailureRetryStream}, FailurePublisher: publisher},
		{Stream: "stream", ConsumerName: "consumer", Policy: ConsumerPolicy{FailureStrategy: FailureDeadLetter}, DeadLetterStream: "dead"},
		{Stream: "stream", ConsumerName: "consumer", Policy: ConsumerPolicy{FailureStrategy: FailureDeadLetter}, FailurePublisher: publisher},
	}
	for _, config := range invalid {
		if _, err := config.Normalized(); !errors.Is(err, ErrInvalidConfiguration) {
			t.Fatalf("Normalized(%#v) error = %v", config, err)
		}
	}
}

func TestConsumerRetryInvokesExactlyTheConfiguredAttempts(t *testing.T) {
	t.Parallel()

	consumer, _ := NewConsumer(ConsumerConfig{
		Stream: "stream", ConsumerName: "consumer",
		Policy: ConsumerPolicy{FailureStrategy: FailureRetry, Retry: RetryPolicy{
			MaxAttempts: 4, InitialBackoff: time.Nanosecond, MaxBackoff: time.Nanosecond,
		}},
	}, newFakeConsumerTransport())
	attempts := 0
	err := consumer.handle(boundedTestContext(), func(context.Context, Message) error {
		attempts++
		return ErrFatal
	}, Message{Stream: "stream"})
	if !errors.Is(err, ErrHandler) || attempts != 4 {
		t.Fatalf("handle() error = %v after %d attempts", err, attempts)
	}
	batchAttempts := 0
	err = consumer.handleBatch(boundedTestContext(), func(context.Context, []Message) error {
		batchAttempts++
		return ErrFatal
	}, []Message{{Stream: "stream"}})
	if !errors.Is(err, ErrHandler) || batchAttempts != 4 {
		t.Fatalf("handleBatch() error = %v after %d attempts", err, batchAttempts)
	}
}

func TestConsumerRetryAndFailurePublicationBoundaries(t *testing.T) {
	t.Parallel()

	message := Message{Stream: "stream", Partition: "stream", Offset: 1, HasOffset: true}
	ctx, cancel := context.WithCancel(context.Background())
	retryConsumer, _ := NewConsumer(ConsumerConfig{
		Stream: "stream", ConsumerName: "consumer",
		Policy: ConsumerPolicy{FailureStrategy: FailureRetry, Retry: RetryPolicy{MaxAttempts: 2, InitialBackoff: time.Second, MaxBackoff: time.Second}},
	}, newFakeConsumerTransport())
	if err := retryConsumer.handle(ctx, func(context.Context, Message) error {
		cancel()
		return ErrFatal
	}, message); !errors.Is(err, ErrCanceled) {
		t.Fatalf("handle(retry canceled) error = %v", err)
	}
	timerCtx, cancelTimer := context.WithCancel(context.Background())
	timerStarted := make(chan struct{}, 1)
	timerDone := make(chan error, 1)
	go func() {
		timerDone <- retryConsumer.handle(timerCtx, func(context.Context, Message) error {
			timerStarted <- struct{}{}
			return ErrFatal
		}, message)
	}()
	receiveTest(t, timerStarted)
	cancelTimer()
	if err := receiveTest(t, timerDone); !errors.Is(err, ErrCanceled) {
		t.Fatalf("handle(canceled backoff) error = %v", err)
	}
	batchCtx, cancelBatch := context.WithCancel(context.Background())
	if err := retryConsumer.handleBatch(batchCtx, func(context.Context, []Message) error {
		cancelBatch()
		return ErrFatal
	}, []Message{message}); !errors.Is(err, ErrCanceled) {
		t.Fatalf("handleBatch(canceled handler) error = %v", err)
	}
	batchTimerCtx, cancelBatchTimer := context.WithCancel(context.Background())
	batchTimerStarted := make(chan struct{}, 1)
	batchTimerDone := make(chan error, 1)
	go func() {
		batchTimerDone <- retryConsumer.handleBatch(batchTimerCtx, func(context.Context, []Message) error {
			batchTimerStarted <- struct{}{}
			return ErrFatal
		}, []Message{message})
	}()
	receiveTest(t, batchTimerStarted)
	cancelBatchTimer()
	if err := receiveTest(t, batchTimerDone); !errors.Is(err, ErrCanceled) {
		t.Fatalf("handleBatch(canceled backoff) error = %v", err)
	}
	retryBatch, _ := NewConsumer(ConsumerConfig{
		Stream: "stream", ConsumerName: "consumer",
		Policy: ConsumerPolicy{FailureStrategy: FailureRetry, Retry: RetryPolicy{MaxAttempts: 2, InitialBackoff: time.Nanosecond, MaxBackoff: time.Nanosecond}},
	}, newFakeConsumerTransport())
	attempts := 0
	if err := retryBatch.handleBatch(boundedTestContext(), func(context.Context, []Message) error {
		attempts++
		if attempts == 1 {
			return ErrFatal
		}
		return nil
	}, []Message{message}); err != nil || attempts != 2 {
		t.Fatalf("handleBatch(retry success) = %v after %d attempts", err, attempts)
	}

	publisher := &fakeFailurePublisher{result: DeliveryResult{State: DeliveryRejected}}
	deadLetter, _ := NewConsumer(ConsumerConfig{
		Stream: "stream", ConsumerName: "consumer", Policy: ConsumerPolicy{FailureStrategy: FailureDeadLetter},
		FailurePublisher: publisher, DeadLetterStream: "dead",
	}, newFakeConsumerTransport())
	collision := message
	collision.Properties = []MetadataEntry{{Key: FailureAttemptMetadata}}
	if err := deadLetter.publishFailure(context.Background(), collision, 1); !errors.Is(err, ErrValidation) {
		t.Fatalf("publishFailure(collision) error = %v", err)
	}
	if err := deadLetter.publishFailure(context.Background(), message, 1); !errors.Is(err, ErrConfirmation) {
		t.Fatalf("publishFailure(unconfirmed) error = %v", err)
	}
	confirmedPublisher := &fakeFailurePublisher{result: DeliveryResult{State: DeliveryConfirmed}}
	batchDeadLetter, _ := NewConsumer(ConsumerConfig{
		Stream: "stream", ConsumerName: "consumer", Policy: ConsumerPolicy{FailureStrategy: FailureDeadLetter},
		FailurePublisher: confirmedPublisher, DeadLetterStream: "dead",
	}, newFakeConsumerTransport())
	if err := batchDeadLetter.handleBatch(boundedTestContext(), func(context.Context, []Message) error { return ErrFatal }, []Message{message}); err != nil {
		t.Fatalf("handleBatch(dead letter) error = %v", err)
	}
	failedPublisher := &fakeFailurePublisher{err: ErrAuthorization}
	failedBatch, _ := NewConsumer(ConsumerConfig{
		Stream: "stream", ConsumerName: "consumer", Policy: ConsumerPolicy{FailureStrategy: FailureDeadLetter},
		FailurePublisher: failedPublisher, DeadLetterStream: "dead",
	}, newFakeConsumerTransport())
	if err := failedBatch.handleBatch(boundedTestContext(), func(context.Context, []Message) error { return ErrFatal }, []Message{message}); !errors.Is(err, ErrAuthorization) {
		t.Fatalf("handleBatch(failure publish) error = %v", err)
	}
}

func TestBatchWorkerFlushesOnSizeAndDeadlineAndPropagatesErrors(t *testing.T) {
	t.Parallel()

	message := Message{Stream: "stream", Partition: "stream", Offset: 1, HasOffset: true}
	workerTransport := newFakeConsumerTransport()
	consumer, _ := NewConsumer(ConsumerConfig{Stream: "stream", ConsumerName: "consumer"}, workerTransport)

	sizeCtx, cancelSize := context.WithCancel(context.Background())
	sizeQueue := make(chan Message, 1)
	sizeQueue <- message
	sizeHandled := make(chan struct{}, 1)
	sizeDone := make(chan error, 1)
	go func() {
		sizeDone <- consumer.runBatchWorker(sizeCtx, sizeQueue, BatchPolicy{MaxMessages: 1, MaxWait: time.Second}, func(context.Context, []Message) error {
			sizeHandled <- struct{}{}
			return nil
		})
	}()
	receiveTest(t, sizeHandled)
	receiveTest(t, workerTransport.stored)
	cancelSize()
	if err := receiveTest(t, sizeDone); err != nil {
		t.Fatalf("runBatchWorker(size) error = %v", err)
	}

	deadlineCtx, cancelDeadline := context.WithCancel(context.Background())
	deadlineQueue := make(chan Message, 1)
	deadlineQueue <- message
	deadlineHandled := make(chan struct{}, 1)
	deadlineDone := make(chan error, 1)
	go func() {
		deadlineDone <- consumer.runBatchWorker(deadlineCtx, deadlineQueue, BatchPolicy{MaxMessages: 2, MaxWait: time.Nanosecond}, func(context.Context, []Message) error {
			deadlineHandled <- struct{}{}
			return nil
		})
	}()
	receiveTest(t, deadlineHandled)
	receiveTest(t, workerTransport.stored)
	cancelDeadline()
	if err := receiveTest(t, deadlineDone); err != nil {
		t.Fatalf("runBatchWorker(deadline) error = %v", err)
	}

	errorQueue := make(chan Message, 1)
	errorQueue <- message
	if err := consumer.runBatchWorker(boundedTestContext(), errorQueue, BatchPolicy{MaxMessages: 1, MaxWait: time.Second}, func(context.Context, []Message) error {
		return ErrFatal
	}); !errors.Is(err, ErrHandler) {
		t.Fatalf("runBatchWorker(handler error) = %v", err)
	}
	deadlineErrorQueue := make(chan Message, 1)
	deadlineErrorQueue <- message
	if err := consumer.runBatchWorker(boundedTestContext(), deadlineErrorQueue, BatchPolicy{MaxMessages: 2, MaxWait: time.Nanosecond}, func(context.Context, []Message) error {
		return ErrFatal
	}); !errors.Is(err, ErrHandler) {
		t.Fatalf("runBatchWorker(deadline handler error) = %v", err)
	}

	now := time.Now()
	due := dueConsumerBatches(map[string]*pendingConsumerBatch{
		"due":   {deadline: now},
		"later": {deadline: now.Add(time.Hour)},
	}, now)
	if len(due) != 1 || due[0] != "due" {
		t.Fatalf("dueConsumerBatches() = %#v", due)
	}
}

func TestConsumerCancellationErrorPrefersWorkerFailure(t *testing.T) {
	t.Parallel()

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	workerErrors := make(chan error, 1)
	workerErrors <- ErrHandler
	if err := consumerCancellationError(canceled, workerErrors); !errors.Is(err, ErrHandler) {
		t.Fatalf("consumerCancellationError(worker) = %v", err)
	}
	if err := consumerCancellationError(canceled, make(chan error)); !errors.Is(err, ErrCanceled) {
		t.Fatalf("consumerCancellationError(canceled) = %v", err)
	}
	fullQueue := make(chan Message, 1)
	fullQueue <- Message{Stream: "stream"}
	if err := enqueueConsumerMessage(canceled, fullQueue, make(chan error), Message{Stream: "stream"}); !errors.Is(err, ErrCanceled) {
		t.Fatalf("enqueueConsumerMessage(canceled) = %v", err)
	}
	emptyQueue := make(chan Message, 1)
	if err := enqueueConsumerMessage(context.Background(), emptyQueue, make(chan error), Message{Stream: "stream", Payload: []byte("owned")}); err != nil {
		t.Fatalf("enqueueConsumerMessage() error = %v", err)
	}
	if got := receiveTest(t, emptyQueue); string(got.Payload) != "owned" {
		t.Fatalf("enqueueConsumerMessage() message = %#v", got)
	}

	canceledQueue := make(chan Message)
	if _, ok := nextConsumerWorkerMessage(canceled, canceledQueue); ok {
		t.Fatal("nextConsumerWorkerMessage(canceled) returned a message")
	}
	readyQueue := make(chan Message, 1)
	readyQueue <- Message{Stream: "stream"}
	ctxAfterReceive := &errOnlyCanceledContext{Context: context.Background()}
	if _, ok := nextConsumerWorkerMessage(ctxAfterReceive, readyQueue); ok {
		t.Fatal("nextConsumerWorkerMessage(post-receive cancellation) returned a message")
	}
	readyQueue <- Message{Stream: "stream"}
	if got, ok := nextConsumerWorkerMessage(context.Background(), readyQueue); !ok || got.Stream != "stream" {
		t.Fatalf("nextConsumerWorkerMessage() = %#v, %t", got, ok)
	}
}

type errOnlyCanceledContext struct{ context.Context }

func (*errOnlyCanceledContext) Done() <-chan struct{} { return nil }
func (*errOnlyCanceledContext) Err() error            { return context.Canceled }

type signalingCancelContext struct {
	context.Context
	signal chan struct{}
	once   sync.Once
}

func (ctx *signalingCancelContext) Err() error {
	err := ctx.Context.Err()
	if err != nil {
		ctx.once.Do(func() { close(ctx.signal) })
	}
	return err
}

type cancelingSequenceTransport struct {
	messages []Message
	index    int
	cancelAt int
	cancel   context.CancelFunc
}

func (transport *cancelingSequenceTransport) Next(context.Context) (Message, error) {
	message := transport.messages[transport.index]
	transport.index++
	if transport.index == transport.cancelAt {
		transport.cancel()
	}
	return message, nil
}

func (*cancelingSequenceTransport) StoreOffset(context.Context, string, uint64) error { return nil }
func (*cancelingSequenceTransport) Close() error                                      { return nil }

func TestConsumerLoopsReturnCanceledEnqueueFailures(t *testing.T) {
	t.Parallel()

	limits := DefaultLimits()
	limits.MaxBufferedMessages = 1
	messages := []Message{
		{Stream: "stream", Partition: "stream", Offset: 1, HasOffset: true},
		{Stream: "stream", Partition: "stream", Offset: 2, HasOffset: true},
		{Stream: "stream", Partition: "stream", Offset: 3, HasOffset: true},
	}
	for _, batch := range []bool{false, true} {
		baseCtx, cancel := context.WithCancel(context.Background())
		ctx := &signalingCancelContext{Context: baseCtx, signal: make(chan struct{})}
		transport := &cancelingSequenceTransport{messages: messages, cancelAt: 3, cancel: cancel}
		consumer, err := NewConsumer(ConsumerConfig{
			Stream: "stream", ConsumerName: "consumer", Limits: limits,
		}, transport)
		if err != nil {
			t.Fatalf("NewConsumer() error = %v", err)
		}
		release := make(chan struct{})
		done := make(chan error, 1)
		if batch {
			go func() {
				done <- consumer.consumeBatch(ctx, cancel, BatchPolicy{MaxMessages: 1, MaxWait: time.Second}, func(context.Context, []Message) error {
					<-release
					return nil
				})
			}()
		} else {
			go func() {
				done <- consumer.consume(ctx, cancel, func(context.Context, Message) error {
					<-release
					return nil
				})
			}()
		}
		receiveTest(t, ctx.signal)
		close(release)
		if err := receiveTest(t, done); !errors.Is(err, ErrCanceled) {
			t.Fatalf("consumer loop batch=%t error = %v", batch, err)
		}
	}
}

func TestConsumerAndBatchLoopsReportPausedTransportAndWorkerFailures(t *testing.T) {
	t.Parallel()

	limits := DefaultLimits()
	limits.MaxBufferedMessages = 3
	config := ConsumerConfig{
		Stream: "stream", ConsumerName: "consumer", Limits: limits,
		Policy: ConsumerPolicy{MaxConcurrency: 2},
	}
	pausedConsumer, _ := NewConsumer(config, newFakeConsumerTransport())
	_ = pausedConsumer.Pause()
	pausedCtx, cancelPaused := context.WithCancel(context.Background())
	cancelPaused()
	if err := pausedConsumer.Run(pausedCtx, func(context.Context, Message) error { return nil }); !errors.Is(err, ErrCanceled) {
		t.Fatalf("Run(paused canceled) error = %v", err)
	}

	pausedBatch, _ := NewConsumer(config, newFakeConsumerTransport())
	_ = pausedBatch.Pause()
	batchCtx, cancelBatch := context.WithCancel(context.Background())
	cancelBatch()
	if err := pausedBatch.RunBatch(batchCtx, BatchPolicy{}, func(context.Context, []Message) error { return nil }); !errors.Is(err, ErrCanceled) {
		t.Fatalf("RunBatch(paused canceled) error = %v", err)
	}

	transportError := newFakeConsumerTransport()
	transportError.nextErr = ErrConnection
	batchTransportConsumer, _ := NewConsumer(config, transportError)
	if err := batchTransportConsumer.RunBatch(boundedTestContext(), BatchPolicy{}, func(context.Context, []Message) error { return nil }); !errors.Is(err, ErrConnection) {
		t.Fatalf("RunBatch(transport error) = %v", err)
	}

	workerTransport := newFakeConsumerTransport(Message{Stream: "stream", Partition: "stream", Offset: 1, HasOffset: true})
	workerConsumer, _ := NewConsumer(config, workerTransport)
	if err := workerConsumer.RunBatch(boundedTestContext(), BatchPolicy{MaxMessages: 1}, func(context.Context, []Message) error { return ErrFatal }); !errors.Is(err, ErrHandler) {
		t.Fatalf("RunBatch(worker error) = %v", err)
	}
}

func TestConsumerCloseReportsNilCancellationTimeoutAndTransportFailure(t *testing.T) {
	t.Parallel()

	consumer, _ := NewConsumer(ConsumerConfig{Stream: "stream", ConsumerName: "consumer"}, newFakeConsumerTransport())
	if err := consumer.Close(absentContext); err == nil {
		t.Fatal("Close(nil) succeeded")
	}

	failedTransport := newFakeConsumerTransport()
	failedTransport.closeErr = ErrAuthorization
	failed, _ := NewConsumer(ConsumerConfig{Stream: "stream", ConsumerName: "consumer"}, failedTransport)
	if err := failed.Close(boundedTestContext()); !errors.Is(err, ErrAuthorization) {
		t.Fatalf("Close(transport failure) error = %v", err)
	}

	timed, _ := NewConsumer(ConsumerConfig{
		Stream: "stream", ConsumerName: "consumer", Policy: ConsumerPolicy{CloseTimeout: time.Millisecond},
	}, newFakeConsumerTransport())
	timed.runDone = make(chan struct{})
	if err := timed.Close(boundedTestContext()); !errors.Is(err, ErrTimeout) {
		t.Fatalf("Close(timeout) error = %v", err)
	}

	release := make(chan struct{})
	blockedTransport := newFakeConsumerTransport()
	blockedTransport.closeBlock = release
	blocked, _ := NewConsumer(ConsumerConfig{Stream: "stream", ConsumerName: "consumer"}, blockedTransport)
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := blocked.Close(canceled); !errors.Is(err, ErrCanceled) {
		t.Fatalf("Close(canceled) error = %v", err)
	}
	close(release)

	activeTransport := newFakeConsumerTransport()
	active, _ := NewConsumer(ConsumerConfig{Stream: "stream", ConsumerName: "consumer"}, activeTransport)
	runDone := make(chan error, 1)
	go func() {
		runDone <- active.Run(boundedTestContext(), func(context.Context, Message) error { return nil })
	}()
	receiveTest(t, activeTransport.nextCalled)
	if err := active.Close(boundedTestContext()); err != nil {
		t.Fatalf("Close(active) error = %v", err)
	}
	if err := receiveTest(t, runDone); !errors.Is(err, ErrCanceled) {
		t.Fatalf("active Run() error = %v", err)
	}
}
