package rabbitstream

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"time"
)

const (
	defaultHandlerTimeout  = 30 * time.Second
	maximumHandlerTimeout  = 30 * time.Minute
	defaultConsumerClose   = 30 * time.Second
	maximumConsumerClose   = 5 * time.Minute
	maximumRetryAttempts   = 32
	maximumConsumerWorkers = 256
	defaultBatchMessages   = 100
	defaultBatchWait       = 100 * time.Millisecond
	maximumBatchWait       = time.Minute

	// FailureSourceStreamMetadata identifies the original stream on a retry or
	// dead-letter publication.
	FailureSourceStreamMetadata = "rabbitstream.source.stream"
	// FailureSourcePartitionMetadata identifies the original backing stream.
	FailureSourcePartitionMetadata = "rabbitstream.source.partition"
	// FailureSourceOffsetMetadata identifies the original numeric offset.
	FailureSourceOffsetMetadata = "rabbitstream.source.offset"
	// FailureAttemptMetadata records the bounded handler attempt count.
	FailureAttemptMetadata = "rabbitstream.failure.attempt"
	// FailureCategoryMetadata carries a safe low-cardinality failure class.
	FailureCategoryMetadata = "rabbitstream.failure.category"
)

// OffsetStartKind selects the first delivery requested from RabbitMQ Streams.
type OffsetStartKind uint8

const (
	// OffsetStartStored resumes at the named consumer's broker-stored offset, so
	// the last stored delivery may be observed again under at-least-once policy.
	OffsetStartStored OffsetStartKind = iota
	// OffsetStartBeginning starts at the first retained message.
	OffsetStartBeginning
	// OffsetStartEnd starts with messages appended after the consumer attaches.
	OffsetStartEnd
	// OffsetStartExplicit starts at an exact numeric offset.
	OffsetStartExplicit
	// OffsetStartTimestamp starts at the first message at or after Timestamp.
	OffsetStartTimestamp
)

// StartPosition models RabbitMQ Streams offsets directly. Offset and Timestamp
// are used only by their corresponding kinds.
type StartPosition struct {
	// Kind selects stored, retained beginning, live end, exact offset, or timestamp.
	Kind OffsetStartKind
	// Offset is used only with OffsetStartExplicit.
	Offset uint64
	// Timestamp is used only with OffsetStartTimestamp.
	Timestamp time.Time
}

// FailureStrategy controls handler failure without implying queue-style NACK
// or broker redelivery behavior.
type FailureStrategy uint8

const (
	// FailureStop stops the ordering scope without advancing its offset.
	FailureStop FailureStrategy = iota
	// FailureRetry retries the handler in process within RetryPolicy bounds.
	FailureRetry
	// FailureRetryStream publishes a new record to an explicit retry stream.
	FailureRetryStream
	// FailureDeadLetter publishes a new record to an explicit dead-letter stream.
	FailureDeadLetter
)

// RetryPolicy bounds in-process handler attempts and backoff. MaxAttempts
// includes the initial attempt.
type RetryPolicy struct {
	// MaxAttempts includes the initial handler invocation.
	MaxAttempts int
	// InitialBackoff is the delay before the first retry.
	InitialBackoff time.Duration
	// MaxBackoff caps exponential retry delay.
	MaxBackoff time.Duration
}

// ConsumerPolicy bounds handler execution, retry, and shutdown.
type ConsumerPolicy struct {
	// MaxConcurrency bounds workers across independent partitions.
	MaxConcurrency int
	// HandlerTimeout bounds one handler or batch invocation.
	HandlerTimeout time.Duration
	// CloseTimeout bounds graceful consumer draining.
	CloseTimeout time.Duration
	// OffsetStoreEveryMessages bounds the processed-but-not-stored crash window.
	OffsetStoreEveryMessages int
	// FailureStrategy selects stop, in-process retry, retry stream, or dead letter.
	FailureStrategy FailureStrategy
	// Retry configures bounded in-process handler retries.
	Retry RetryPolicy
}

// ConsumerConfig binds a durable named consumer to one stream or Super Stream.
// Broker offset storage records the last successfully handled message; it is
// not transactional with handler side effects.
type ConsumerConfig struct {
	// Stream selects one direct stream when SuperStream is empty.
	Stream string
	// SuperStream selects a logical partitioned stream when Stream is empty.
	SuperStream string
	// ConsumerName is the stable broker offset-tracking identity.
	ConsumerName string
	// Start selects initial delivery position before stored progress advances.
	Start StartPosition
	// Limits bounds delivered messages and retained metadata.
	Limits Limits
	// Policy bounds concurrency, handler time, retry, offset storage, and close.
	Policy ConsumerPolicy
	// Observer receives bounded best-effort lifecycle signals.
	Observer Observer

	// FailurePublisher confirms retry or dead-letter publication before offset storage.
	FailurePublisher FailurePublisher
	// RetryStream is required by FailureRetryStream.
	RetryStream string
	// DeadLetterStream is required by FailureDeadLetter.
	DeadLetterStream string
}

// FailurePublisher is satisfied by Producer and deliberately exposes only the
// confirmed publish operation needed before source-offset advancement.
type FailurePublisher interface {
	// Publish must return a broker-confirmed outcome before source progress advances.
	Publish(context.Context, Message) (DeliveryResult, error)
}

// MessageHandler handles one borrowed delivery. The message is valid only for
// the call unless Retain is used.
type MessageHandler func(context.Context, Message) error

// BatchMessageHandler handles one borrowed, single-partition delivery batch.
// Messages remain ordered by delivery offset and are valid only for the call
// unless Retain is used on each message that must escape it.
type BatchMessageHandler func(context.Context, []Message) error

// BatchPolicy bounds one consumer handler invocation. Successful batches store
// only their terminal offset, so the crash-redelivery window is at most
// MaxMessages records per partition.
type BatchPolicy struct {
	// MaxMessages bounds records supplied to one batch invocation.
	MaxMessages int
	// MaxWait bounds how long a partial batch waits for another record.
	MaxWait time.Duration
}

func (policy BatchPolicy) normalized(limits Limits) (BatchPolicy, error) {
	if policy.MaxMessages < 0 || policy.MaxWait < 0 {
		return BatchPolicy{}, invalidConfiguration(errors.New("batch policy cannot be negative"))
	}
	if policy.MaxMessages == 0 {
		policy.MaxMessages = min(defaultBatchMessages, limits.MaxBatchMessages)
	}
	if policy.MaxWait == 0 {
		policy.MaxWait = defaultBatchWait
	}
	if policy.MaxMessages > limits.MaxBatchMessages || policy.MaxWait > maximumBatchWait {
		return BatchPolicy{}, invalidConfiguration(errors.New("batch policy exceeds bounds"))
	}
	return policy, nil
}

// ConsumerTransport is the narrow client-adapter boundary. Next and
// StoreOffset must honor context cancellation. A nil StoreOffset error means
// the client accepted the one-way broker command; RabbitMQ does not confirm
// that command transactionally, so a crash may cause safe redelivery.
type ConsumerTransport interface {
	// Next returns the next owned delivery or a stable terminal error.
	Next(context.Context) (Message, error)
	// StoreOffset submits the last successfully handled partition offset.
	StoreOffset(context.Context, string, uint64) error
	// Close releases every transport-owned consumer and connection resource.
	Close() error
}

// Consumer owns bounded workers and broker offset lifecycles. A stable
// partition-to-worker assignment preserves sequential handling within each
// backing stream while allowing independent partitions to run concurrently.
type Consumer struct {
	config    ConsumerConfig
	transport ConsumerTransport

	stateMutex sync.Mutex
	running    bool
	closed     bool
	runCancel  context.CancelFunc
	runDone    chan struct{}

	pauseMutex sync.Mutex
	paused     bool
	resume     chan struct{}

	closeOnce sync.Once
	closeDone chan struct{}
	closeErr  error
}

// Pause stops admission from the transport before the next read. Already
// admitted messages remain bounded and continue processing. Pause is
// idempotent and may be selected before Run starts.
func (consumer *Consumer) Pause() error {
	consumer.stateMutex.Lock()
	defer consumer.stateMutex.Unlock()
	if consumer.closed {
		return &OperationError{Operation: OperationConsume, Category: CategoryClosed}
	}
	consumer.pauseMutex.Lock()
	defer consumer.pauseMutex.Unlock()
	if !consumer.paused {
		consumer.paused = true
		consumer.resume = make(chan struct{})
	}
	return nil
}

// Resume permits transport admission after Pause. It is idempotent.
func (consumer *Consumer) Resume() error {
	consumer.stateMutex.Lock()
	defer consumer.stateMutex.Unlock()
	if consumer.closed {
		return &OperationError{Operation: OperationConsume, Category: CategoryClosed}
	}
	consumer.pauseMutex.Lock()
	defer consumer.pauseMutex.Unlock()
	if consumer.paused {
		consumer.paused = false
		close(consumer.resume)
		consumer.resume = nil
	}
	return nil
}

func (consumer *Consumer) waitWhilePaused(ctx context.Context) error {
	consumer.pauseMutex.Lock()
	if !consumer.paused {
		consumer.pauseMutex.Unlock()
		return nil
	}
	resume := consumer.resume
	consumer.pauseMutex.Unlock()
	select {
	case <-resume:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// NewConsumer constructs a durable policy wrapper and takes ownership of
// transport after a successful return.
func NewConsumer(config ConsumerConfig, transport ConsumerTransport) (*Consumer, error) {
	normalized, err := config.Normalized()
	if err != nil {
		return nil, err
	}
	if transport == nil {
		return nil, invalidConfiguration(errors.New("consumer transport is required"))
	}
	return &Consumer{
		config: normalized, transport: transport, closeDone: make(chan struct{}),
	}, nil
}

// Normalized validates ConsumerConfig and applies finite defaults.
func (config ConsumerConfig) Normalized() (ConsumerConfig, error) {
	if (config.Stream == "") == (config.SuperStream == "") {
		return ConsumerConfig{}, invalidConfiguration(errors.New("exactly one consumer target is required"))
	}
	if config.Limits == (Limits{}) {
		config.Limits = DefaultLimits()
	}
	if err := config.Limits.validate(); err != nil {
		return ConsumerConfig{}, invalidConfiguration(err)
	}
	if config.Stream != "" && invalidIdentifier(config.Stream, config.Limits.MaxStreamNameBytes) {
		return ConsumerConfig{}, invalidConfiguration(errors.New("consumer stream is invalid"))
	}
	if config.SuperStream != "" && invalidIdentifier(config.SuperStream, config.Limits.MaxStreamNameBytes) {
		return ConsumerConfig{}, invalidConfiguration(errors.New("consumer super stream is invalid"))
	}
	if invalidIdentifier(config.ConsumerName, config.Limits.MaxStreamNameBytes) {
		return ConsumerConfig{}, invalidConfiguration(errors.New("consumer name is invalid"))
	}
	if config.Start.Kind > OffsetStartTimestamp ||
		(config.Start.Kind == OffsetStartTimestamp && config.Start.Timestamp.IsZero()) ||
		(config.Start.Kind != OffsetStartTimestamp && !config.Start.Timestamp.IsZero()) ||
		(config.Start.Kind != OffsetStartExplicit && config.Start.Offset != 0) {
		return ConsumerConfig{}, invalidConfiguration(errors.New("consumer start position is invalid"))
	}
	policy := &config.Policy
	if policy.HandlerTimeout < 0 || policy.CloseTimeout < 0 ||
		policy.MaxConcurrency < 0 || policy.OffsetStoreEveryMessages < 0 ||
		policy.Retry.MaxAttempts < 0 || policy.Retry.InitialBackoff < 0 ||
		policy.Retry.MaxBackoff < 0 {
		return ConsumerConfig{}, invalidConfiguration(errors.New("consumer policy cannot be negative"))
	}
	if policy.HandlerTimeout == 0 {
		policy.HandlerTimeout = defaultHandlerTimeout
	}
	if policy.MaxConcurrency == 0 {
		policy.MaxConcurrency = 1
	}
	if policy.OffsetStoreEveryMessages == 0 {
		policy.OffsetStoreEveryMessages = 1
	}
	if policy.CloseTimeout == 0 {
		policy.CloseTimeout = defaultConsumerClose
	}
	if policy.HandlerTimeout > maximumHandlerTimeout || policy.CloseTimeout > maximumConsumerClose ||
		policy.FailureStrategy > FailureDeadLetter ||
		policy.MaxConcurrency > maximumConsumerWorkers ||
		policy.MaxConcurrency > config.Limits.MaxBufferedMessages ||
		policy.OffsetStoreEveryMessages > config.Limits.MaxBatchMessages {
		return ConsumerConfig{}, invalidConfiguration(errors.New("consumer policy exceeds bounds"))
	}
	if policy.FailureStrategy == FailureRetry {
		if policy.Retry.MaxAttempts == 0 {
			policy.Retry.MaxAttempts = 3
		}
		if policy.Retry.InitialBackoff == 0 {
			policy.Retry.InitialBackoff = 100 * time.Millisecond
		}
		if policy.Retry.MaxBackoff == 0 {
			policy.Retry.MaxBackoff = time.Second
		}
		if policy.Retry.MaxAttempts > maximumRetryAttempts ||
			policy.Retry.InitialBackoff > policy.Retry.MaxBackoff {
			return ConsumerConfig{}, invalidConfiguration(errors.New("retry policy exceeds bounds"))
		}
	} else if policy.Retry != (RetryPolicy{}) {
		return ConsumerConfig{}, invalidConfiguration(errors.New("retry policy requires retry strategy"))
	}
	switch policy.FailureStrategy {
	case FailureRetryStream:
		if config.FailurePublisher == nil ||
			invalidIdentifier(config.RetryStream, config.Limits.MaxStreamNameBytes) ||
			config.DeadLetterStream != "" {
			return ConsumerConfig{}, invalidConfiguration(errors.New("retry stream policy is invalid"))
		}
	case FailureDeadLetter:
		if config.FailurePublisher == nil ||
			invalidIdentifier(config.DeadLetterStream, config.Limits.MaxStreamNameBytes) ||
			config.RetryStream != "" {
			return ConsumerConfig{}, invalidConfiguration(errors.New("dead-letter stream policy is invalid"))
		}
	default:
		if config.FailurePublisher != nil || config.RetryStream != "" || config.DeadLetterStream != "" {
			return ConsumerConfig{}, invalidConfiguration(errors.New("failure publisher requires a stream strategy"))
		}
	}
	return config, nil
}

// Run consumes until cancellation or the first transport, handler, or offset
// failure. It never stores an offset before successful handler completion.
func (consumer *Consumer) Run(ctx context.Context, handler MessageHandler) error {
	if ctx == nil || handler == nil {
		return &OperationError{Operation: OperationConsume, Category: CategoryValidation}
	}
	runCtx, cancel := context.WithCancel(ctx)
	consumer.stateMutex.Lock()
	if consumer.closed {
		consumer.stateMutex.Unlock()
		cancel()
		return &OperationError{Operation: OperationConsume, Category: CategoryClosed}
	}
	if consumer.running {
		consumer.stateMutex.Unlock()
		cancel()
		return &OperationError{Operation: OperationConsume, Category: CategoryInvalidConfiguration}
	}
	consumer.running = true
	consumer.runCancel = cancel
	consumer.runDone = make(chan struct{})
	done := consumer.runDone
	consumer.stateMutex.Unlock()
	defer func() {
		cancel()
		consumer.stateMutex.Lock()
		consumer.running = false
		consumer.runCancel = nil
		close(done)
		consumer.stateMutex.Unlock()
	}()

	return consumer.consume(runCtx, cancel, handler)
}

// RunBatch consumes single-partition batches until cancellation or the first
// transport, handler, publication, or offset failure. A partial batch held at
// cancellation is left unstored for safe redelivery.
func (consumer *Consumer) RunBatch(
	ctx context.Context,
	policy BatchPolicy,
	handler BatchMessageHandler,
) error {
	if ctx == nil || handler == nil {
		return &OperationError{Operation: OperationConsume, Category: CategoryValidation}
	}
	normalizedPolicy, err := policy.normalized(consumer.config.Limits)
	if err != nil {
		return err
	}
	runCtx, cancel := context.WithCancel(ctx)
	consumer.stateMutex.Lock()
	if consumer.closed {
		consumer.stateMutex.Unlock()
		cancel()
		return &OperationError{Operation: OperationConsume, Category: CategoryClosed}
	}
	if consumer.running {
		consumer.stateMutex.Unlock()
		cancel()
		return &OperationError{Operation: OperationConsume, Category: CategoryInvalidConfiguration}
	}
	consumer.running = true
	consumer.runCancel = cancel
	consumer.runDone = make(chan struct{})
	done := consumer.runDone
	consumer.stateMutex.Unlock()
	defer func() {
		cancel()
		consumer.stateMutex.Lock()
		consumer.running = false
		consumer.runCancel = nil
		close(done)
		consumer.stateMutex.Unlock()
	}()

	return consumer.consumeBatch(runCtx, cancel, normalizedPolicy, handler)
}

type pendingConsumerBatch struct {
	messages []Message
	deadline time.Time
}

func (consumer *Consumer) consumeBatch(
	ctx context.Context,
	cancel context.CancelFunc,
	policy BatchPolicy,
	handler BatchMessageHandler,
) error {
	workerCount := consumer.config.Policy.MaxConcurrency
	workerQueues := make([]chan Message, workerCount)
	workerErrors := make(chan error, 1)
	var workers sync.WaitGroup
	for index := range workerQueues {
		capacity := consumerWorkerQueueCapacity(consumer.config.Limits.MaxBufferedMessages, workerCount, index)
		workerQueues[index] = make(chan Message, capacity)
		workers.Add(1)
		go func(queue <-chan Message) {
			defer workers.Done()
			if err := consumer.runBatchWorker(ctx, queue, policy, handler); err != nil {
				select {
				case workerErrors <- err:
				default:
				}
				cancel()
			}
		}(workerQueues[index])
	}
	defer func() {
		cancel()
		workers.Wait()
	}()

	for {
		if err := consumer.waitWhilePaused(ctx); err != nil {
			return &OperationError{Operation: OperationConsume, Category: CategoryCanceled, Cause: err}
		}
		message, err := consumer.transport.Next(ctx)
		if err != nil {
			select {
			case workerErr := <-workerErrors:
				return workerErr
			default:
			}
			if ctx.Err() != nil {
				return consumerCancellationError(ctx, workerErrors)
			}
			return &OperationError{
				Operation: OperationConsume, Category: categoryForError(err, CategoryConnection), Cause: err,
			}
		}
		observe(consumer.config.Observer, Observation{
			Kind: ObservationConsumerMessage, Count: 1, Bytes: uint64(len(message.Payload)),
		})
		queue := workerQueues[consumerWorker(message.Partition, workerCount)]
		if err := enqueueConsumerMessage(ctx, queue, workerErrors, message); err != nil {
			return err
		}
	}
}

func (consumer *Consumer) runBatchWorker(
	ctx context.Context,
	queue <-chan Message,
	policy BatchPolicy,
	handler BatchMessageHandler,
) error {
	batches := make(map[string]*pendingConsumerBatch)
	timer := time.NewTimer(time.Hour)
	timer.Stop()
	defer timer.Stop()
	for {
		var timerChannel <-chan time.Time
		if deadline, ok := earliestBatchDeadline(batches); ok {
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			delay := max(time.Until(deadline), 0)
			timer.Reset(delay)
			timerChannel = timer.C
		}
		select {
		case <-ctx.Done():
			return nil
		case message := <-queue:
			batch := batches[message.Partition]
			if batch == nil {
				batch = &pendingConsumerBatch{deadline: time.Now().Add(policy.MaxWait)}
				batches[message.Partition] = batch
			}
			batch.messages = append(batch.messages, message)
			if len(batch.messages) >= policy.MaxMessages {
				if err := consumer.processBatch(ctx, handler, batch.messages); err != nil {
					return err
				}
				delete(batches, message.Partition)
			}
		case now := <-timerChannel:
			for _, partition := range dueConsumerBatches(batches, now) {
				batch := batches[partition]
				if err := consumer.processBatch(ctx, handler, batch.messages); err != nil {
					return err
				}
				delete(batches, partition)
			}
		}
	}
}

func dueConsumerBatches(batches map[string]*pendingConsumerBatch, now time.Time) []string {
	due := make([]string, 0, len(batches))
	for partition, batch := range batches {
		if !batch.deadline.After(now) {
			due = append(due, partition)
		}
	}
	return due
}

func earliestBatchDeadline(batches map[string]*pendingConsumerBatch) (time.Time, bool) {
	var earliest time.Time
	for _, batch := range batches {
		if earliest.IsZero() || batch.deadline.Before(earliest) {
			earliest = batch.deadline
		}
	}
	return earliest, !earliest.IsZero()
}

func (consumer *Consumer) processBatch(
	ctx context.Context,
	handler BatchMessageHandler,
	messages []Message,
) error {
	if len(messages) == 0 {
		return &OperationError{Operation: OperationConsume, Category: CategoryValidation}
	}
	partition := messages[0].Partition
	lastOffset := messages[0].Offset
	for _, message := range messages[1:] {
		if message.Partition != partition || message.Offset < lastOffset {
			return &OperationError{Operation: OperationConsume, Category: CategoryValidation}
		}
		lastOffset = message.Offset
	}
	started := time.Now()
	if err := consumer.handleBatch(ctx, handler, messages); err != nil {
		observe(consumer.config.Observer, Observation{
			Kind: ObservationHandlerError, Count: uint64(len(messages)), Duration: time.Since(started),
		})
		return err
	}
	observe(consumer.config.Observer, Observation{
		Kind: ObservationHandlerSuccess, Count: uint64(len(messages)), Duration: time.Since(started),
	})
	if err := consumer.transport.StoreOffset(ctx, partition, lastOffset); err != nil {
		if ctx.Err() != nil {
			return &OperationError{Operation: OperationConsume, Category: CategoryCanceled, Cause: ctx.Err()}
		}
		return &OperationError{Operation: OperationConsume, Category: CategoryOffset, Cause: err}
	}
	observe(consumer.config.Observer, Observation{
		Kind: ObservationOffsetStoreAccepted, Count: 1, Value: lastOffset,
	})
	return nil
}

func (consumer *Consumer) handleBatch(
	ctx context.Context,
	handler BatchMessageHandler,
	messages []Message,
) error {
	attempts := 1
	if consumer.config.Policy.FailureStrategy == FailureRetry {
		attempts = consumer.config.Policy.Retry.MaxAttempts
	}
	backoff := consumer.config.Policy.Retry.InitialBackoff
	var lastErr error
	for attempt := 0; attempt < attempts; attempt++ {
		if attempt > 0 {
			timer := time.NewTimer(backoff)
			select {
			case <-timer.C:
			case <-ctx.Done():
				timer.Stop()
				return &OperationError{Operation: OperationConsume, Category: CategoryCanceled, Cause: ctx.Err()}
			}
			backoff = nextConsumerBackoff(backoff, consumer.config.Policy.Retry.MaxBackoff)
			observe(consumer.config.Observer, Observation{
				Kind: ObservationHandlerRetry, Count: 1, Value: uint64(attempt + 1),
			})
		}
		handlerCtx, cancel := context.WithTimeout(ctx, consumer.config.Policy.HandlerTimeout)
		lastErr = callBatchHandler(handlerCtx, handler, messages)
		cancel()
		if lastErr == nil {
			return nil
		}
		if ctx.Err() != nil {
			return &OperationError{Operation: OperationConsume, Category: CategoryCanceled, Cause: ctx.Err()}
		}
	}
	if consumer.config.Policy.FailureStrategy == FailureRetryStream ||
		consumer.config.Policy.FailureStrategy == FailureDeadLetter {
		for _, message := range messages {
			if err := consumer.publishFailure(ctx, message, attempts); err != nil {
				return err
			}
		}
		return nil
	}
	return &OperationError{Operation: OperationConsume, Category: CategoryHandler, Cause: lastErr}
}

func callBatchHandler(
	ctx context.Context,
	handler BatchMessageHandler,
	messages []Message,
) (err error) {
	defer func() {
		if recover() != nil {
			err = errors.New("batch message handler panicked")
		}
	}()
	return handler(ctx, messages)
}

func (consumer *Consumer) consume(
	ctx context.Context,
	cancel context.CancelFunc,
	handler MessageHandler,
) error {
	workerCount := consumer.config.Policy.MaxConcurrency
	workerQueues := make([]chan Message, workerCount)
	workerErrors := make(chan error, 1)
	var workers sync.WaitGroup
	for index := range workerQueues {
		capacity := consumerWorkerQueueCapacity(consumer.config.Limits.MaxBufferedMessages, workerCount, index)
		workerQueues[index] = make(chan Message, capacity)
		workers.Add(1)
		go func(queue <-chan Message) {
			defer workers.Done()
			successfulSinceStore := make(map[string]int)
			for {
				message, ok := nextConsumerWorkerMessage(ctx, queue)
				if !ok {
					return
				}
				count := successfulSinceStore[message.Partition] + 1
				store := count >= consumer.config.Policy.OffsetStoreEveryMessages
				if err := consumer.process(ctx, handler, message, store); err != nil {
					select {
					case workerErrors <- err:
					default:
					}
					cancel()
					return
				}
				if store {
					delete(successfulSinceStore, message.Partition)
				} else {
					successfulSinceStore[message.Partition] = count
				}
			}
		}(workerQueues[index])
	}
	defer func() {
		cancel()
		workers.Wait()
	}()

	for {
		if err := consumer.waitWhilePaused(ctx); err != nil {
			return &OperationError{Operation: OperationConsume, Category: CategoryCanceled, Cause: err}
		}
		message, err := consumer.transport.Next(ctx)
		if err != nil {
			select {
			case workerErr := <-workerErrors:
				return workerErr
			default:
			}
			if ctx.Err() != nil {
				return consumerCancellationError(ctx, workerErrors)
			}
			return &OperationError{
				Operation: OperationConsume, Category: categoryForError(err, CategoryConnection), Cause: err,
			}
		}
		observe(consumer.config.Observer, Observation{
			Kind: ObservationConsumerMessage, Count: 1, Bytes: uint64(len(message.Payload)),
		})
		queue := workerQueues[consumerWorker(message.Partition, workerCount)]
		if err := enqueueConsumerMessage(ctx, queue, workerErrors, message); err != nil {
			return err
		}
	}
}

func (consumer *Consumer) process(
	ctx context.Context,
	handler MessageHandler,
	message Message,
	store bool,
) error {
	started := time.Now()
	if err := consumer.handle(ctx, handler, message); err != nil {
		observation := Observation{Kind: ObservationHandlerError, Count: 1, Duration: time.Since(started)}
		var operationErr *OperationError
		if errors.As(err, &operationErr) {
			observation.Category = operationErr.Category
		}
		observe(consumer.config.Observer, observation)
		return err
	}
	observe(consumer.config.Observer, Observation{
		Kind: ObservationHandlerSuccess, Count: 1, Duration: time.Since(started),
	})
	if !store {
		return nil
	}
	if err := consumer.transport.StoreOffset(ctx, message.Partition, message.Offset); err != nil {
		if ctx.Err() != nil {
			return &OperationError{Operation: OperationConsume, Category: CategoryCanceled, Cause: ctx.Err()}
		}
		return &OperationError{Operation: OperationConsume, Category: CategoryOffset, Cause: err}
	}
	observe(consumer.config.Observer, Observation{
		Kind: ObservationOffsetStoreAccepted, Count: 1, Value: message.Offset,
	})
	return nil
}

func consumerWorker(partition string, workerCount int) int {
	hash := uint32(2166136261)
	for index := 0; index < len(partition); index++ {
		hash ^= uint32(partition[index])
		hash *= 16777619
	}
	return int(hash % uint32(workerCount))
}

func (consumer *Consumer) handle(ctx context.Context, handler MessageHandler, message Message) error {
	attempts := 1
	if consumer.config.Policy.FailureStrategy == FailureRetry {
		attempts = consumer.config.Policy.Retry.MaxAttempts
	}
	backoff := consumer.config.Policy.Retry.InitialBackoff
	var lastErr error
	for attempt := 0; attempt < attempts; attempt++ {
		if attempt > 0 {
			timer := time.NewTimer(backoff)
			select {
			case <-timer.C:
			case <-ctx.Done():
				timer.Stop()
				return &OperationError{Operation: OperationConsume, Category: CategoryCanceled, Cause: ctx.Err()}
			}
			backoff = nextConsumerBackoff(backoff, consumer.config.Policy.Retry.MaxBackoff)
			observe(consumer.config.Observer, Observation{
				Kind: ObservationHandlerRetry, Count: 1, Value: uint64(attempt + 1),
			})
		}
		handlerCtx, cancel := context.WithTimeout(ctx, consumer.config.Policy.HandlerTimeout)
		lastErr = callHandler(handlerCtx, handler, message)
		cancel()
		if lastErr == nil {
			return nil
		}
		if ctx.Err() != nil {
			return &OperationError{Operation: OperationConsume, Category: CategoryCanceled, Cause: ctx.Err()}
		}
	}
	if consumer.config.Policy.FailureStrategy == FailureRetryStream ||
		consumer.config.Policy.FailureStrategy == FailureDeadLetter {
		return consumer.publishFailure(ctx, message, attempts)
	}
	return &OperationError{Operation: OperationConsume, Category: CategoryHandler, Cause: lastErr}
}

func consumerCancellationError(ctx context.Context, workerErrors <-chan error) error {
	select {
	case workerErr := <-workerErrors:
		return workerErr
	default:
		return &OperationError{Operation: OperationConsume, Category: CategoryCanceled, Cause: ctx.Err()}
	}
}

func consumerWorkerQueueCapacity(bufferedMessages, workerCount, index int) int {
	baseCapacity := bufferedMessages / workerCount
	if index < bufferedMessages%workerCount {
		return baseCapacity + 1
	}
	return baseCapacity
}

func nextConsumerBackoff(current, maximum time.Duration) time.Duration {
	return min(current*2, maximum)
}

func enqueueConsumerMessage(
	ctx context.Context,
	queue chan<- Message,
	workerErrors <-chan error,
	message Message,
) error {
	select {
	case queue <- message.Retain():
		return nil
	case <-ctx.Done():
		return consumerCancellationError(ctx, workerErrors)
	}
}

func nextConsumerWorkerMessage(ctx context.Context, queue <-chan Message) (Message, bool) {
	select {
	case <-ctx.Done():
		return Message{}, false
	case message := <-queue:
		if ctx.Err() != nil {
			return Message{}, false
		}
		return message, true
	}
}

func (consumer *Consumer) publishFailure(ctx context.Context, source Message, attempts int) error {
	target := consumer.config.RetryStream
	if consumer.config.Policy.FailureStrategy == FailureDeadLetter {
		target = consumer.config.DeadLetterStream
	}
	reserved := map[string]struct{}{
		FailureSourceStreamMetadata:    {},
		FailureSourcePartitionMetadata: {},
		FailureSourceOffsetMetadata:    {},
		FailureAttemptMetadata:         {},
		FailureCategoryMetadata:        {},
	}
	for _, property := range source.Properties {
		if _, collision := reserved[property.Key]; collision {
			return &OperationError{Operation: OperationConsume, Category: CategoryValidation}
		}
	}
	failure := source.Retain()
	failure.Stream = target
	failure.SuperStream = ""
	failure.Partition = ""
	failure.Offset = 0
	failure.HasOffset = false
	failure.PublishingID = 0
	failure.HasPublishingID = false
	failure.BrokerMetadata = nil
	failure.Properties = append(failure.Properties,
		MetadataEntry{Key: FailureSourceStreamMetadata, Value: []byte(source.Stream)},
		MetadataEntry{Key: FailureSourcePartitionMetadata, Value: []byte(source.Partition)},
		MetadataEntry{Key: FailureSourceOffsetMetadata, Value: []byte(strconv.FormatUint(source.Offset, 10))},
		MetadataEntry{Key: FailureAttemptMetadata, Value: []byte(strconv.FormatUint(uint64(attempts), 10))},
		MetadataEntry{Key: FailureCategoryMetadata, Value: []byte(CategoryHandler)},
	)
	result, err := consumer.config.FailurePublisher.Publish(ctx, failure)
	if err != nil {
		observe(consumer.config.Observer, Observation{
			Kind: ObservationFailurePublishError, Count: 1,
			Category: categoryForError(err, CategoryConfirmation),
		})
		return err
	}
	if result.State != DeliveryConfirmed {
		observe(consumer.config.Observer, Observation{
			Kind: ObservationFailurePublishError, Count: 1, Category: CategoryConfirmation,
		})
		return &OperationError{Operation: OperationPublish, Category: CategoryConfirmation}
	}
	kind := ObservationRetryStreamPublished
	if consumer.config.Policy.FailureStrategy == FailureDeadLetter {
		kind = ObservationDeadLetterPublished
	}
	observe(consumer.config.Observer, Observation{Kind: kind, Count: 1})
	return nil
}

func callHandler(ctx context.Context, handler MessageHandler, message Message) (err error) {
	defer func() {
		if recover() != nil {
			err = errors.New("message handler panicked")
		}
	}()
	return handler(ctx, message)
}

// Close cancels an active Run, waits within caller and policy bounds, and
// closes the owned transport exactly once.
func (consumer *Consumer) Close(ctx context.Context) error {
	if ctx == nil {
		return validationError(errors.New("close context is nil"))
	}
	consumer.closeOnce.Do(func() {
		consumer.stateMutex.Lock()
		consumer.closed = true
		if consumer.runCancel != nil {
			consumer.runCancel()
		}
		runDone := consumer.runDone
		consumer.stateMutex.Unlock()
		go consumer.close(runDone)
	})
	select {
	case <-consumer.closeDone:
		return consumer.closeErr
	case <-ctx.Done():
		return &OperationError{Operation: OperationClose, Category: CategoryCanceled, Cause: ctx.Err()}
	}
}

func (consumer *Consumer) close(runDone <-chan struct{}) {
	started := time.Now()
	defer func() {
		observe(consumer.config.Observer, Observation{
			Kind: ObservationConsumerShutdown, Count: 1, Duration: time.Since(started),
		})
		close(consumer.closeDone)
	}()
	timer := time.NewTimer(consumer.config.Policy.CloseTimeout)
	defer timer.Stop()
	if runDone != nil {
		select {
		case <-runDone:
		case <-timer.C:
			consumer.closeErr = &OperationError{Operation: OperationClose, Category: CategoryTimeout, Cause: ErrTimeout}
			return
		}
	}
	if err := consumer.transport.Close(); err != nil {
		consumer.closeErr = &OperationError{
			Operation: OperationClose, Category: categoryForError(err, CategoryConnection), Cause: err,
		}
	}
}
