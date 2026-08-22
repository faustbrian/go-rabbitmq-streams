package rabbitstream

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"time"
)

const (
	defaultMaxOutstanding       = 256
	maximumMaxOutstanding       = 65_536
	defaultConfirmationTimeout  = 10 * time.Second
	maximumConfirmationTimeout  = 5 * time.Minute
	defaultProducerCloseTimeout = 30 * time.Second
	maximumProducerCloseTimeout = 5 * time.Minute
	// MaxSuperStreamPartitions bounds topology snapshots and producer fan-out.
	MaxSuperStreamPartitions = 1024
)

// RoutingStrategy selects a reviewed Super Stream routing contract.
type RoutingStrategy uint8

const (
	// RoutingHash uses the RabbitMQ client-compatible Murmur3 strategy. Ordering
	// is stable only while the ordered backing-partition topology is unchanged.
	RoutingHash RoutingStrategy = iota
)

// DeduplicationPolicy selects whether publishing IDs participate in RabbitMQ
// broker-side producer deduplication. It is not an end-to-end exactly-once
// guarantee.
type DeduplicationPolicy uint8

const (
	// DeduplicationNone leaves broker-side publishing-ID deduplication disabled.
	DeduplicationNone DeduplicationPolicy = iota
	// DeduplicationPublishingID requires a stable producer name and an explicit
	// publishing ID on every message.
	DeduplicationPublishingID
)

// ProducerPolicy bounds confirmations, in-flight memory, and shutdown. A
// producer name activates broker-side deduplication and must therefore remain
// stable and unique for the target stream.
type ProducerPolicy struct {
	// MaxOutstanding bounds sent messages awaiting confirmation.
	MaxOutstanding int
	// ConfirmationTimeout bounds how long a sent message awaits broker certainty.
	ConfirmationTimeout time.Duration
	// CloseTimeout bounds confirmation draining during shutdown.
	CloseTimeout time.Duration
	// Deduplication selects explicit broker publishing-ID deduplication policy.
	Deduplication DeduplicationPolicy
	// ProducerName is the stable broker deduplication identity when enabled.
	ProducerName string
}

// ProducerConfig binds one producer to exactly one stream or Super Stream.
// Super Stream publishing always requires a non-empty routing key so ordering
// decisions cannot silently fall back to an unstable default.
type ProducerConfig struct {
	// Stream selects one direct stream when SuperStream is empty.
	Stream string
	// SuperStream selects a logical partitioned stream when Stream is empty.
	SuperStream string
	// RoutingStrategy selects the reviewed key-to-partition algorithm.
	RoutingStrategy RoutingStrategy
	// ExpectedPartitions rejects an unexpected non-zero Super Stream partition count.
	ExpectedPartitions int
	// Limits bounds messages, batches, metadata, and asynchronous buffering.
	Limits Limits
	// Policy bounds confirmations, outstanding sends, deduplication, and close.
	Policy ProducerPolicy
	// Observer receives bounded best-effort lifecycle signals.
	Observer Observer
}

// DeliveryState describes the caller-visible certainty of one publish.
type DeliveryState uint8

const (
	// DeliveryNotSent means validation, cancellation, closure, or local send
	// failure happened before the transport accepted the message.
	DeliveryNotSent DeliveryState = iota
	// DeliveryConfirmed means the broker confirmed persistence.
	DeliveryConfirmed
	// DeliveryRejected means the broker definitively rejected the message.
	DeliveryRejected
	// DeliveryAmbiguous means transmission occurred but confirmation was not
	// observed before cancellation, timeout, or connection loss.
	DeliveryAmbiguous
)

// DeliveryResult is the per-message publish outcome. Ordering is scoped to
// Stream or Partition; no global order exists across Super Stream partitions.
type DeliveryResult struct {
	// State expresses whether the outcome is unsent, confirmed, rejected, or ambiguous.
	State DeliveryState
	// Stream is the direct target when publishing without a Super Stream.
	Stream string
	// SuperStream is the logical target for partitioned publishing.
	SuperStream string
	// Partition is the backing stream selected by routing when known.
	Partition string
	// PublishingID is the broker sequence associated with this result.
	PublishingID uint64
}

// PublishOutcome is the terminal result of one accepted asynchronous publish.
// The result channel returned by PublishAsync always receives exactly one
// outcome and is then closed.
type PublishOutcome struct {
	// Result is the caller-visible delivery certainty.
	Result DeliveryResult
	// Err is the stable operation failure associated with Result.
	Err error
}

// BatchPublishError identifies the first message that did not complete. Later
// results remain DeliveryNotSent. Index is -1 when aggregate batch validation
// failed before an individual message could be selected.
type BatchPublishError struct {
	// Index is the first failed message, or -1 for aggregate validation failure.
	Index int
	// Cause is the stable underlying per-message or validation failure.
	Cause error
}

// Error renders only the failed index and never message contents.
func (err *BatchPublishError) Error() string {
	if err == nil {
		return "<nil>"
	}
	return fmt.Sprintf("rabbitstream batch publish failed at index %d", err.Index)
}

// Unwrap preserves the stable per-message failure category.
func (err *BatchPublishError) Unwrap() error {
	if err == nil {
		return nil
	}
	return err.Cause
}

// TransportConfirmation is the stable result supplied by a ProducerTransport.
// Cause must already be safe for programmatic exposure and diagnostics.
type TransportConfirmation struct {
	// Confirmed reports a definitive broker confirmation.
	Confirmed bool
	// BrokerRejected reports a definitive broker rejection.
	BrokerRejected bool
	// Ambiguous reports transmission without observed confirmation or rejection.
	Ambiguous bool
	// PublishingID is the broker sequence returned by the adapter.
	PublishingID uint64
	// Partition is the backing stream that handled the publish.
	Partition string
	// Cause is a safe programmatic transport failure.
	Cause error
}

// ProducerTransport is the narrow adapter boundary implemented by the nested
// RabbitMQ client module. Send receives an owned Message and must invoke confirm
// at most once. It may use ctx only for work known to precede transmission,
// such as bounded connection admission. A nil Send error means transmission may
// have occurred and later cancellation must be represented as ambiguous.
type ProducerTransport interface {
	// Send admits one owned message and invokes confirm at most once.
	Send(context.Context, Message, func(TransportConfirmation)) error
	// Close releases all transport resources and is idempotent.
	Close() error
}

// Producer owns one bounded publishing lifecycle. It is safe for concurrent
// use. Message confirmation order can differ from caller goroutine completion
// order; RabbitMQ ordering remains scoped to the selected backing stream.
type Producer struct {
	config     ProducerConfig
	transport  ProducerTransport
	slots      chan struct{}
	asyncSlots chan struct{}

	stateMutex  sync.Mutex
	closed      bool
	active      sync.WaitGroup
	asyncActive sync.WaitGroup

	closeOnce sync.Once
	closeDone chan struct{}
	closeErr  error
}

// NewProducer constructs the policy wrapper around a transport. The wrapper
// owns transport after a successful return and must be closed.
func NewProducer(config ProducerConfig, transport ProducerTransport) (*Producer, error) {
	return newProducer(config, transport)
}

func newProducer(config ProducerConfig, transport ProducerTransport) (*Producer, error) {
	normalized, err := config.normalized()
	if err != nil {
		return nil, err
	}
	if transport == nil {
		return nil, invalidConfiguration(errors.New("producer transport is required"))
	}
	return &Producer{
		config:     normalized,
		transport:  transport,
		slots:      make(chan struct{}, normalized.Policy.MaxOutstanding),
		asyncSlots: make(chan struct{}, normalized.Limits.MaxBufferedMessages),
		closeDone:  make(chan struct{}),
	}, nil
}

// PublishBatch validates the entire bounded batch before allocating per-message
// results or sending anything, then publishes in input order. It stops at the
// first per-message failure so partial delivery is explicit.
func (producer *Producer) PublishBatch(
	ctx context.Context,
	messages []Message,
) ([]DeliveryResult, error) {
	if err := ValidateBatch(messages, producer.config.Limits); err != nil {
		return nil, &BatchPublishError{Index: -1, Cause: err}
	}
	results := make([]DeliveryResult, len(messages))
	for index := range results {
		results[index] = DeliveryResult{
			State:       DeliveryNotSent,
			Stream:      producer.config.Stream,
			SuperStream: producer.config.SuperStream,
		}
	}
	for index, message := range messages {
		if err := producer.validateMessage(message); err != nil {
			return results, &BatchPublishError{Index: index, Cause: err}
		}
	}
	for index, message := range messages {
		result, err := producer.Publish(ctx, message)
		results[index] = result
		if err != nil {
			return results, &BatchPublishError{Index: index, Cause: err}
		}
	}
	return results, nil
}

// PublishAsync retains message before returning and admits at most
// Limits.MaxBufferedMessages asynchronous operations. Cancellation before
// admission is definite; cancellation after transport send remains ambiguous.
func (producer *Producer) PublishAsync(
	ctx context.Context,
	message Message,
) (<-chan PublishOutcome, error) {
	if ctx == nil {
		return nil, validationError(errors.New("publish context is nil"))
	}
	if err := ctx.Err(); err != nil {
		return nil, &OperationError{Operation: OperationPublish, Category: CategoryCanceled, Cause: err}
	}
	if err := producer.validateMessage(message); err != nil {
		return nil, err
	}
	select {
	case producer.asyncSlots <- struct{}{}:
	case <-ctx.Done():
		return nil, &OperationError{Operation: OperationPublish, Category: CategoryCanceled, Cause: ctx.Err()}
	}

	producer.stateMutex.Lock()
	if producer.closed {
		producer.stateMutex.Unlock()
		<-producer.asyncSlots
		return nil, &OperationError{Operation: OperationPublish, Category: CategoryClosed}
	}
	producer.asyncActive.Add(1)
	producer.stateMutex.Unlock()

	outcomes := make(chan PublishOutcome, 1)
	owned := message.Retain()
	go func() {
		defer producer.asyncActive.Done()
		defer func() { <-producer.asyncSlots }()
		result, err := producer.Publish(ctx, owned)
		outcomes <- PublishOutcome{Result: result, Err: err}
		close(outcomes)
	}()
	return outcomes, nil
}

func (config ProducerConfig) normalized() (ProducerConfig, error) {
	if (config.Stream == "") == (config.SuperStream == "") {
		return ProducerConfig{}, invalidConfiguration(errors.New("exactly one producer target is required"))
	}
	if config.Limits == (Limits{}) {
		config.Limits = DefaultLimits()
	}
	if err := config.Limits.validate(); err != nil {
		return ProducerConfig{}, invalidConfiguration(err)
	}
	if config.Stream != "" && invalidIdentifier(config.Stream, config.Limits.MaxStreamNameBytes) {
		return ProducerConfig{}, invalidConfiguration(errors.New("producer stream is invalid"))
	}
	if config.RoutingStrategy > RoutingHash || config.ExpectedPartitions < 0 ||
		config.ExpectedPartitions > MaxSuperStreamPartitions ||
		(config.Stream != "" && (config.RoutingStrategy != RoutingHash || config.ExpectedPartitions != 0)) {
		return ProducerConfig{}, invalidConfiguration(errors.New("producer routing policy is invalid"))
	}
	if config.SuperStream != "" && invalidIdentifier(config.SuperStream, config.Limits.MaxStreamNameBytes) {
		return ProducerConfig{}, invalidConfiguration(errors.New("producer super stream is invalid"))
	}
	if config.Policy.MaxOutstanding < 0 || config.Policy.ConfirmationTimeout < 0 ||
		config.Policy.CloseTimeout < 0 {
		return ProducerConfig{}, invalidConfiguration(errors.New("producer policy cannot be negative"))
	}
	if config.Policy.MaxOutstanding == 0 {
		config.Policy.MaxOutstanding = defaultMaxOutstanding
	}
	if config.Policy.ConfirmationTimeout == 0 {
		config.Policy.ConfirmationTimeout = defaultConfirmationTimeout
	}
	if config.Policy.CloseTimeout == 0 {
		config.Policy.CloseTimeout = defaultProducerCloseTimeout
	}
	if config.Policy.MaxOutstanding > maximumMaxOutstanding ||
		config.Policy.ConfirmationTimeout > maximumConfirmationTimeout ||
		config.Policy.CloseTimeout > maximumProducerCloseTimeout {
		return ProducerConfig{}, invalidConfiguration(errors.New("producer policy exceeds maximum"))
	}
	if config.Policy.Deduplication > DeduplicationPublishingID {
		return ProducerConfig{}, invalidConfiguration(errors.New("deduplication policy is invalid"))
	}
	if config.Policy.Deduplication == DeduplicationPublishingID {
		if invalidIdentifier(config.Policy.ProducerName, config.Limits.MaxStreamNameBytes) {
			return ProducerConfig{}, invalidConfiguration(errors.New("deduplicating producer name is invalid"))
		}
	} else if config.Policy.ProducerName != "" {
		return ProducerConfig{}, invalidConfiguration(errors.New("producer name would implicitly enable deduplication"))
	}
	return config, nil
}

// Normalized validates ProducerConfig and returns finite defaults.
func (config ProducerConfig) Normalized() (ProducerConfig, error) {
	return config.normalized()
}

// Publish validates and owns message bytes until a definitive confirmation or
// an explicitly ambiguous outcome. Cancellation before transport admission is
// definite; cancellation after Send succeeds is ambiguous.
func (producer *Producer) Publish(ctx context.Context, message Message) (result DeliveryResult, err error) {
	result = DeliveryResult{
		State:       DeliveryNotSent,
		Stream:      producer.config.Stream,
		SuperStream: producer.config.SuperStream,
	}
	started := time.Now()
	observe(producer.config.Observer, Observation{
		Kind: ObservationPublishAttempt, Count: 1, Bytes: uint64(len(message.Payload)),
	})
	defer func() {
		observation := Observation{Count: 1, Duration: time.Since(started)}
		switch result.State {
		case DeliveryConfirmed:
			observation.Kind = ObservationPublishConfirmed
		case DeliveryRejected:
			observation.Kind = ObservationPublishRejected
		case DeliveryAmbiguous:
			observation.Kind = ObservationPublishAmbiguous
		default:
			observation.Kind = ObservationPublishError
		}
		var operationErr *OperationError
		if errors.As(err, &operationErr) {
			observation.Category = operationErr.Category
		}
		observe(producer.config.Observer, observation)
	}()
	if ctx == nil {
		return result, validationError(errors.New("publish context is nil"))
	}
	if err := ctx.Err(); err != nil {
		return result, &OperationError{Operation: OperationPublish, Category: CategoryCanceled, Cause: err}
	}
	if err := producer.validateMessage(message); err != nil {
		return result, err
	}

	select {
	case producer.slots <- struct{}{}:
	case <-ctx.Done():
		return result, &OperationError{Operation: OperationPublish, Category: CategoryCanceled, Cause: ctx.Err()}
	}
	defer func() { <-producer.slots }()

	producer.stateMutex.Lock()
	if producer.closed {
		producer.stateMutex.Unlock()
		return result, &OperationError{Operation: OperationPublish, Category: CategoryClosed}
	}
	producer.active.Add(1)
	producer.stateMutex.Unlock()
	defer producer.active.Done()

	confirmationChannel := make(chan TransportConfirmation, 1)
	owned := message.Retain()
	if err := producer.transport.Send(ctx, owned, func(confirmation TransportConfirmation) {
		select {
		case confirmationChannel <- confirmation:
		default:
		}
	}); err != nil {
		if errors.Is(err, context.Canceled) {
			return result, &OperationError{
				Operation: OperationPublish,
				Category:  CategoryCanceled,
				Cause:     err,
			}
		}
		if errors.Is(err, context.DeadlineExceeded) {
			return result, &OperationError{
				Operation: OperationPublish,
				Category:  CategoryTimeout,
				Cause:     err,
			}
		}
		if errors.Is(err, ErrMessageTooLarge) {
			return result, &OperationError{
				Operation: OperationPublish,
				Category:  CategoryMessageTooLarge,
				Cause:     err,
			}
		}
		if errors.Is(err, ErrClosed) {
			return result, &OperationError{
				Operation: OperationPublish,
				Category:  CategoryClosed,
				Cause:     err,
			}
		}
		return result, &OperationError{
			Operation: OperationPublish, Category: categoryForError(err, CategoryConnection), Cause: err,
		}
	}

	timer := time.NewTimer(producer.config.Policy.ConfirmationTimeout)
	defer timer.Stop()
	select {
	case confirmation := <-confirmationChannel:
		result.PublishingID = confirmation.PublishingID
		result.Partition = confirmation.Partition
		switch {
		case confirmation.Confirmed:
			result.State = DeliveryConfirmed
			return result, nil
		case confirmation.BrokerRejected:
			result.State = DeliveryRejected
			return result, &OperationError{
				Operation: OperationPublish,
				Category:  CategoryBrokerRejected,
				Cause:     confirmation.Cause,
			}
		case confirmation.Ambiguous:
			result.State = DeliveryAmbiguous
			return result, &OperationError{
				Operation: OperationPublish,
				Category:  CategoryPublishAmbiguous,
				Cause:     confirmation.Cause,
			}
		default:
			result.State = DeliveryAmbiguous
			return result, &OperationError{
				Operation: OperationPublish,
				Category:  CategoryConfirmation,
				Cause:     confirmation.Cause,
			}
		}
	case <-ctx.Done():
		result.State = DeliveryAmbiguous
		return result, &OperationError{
			Operation: OperationPublish,
			Category:  CategoryPublishAmbiguous,
			Cause:     ctx.Err(),
		}
	case <-timer.C:
		result.State = DeliveryAmbiguous
		return result, &OperationError{
			Operation: OperationPublish,
			Category:  CategoryPublishAmbiguous,
			Cause:     ErrTimeout,
		}
	}
}

func (producer *Producer) validateMessage(message Message) error {
	if message.Partition != "" || message.Offset != 0 || message.HasOffset ||
		len(message.BrokerMetadata) != 0 {
		return validationError(errors.New("broker delivery metadata cannot be published"))
	}
	if err := message.Validate(producer.config.Limits); err != nil {
		return err
	}
	if message.Stream != producer.config.Stream || message.SuperStream != producer.config.SuperStream {
		return validationError(errors.New("message target differs from producer target"))
	}
	if producer.config.SuperStream != "" && message.RoutingKey == "" {
		return validationError(errors.New("super stream routing key is required"))
	}
	if producer.config.Policy.Deduplication == DeduplicationPublishingID && !message.HasPublishingID {
		return validationError(errors.New("publishing ID is required by deduplication policy"))
	}
	if message.HasPublishingID && message.PublishingID > math.MaxInt64 {
		return validationError(errors.New("publishing ID exceeds protocol range"))
	}
	if hasDuplicateMetadataKey(message.Headers) || hasDuplicateMetadataKey(message.Properties) {
		return validationError(errors.New("duplicate metadata keys cannot be represented on the wire"))
	}
	return nil
}

func hasDuplicateMetadataKey(entries []MetadataEntry) bool {
	seen := make(map[string]struct{}, len(entries))
	for _, entry := range entries {
		if _, exists := seen[entry.Key]; exists {
			return true
		}
		seen[entry.Key] = struct{}{}
	}
	return false
}

// Close stops admission, waits for admitted publishes within their finite
// confirmation bounds, then closes the transport. It is idempotent. A caller
// cancellation stops waiting but does not undo the close already in progress.
func (producer *Producer) Close(ctx context.Context) error {
	if ctx == nil {
		return validationError(errors.New("close context is nil"))
	}
	producer.closeOnce.Do(func() {
		producer.stateMutex.Lock()
		producer.closed = true
		producer.stateMutex.Unlock()
		go producer.close()
	})

	select {
	case <-producer.closeDone:
		return producer.closeErr
	case <-ctx.Done():
		return &OperationError{Operation: OperationClose, Category: CategoryCanceled, Cause: ctx.Err()}
	}
}

func (producer *Producer) close() {
	started := time.Now()
	defer func() {
		observe(producer.config.Observer, Observation{
			Kind: ObservationProducerShutdown, Count: 1, Duration: time.Since(started),
		})
		close(producer.closeDone)
	}()
	producer.asyncActive.Wait()
	producer.active.Wait()
	transportResult := make(chan error, 1)
	go func() { transportResult <- producer.transport.Close() }()

	timer := time.NewTimer(producer.config.Policy.CloseTimeout)
	defer timer.Stop()
	select {
	case err := <-transportResult:
		if err != nil {
			producer.closeErr = &OperationError{
				Operation: OperationClose, Category: categoryForError(err, CategoryConnection), Cause: err,
			}
		}
	case <-timer.C:
		producer.closeErr = &OperationError{Operation: OperationClose, Category: CategoryTimeout, Cause: ErrTimeout}
	}
}
