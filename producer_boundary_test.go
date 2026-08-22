package rabbitstream

import (
	"context"
	"errors"
	"testing"
	"time"
)

type selectCanceledContext struct {
	context.Context
	done  chan struct{}
	calls int
}

func newSelectCanceledContext() *selectCanceledContext {
	done := make(chan struct{})
	close(done)
	return &selectCanceledContext{Context: context.Background(), done: done}
}

func (ctx *selectCanceledContext) Done() <-chan struct{} { return ctx.done }

func (ctx *selectCanceledContext) Err() error {
	ctx.calls++
	if ctx.calls == 1 {
		return nil
	}
	return context.Canceled
}

func TestProducerPublicConstructionAndBatchErrorContracts(t *testing.T) {
	t.Parallel()

	var nilBatch *BatchPublishError
	if nilBatch.Error() != "<nil>" || nilBatch.Unwrap() != nil {
		t.Fatal("nil batch error contract changed")
	}
	cause := errors.New("cause")
	batchErr := &BatchPublishError{Index: 2, Cause: cause}
	if batchErr.Error() != "rabbitstream batch publish failed at index 2" || !errors.Is(batchErr, cause) {
		t.Fatalf("batch error = %v", batchErr)
	}
	transport := newFakeProducerTransport()
	if producer, err := NewProducer(ProducerConfig{Stream: "stream"}, transport); err != nil || producer == nil {
		t.Fatalf("NewProducer() = %#v, %v", producer, err)
	}
	if _, err := NewProducer(ProducerConfig{}, transport); err == nil {
		t.Fatal("NewProducer() accepted invalid configuration")
	}
	if _, err := NewProducer(ProducerConfig{Stream: "stream"}, nil); err == nil {
		t.Fatal("NewProducer() accepted nil transport")
	}
}

func TestProducerNormalizationUsesExactDefaultsAndAcceptsExactMaxima(t *testing.T) {
	t.Parallel()

	normalized, err := (ProducerConfig{Stream: "stream"}).Normalized()
	if err != nil {
		t.Fatalf("Normalized() error = %v", err)
	}
	if normalized.Policy.MaxOutstanding != 256 || normalized.Policy.ConfirmationTimeout != 10*time.Second ||
		normalized.Policy.CloseTimeout != 30*time.Second {
		t.Fatalf("producer defaults = %#v", normalized.Policy)
	}

	maximum := ProducerConfig{
		SuperStream: "super", ExpectedPartitions: MaxSuperStreamPartitions,
		Policy: ProducerPolicy{
			MaxOutstanding: maximumMaxOutstanding, ConfirmationTimeout: maximumConfirmationTimeout,
			CloseTimeout: maximumProducerCloseTimeout,
		},
	}
	if _, err := maximum.Normalized(); err != nil {
		t.Fatalf("Normalized() exact maxima error = %v", err)
	}
	for name, exceed := range map[string]func(*ProducerConfig){
		"partitions":   func(config *ProducerConfig) { config.ExpectedPartitions++ },
		"outstanding":  func(config *ProducerConfig) { config.Policy.MaxOutstanding++ },
		"confirmation": func(config *ProducerConfig) { config.Policy.ConfirmationTimeout++ },
		"close":        func(config *ProducerConfig) { config.Policy.CloseTimeout++ },
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

func TestProducerValidationDistinguishesEveryMessageInvariant(t *testing.T) {
	t.Parallel()

	stream, err := NewProducer(ProducerConfig{Stream: "stream"}, newFakeProducerTransport())
	if err != nil {
		t.Fatalf("NewProducer() error = %v", err)
	}
	valid := Message{Stream: "stream"}
	for name, mutate := range map[string]func(*Message){
		"partition":          func(message *Message) { message.Partition = "stream" },
		"offset":             func(message *Message) { message.Offset = 1 },
		"has offset":         func(message *Message) { message.HasOffset = true },
		"broker metadata":    func(message *Message) { message.BrokerMetadata = []MetadataEntry{{Key: "broker"}} },
		"stream":             func(message *Message) { message.Stream = "other" },
		"super stream":       func(message *Message) { message.Stream = ""; message.SuperStream = "super" },
		"header duplicate":   func(message *Message) { message.Headers = []MetadataEntry{{Key: "x"}, {Key: "x"}} },
		"property duplicate": func(message *Message) { message.Properties = []MetadataEntry{{Key: "x"}, {Key: "x"}} },
	} {
		t.Run(name, func(t *testing.T) {
			message := valid.Retain()
			mutate(&message)
			if err := stream.validateMessage(message); !errors.Is(err, ErrValidation) {
				t.Fatalf("validateMessage() error = %v", err)
			}
		})
	}
	if err := stream.validateMessage(valid); err != nil {
		t.Fatalf("validateMessage(valid) error = %v", err)
	}

	deduplicating, err := NewProducer(ProducerConfig{
		Stream: "stream", Policy: ProducerPolicy{Deduplication: DeduplicationPublishingID, ProducerName: "producer"},
	}, newFakeProducerTransport())
	if err != nil {
		t.Fatalf("NewProducer(deduplicating) error = %v", err)
	}
	if err := deduplicating.validateMessage(Message{Stream: "stream", HasPublishingID: true, PublishingID: uint64(^uint64(0) >> 1)}); err != nil {
		t.Fatalf("validateMessage(max publishing ID) error = %v", err)
	}
	if err := deduplicating.validateMessage(Message{Stream: "stream", HasPublishingID: true, PublishingID: uint64(^uint64(0)>>1) + 1}); !errors.Is(err, ErrValidation) {
		t.Fatalf("validateMessage(excess publishing ID) error = %v", err)
	}
	if err := deduplicating.validateMessage(Message{Stream: "stream"}); !errors.Is(err, ErrValidation) {
		t.Fatalf("validateMessage(missing publishing ID) error = %v", err)
	}

	super, err := NewProducer(ProducerConfig{SuperStream: "super"}, newFakeProducerTransport())
	if err != nil {
		t.Fatalf("NewProducer(super stream) error = %v", err)
	}
	if err := super.validateMessage(Message{SuperStream: "super"}); !errors.Is(err, ErrValidation) {
		t.Fatalf("validateMessage(missing routing key) error = %v", err)
	}
	if err := super.validateMessage(Message{SuperStream: "super", RoutingKey: "key"}); err != nil {
		t.Fatalf("validateMessage(super stream) error = %v", err)
	}
}

func TestProducerConfigurationRejectsEveryUnsafePolicyShape(t *testing.T) {
	t.Parallel()

	badLimits := DefaultLimits()
	badLimits.MaxPayloadBytes = 0
	invalid := []ProducerConfig{
		{},
		{Stream: "stream", Limits: badLimits},
		{Stream: " bad"},
		{SuperStream: " bad"},
		{Stream: "stream", Policy: ProducerPolicy{MaxOutstanding: -1}},
		{Stream: "stream", Policy: ProducerPolicy{MaxOutstanding: maximumMaxOutstanding + 1}},
		{Stream: "stream", Policy: ProducerPolicy{Deduplication: DeduplicationPolicy(255)}},
		{Stream: "stream", Policy: ProducerPolicy{Deduplication: DeduplicationPublishingID}},
		{Stream: "stream", Policy: ProducerPolicy{ProducerName: "implicit"}},
	}
	for _, config := range invalid {
		if _, err := config.Normalized(); !errors.Is(err, ErrInvalidConfiguration) {
			t.Fatalf("Normalized(%#v) error = %v", config, err)
		}
	}
}

func TestProducerBatchAndAsyncPreAdmissionFailuresAreDefinite(t *testing.T) {
	t.Parallel()

	producer, err := NewProducer(ProducerConfig{Stream: "stream"}, newFakeProducerTransport())
	if err != nil {
		t.Fatalf("NewProducer() error = %v", err)
	}
	if results, err := producer.PublishBatch(boundedTestContext(), nil); len(results) != 0 || err == nil {
		t.Fatalf("PublishBatch(nil) = %#v, %v", results, err)
	} else {
		var batchErr *BatchPublishError
		if !errors.As(err, &batchErr) || batchErr.Index != -1 {
			t.Fatalf("PublishBatch(nil) error = %#v", err)
		}
	}
	limits := DefaultLimits()
	limits.MaxBatchMessages = 1
	boundedProducer, err := NewProducer(
		ProducerConfig{Stream: "stream", Limits: limits},
		newFakeProducerTransport(),
	)
	if err != nil {
		t.Fatalf("NewProducer(bounded batch) error = %v", err)
	}
	if results, err := boundedProducer.PublishBatch(boundedTestContext(), []Message{{Stream: "stream"}, {Stream: "stream"}}); results != nil || err == nil {
		t.Fatalf("PublishBatch(oversized) = %#v, %v", results, err)
	}
	if results, err := producer.PublishBatch(boundedTestContext(), []Message{{Stream: "other"}}); len(results) != 1 || err == nil {
		t.Fatalf("PublishBatch(target mismatch) = %#v, %v", results, err)
	}
	message := Message{Stream: "stream", Payload: []byte("payload")}
	if outcome, err := producer.PublishAsync(absentContext, message); outcome != nil || err == nil {
		t.Fatalf("PublishAsync(nil) = %#v, %v", outcome, err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if outcome, err := producer.PublishAsync(canceled, message); outcome != nil || !errors.Is(err, ErrCanceled) {
		t.Fatalf("PublishAsync(canceled) = %#v, %v", outcome, err)
	}
	if outcome, err := producer.PublishAsync(boundedTestContext(), Message{Stream: "other"}); outcome != nil || err == nil {
		t.Fatalf("PublishAsync(invalid) = %#v, %v", outcome, err)
	}
	for range cap(producer.asyncSlots) {
		producer.asyncSlots <- struct{}{}
	}
	blocked := newSelectCanceledContext()
	if outcome, err := producer.PublishAsync(blocked, message); outcome != nil || !errors.Is(err, ErrCanceled) {
		t.Fatalf("PublishAsync(blocked) = %#v, %v", outcome, err)
	}
	for range cap(producer.asyncSlots) {
		receiveTest(t, producer.asyncSlots)
	}
	if err := producer.Close(boundedTestContext()); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if outcome, err := producer.PublishAsync(boundedTestContext(), message); outcome != nil || !errors.Is(err, ErrClosed) {
		t.Fatalf("PublishAsync(closed) = %#v, %v", outcome, err)
	}
}

func TestProducerClassifiesAllTransportAndConfirmationBoundaries(t *testing.T) {
	t.Parallel()

	message := Message{Stream: "stream", Payload: []byte("payload")}
	producerWithNilContext, err := NewProducer(ProducerConfig{Stream: "stream"}, newFakeProducerTransport())
	if err != nil {
		t.Fatalf("NewProducer() error = %v", err)
	}
	if _, err := producerWithNilContext.Publish(absentContext, message); err == nil {
		t.Fatal("Publish(nil) succeeded")
	}
	blockedProducer, _ := NewProducer(ProducerConfig{Stream: "stream", Policy: ProducerPolicy{MaxOutstanding: 1}}, newFakeProducerTransport())
	blockedProducer.slots <- struct{}{}
	if _, err := blockedProducer.Publish(newSelectCanceledContext(), message); !errors.Is(err, ErrCanceled) {
		t.Fatalf("Publish() blocked admission error = %v", err)
	}
	receiveTest(t, blockedProducer.slots)
	for _, sendErr := range []error{context.DeadlineExceeded, ErrClosed} {
		transport := newFakeProducerTransport()
		transport.sendErr = sendErr
		producer, _ := NewProducer(ProducerConfig{Stream: "stream"}, transport)
		if _, err := producer.Publish(boundedTestContext(), message); !errors.Is(err, sendErr) {
			t.Fatalf("Publish() send error = %v, want %v", err, sendErr)
		}
	}
	transport := newFakeProducerTransport()
	producer, _ := NewProducer(ProducerConfig{Stream: "stream"}, transport)
	done := make(chan error, 1)
	go func() {
		_, err := producer.Publish(boundedTestContext(), message)
		done <- err
	}()
	sent := receiveTest(t, transport.sent)
	sent.confirm(TransportConfirmation{Cause: ErrConfirmation})
	if err := receiveTest(t, done); !errors.Is(err, ErrConfirmation) {
		t.Fatalf("Publish() malformed confirmation error = %v", err)
	}
}

func TestProducerCloseReportsCancellationTransportFailureAndTimeout(t *testing.T) {
	t.Parallel()

	producerWithNilContext, err := NewProducer(ProducerConfig{Stream: "stream"}, newFakeProducerTransport())
	if err != nil {
		t.Fatalf("NewProducer() error = %v", err)
	}
	if err := producerWithNilContext.Close(absentContext); err == nil {
		t.Fatal("Close(nil) succeeded")
	}

	failedTransport := newFakeProducerTransport()
	failedTransport.closeErr = ErrAuthorization
	failedProducer, _ := NewProducer(ProducerConfig{Stream: "stream"}, failedTransport)
	if err := failedProducer.Close(boundedTestContext()); !errors.Is(err, ErrAuthorization) {
		t.Fatalf("Close() transport error = %v", err)
	}

	release := make(chan struct{})
	blockedTransport := newFakeProducerTransport()
	blockedTransport.closeBlock = release
	blockedProducer, _ := NewProducer(ProducerConfig{
		Stream: "stream", Policy: ProducerPolicy{CloseTimeout: time.Millisecond},
	}, blockedTransport)
	if err := blockedProducer.Close(boundedTestContext()); !errors.Is(err, ErrTimeout) {
		t.Fatalf("Close() timeout error = %v", err)
	}
	close(release)

	releaseCanceled := make(chan struct{})
	canceledTransport := newFakeProducerTransport()
	canceledTransport.closeBlock = releaseCanceled
	canceledProducer, _ := NewProducer(ProducerConfig{Stream: "stream"}, canceledTransport)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := canceledProducer.Close(ctx); !errors.Is(err, ErrCanceled) {
		t.Fatalf("Close() canceled error = %v", err)
	}
	close(releaseCanceled)
}
