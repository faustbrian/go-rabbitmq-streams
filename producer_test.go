package rabbitstream

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestProducerReturnsConfirmedDeliveryAndOwnsSentBytes(t *testing.T) {
	t.Parallel()

	transport := newFakeProducerTransport()
	producer, err := newProducer(ProducerConfig{Stream: "tracking.events"}, transport)
	if err != nil {
		t.Fatalf("newProducer() error = %v", err)
	}

	message := Message{
		Stream:  "tracking.events",
		Payload: []byte("payload"),
		Headers: []MetadataEntry{{Key: "traceparent", Value: []byte("trace")}},
	}
	resultChannel := make(chan DeliveryResult, 1)
	errorChannel := make(chan error, 1)
	go func() {
		result, publishErr := producer.Publish(boundedTestContext(), message)
		resultChannel <- result
		errorChannel <- publishErr
	}()

	outbound := receiveTest(t, transport.sent)
	message.Payload[0] = 'X'
	message.Headers[0].Value[0] = 'X'
	if got := string(outbound.message.Payload); got != "payload" {
		t.Fatalf("transport payload = %q", got)
	}
	if got := string(outbound.message.Headers[0].Value); got != "trace" {
		t.Fatalf("transport header = %q", got)
	}
	outbound.confirm(TransportConfirmation{Confirmed: true, PublishingID: 42})

	if err := receiveTest(t, errorChannel); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	if result := receiveTest(t, resultChannel); result != (DeliveryResult{
		State: DeliveryConfirmed, Stream: "tracking.events", PublishingID: 42,
	}) {
		t.Fatalf("Publish() result = %#v", result)
	}
}

func TestProducerCancellationBeforeSendIsDefiniteAndDoesNotCallTransport(t *testing.T) {
	t.Parallel()

	transport := newFakeProducerTransport()
	producer, err := newProducer(ProducerConfig{Stream: "tracking.events"}, transport)
	if err != nil {
		t.Fatalf("newProducer() error = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	result, err := producer.Publish(ctx, Message{Stream: "tracking.events", Payload: []byte("payload")})
	if !errors.Is(err, ErrCanceled) || result.State != DeliveryNotSent {
		t.Fatalf("Publish() = %#v, %v", result, err)
	}
	select {
	case <-transport.sent:
		t.Fatal("transport was called after pre-send cancellation")
	default:
	}
}

func TestProducerCancellationDuringTransportAdmissionIsDefinite(t *testing.T) {
	t.Parallel()

	transport := &cancelingProducerTransport{entered: make(chan struct{})}
	producer, err := newProducer(ProducerConfig{Stream: "tracking.events"}, transport)
	if err != nil {
		t.Fatalf("newProducer() error = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	resultChannel := make(chan DeliveryResult, 1)
	errorChannel := make(chan error, 1)
	go func() {
		result, publishErr := producer.Publish(ctx, Message{Stream: "tracking.events", Payload: []byte("payload")})
		resultChannel <- result
		errorChannel <- publishErr
	}()
	receiveTest(t, transport.entered)
	cancel()
	result, err := receiveTest(t, resultChannel), receiveTest(t, errorChannel)
	if !errors.Is(err, ErrCanceled) || result.State != DeliveryNotSent {
		t.Fatalf("Publish() = %#v, %v", result, err)
	}
}

func TestProducerCancellationAfterSendIsAmbiguous(t *testing.T) {
	t.Parallel()

	transport := newFakeProducerTransport()
	producer, err := newProducer(ProducerConfig{Stream: "tracking.events"}, transport)
	if err != nil {
		t.Fatalf("newProducer() error = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	resultChannel := make(chan DeliveryResult, 1)
	errorChannel := make(chan error, 1)
	go func() {
		result, publishErr := producer.Publish(ctx, Message{Stream: "tracking.events", Payload: []byte("payload")})
		resultChannel <- result
		errorChannel <- publishErr
	}()

	receiveTest(t, transport.sent)
	cancel()
	if err := receiveTest(t, errorChannel); !errors.Is(err, ErrPublishAmbiguous) || !errors.Is(err, context.Canceled) {
		t.Fatalf("Publish() error = %v", err)
	}
	if result := receiveTest(t, resultChannel); result.State != DeliveryAmbiguous {
		t.Fatalf("Publish() result = %#v", result)
	}
}

func TestProducerConfirmationTimeoutIsAmbiguous(t *testing.T) {
	t.Parallel()

	transport := newFakeProducerTransport()
	producer, err := newProducer(ProducerConfig{
		Stream: "tracking.events",
		Policy: ProducerPolicy{ConfirmationTimeout: time.Millisecond},
	}, transport)
	if err != nil {
		t.Fatalf("newProducer() error = %v", err)
	}

	result, err := producer.Publish(boundedTestContext(), Message{Stream: "tracking.events", Payload: []byte("payload")})
	if !errors.Is(err, ErrPublishAmbiguous) || !errors.Is(err, ErrTimeout) {
		t.Fatalf("Publish() error = %v", err)
	}
	if result.State != DeliveryAmbiguous {
		t.Fatalf("Publish() result = %#v", result)
	}
}

func TestProducerBrokerRejectionIsDefinite(t *testing.T) {
	t.Parallel()

	transport := newFakeProducerTransport()
	producer, err := newProducer(ProducerConfig{Stream: "tracking.events"}, transport)
	if err != nil {
		t.Fatalf("newProducer() error = %v", err)
	}
	resultChannel := make(chan DeliveryResult, 1)
	errorChannel := make(chan error, 1)
	go func() {
		result, publishErr := producer.Publish(boundedTestContext(), Message{Stream: "tracking.events", Payload: []byte("payload")})
		resultChannel <- result
		errorChannel <- publishErr
	}()

	outbound := receiveTest(t, transport.sent)
	outbound.confirm(TransportConfirmation{BrokerRejected: true})
	if err := receiveTest(t, errorChannel); !errors.Is(err, ErrBrokerRejected) {
		t.Fatalf("Publish() error = %v", err)
	}
	if result := receiveTest(t, resultChannel); result.State != DeliveryRejected {
		t.Fatalf("Publish() result = %#v", result)
	}
}

func TestProducerConnectionLossAfterSendIsAmbiguous(t *testing.T) {
	t.Parallel()

	transport := newFakeProducerTransport()
	producer, err := newProducer(ProducerConfig{Stream: "tracking.events"}, transport)
	if err != nil {
		t.Fatalf("newProducer() error = %v", err)
	}
	resultChannel := make(chan DeliveryResult, 1)
	errorChannel := make(chan error, 1)
	go func() {
		result, publishErr := producer.Publish(boundedTestContext(), Message{
			Stream: "tracking.events", Payload: []byte("payload"),
		})
		resultChannel <- result
		errorChannel <- publishErr
	}()

	outbound := receiveTest(t, transport.sent)
	outbound.confirm(TransportConfirmation{Ambiguous: true, Cause: ErrConnection})
	if err := receiveTest(t, errorChannel); !errors.Is(err, ErrPublishAmbiguous) || !errors.Is(err, ErrConnection) {
		t.Fatalf("Publish() error = %v", err)
	}
	if result := receiveTest(t, resultChannel); result.State != DeliveryAmbiguous {
		t.Fatalf("Publish() result = %#v", result)
	}
}

func TestProducerBoundsOutstandingMessagesBeforeTransportAdmission(t *testing.T) {
	t.Parallel()

	transport := newFakeProducerTransport()
	producer, err := newProducer(ProducerConfig{
		Stream: "tracking.events",
		Policy: ProducerPolicy{MaxOutstanding: 1, ConfirmationTimeout: time.Second},
	}, transport)
	if err != nil {
		t.Fatalf("newProducer() error = %v", err)
	}
	firstDone := make(chan error, 1)
	go func() {
		_, publishErr := producer.Publish(boundedTestContext(), Message{Stream: "tracking.events", Payload: []byte("first")})
		firstDone <- publishErr
	}()
	first := receiveTest(t, transport.sent)

	secondCtx, cancelSecond := context.WithCancel(context.Background())
	secondDone := make(chan error, 1)
	go func() {
		_, publishErr := producer.Publish(secondCtx, Message{Stream: "tracking.events", Payload: []byte("second")})
		secondDone <- publishErr
	}()
	cancelSecond()
	if err := receiveTest(t, secondDone); !errors.Is(err, ErrCanceled) {
		t.Fatalf("second Publish() error = %v", err)
	}
	select {
	case sent := <-transport.sent:
		t.Fatalf("second message reached transport: %q", sent.message.Payload)
	default:
	}

	first.confirm(TransportConfirmation{Confirmed: true})
	if err := receiveTest(t, firstDone); err != nil {
		t.Fatalf("first Publish() error = %v", err)
	}
}

func TestProducerValidatesTargetRoutingDeduplicationAndLimitsBeforeSend(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		config  ProducerConfig
		message Message
	}{
		"target mismatch": {
			config:  ProducerConfig{Stream: "tracking.events"},
			message: Message{Stream: "other", Payload: []byte("payload")},
		},
		"super stream routing required": {
			config:  ProducerConfig{SuperStream: "tracking"},
			message: Message{SuperStream: "tracking", Payload: []byte("payload")},
		},
		"deduplication publishing id required": {
			config: ProducerConfig{Stream: "tracking.events", Policy: ProducerPolicy{
				Deduplication: DeduplicationPublishingID,
				ProducerName:  "tracking-writer",
			}},
			message: Message{Stream: "tracking.events", Payload: []byte("payload")},
		},
		"payload bound": {
			config: ProducerConfig{Stream: "tracking.events", Limits: Limits{
				MaxStreamNameBytes: 255, MaxRoutingKeyBytes: 255, MaxPayloadBytes: 1,
				MaxMetadataEntries: 1, MaxMetadataKeyBytes: 1, MaxMetadataValueBytes: 1,
				MaxMetadataBytes: 1, MaxBatchMessages: 1, MaxBatchBytes: 1,
				MaxBufferedMessages: 1,
			}},
			message: Message{Stream: "tracking.events", Payload: []byte("too large")},
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			transport := newFakeProducerTransport()
			producer, err := newProducer(test.config, transport)
			if err != nil {
				t.Fatalf("newProducer() error = %v", err)
			}
			result, err := producer.Publish(boundedTestContext(), test.message)
			if !errors.Is(err, ErrValidation) || result.State != DeliveryNotSent {
				t.Fatalf("Publish() = %#v, %v", result, err)
			}
			select {
			case <-transport.sent:
				t.Fatal("invalid message reached transport")
			default:
			}
		})
	}
}

func TestProducerRejectsValuesThatCannotBeRepresentedOnTheWire(t *testing.T) {
	t.Parallel()

	tests := map[string]Message{
		"publishing ID exceeds signed protocol range": {
			Stream:          "tracking.events",
			Payload:         []byte("payload"),
			HasPublishingID: true,
			PublishingID:    uint64(^uint64(0)),
		},
		"duplicate annotation key": {
			Stream:  "tracking.events",
			Payload: []byte("payload"),
			Headers: []MetadataEntry{{Key: "traceparent"}, {Key: "traceparent"}},
		},
		"duplicate application property key": {
			Stream:     "tracking.events",
			Payload:    []byte("payload"),
			Properties: []MetadataEntry{{Key: "schema"}, {Key: "schema"}},
		},
		"broker delivery metadata": {
			Stream: "tracking.events", Partition: "tracking.events", Offset: 41, HasOffset: true,
			Payload: []byte("payload"),
		},
	}

	for name, message := range tests {
		t.Run(name, func(t *testing.T) {
			transport := newFakeProducerTransport()
			producer, err := newProducer(ProducerConfig{Stream: "tracking.events"}, transport)
			if err != nil {
				t.Fatalf("newProducer() error = %v", err)
			}
			result, err := producer.Publish(boundedTestContext(), message)
			if !errors.Is(err, ErrValidation) || result.State != DeliveryNotSent {
				t.Fatalf("Publish() = %#v, %v", result, err)
			}
			select {
			case <-transport.sent:
				t.Fatal("unrepresentable message reached transport")
			default:
			}
		})
	}
}

func TestSuperStreamProducerPolicyBoundsTopologyAssumptions(t *testing.T) {
	t.Parallel()

	normalized, err := (ProducerConfig{
		SuperStream:        "tracking",
		RoutingStrategy:    RoutingHash,
		ExpectedPartitions: 6,
	}).Normalized()
	if err != nil {
		t.Fatalf("Normalized() error = %v", err)
	}
	if normalized.RoutingStrategy != RoutingHash || normalized.ExpectedPartitions != 6 {
		t.Fatalf("normalized topology = %#v", normalized)
	}

	invalid := []ProducerConfig{
		{Stream: "tracking.events", ExpectedPartitions: 2},
		{SuperStream: "tracking", RoutingStrategy: RoutingStrategy(255)},
		{SuperStream: "tracking", ExpectedPartitions: MaxSuperStreamPartitions + 1},
	}
	for _, config := range invalid {
		if _, err := config.Normalized(); !errors.Is(err, ErrInvalidConfiguration) {
			t.Fatalf("Normalized(%#v) error = %v", config, err)
		}
	}
}

func TestProducerClassifiesDefiniteTransportSizeRejection(t *testing.T) {
	t.Parallel()

	transport := newFakeProducerTransport()
	transport.sendErr = ErrMessageTooLarge
	producer, err := newProducer(ProducerConfig{Stream: "tracking.events"}, transport)
	if err != nil {
		t.Fatalf("newProducer() error = %v", err)
	}
	result, err := producer.Publish(
		boundedTestContext(),
		Message{Stream: "tracking.events", Payload: []byte("payload")},
	)
	if !errors.Is(err, ErrMessageTooLarge) || result.State != DeliveryNotSent {
		t.Fatalf("Publish() = %#v, %v", result, err)
	}
}

func TestProducerPreservesStableTransportAuthorizationCategory(t *testing.T) {
	t.Parallel()

	transport := newFakeProducerTransport()
	transport.sendErr = ErrAuthorization
	producer, err := newProducer(ProducerConfig{Stream: "tracking.events"}, transport)
	if err != nil {
		t.Fatalf("newProducer() error = %v", err)
	}
	result, err := producer.Publish(
		boundedTestContext(), Message{Stream: "tracking.events", Payload: []byte("payload")},
	)
	var operationError *OperationError
	if !errors.Is(err, ErrAuthorization) || !errors.As(err, &operationError) ||
		operationError.Category != CategoryAuthorization || result.State != DeliveryNotSent {
		t.Fatalf("Publish() = %#v, %v", result, err)
	}
}

func TestProducerCloseIsIdempotentAndRejectsLaterOperations(t *testing.T) {
	t.Parallel()

	transport := newFakeProducerTransport()
	producer, err := newProducer(ProducerConfig{Stream: "tracking.events"}, transport)
	if err != nil {
		t.Fatalf("newProducer() error = %v", err)
	}

	if err := producer.Close(boundedTestContext()); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if err := producer.Close(boundedTestContext()); err != nil {
		t.Fatalf("Close() second error = %v", err)
	}
	if calls := transport.closeCalls(); calls != 1 {
		t.Fatalf("transport close calls = %d", calls)
	}
	result, err := producer.Publish(boundedTestContext(), Message{Stream: "tracking.events", Payload: []byte("payload")})
	if !errors.Is(err, ErrClosed) || result.State != DeliveryNotSent {
		t.Fatalf("Publish() after close = %#v, %v", result, err)
	}
}

func TestProducerPublishesValidatedBatchInOrderWithPerMessageResults(t *testing.T) {
	t.Parallel()

	transport := newFakeProducerTransport()
	producer, err := newProducer(ProducerConfig{Stream: "tracking.events"}, transport)
	if err != nil {
		t.Fatalf("newProducer() error = %v", err)
	}

	resultsChannel := make(chan []DeliveryResult, 1)
	errorChannel := make(chan error, 1)
	go func() {
		results, publishErr := producer.PublishBatch(boundedTestContext(), []Message{
			{Stream: "tracking.events", PublishingID: 41, HasPublishingID: true, Payload: []byte("first")},
			{Stream: "tracking.events", PublishingID: 42, HasPublishingID: true, Payload: []byte("second")},
		})
		resultsChannel <- results
		errorChannel <- publishErr
	}()

	first := receiveTest(t, transport.sent)
	first.confirm(TransportConfirmation{Confirmed: true, PublishingID: 41})
	second := receiveTest(t, transport.sent)
	second.confirm(TransportConfirmation{Confirmed: true, PublishingID: 42})

	if err := receiveTest(t, errorChannel); err != nil {
		t.Fatalf("PublishBatch() error = %v", err)
	}
	results := receiveTest(t, resultsChannel)
	if len(results) != 2 || results[0].State != DeliveryConfirmed ||
		results[1].State != DeliveryConfirmed || results[0].PublishingID != 41 ||
		results[1].PublishingID != 42 {
		t.Fatalf("PublishBatch() results = %#v", results)
	}
}

func TestProducerBatchFailureIdentifiesPartialDeliveryAndStops(t *testing.T) {
	t.Parallel()

	transport := newFakeProducerTransport()
	producer, err := newProducer(ProducerConfig{Stream: "tracking.events"}, transport)
	if err != nil {
		t.Fatalf("newProducer() error = %v", err)
	}

	resultsChannel := make(chan []DeliveryResult, 1)
	errorChannel := make(chan error, 1)
	go func() {
		results, publishErr := producer.PublishBatch(boundedTestContext(), []Message{
			{Stream: "tracking.events", Payload: []byte("first")},
			{Stream: "tracking.events", Payload: []byte("second")},
			{Stream: "tracking.events", Payload: []byte("third")},
		})
		resultsChannel <- results
		errorChannel <- publishErr
	}()

	first := receiveTest(t, transport.sent)
	first.confirm(TransportConfirmation{Confirmed: true})
	second := receiveTest(t, transport.sent)
	second.confirm(TransportConfirmation{BrokerRejected: true})

	err = receiveTest(t, errorChannel)
	var batchErr *BatchPublishError
	if !errors.As(err, &batchErr) || batchErr.Index != 1 || !errors.Is(err, ErrBrokerRejected) {
		t.Fatalf("PublishBatch() error = %#v", err)
	}
	results := receiveTest(t, resultsChannel)
	if len(results) != 3 || results[0].State != DeliveryConfirmed ||
		results[1].State != DeliveryRejected || results[2].State != DeliveryNotSent {
		t.Fatalf("PublishBatch() results = %#v", results)
	}
	select {
	case sent := <-transport.sent:
		t.Fatalf("message after failure reached transport: %q", sent.message.Payload)
	default:
	}
}

func TestProducerAsyncAdmissionAndMemoryAreBounded(t *testing.T) {
	t.Parallel()

	transport := newFakeProducerTransport()
	producer, err := newProducer(ProducerConfig{
		Stream: "tracking.events",
		Limits: Limits{
			MaxStreamNameBytes: 255, MaxRoutingKeyBytes: 255, MaxPayloadBytes: 1024,
			MaxMetadataEntries: 8, MaxMetadataKeyBytes: 128, MaxMetadataValueBytes: 1024,
			MaxMetadataBytes: 4096, MaxBatchMessages: 8, MaxBatchBytes: 8192,
			MaxBufferedMessages: 1,
		},
		Policy: ProducerPolicy{MaxOutstanding: 1, ConfirmationTimeout: time.Second},
	}, transport)
	if err != nil {
		t.Fatalf("newProducer() error = %v", err)
	}

	payload := []byte("first")
	firstOutcome, err := producer.PublishAsync(
		boundedTestContext(),
		Message{Stream: "tracking.events", Payload: payload},
	)
	if err != nil {
		t.Fatalf("PublishAsync() error = %v", err)
	}
	first := receiveTest(t, transport.sent)
	payload[0] = 'X'
	if got := string(first.message.Payload); got != "first" {
		t.Fatalf("async transport payload = %q", got)
	}

	secondCtx, cancelSecond := context.WithCancel(context.Background())
	cancelSecond()
	secondOutcome, err := producer.PublishAsync(
		secondCtx,
		Message{Stream: "tracking.events", Payload: []byte("second")},
	)
	if secondOutcome != nil || !errors.Is(err, ErrCanceled) {
		t.Fatalf("second PublishAsync() = %#v, %v", secondOutcome, err)
	}

	first.confirm(TransportConfirmation{Confirmed: true})
	outcome := receiveTest(t, firstOutcome)
	if outcome.Err != nil || outcome.Result.State != DeliveryConfirmed {
		t.Fatalf("first async outcome = %#v", outcome)
	}
}

type fakeProducerTransport struct {
	sent chan transportSend

	mutex      sync.Mutex
	closed     int
	sendErr    error
	closeErr   error
	closeBlock <-chan struct{}
}

type cancelingProducerTransport struct {
	entered chan struct{}
}

func (transport *cancelingProducerTransport) Send(
	ctx context.Context,
	_ Message,
	_ func(TransportConfirmation),
) error {
	close(transport.entered)
	<-ctx.Done()
	return ctx.Err()
}

func (cancelingProducerTransport) Close() error { return nil }

func newFakeProducerTransport() *fakeProducerTransport {
	return &fakeProducerTransport{sent: make(chan transportSend, 8)}
}

func (transport *fakeProducerTransport) Send(
	_ context.Context,
	message Message,
	confirm func(TransportConfirmation),
) error {
	time.AfterFunc(50*time.Millisecond, func() {
		confirm(TransportConfirmation{Cause: ErrConfirmation})
	})
	if transport.sendErr != nil {
		return transport.sendErr
	}
	transport.sent <- transportSend{message: message, confirm: confirm}
	return nil
}

func (transport *fakeProducerTransport) Close() error {
	if transport.closeBlock != nil {
		<-transport.closeBlock
	}
	transport.mutex.Lock()
	defer transport.mutex.Unlock()
	transport.closed++
	return transport.closeErr
}

func (transport *fakeProducerTransport) closeCalls() int {
	transport.mutex.Lock()
	defer transport.mutex.Unlock()
	return transport.closed
}

type transportSend struct {
	message Message
	confirm func(TransportConfirmation)
}
