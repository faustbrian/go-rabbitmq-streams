// Package rabbitstreamotel adapts bounded rabbitstream observations and W3C
// trace context to caller-owned OpenTelemetry providers. It does not configure
// exporters, own provider shutdown, or add caller-controlled metric labels.
package rabbitstreamotel

import (
	"bytes"
	"context"
	"errors"
	"math"
	"reflect"
	"sync"
	"time"

	"github.com/faustbrian/golib/pkg/rabbitstream"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
)

const instrumentationName = "github.com/faustbrian/golib/pkg/rabbitstream/otel"

// Config supplies a caller-owned meter provider and the same message limits
// used by producers. The adapter never uses global OpenTelemetry providers.
type Config struct {
	// MeterProvider owns meter creation and exporter lifecycle outside this adapter.
	MeterProvider metric.MeterProvider
	// Limits bounds trace propagation metadata using the producer's message policy.
	Limits rabbitstream.Limits
}

// Adapter implements rabbitstream.Observer and W3C Trace Context injection.
// It is safe for concurrent use and starts no goroutines.
type Adapter struct {
	limits     rabbitstream.Limits
	propagator propagation.TraceContext

	connectionState  metric.Int64Gauge
	reconnects       metric.Int64Counter
	publishMessages  metric.Int64Counter
	publishBytes     metric.Int64Counter
	publishDuration  metric.Float64Histogram
	unconfirmed      metric.Int64Gauge
	consumerMessages metric.Int64Counter
	consumerBytes    metric.Int64Counter
	handlerDuration  metric.Float64Histogram
	handlerRetries   metric.Int64Counter
	retryPublished   metric.Int64Counter
	deadPublished    metric.Int64Counter
	failureErrors    metric.Int64Counter
	currentOffset    metric.Int64Gauge
	streamEndOffset  metric.Int64Gauge
	consumerLag      metric.Int64Gauge
	replayMessages   metric.Int64Counter
	producerShutdown metric.Float64Histogram
	consumerShutdown metric.Float64Histogram
	errors           metric.Int64Counter

	outstandingMu sync.Mutex
	outstanding   int64
}

// New creates bounded instruments. Providers remain caller-owned.
func New(config Config) (adapter *Adapter, err error) {
	defer func() {
		if recover() != nil {
			adapter = nil
			err = invalidConfiguration(errors.New("OpenTelemetry provider panicked"))
		}
	}()
	if isNil(config.MeterProvider) || !validLimits(config.Limits) {
		return nil, invalidConfiguration(errors.New("OpenTelemetry adapter configuration is invalid"))
	}
	meter := config.MeterProvider.Meter(instrumentationName)
	connectionState, err := meter.Int64Gauge("rabbitstream.connection.state",
		metric.WithDescription("Last observed RabbitMQ Streams connection state"), metric.WithUnit("1"))
	if err = validateInstrument(connectionState, err); err != nil {
		return nil, invalidConfiguration(err)
	}
	reconnects, err := meter.Int64Counter("rabbitstream.reconnects",
		metric.WithDescription("Bounded reconnect attempts"), metric.WithUnit("{attempt}"))
	if err = validateInstrument(reconnects, err); err != nil {
		return nil, invalidConfiguration(err)
	}
	publishMessages, err := meter.Int64Counter("rabbitstream.publish.messages",
		metric.WithDescription("Confirmed published messages"), metric.WithUnit("{message}"))
	if err = validateInstrument(publishMessages, err); err != nil {
		return nil, invalidConfiguration(err)
	}
	publishBytes, err := meter.Int64Counter("rabbitstream.publish.bytes",
		metric.WithDescription("Attempted publish payload bytes"), metric.WithUnit("By"))
	if err = validateInstrument(publishBytes, err); err != nil {
		return nil, invalidConfiguration(err)
	}
	publishDuration, err := meter.Float64Histogram("rabbitstream.publish.confirmation.duration",
		metric.WithDescription("Publish completion latency"), metric.WithUnit("s"))
	if err = validateInstrument(publishDuration, err); err != nil {
		return nil, invalidConfiguration(err)
	}
	unconfirmed, err := meter.Int64Gauge("rabbitstream.publish.unconfirmed",
		metric.WithDescription("Locally outstanding publish operations"), metric.WithUnit("{message}"))
	if err = validateInstrument(unconfirmed, err); err != nil {
		return nil, invalidConfiguration(err)
	}
	consumerMessages, err := meter.Int64Counter("rabbitstream.consumer.messages",
		metric.WithDescription("Delivered consumer messages"), metric.WithUnit("{message}"))
	if err = validateInstrument(consumerMessages, err); err != nil {
		return nil, invalidConfiguration(err)
	}
	consumerBytes, err := meter.Int64Counter("rabbitstream.consumer.bytes",
		metric.WithDescription("Delivered consumer payload bytes"), metric.WithUnit("By"))
	if err = validateInstrument(consumerBytes, err); err != nil {
		return nil, invalidConfiguration(err)
	}
	handlerDuration, err := meter.Float64Histogram("rabbitstream.consumer.handler.duration",
		metric.WithDescription("Consumer handler duration"), metric.WithUnit("s"))
	if err = validateInstrument(handlerDuration, err); err != nil {
		return nil, invalidConfiguration(err)
	}
	handlerRetries, err := meter.Int64Counter("rabbitstream.consumer.handler.retries",
		metric.WithDescription("In-process handler retry attempts"), metric.WithUnit("{attempt}"))
	if err = validateInstrument(handlerRetries, err); err != nil {
		return nil, invalidConfiguration(err)
	}
	retryPublished, err := meter.Int64Counter("rabbitstream.consumer.retry_stream.messages",
		metric.WithDescription("Confirmed retry-stream publications"), metric.WithUnit("{message}"))
	if err = validateInstrument(retryPublished, err); err != nil {
		return nil, invalidConfiguration(err)
	}
	deadPublished, err := meter.Int64Counter("rabbitstream.consumer.dead_letter.messages",
		metric.WithDescription("Confirmed dead-letter-stream publications"), metric.WithUnit("{message}"))
	if err = validateInstrument(deadPublished, err); err != nil {
		return nil, invalidConfiguration(err)
	}
	failureErrors, err := meter.Int64Counter("rabbitstream.consumer.failure_publish.errors",
		metric.WithDescription("Failed retry-stream or dead-letter-stream publications"), metric.WithUnit("{error}"))
	if err = validateInstrument(failureErrors, err); err != nil {
		return nil, invalidConfiguration(err)
	}
	currentOffset, err := meter.Int64Gauge("rabbitstream.consumer.offset",
		metric.WithDescription("Last offset accepted for broker storage"), metric.WithUnit("{message}"))
	if err = validateInstrument(currentOffset, err); err != nil {
		return nil, invalidConfiguration(err)
	}
	streamEndOffset, err := meter.Int64Gauge("rabbitstream.stream.end_offset",
		metric.WithDescription("Exact last retained message offset observed during inspection"), metric.WithUnit("{message}"))
	if err = validateInstrument(streamEndOffset, err); err != nil {
		return nil, invalidConfiguration(err)
	}
	consumerLag, err := meter.Int64Gauge("rabbitstream.consumer.lag",
		metric.WithDescription("Exact inspected distance between stored and stream-end offsets"), metric.WithUnit("{message}"))
	if err = validateInstrument(consumerLag, err); err != nil {
		return nil, invalidConfiguration(err)
	}
	replayMessages, err := meter.Int64Counter("rabbitstream.replay.messages",
		metric.WithDescription("Replay messages handled"), metric.WithUnit("{message}"))
	if err = validateInstrument(replayMessages, err); err != nil {
		return nil, invalidConfiguration(err)
	}
	producerShutdown, err := meter.Float64Histogram("rabbitstream.producer.shutdown.duration",
		metric.WithDescription("Producer shutdown duration"), metric.WithUnit("s"))
	if err = validateInstrument(producerShutdown, err); err != nil {
		return nil, invalidConfiguration(err)
	}
	consumerShutdown, err := meter.Float64Histogram("rabbitstream.consumer.shutdown.duration",
		metric.WithDescription("Consumer shutdown duration"), metric.WithUnit("s"))
	if err = validateInstrument(consumerShutdown, err); err != nil {
		return nil, invalidConfiguration(err)
	}
	errorCounter, err := meter.Int64Counter("rabbitstream.errors",
		metric.WithDescription("Rabbitstream operation errors by closed category"), metric.WithUnit("{error}"))
	if err = validateInstrument(errorCounter, err); err != nil {
		return nil, invalidConfiguration(err)
	}
	return &Adapter{
		limits: config.Limits, propagator: propagation.TraceContext{},
		connectionState: connectionState, reconnects: reconnects,
		publishMessages: publishMessages, publishBytes: publishBytes,
		publishDuration: publishDuration, unconfirmed: unconfirmed,
		consumerMessages: consumerMessages, consumerBytes: consumerBytes,
		handlerDuration: handlerDuration, handlerRetries: handlerRetries,
		retryPublished: retryPublished, deadPublished: deadPublished,
		failureErrors: failureErrors, currentOffset: currentOffset,
		streamEndOffset: streamEndOffset, consumerLag: consumerLag,
		replayMessages: replayMessages, producerShutdown: producerShutdown,
		consumerShutdown: consumerShutdown, errors: errorCounter,
	}, nil
}

// Observe implements rabbitstream.Observer. Provider panics are contained and
// never affect message delivery.
func (adapter *Adapter) Observe(observation rabbitstream.Observation) {
	if adapter == nil {
		return
	}
	defer func() { _ = recover() }()
	ctx := context.Background()
	count := boundedInt64(observation.Count)
	switch observation.Kind {
	case rabbitstream.ObservationConnectionConnecting:
		adapter.connectionState.Record(ctx, 0)
	case rabbitstream.ObservationConnectionReady:
		adapter.connectionState.Record(ctx, 1)
	case rabbitstream.ObservationConnectionLost:
		adapter.connectionState.Record(ctx, 0)
		adapter.recordError(ctx, observation.Category, count)
	case rabbitstream.ObservationReconnectAttempt:
		adapter.reconnects.Add(ctx, count)
	case rabbitstream.ObservationAuthenticationError:
		adapter.recordError(ctx, rabbitstream.CategoryAuthentication, count)
	case rabbitstream.ObservationPublishAttempt:
		adapter.publishBytes.Add(ctx, boundedInt64(observation.Bytes))
		adapter.unconfirmed.Record(ctx, adapter.addOutstanding(count))
	case rabbitstream.ObservationPublishConfirmed:
		adapter.publishMessages.Add(ctx, count)
		adapter.recordPublishCompletion(ctx, observation, count)
	case rabbitstream.ObservationPublishRejected,
		rabbitstream.ObservationPublishAmbiguous,
		rabbitstream.ObservationPublishError:
		adapter.recordPublishCompletion(ctx, observation, count)
		adapter.recordError(ctx, observation.Category, count)
	case rabbitstream.ObservationConsumerMessage:
		adapter.consumerMessages.Add(ctx, count)
		adapter.consumerBytes.Add(ctx, boundedInt64(observation.Bytes))
	case rabbitstream.ObservationHandlerSuccess:
		adapter.handlerDuration.Record(ctx, nonnegativeSeconds(observation.Duration))
	case rabbitstream.ObservationHandlerError:
		adapter.handlerDuration.Record(ctx, nonnegativeSeconds(observation.Duration))
		adapter.recordError(ctx, observation.Category, count)
	case rabbitstream.ObservationHandlerRetry:
		adapter.handlerRetries.Add(ctx, count)
	case rabbitstream.ObservationRetryStreamPublished:
		adapter.retryPublished.Add(ctx, count)
	case rabbitstream.ObservationDeadLetterPublished:
		adapter.deadPublished.Add(ctx, count)
	case rabbitstream.ObservationFailurePublishError:
		adapter.failureErrors.Add(ctx, count)
		adapter.recordError(ctx, observation.Category, count)
	case rabbitstream.ObservationOffsetStoreAccepted:
		adapter.currentOffset.Record(ctx, boundedInt64(observation.Value))
	case rabbitstream.ObservationStreamEndOffset:
		adapter.streamEndOffset.Record(ctx, boundedInt64(observation.Value))
	case rabbitstream.ObservationConsumerLag:
		adapter.consumerLag.Record(ctx, boundedInt64(observation.Value))
	case rabbitstream.ObservationReplayProgress:
		adapter.replayMessages.Add(ctx, count)
	case rabbitstream.ObservationProducerShutdown:
		adapter.producerShutdown.Record(ctx, nonnegativeSeconds(observation.Duration))
	case rabbitstream.ObservationConsumerShutdown:
		adapter.consumerShutdown.Record(ctx, nonnegativeSeconds(observation.Duration))
	}
}

func (adapter *Adapter) recordPublishCompletion(ctx context.Context, observation rabbitstream.Observation, count int64) {
	adapter.publishDuration.Record(ctx, nonnegativeSeconds(observation.Duration))
	adapter.unconfirmed.Record(ctx, adapter.completeOutstanding(count))
}

func (adapter *Adapter) addOutstanding(count int64) int64 {
	adapter.outstandingMu.Lock()
	defer adapter.outstandingMu.Unlock()
	adapter.outstanding += min(count, math.MaxInt64-adapter.outstanding)
	return adapter.outstanding
}

func (adapter *Adapter) completeOutstanding(count int64) int64 {
	adapter.outstandingMu.Lock()
	defer adapter.outstandingMu.Unlock()
	adapter.outstanding = max(adapter.outstanding-count, int64(0))
	return adapter.outstanding
}

func (adapter *Adapter) recordError(ctx context.Context, category rabbitstream.ErrorCategory, count int64) {
	adapter.errors.Add(ctx, count, metric.WithAttributes(
		attribute.String("rabbitstream.error.category", closedCategory(category)),
	))
}

// Inject returns an owned message with W3C traceparent and tracestate headers.
// The input message is never mutated.
func (adapter *Adapter) Inject(ctx context.Context, message rabbitstream.Message) (result rabbitstream.Message, err error) {
	if adapter == nil || ctx == nil {
		return rabbitstream.Message{}, validationError(rabbitstream.OperationPublish)
	}
	if err := message.Validate(adapter.limits); err != nil {
		return rabbitstream.Message{}, validationError(rabbitstream.OperationPublish)
	}
	result = message.Retain()
	result.Headers = removePropagationHeaders(result.Headers, adapter.propagator.Fields())
	carrier := metadataCarrier{entries: result.Headers}
	adapter.propagator.Inject(ctx, &carrier)
	result.Headers = carrier.entries
	if err := result.Validate(adapter.limits); err != nil {
		return rabbitstream.Message{}, validationError(rabbitstream.OperationPublish)
	}
	return result, nil
}

// Extract returns a context containing remote W3C trace context from message
// headers. It never mutates the message.
func (adapter *Adapter) Extract(ctx context.Context, message rabbitstream.Message) (result context.Context, err error) {
	if adapter == nil || ctx == nil {
		return nil, validationError(rabbitstream.OperationConsume)
	}
	if err := validateExtractMessage(message, adapter.limits); err != nil {
		return nil, validationError(rabbitstream.OperationConsume)
	}
	fields := adapter.propagator.Fields()
	if hasDuplicatePropagationFields(message.Headers, fields) {
		return ctx, nil
	}
	return adapter.propagator.Extract(ctx, &metadataCarrier{entries: message.Headers}), nil
}

func validateExtractMessage(message rabbitstream.Message, limits rabbitstream.Limits) error {
	if message.Partition != "" || message.HasOffset || message.Offset != 0 {
		return message.ValidateDelivery(limits)
	}
	return message.Validate(limits)
}

type metadataCarrier struct {
	entries []rabbitstream.MetadataEntry
}

// Get returns one case-insensitive propagation value and rejects duplicates.
func (carrier *metadataCarrier) Get(key string) string {
	value := ""
	found := false
	for _, entry := range carrier.entries {
		if equalASCIIFold(entry.Key, key) {
			if found {
				return ""
			}
			value = string(entry.Value)
			found = true
		}
	}
	return value
}

// Set replaces case-insensitive duplicates with one owned propagation value.
func (carrier *metadataCarrier) Set(key string, value string) {
	carrier.entries = removePropagationHeaders(carrier.entries, []string{key})
	carrier.entries = append(carrier.entries, rabbitstream.MetadataEntry{
		Key: key, Value: []byte(value),
	})
}

// Keys returns metadata names in preserved carrier order.
func (carrier *metadataCarrier) Keys() []string {
	keys := make([]string, len(carrier.entries))
	for index, entry := range carrier.entries {
		keys[index] = entry.Key
	}
	return keys
}

func hasDuplicatePropagationFields(entries []rabbitstream.MetadataEntry, fields []string) bool {
	for _, field := range fields {
		found := false
		for _, entry := range entries {
			if !equalASCIIFold(field, entry.Key) {
				continue
			}
			if found {
				return true
			}
			found = true
		}
	}
	return false
}

func removePropagationHeaders(entries []rabbitstream.MetadataEntry, fields []string) []rabbitstream.MetadataEntry {
	retained := entries[:0]
	for _, entry := range entries {
		if propagationField(fields, entry.Key) {
			continue
		}
		retained = append(retained, rabbitstream.MetadataEntry{
			Key: entry.Key, Value: bytes.Clone(entry.Value),
		})
	}
	return retained
}

func propagationField(fields []string, key string) bool {
	for _, field := range fields {
		if equalASCIIFold(field, key) {
			return true
		}
	}
	return false
}

func equalASCIIFold(left string, right string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range len(left) {
		leftByte := left[index]
		if leftByte >= 'A' && leftByte <= 'Z' {
			leftByte += 'a' - 'A'
		}
		rightByte := right[index]
		if rightByte >= 'A' && rightByte <= 'Z' {
			rightByte += 'a' - 'A'
		}
		if leftByte != rightByte {
			return false
		}
	}
	return true
}

func validLimits(limits rabbitstream.Limits) bool {
	return limits.MaxStreamNameBytes > 0 && limits.MaxRoutingKeyBytes > 0 &&
		limits.MaxPayloadBytes > 0 && limits.MaxMetadataEntries > 0 &&
		limits.MaxMetadataKeyBytes > 0 && limits.MaxMetadataValueBytes > 0 &&
		limits.MaxMetadataBytes > 0 && limits.MaxBatchMessages > 0 &&
		limits.MaxBatchBytes > 0 && limits.MaxBufferedMessages > 0
}

func closedCategory(category rabbitstream.ErrorCategory) string {
	switch category {
	case rabbitstream.CategoryInvalidConfiguration, rabbitstream.CategoryValidation,
		rabbitstream.CategoryClosed, rabbitstream.CategoryCanceled, rabbitstream.CategoryTimeout,
		rabbitstream.CategoryAuthentication, rabbitstream.CategoryAuthorization,
		rabbitstream.CategoryConnection, rabbitstream.CategoryStreamUnavailable,
		rabbitstream.CategoryPartitionUnavailable, rabbitstream.CategoryBrokerRejected,
		rabbitstream.CategoryMessageTooLarge, rabbitstream.CategoryPublishAmbiguous,
		rabbitstream.CategoryConfirmation, rabbitstream.CategoryRetentionGap,
		rabbitstream.CategoryReplayRange, rabbitstream.CategoryOffset,
		rabbitstream.CategoryHandler, rabbitstream.CategoryFatal:
		return string(category)
	default:
		return "unknown"
	}
}

func boundedInt64(value uint64) int64 {
	return int64(min(value, uint64(math.MaxInt64)))
}

func nonnegativeSeconds(duration time.Duration) float64 {
	return max(duration.Seconds(), 0)
}

func isNil(value any) bool {
	if value == nil {
		return true
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return reflected.IsNil()
	default:
		return false
	}
}

func validateInstrument(instrument any, err error) error {
	if err != nil {
		return err
	}
	if isNil(instrument) {
		return errors.New("OpenTelemetry provider returned a nil instrument")
	}
	return nil
}

func invalidConfiguration(cause error) error {
	return &rabbitstream.OperationError{
		Operation: rabbitstream.OperationConnect,
		Category:  rabbitstream.CategoryInvalidConfiguration,
		Cause:     cause,
	}
}

func validationError(operation rabbitstream.Operation) error {
	return &rabbitstream.OperationError{
		Operation: operation,
		Category:  rabbitstream.CategoryValidation,
		Cause:     errors.New("trace context propagation failed validation"),
	}
}

var _ rabbitstream.Observer = (*Adapter)(nil)
