// Package rabbitmq adapts the supported RabbitMQ Streams Go client to the
// stable policy types in the root rabbitstream module.
package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/faustbrian/golib/pkg/rabbitstream"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/message"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"
)

const routingKeyAnnotation = rabbitstream.RoutingKeyMetadata

// The upstream client rejects smaller confirmation queues. The root producer
// still enforces the caller-visible MaxOutstanding limit before transport
// admission, so this floor does not widen the package's in-flight policy.
const minimumUpstreamProducerQueueSize = 500

// OpenProducer resolves credentials, establishes the supported RabbitMQ client,
// and returns a root policy Producer that owns all created resources.
func OpenProducer(
	ctx context.Context,
	connection rabbitstream.ConnectionConfig,
	config rabbitstream.ProducerConfig,
) (*rabbitstream.Producer, error) {
	if ctx == nil {
		return nil, &rabbitstream.OperationError{
			Operation: rabbitstream.OperationConnect,
			Category:  rabbitstream.CategoryInvalidConfiguration,
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, &rabbitstream.OperationError{
			Operation: rabbitstream.OperationConnect,
			Category:  rabbitstream.CategoryCanceled,
			Cause:     err,
		}
	}
	normalizedConnection, err := connection.Normalized()
	if err != nil {
		return nil, err
	}
	normalizedProducer, err := config.Normalized()
	if err != nil {
		return nil, err
	}
	connectCtx, cancelConnect := context.WithTimeout(ctx, normalizedConnection.ConnectTimeout)
	defer cancelConnect()
	transport, err := openTransport(connectCtx, normalizedConnection, normalizedProducer)
	if err != nil {
		if ctxErr := connectCtx.Err(); ctxErr != nil {
			category := rabbitstream.CategoryCanceled
			if errors.Is(ctxErr, context.DeadlineExceeded) {
				category = rabbitstream.CategoryTimeout
			}
			return nil, &rabbitstream.OperationError{
				Operation: rabbitstream.OperationConnect,
				Category:  category,
				Cause:     ctxErr,
			}
		}
		category := brokerErrorCategory(err)
		return nil, &rabbitstream.OperationError{
			Operation: rabbitstream.OperationConnect,
			Category:  category,
			Cause:     err,
		}
	}
	return finishOpenProducer(normalizedProducer, transport)
}

func finishOpenProducer(
	config rabbitstream.ProducerConfig,
	transport rabbitstream.ProducerTransport,
) (*rabbitstream.Producer, error) {
	producer, err := rabbitstream.NewProducer(config, transport)
	if err != nil {
		_ = transport.Close()
		return nil, err
	}
	return producer, nil
}

func openTransport(
	ctx context.Context,
	connection rabbitstream.ConnectionConfig,
	config rabbitstream.ProducerConfig,
) (rabbitstream.ProducerTransport, error) {
	transport, err := newReconnectingProducerTransport(ctx, func(openCtx context.Context) (producerSession, error) {
		return openProducerSession(openCtx, connection, config)
	}, connection.Observer)
	if err != nil {
		return nil, err
	}
	transport.retryDelay = connection.InitialReconnectDelay
	return transport, nil
}

func openProducerSession(
	ctx context.Context,
	connection rabbitstream.ConnectionConfig,
	config rabbitstream.ProducerConfig,
) (producerSession, error) {
	return openSessionWithRetries(ctx, connection, func(
		openCtx context.Context,
		attemptConnection rabbitstream.ConnectionConfig,
	) (producerSession, error) {
		return openProducerSessionWithEnvironment(openCtx, config, func(environmentCtx context.Context) (producerEnvironment, error) {
			return openFreshEnvironment(environmentCtx, attemptConnection)
		})
	})
}

func openSessionWithRetries[T any](
	ctx context.Context,
	connection rabbitstream.ConnectionConfig,
	opener func(context.Context, rabbitstream.ConnectionConfig) (T, error),
) (T, error) {
	var zero T
	operationCtx, cancel := context.WithTimeout(ctx, connection.ConnectTimeout)
	defer cancel()
	backoff := connection.InitialReconnectDelay
	var lastErr error
	for attempt := 0; attempt < connection.MaxReconnectAttempts; attempt++ {
		if shouldWaitBeforeConnectionAttempt(attempt) {
			timer := time.NewTimer(backoff)
			select {
			case <-timer.C:
			case <-operationCtx.Done():
				stopAndDrainTimer(timer)
				return zero, operationCtx.Err()
			}
			backoff = nextReconnectBackoff(backoff, connection.MaxReconnectBackoff)
		}
		remaining := time.Until(operationDeadline(operationCtx, connection.ConnectTimeout))
		attemptTimeout := boundedAttemptTimeout(
			remaining, connection.MaxReconnectAttempts, attempt, connection.RPCTimeout,
		)
		if attemptTimeout <= 0 {
			if err := operationCtx.Err(); err != nil {
				return zero, err
			}
			return zero, context.DeadlineExceeded
		}
		attemptConnection := connection
		endpoint := attempt % len(connection.Endpoints)
		attemptConnection.Endpoints = append(
			append([]rabbitstream.Endpoint(nil), connection.Endpoints[endpoint:]...),
			connection.Endpoints[:endpoint]...,
		)
		attemptConnection.ConnectTimeout = attemptTimeout
		attemptConnection.RPCTimeout = attemptTimeout
		attemptConnection.MaxReconnectAttempts = 1
		session, err := opener(operationCtx, attemptConnection)
		if err == nil {
			return session, nil
		}
		lastErr = err
		switch brokerErrorCategory(err) {
		case rabbitstream.CategoryAuthentication:
			if !errors.Is(err, stream.AuthenticationFailure) &&
				!errors.Is(err, stream.AuthenticationFailureLoopbackError) {
				return zero, err
			}
		case rabbitstream.CategoryAuthorization,
			rabbitstream.CategoryInvalidConfiguration:
			return zero, err
		}
	}
	if lastErr != nil {
		return zero, lastErr
	}
	return zero, rabbitstream.ErrConnection
}

func openProducerSessionWithEnvironment(
	ctx context.Context,
	config rabbitstream.ProducerConfig,
	openEnvironment func(context.Context) (producerEnvironment, error),
) (producerSession, error) {
	environment, err := openEnvironment(ctx)
	if err != nil {
		return nil, err
	}
	queueSize := max(config.Policy.MaxOutstanding, minimumUpstreamProducerQueueSize)
	producerOptions := stream.NewProducerOptions().
		SetQueueSize(queueSize).
		SetConfirmationTimeOut(config.Policy.ConfirmationTimeout)
	if config.Policy.Deduplication == rabbitstream.DeduplicationPublishingID {
		producerOptions = producerOptions.SetProducerName(config.Policy.ProducerName)
	}
	if config.Stream != "" {
		producer, err := environment.NewProducer(config.Stream, producerOptions)
		if err != nil {
			_ = environment.Close()
			return nil, err
		}
		return newRabbitProducerSession(environment, producer), nil
	}

	partitions, err := environment.QueryPartitions(config.SuperStream)
	if err != nil || !validSuperStreamPartitions(partitions, config.Limits) ||
		(config.ExpectedPartitions != 0 && len(partitions) != config.ExpectedPartitions) {
		_ = environment.Close()
		if err != nil {
			return nil, err
		}
		return nil, rabbitstream.ErrPartitionUnavailable
	}
	producers := make(map[string]rabbitProducer, len(partitions))
	for _, partition := range partitions {
		partitionProducer, producerErr := environment.NewProducer(partition, producerOptions)
		if producerErr != nil {
			for _, opened := range producers {
				_ = opened.Close()
			}
			_ = environment.Close()
			return nil, producerErr
		}
		producers[partition] = partitionProducer
	}
	return newRabbitSuperProducerSession(environment, producers, partitions), nil
}

type producerSession interface {
	// Send submits one message and registers one terminal confirmation callback.
	Send(rabbitstream.Message, func(rabbitstream.TransportConfirmation)) error
	// Failures reports the first terminal session failure.
	Failures() <-chan error
	// Abort resolves every admitted pending message as ambiguous.
	Abort(error)
	// Close releases producer and environment resources.
	Close() error
}

type producerTransport struct {
	mutex        sync.Mutex
	opener       func(context.Context) (producerSession, error)
	current      producerSession
	reconnecting chan struct{}
	lastErr      error
	retryAfter   time.Time
	retryDelay   time.Duration
	closed       bool
	done         chan struct{}
	closeOnce    sync.Once
	closeErr     error
	observer     rabbitstream.Observer
}

func newReconnectingProducerTransport(
	ctx context.Context,
	opener func(context.Context) (producerSession, error),
	observer rabbitstream.Observer,
) (*producerTransport, error) {
	if ctx == nil || opener == nil {
		return nil, rabbitstream.ErrInvalidConfiguration
	}
	session, err := opener(ctx)
	if err != nil {
		return nil, err
	}
	transport := &producerTransport{
		opener:     opener,
		current:    session,
		retryDelay: time.Millisecond,
		done:       make(chan struct{}),
		observer:   observer,
	}
	transport.watch(session)
	return transport, nil
}

func (transport *producerTransport) watch(session producerSession) {
	go func() {
		select {
		case err, ok := <-session.Failures():
			if !ok || err == nil {
				err = rabbitstream.ErrConnection
			}
			transport.invalidate(session, err)
		case <-transport.done:
		}
	}()
}

func (transport *producerTransport) invalidate(session producerSession, cause error) {
	transport.mutex.Lock()
	if transport.current != session {
		transport.mutex.Unlock()
		return
	}
	transport.current = nil
	transport.lastErr = cause
	transport.retryAfter = time.Now().Add(transport.retryDelay)
	transport.mutex.Unlock()
	safeObserve(transport.observer, rabbitstream.Observation{
		Kind: rabbitstream.ObservationConnectionLost, Count: 1,
		Category: brokerErrorCategory(cause),
	})

	session.Abort(cause)
	_ = session.Close()
}

func (transport *producerTransport) session(ctx context.Context) (producerSession, error) {
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		transport.mutex.Lock()
		if transport.closed {
			transport.mutex.Unlock()
			return nil, rabbitstream.ErrClosed
		}
		if transport.current != nil {
			session := transport.current
			transport.mutex.Unlock()
			return session, nil
		}
		if transport.reconnecting != nil {
			done := transport.reconnecting
			transport.mutex.Unlock()
			select {
			case <-done:
				continue
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		wait := time.Until(transport.retryAfter)
		if shouldWaitForRetry(transport.lastErr, wait) {
			transport.mutex.Unlock()
			timer := time.NewTimer(wait)
			select {
			case <-timer.C:
			case <-ctx.Done():
				stopAndDrainTimer(timer)
				return nil, ctx.Err()
			}
			continue
		}
		done := make(chan struct{})
		transport.reconnecting = done
		opener := transport.opener
		transport.mutex.Unlock()
		safeObserve(transport.observer, rabbitstream.Observation{
			Kind: rabbitstream.ObservationReconnectAttempt, Count: 1,
		})

		session, err := opener(ctx)
		transport.mutex.Lock()
		if err == nil && !transport.closed {
			transport.current = session
			transport.lastErr = nil
			transport.retryAfter = time.Time{}
		} else if err != nil {
			transport.lastErr = err
			transport.retryAfter = time.Now().Add(transport.retryDelay)
		}
		transport.reconnecting = nil
		close(done)
		closed := transport.closed
		transport.mutex.Unlock()
		if err != nil {
			return nil, err
		}
		if closed {
			_ = session.Close()
			return nil, rabbitstream.ErrClosed
		}
		safeObserve(transport.observer, rabbitstream.Observation{
			Kind: rabbitstream.ObservationConnectionReady, Count: 1,
		})
		transport.watch(session)
		return session, nil
	}
}

func shouldWaitForRetry(lastErr error, wait time.Duration) bool {
	return lastErr != nil && wait > 0
}

var errProducerSessionClosed = errors.New("RabbitMQ producer session is closed")

// Send admits one message and retries only failures known to precede admission.
func (transport *producerTransport) Send(
	ctx context.Context,
	outbound rabbitstream.Message,
	confirm func(rabbitstream.TransportConfirmation),
) error {
	for attempt := 0; attempt < 2; attempt++ {
		session, err := transport.session(ctx)
		if err != nil {
			return err
		}
		err = session.Send(outbound, confirm)
		if !errors.Is(err, errProducerSessionClosed) && !errors.Is(err, rabbitstream.ErrConnection) {
			return err
		}
		transport.invalidate(session, err)
	}
	return rabbitstream.ErrConnection
}

// Close stops reconnect work and releases the current session exactly once.
func (transport *producerTransport) Close() error {
	transport.closeOnce.Do(func() {
		transport.mutex.Lock()
		transport.closed = true
		session := transport.current
		transport.current = nil
		close(transport.done)
		transport.mutex.Unlock()
		if session != nil {
			transport.closeErr = session.Close()
		}
	})
	return transport.closeErr
}

type rabbitProducerSession struct {
	send             func(message.StreamMessage) error
	partitionSenders map[string]func(message.StreamMessage) error
	producerClosers  []func() error
	environmentClose func() error
	partitions       []string

	mutex       sync.Mutex
	pending     map[message.StreamMessage]*pendingConfirmation
	aborted     bool
	failed      bool
	failures    chan error
	failureOnce sync.Once
	closeOnce   sync.Once
	closeErr    error
}

type pendingConfirmation struct {
	partition string
	confirm   func(rabbitstream.TransportConfirmation)
	admitted  bool
	result    *rabbitstream.TransportConfirmation
}

func newRabbitProducerSession(
	environment rabbitEnvironment,
	producer rabbitProducer,
) *rabbitProducerSession {
	session := &rabbitProducerSession{
		send:             producer.Send,
		producerClosers:  []func() error{producer.Close},
		environmentClose: environment.Close,
		pending:          make(map[message.StreamMessage]*pendingConfirmation),
		failures:         make(chan error, 1),
	}
	confirmations := producer.NotifyPublishConfirmation()
	go session.handleConfirmations(confirmations, producer.GetStreamName())
	go session.watchProducer(producer.NotifyClose())
	return session
}

func newRabbitSuperProducerSession(
	environment rabbitEnvironment,
	producers map[string]rabbitProducer,
	partitions []string,
) *rabbitProducerSession {
	session := &rabbitProducerSession{
		partitionSenders: make(map[string]func(message.StreamMessage) error, len(producers)),
		producerClosers:  make([]func() error, 0, len(producers)),
		environmentClose: environment.Close,
		partitions:       append([]string(nil), partitions...),
		pending:          make(map[message.StreamMessage]*pendingConfirmation),
		failures:         make(chan error, 1),
	}
	for partition, producer := range producers {
		session.partitionSenders[partition] = producer.Send
		session.producerClosers = append(session.producerClosers, producer.Close)
		confirmations := producer.NotifyPublishConfirmation()
		go session.handleConfirmations(confirmations, partition)
		go session.watchProducer(producer.NotifyClose())
	}
	return session
}

// Send copies a message to the wire model and tracks its confirmation callback.
func (session *rabbitProducerSession) Send(
	outbound rabbitstream.Message,
	confirm func(rabbitstream.TransportConfirmation),
) error {
	wireMessage := toWireMessage(outbound)
	send := session.send
	partition := outbound.Stream
	if len(session.partitionSenders) > 0 {
		var err error
		partition, err = hashPartition(outbound.RoutingKey, session.partitions)
		if err != nil {
			return err
		}
		send = session.partitionSenders[partition]
		if send == nil {
			return rabbitstream.ErrPartitionUnavailable
		}
	}

	session.mutex.Lock()
	if session.aborted {
		session.mutex.Unlock()
		return errProducerSessionClosed
	}
	pending := &pendingConfirmation{partition: partition, confirm: confirm}
	session.pending[wireMessage] = pending
	session.mutex.Unlock()
	if err := send(wireMessage); err != nil {
		session.mutex.Lock()
		delete(session.pending, wireMessage)
		session.mutex.Unlock()
		if errors.Is(err, stream.FrameTooLarge) {
			return rabbitstream.ErrMessageTooLarge
		}
		return errProducerSessionClosed
	}
	session.mutex.Lock()
	pending.admitted = true
	result := pending.result
	if result != nil {
		delete(session.pending, wireMessage)
	}
	session.mutex.Unlock()
	if result != nil {
		pending.confirm(*result)
	}
	return nil
}

func toWireMessage(outbound rabbitstream.Message) *amqp.AMQP10 {
	wireMessage := amqp.NewMessage(append([]byte(nil), outbound.Payload...))
	if outbound.HasPublishingID {
		wireMessage.SetPublishingId(int64(outbound.PublishingID))
	}
	if outbound.ContentType != "" || outbound.MessageID != "" ||
		outbound.CorrelationID != "" || !outbound.Timestamp.IsZero() {
		wireMessage.Properties = &amqp.MessageProperties{
			ContentType:   outbound.ContentType,
			MessageID:     outbound.MessageID,
			CorrelationID: outbound.CorrelationID,
			CreationTime:  outbound.Timestamp,
		}
	}
	if len(outbound.Headers) > 0 || outbound.RoutingKey != "" {
		wireMessage.Annotations = make(amqp.Annotations, len(outbound.Headers))
		for _, entry := range outbound.Headers {
			wireMessage.Annotations[entry.Key] = append([]byte(nil), entry.Value...)
		}
		if outbound.RoutingKey != "" {
			wireMessage.Annotations[routingKeyAnnotation] = outbound.RoutingKey
		}
	}
	if len(outbound.Properties) > 0 {
		wireMessage.ApplicationProperties = make(map[string]any, len(outbound.Properties))
		for _, entry := range outbound.Properties {
			wireMessage.ApplicationProperties[entry.Key] = append([]byte(nil), entry.Value...)
		}
	}
	return wireMessage
}

func fromWireMessage(
	superStream string,
	partition string,
	offset uint64,
	wireMessage any,
) (rabbitstream.Message, error) {
	var data [][]byte
	var properties *amqp.MessageProperties
	var annotations amqp.Annotations
	var applicationProperties map[string]any
	switch typed := wireMessage.(type) {
	case message.StreamMessage:
		data = typed.GetData()
		properties = typed.GetMessageProperties()
		annotations = typed.GetMessageAnnotations()
		applicationProperties = typed.GetApplicationProperties()
	case *amqp.Message:
		data = typed.Data
		properties = typed.Properties
		annotations = typed.Annotations
		applicationProperties = typed.ApplicationProperties
	default:
		return rabbitstream.Message{}, unsupportedWireMetadata()
	}
	if len(data) > 1 {
		return rabbitstream.Message{}, &rabbitstream.OperationError{
			Operation: rabbitstream.OperationConsume,
			Category:  rabbitstream.CategoryValidation,
		}
	}
	delivery := rabbitstream.Message{
		Stream:      partition,
		SuperStream: superStream,
		Partition:   partition,
		Offset:      offset,
		HasOffset:   true,
	}
	if len(data) == 1 {
		delivery.Payload = append([]byte(nil), data[0]...)
	}
	if properties != nil {
		var err error
		delivery.ContentType = properties.ContentType
		delivery.MessageID, err = stringProperty(properties.MessageID)
		if err != nil {
			return rabbitstream.Message{}, err
		}
		delivery.CorrelationID, err = stringProperty(properties.CorrelationID)
		if err != nil {
			return rabbitstream.Message{}, err
		}
		delivery.Timestamp = properties.CreationTime
	}

	annotationKeys := make([]string, 0, len(annotations))
	for rawKey := range annotations {
		key, ok := rawKey.(string)
		if !ok {
			return rabbitstream.Message{}, unsupportedWireMetadata()
		}
		annotationKeys = append(annotationKeys, key)
	}
	sort.Strings(annotationKeys)
	for _, key := range annotationKeys {
		value, err := bytesProperty(annotations[key])
		if err != nil {
			return rabbitstream.Message{}, err
		}
		if key == routingKeyAnnotation {
			delivery.RoutingKey = string(value)
			continue
		}
		delivery.Headers = append(delivery.Headers, rabbitstream.MetadataEntry{Key: key, Value: value})
	}

	propertyKeys := make([]string, 0, len(applicationProperties))
	for key := range applicationProperties {
		propertyKeys = append(propertyKeys, key)
	}
	sort.Strings(propertyKeys)
	for _, key := range propertyKeys {
		value, err := bytesProperty(applicationProperties[key])
		if err != nil {
			return rabbitstream.Message{}, err
		}
		delivery.Properties = append(delivery.Properties, rabbitstream.MetadataEntry{Key: key, Value: value})
	}
	return delivery, nil
}

func stringProperty(value any) (string, error) {
	switch typed := value.(type) {
	case nil:
		return "", nil
	case string:
		return typed, nil
	case []byte:
		return string(typed), nil
	default:
		return "", unsupportedWireMetadata()
	}
}

func bytesProperty(value any) ([]byte, error) {
	switch typed := value.(type) {
	case string:
		return []byte(typed), nil
	case []byte:
		return append([]byte(nil), typed...), nil
	default:
		return nil, unsupportedWireMetadata()
	}
}

func unsupportedWireMetadata() error {
	return &rabbitstream.OperationError{
		Operation: rabbitstream.OperationConsume,
		Category:  rabbitstream.CategoryValidation,
	}
}

func toOffsetSpecification(start rabbitstream.StartPosition) stream.OffsetSpecification {
	specification := stream.OffsetSpecification{}
	switch start.Kind {
	case rabbitstream.OffsetStartBeginning:
		return specification.First()
	case rabbitstream.OffsetStartEnd:
		return specification.Next()
	case rabbitstream.OffsetStartExplicit:
		return specification.Offset(int64(start.Offset))
	case rabbitstream.OffsetStartTimestamp:
		return specification.Timestamp(start.Timestamp.UnixMilli())
	default:
		return specification
	}
}

func hashPartition(routingKey string, partitions []string) (string, error) {
	if len(partitions) == 0 {
		return "", errors.New("super stream has no backing partitions")
	}
	strategy := stream.NewHashRoutingStrategy(func(message.StreamMessage) string {
		return routingKey
	})
	routed, err := strategy.Route(amqp.NewMessage(nil), partitions)
	return routedPartition(routed, err)
}

func routedPartition(routed []string, err error) (string, error) {
	if err != nil {
		return "", err
	}
	if len(routed) != 1 {
		return "", errors.New("hash routing did not select one partition")
	}
	return routed[0], nil
}

func (session *rabbitProducerSession) handleConfirmations(
	confirmations stream.ChannelPublishConfirm,
	partition string,
) {
	for statuses := range confirmations {
		for _, status := range statuses {
			session.handleConfirmation(status, partition)
		}
	}
	session.signalFailure(rabbitstream.ErrConnection)
}

func (session *rabbitProducerSession) handleConfirmation(
	status *stream.ConfirmationStatus,
	partition string,
) {
	wireMessage := status.GetMessage()
	session.mutex.Lock()
	confirmation := classifyConfirmation(
		status.IsConfirmed(),
		status.GetError(),
		uint64(status.GetPublishingId()),
	)
	confirmation.Partition = partition
	pending := session.pending[wireMessage]
	if pending == nil {
		session.mutex.Unlock()
		return
	}
	if !pending.admitted {
		pending.result = &confirmation
		session.mutex.Unlock()
		return
	}
	delete(session.pending, wireMessage)
	session.mutex.Unlock()
	pending.confirm(confirmation)
}

func classifyConfirmation(
	confirmed bool,
	cause error,
	publishingID uint64,
) rabbitstream.TransportConfirmation {
	return rabbitstream.TransportConfirmation{
		Confirmed:      confirmed,
		BrokerRejected: !confirmed && !errors.Is(cause, stream.ConfirmationTimoutError),
		Ambiguous:      !confirmed && errors.Is(cause, stream.ConfirmationTimoutError),
		PublishingID:   publishingID,
		Cause:          cause,
	}
}

func (session *rabbitProducerSession) watchProducer(closed stream.ChannelClose) {
	event := <-closed
	session.signalFailure(event.Err)
}

func (session *rabbitProducerSession) signalFailure(err error) {
	session.failureOnce.Do(func() {
		if err == nil {
			err = rabbitstream.ErrConnection
		}
		session.mutex.Lock()
		session.failed = true
		session.mutex.Unlock()
		session.failures <- err
		close(session.failures)
	})
}

// Failures returns the session's single buffered terminal failure stream.
func (session *rabbitProducerSession) Failures() <-chan error { return session.failures }

// Abort marks the session unusable and resolves pending outcomes as ambiguous.
func (session *rabbitProducerSession) Abort(cause error) {
	session.mutex.Lock()
	if session.aborted {
		session.mutex.Unlock()
		return
	}
	session.aborted = true
	pending := make([]*pendingConfirmation, 0, len(session.pending))
	for wireMessage, confirmation := range session.pending {
		pending = append(pending, confirmation)
		delete(session.pending, wireMessage)
	}
	session.mutex.Unlock()
	for _, confirmation := range pending {
		confirmation.confirm(rabbitstream.TransportConfirmation{
			Ambiguous: true,
			Partition: confirmation.partition,
			Cause:     cause,
		})
	}
}

// Close releases every producer before its owning environment exactly once.
func (session *rabbitProducerSession) Close() error {
	session.closeOnce.Do(func() {
		var producerErr error
		for _, closeProducer := range session.producerClosers {
			if err := closeProducer(); err != nil && producerErr == nil {
				producerErr = err
			}
		}
		environmentErr := session.environmentClose()
		if producerErr != nil && !errors.Is(producerErr, stream.AlreadyClosed) {
			session.closeErr = fmt.Errorf("close RabbitMQ Streams producer: %w", producerErr)
			return
		}
		if environmentErr != nil {
			session.closeErr = fmt.Errorf("close RabbitMQ Streams environment: %w", environmentErr)
		}
	})
	return session.closeErr
}
