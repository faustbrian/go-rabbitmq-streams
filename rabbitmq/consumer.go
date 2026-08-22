package rabbitmq

import (
	"context"
	"errors"
	"math"
	"sync"
	"time"

	"github.com/faustbrian/golib/pkg/rabbitstream"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"
)

// OpenConsumer resolves credentials, opens one consumer per applicable backing
// stream, and returns the root durable-consumer policy wrapper.
func OpenConsumer(
	ctx context.Context,
	connection rabbitstream.ConnectionConfig,
	config rabbitstream.ConsumerConfig,
) (*rabbitstream.Consumer, error) {
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
	normalizedConsumer, err := config.Normalized()
	if err != nil {
		return nil, err
	}
	if !consumerStartOffsetFits(normalizedConsumer.Start) {
		return nil, &rabbitstream.OperationError{
			Operation: rabbitstream.OperationConnect,
			Category:  rabbitstream.CategoryInvalidConfiguration,
		}
	}
	connectCtx, cancel := context.WithTimeout(ctx, normalizedConnection.ConnectTimeout)
	defer cancel()
	transport, err := openConsumerTransport(connectCtx, normalizedConnection, normalizedConsumer)
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
	return finishOpenConsumer(normalizedConsumer, transport)
}

func consumerStartOffsetFits(start rabbitstream.StartPosition) bool {
	return start.Kind != rabbitstream.OffsetStartExplicit || start.Offset <= math.MaxInt64
}

func finishOpenConsumer(
	config rabbitstream.ConsumerConfig,
	transport *consumerTransport,
) (*rabbitstream.Consumer, error) {
	consumer, err := rabbitstream.NewConsumer(config, transport)
	if err != nil {
		_ = transport.Close()
		return nil, err
	}
	return consumer, nil
}

func openConsumerTransport(
	ctx context.Context,
	connection rabbitstream.ConnectionConfig,
	config rabbitstream.ConsumerConfig,
) (*consumerTransport, error) {
	transport, err := newReconnectingConsumerTransport(
		ctx,
		func(openCtx context.Context, reconnect bool) (consumerSession, error) {
			sessionConfig := config
			if reconnect {
				sessionConfig.Start = rabbitstream.StartPosition{Kind: rabbitstream.OffsetStartStored}
			}
			return openConsumerSession(openCtx, connection, sessionConfig)
		},
		connection.Observer,
	)
	if err != nil {
		return nil, err
	}
	transport.retryDelay = connection.InitialReconnectDelay
	transport.maxReconnectAttempts = connection.MaxReconnectAttempts
	return transport, nil
}

func openConsumerSession(
	ctx context.Context,
	connection rabbitstream.ConnectionConfig,
	config rabbitstream.ConsumerConfig,
) (consumerSession, error) {
	return openSessionWithRetries(ctx, connection, func(
		openCtx context.Context,
		attemptConnection rabbitstream.ConnectionConfig,
	) (consumerSession, error) {
		return openConsumerSessionWithEnvironment(openCtx, config, func(environmentCtx context.Context) (rabbitEnvironment, error) {
			return openFreshEnvironment(environmentCtx, attemptConnection)
		})
	})
}

func openConsumerSessionWithEnvironment(
	ctx context.Context,
	config rabbitstream.ConsumerConfig,
	openEnvironment func(context.Context) (rabbitEnvironment, error),
) (consumerSession, error) {
	environment, err := openEnvironment(ctx)
	if err != nil {
		return nil, err
	}
	partitions := []string{config.Stream}
	if config.SuperStream != "" {
		partitions, err = environment.QueryPartitions(config.SuperStream)
		if err != nil || !validSuperStreamPartitions(partitions, config.Limits) {
			_ = environment.Close()
			if err != nil {
				return nil, err
			}
			return nil, rabbitstream.ErrPartitionUnavailable
		}
	}
	session := newRabbitConsumerSession(environment, config, partitions)
	if err := session.open(partitions); err != nil {
		_ = session.Close()
		return nil, err
	}
	return session, nil
}

type consumerSession interface {
	// Next returns one owned delivery or a terminal session error.
	Next(context.Context) (rabbitstream.Message, error)
	// StoreOffset submits one successfully handled partition offset.
	StoreOffset(context.Context, string, uint64) error
	// Close releases every consumer and environment resource.
	Close() error
}

type consumerTransport struct {
	mutex                sync.Mutex
	opener               func(context.Context, bool) (consumerSession, error)
	current              consumerSession
	reconnecting         chan struct{}
	lastErr              error
	retryAfter           time.Time
	retryDelay           time.Duration
	maxReconnectAttempts int
	reconnectFailures    int
	closed               bool
	done                 chan struct{}
	closeOnce            sync.Once
	closeErr             error
	observer             rabbitstream.Observer
}

func newReconnectingConsumerTransport(
	ctx context.Context,
	opener func(context.Context, bool) (consumerSession, error),
	observer rabbitstream.Observer,
) (*consumerTransport, error) {
	if ctx == nil || opener == nil {
		return nil, rabbitstream.ErrInvalidConfiguration
	}
	session, err := opener(ctx, false)
	if err != nil {
		return nil, err
	}
	return &consumerTransport{
		opener:               opener,
		current:              session,
		retryDelay:           time.Millisecond,
		maxReconnectAttempts: rabbitstream.MaxReconnectAttempts,
		done:                 make(chan struct{}),
		observer:             observer,
	}, nil
}

func (transport *consumerTransport) session(ctx context.Context) (consumerSession, error) {
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
		reconnectLimit := transport.maxReconnectAttempts
		if reconnectLimit <= 0 {
			reconnectLimit = 1
		}
		if transport.reconnectFailures >= reconnectLimit {
			err := transport.lastErr
			transport.mutex.Unlock()
			return nil, err
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

		session, err := opener(ctx, true)
		transport.mutex.Lock()
		if err == nil && !transport.closed {
			transport.current = session
			transport.lastErr = nil
			transport.retryAfter = time.Time{}
			transport.reconnectFailures = 0
		} else if err != nil {
			transport.lastErr = err
			transport.retryAfter = time.Now().Add(transport.retryDelay)
			transport.reconnectFailures++
		}
		transport.reconnecting = nil
		close(done)
		closed := transport.closed
		transport.mutex.Unlock()
		if err != nil {
			if retryableConsumerReconnect(err) {
				continue
			}
			return nil, err
		}
		if closed {
			_ = session.Close()
			return nil, rabbitstream.ErrClosed
		}
		safeObserve(transport.observer, rabbitstream.Observation{
			Kind: rabbitstream.ObservationConnectionReady, Count: 1,
		})
		return session, nil
	}
}

func (transport *consumerTransport) invalidate(session consumerSession, cause error) {
	transport.mutex.Lock()
	if transport.current != session {
		transport.mutex.Unlock()
		return
	}
	transport.current = nil
	transport.lastErr = cause
	transport.retryAfter = time.Now().Add(transport.retryDelay)
	transport.reconnectFailures = 0
	transport.mutex.Unlock()
	safeObserve(transport.observer, rabbitstream.Observation{
		Kind: rabbitstream.ObservationConnectionLost, Count: 1,
		Category: brokerErrorCategory(cause),
	})
	_ = session.Close()
}

func retryableConsumerReconnect(err error) bool {
	category := brokerErrorCategory(err)
	return category == rabbitstream.CategoryConnection || category == rabbitstream.CategoryTimeout
}

// Next restores retryable lost sessions before returning a delivery.
func (transport *consumerTransport) Next(ctx context.Context) (rabbitstream.Message, error) {
	for {
		session, err := transport.session(ctx)
		if err != nil {
			return rabbitstream.Message{}, err
		}
		message, err := session.Next(ctx)
		if shouldReturnConsumerMessage(err, ctx.Err()) {
			return message, err
		}
		transport.invalidate(session, err)
	}
}

func shouldReturnConsumerMessage(nextErr error, contextErr error) bool {
	return nextErr == nil || contextErr != nil || errors.Is(nextErr, rabbitstream.ErrClosed)
}

// StoreOffset retries only transport failures and preserves definite offset errors.
func (transport *consumerTransport) StoreOffset(
	ctx context.Context,
	partition string,
	offset uint64,
) error {
	for attempt := 0; attempt < 2; attempt++ {
		session, err := transport.session(ctx)
		if err != nil {
			return err
		}
		err = session.StoreOffset(ctx, partition, offset)
		if err == nil || ctx.Err() != nil || errors.Is(err, rabbitstream.ErrOffset) ||
			errors.Is(err, rabbitstream.ErrPartitionUnavailable) {
			return err
		}
		transport.invalidate(session, err)
	}
	return rabbitstream.ErrConnection
}

// Close stops reconnect work and releases the current session exactly once.
func (transport *consumerTransport) Close() error {
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

type rabbitConsumerSession struct {
	environment rabbitEnvironment
	config      rabbitstream.ConsumerConfig
	partitions  map[string]struct{}
	stores      map[string]func(int64) error
	closers     []func() error
	closeEnv    func() error
	messages    chan rabbitstream.Message
	done        chan struct{}

	failureOnce sync.Once
	failure     error
	failed      chan struct{}
	closeOnce   sync.Once
	closeErr    error
}

func newRabbitConsumerSession(
	environment rabbitEnvironment,
	config rabbitstream.ConsumerConfig,
	partitions []string,
) *rabbitConsumerSession {
	partitionSet := make(map[string]struct{}, len(partitions))
	for _, partition := range partitions {
		partitionSet[partition] = struct{}{}
	}
	return &rabbitConsumerSession{
		environment: environment,
		config:      config,
		partitions:  partitionSet,
		stores:      make(map[string]func(int64) error, len(partitions)),
		closers:     make([]func() error, 0, len(partitions)),
		closeEnv:    environment.Close,
		messages:    make(chan rabbitstream.Message, config.Limits.MaxBufferedMessages),
		done:        make(chan struct{}),
		failed:      make(chan struct{}),
	}
}

func (transport *rabbitConsumerSession) open(partitions []string) error {
	for _, partition := range partitions {
		capturedPartition := partition
		offset, err := transport.startOffset(partition)
		if err != nil {
			return err
		}
		options := stream.NewConsumerOptions().
			SetConsumerName(transport.config.ConsumerName).
			SetManualCommit().
			SetOffset(offset)
		consumer, err := transport.environment.NewConsumer(
			partition,
			func(ctx stream.ConsumerContext, wireMessage *amqp.Message) {
				transport.accept(capturedPartition, ctx.Consumer.GetOffset(), wireMessage)
			},
			options,
		)
		if err != nil {
			return err
		}
		transport.stores[partition] = consumer.StoreCustomOffset
		transport.closers = append(transport.closers, consumer.Close)
		closed := consumer.NotifyClose()
		go func() {
			select {
			case event := <-closed:
				transport.reportFailure(event.Err)
			case <-transport.done:
			}
		}()
	}
	return nil
}

func (transport *rabbitConsumerSession) startOffset(
	partition string,
) (stream.OffsetSpecification, error) {
	if transport.config.Start.Kind != rabbitstream.OffsetStartStored {
		return toOffsetSpecification(transport.config.Start), nil
	}
	offset, err := transport.environment.QueryOffset(transport.config.ConsumerName, partition)
	if err == nil {
		return stream.OffsetSpecification{}.Offset(offset), nil
	}
	if errors.Is(err, stream.OffsetNotFoundError) {
		return stream.OffsetSpecification{}.First(), nil
	}
	return stream.OffsetSpecification{}, err
}

func (transport *rabbitConsumerSession) accept(
	partition string,
	offset int64,
	wireMessage *amqp.Message,
) {
	select {
	case <-transport.failed:
		return
	case <-transport.done:
		return
	default:
	}
	if offset < 0 {
		transport.reportFailure(errors.New("consumer returned a negative offset"))
		return
	}
	delivery, conversionErr := fromWireMessage(
		transport.config.SuperStream,
		partition,
		uint64(offset),
		wireMessage,
	)
	if conversionErr != nil {
		transport.reportFailure(conversionErr)
		return
	}
	if validationErr := validateDelivery(delivery, transport.config.Limits); validationErr != nil {
		transport.reportFailure(validationErr)
		return
	}
	deliverUntilTerminal(transport.messages, transport.failed, transport.done, delivery)
}

func deliverUntilTerminal(
	messages chan<- rabbitstream.Message,
	failed <-chan struct{},
	done <-chan struct{},
	delivery rabbitstream.Message,
) bool {
	select {
	case messages <- delivery:
		return true
	case <-failed:
		return false
	case <-done:
		return false
	}
}

func validateDelivery(delivery rabbitstream.Message, limits rabbitstream.Limits) error {
	return delivery.ValidateDelivery(limits)
}

func (transport *rabbitConsumerSession) reportFailure(err error) {
	transport.failureOnce.Do(func() {
		if err == nil {
			err = errors.New("consumer transport closed unexpectedly")
		}
		transport.failure = err
		close(transport.failed)
	})
}

// Next waits for a delivery, terminal failure, close, or caller cancellation.
func (transport *rabbitConsumerSession) Next(ctx context.Context) (rabbitstream.Message, error) {
	select {
	case message := <-transport.messages:
		return message, nil
	case <-transport.failed:
		return rabbitstream.Message{}, transport.failure
	case <-transport.done:
		return rabbitstream.Message{}, rabbitstream.ErrClosed
	case <-ctx.Done():
		return rabbitstream.Message{}, ctx.Err()
	}
}

// StoreOffset validates partition ownership and integer range before submission.
func (transport *rabbitConsumerSession) StoreOffset(
	ctx context.Context,
	partition string,
	offset uint64,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if _, exists := transport.partitions[partition]; !exists || offset > math.MaxInt64 {
		return rabbitstream.ErrOffset
	}
	store := transport.stores[partition]
	if store == nil {
		return rabbitstream.ErrPartitionUnavailable
	}
	return store(int64(offset))
}

// Close releases every partition consumer before the owning environment.
func (transport *rabbitConsumerSession) Close() error {
	transport.closeOnce.Do(func() {
		close(transport.done)
		for _, closeConsumer := range transport.closers {
			if err := closeConsumer(); err != nil && !errors.Is(err, stream.AlreadyClosed) && transport.closeErr == nil {
				transport.closeErr = err
			}
		}
		if err := transport.closeEnv(); err != nil && transport.closeErr == nil {
			transport.closeErr = err
		}
	})
	return transport.closeErr
}
