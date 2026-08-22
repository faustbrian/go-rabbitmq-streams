package rabbitmq

import (
	"context"
	"errors"
	"io"
	"math"
	"sync"
	"time"

	"github.com/faustbrian/golib/pkg/rabbitstream"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"
)

// Replayer owns the stable root replay policy backed by fresh bounded RabbitMQ
// environments per inspection or cursor. It never uses a live consumer name or
// stores offsets.
type Replayer struct {
	core *rabbitstream.Replayer
}

// NewReplayer validates connection and message policy without connecting.
// Credentials are resolved freshly for each later replay operation.
func NewReplayer(
	connection rabbitstream.ConnectionConfig,
	limits rabbitstream.Limits,
) (*Replayer, error) {
	normalized, err := connection.Normalized()
	if err != nil {
		return nil, err
	}
	if limits == (rabbitstream.Limits{}) {
		limits = rabbitstream.DefaultLimits()
	}
	source := &replaySource{
		connection: normalized,
		limits:     limits,
		openEnvironment: func(ctx context.Context) (rabbitEnvironment, error) {
			return openFreshEnvironment(ctx, normalized)
		},
	}
	core, err := rabbitstream.NewReplayer(limits, source, normalized.Observer)
	if err != nil {
		return nil, err
	}
	return &Replayer{core: core}, nil
}

// Inspect returns an exact retained message range snapshot.
func (replayer *Replayer) Inspect(
	ctx context.Context,
	request rabbitstream.ReplayRequest,
) (rabbitstream.RetainedRange, error) {
	return replayer.core.Inspect(ctx, request)
}

// Run executes isolated replay without mutating a named consumer offset.
func (replayer *Replayer) Run(
	ctx context.Context,
	request rabbitstream.ReplayRequest,
	handler rabbitstream.ReplayHandler,
) error {
	return replayer.core.Run(ctx, request, handler)
}

type replaySource struct {
	connection      rabbitstream.ConnectionConfig
	limits          rabbitstream.Limits
	openEnvironment func(context.Context) (rabbitEnvironment, error)
}

// RetainedRange resolves the broker's exact current retained offset interval.
func (source *replaySource) RetainedRange(
	ctx context.Context,
	request rabbitstream.ReplayRequest,
) (rabbitstream.RetainedRange, error) {
	target := replayTarget(request)
	environment, err := source.openEnvironment(ctx)
	if err != nil {
		return rabbitstream.RetainedRange{}, err
	}
	defer func() { _ = environment.Close() }()
	if err := ensureReplayTopology(environment, request); err != nil {
		return rabbitstream.RetainedRange{}, err
	}
	exists, err := environment.StreamExists(target)
	if err != nil {
		return rabbitstream.RetainedRange{}, err
	}
	if !exists {
		return rabbitstream.RetainedRange{}, rabbitstream.ErrStreamUnavailable
	}
	stats, err := environment.StreamStats(target)
	if err != nil {
		return rabbitstream.RetainedRange{}, err
	}
	return retainedRangeFromStats(ctx, environment, target, stats)
}

func retainedRangeFromStats(
	ctx context.Context,
	environment rabbitEnvironment,
	target string,
	stats streamStatistics,
) (rabbitstream.RetainedRange, error) {
	first, err := stats.FirstOffset()
	firstOffset, empty, err := retainedStart(first, err)
	if err != nil {
		return rabbitstream.RetainedRange{}, err
	}
	if empty {
		return rabbitstream.RetainedRange{Empty: true}, nil
	}
	last, err := snapshotLastOffset(ctx, environment, target)
	if err != nil {
		return rabbitstream.RetainedRange{}, err
	}
	return rabbitstream.RetainedRange{FirstOffset: firstOffset, LastOffset: last}, nil
}

func retainedStart(first int64, firstErr error) (uint64, bool, error) {
	if firstErr != nil {
		return 0, true, nil
	}
	if first < 0 {
		return 0, false, rabbitstream.ErrReplayRange
	}
	return uint64(first), false, nil
}

func snapshotLastOffset(
	ctx context.Context,
	environment rabbitEnvironment,
	target string,
) (uint64, error) {
	type result struct {
		last uint64
		err  error
	}
	results := make(chan result, 1)
	var once sync.Once
	consumer, err := environment.NewConsumer(
		target,
		func(consumerContext stream.ConsumerContext, _ *amqp.Message) {
			once.Do(func() {
				last, rangeErr := snapshotChunkLastOffset(
					consumerContext.Consumer.GetOffset(),
					consumerContext.GetEntriesCount(),
				)
				results <- result{last: last, err: rangeErr}
			})
		},
		stream.NewConsumerOptions().
			SetManualCommit().
			SetOffset(stream.OffsetSpecification{}.Last()),
	)
	if err != nil {
		return 0, err
	}
	defer func() { _ = consumer.Close() }()
	closed := consumer.NotifyClose()
	select {
	case result := <-results:
		return result.last, result.err
	case event := <-closed:
		if event.Err != nil {
			return 0, event.Err
		}
		return 0, rabbitstream.ErrConnection
	case <-ctx.Done():
		return 0, ctx.Err()
	}
}

func snapshotChunkLastOffset(first int64, entries uint16) (uint64, error) {
	if first < 0 {
		return 0, rabbitstream.ErrReplayRange
	}
	return chunkLastOffset(uint64(first), entries)
}

func chunkLastOffset(first uint64, entries uint16) (uint64, error) {
	if entries == 0 || first > math.MaxUint64-uint64(entries-1) {
		return 0, rabbitstream.ErrReplayRange
	}
	return first + uint64(entries-1), nil
}

// Open creates an isolated consumer cursor without broker offset storage.
func (source *replaySource) Open(
	ctx context.Context,
	request rabbitstream.ReplayRequest,
) (rabbitstream.ReplayCursor, error) {
	if request.EndOffset == nil {
		return nil, rabbitstream.ErrReplayRange
	}
	environment, err := source.openEnvironment(ctx)
	if err != nil {
		return nil, err
	}
	if err := ensureReplayTopology(environment, request); err != nil {
		_ = environment.Close()
		return nil, err
	}
	cursor := &replayCursor{
		environment: environment,
		target:      replayTarget(request),
		superStream: request.SuperStream,
		end:         *request.EndOffset,
		limits:      source.limits,
		messages:    make(chan rabbitstream.Message, source.limits.MaxBufferedMessages),
		done:        make(chan struct{}),
		completed:   make(chan struct{}),
		failed:      make(chan struct{}),
	}
	options := stream.NewConsumerOptions().
		SetManualCommit().
		SetOffset(toOffsetSpecification(request.Start))
	consumer, err := environment.NewConsumer(
		cursor.target,
		func(consumerContext stream.ConsumerContext, wireMessage *amqp.Message) {
			cursor.accept(consumerContext.Consumer.GetOffset(), wireMessage)
		},
		options,
	)
	if err != nil {
		_ = environment.Close()
		return nil, err
	}
	cursor.consumer = consumer
	closed := consumer.NotifyClose()
	go func() {
		select {
		case event := <-closed:
			cursor.reportFailure(event.Err)
		case <-cursor.done:
		case <-cursor.completed:
		}
	}()
	return cursor, nil
}

func (cursor *replayCursor) accept(offset int64, wireMessage *amqp.Message) {
	select {
	case <-cursor.done:
		return
	case <-cursor.completed:
		return
	case <-cursor.failed:
		return
	default:
	}
	if offset < 0 {
		cursor.reportFailure(rabbitstream.ErrReplayRange)
		return
	}
	if uint64(offset) > cursor.end {
		cursor.complete()
		return
	}
	message, conversionErr := fromWireMessage(
		cursor.superStream, cursor.target, uint64(offset), wireMessage,
	)
	if conversionErr == nil {
		conversionErr = validateDelivery(message, cursor.limits)
	}
	if conversionErr != nil {
		cursor.reportFailure(conversionErr)
		return
	}
	if deliverUntilTerminal(cursor.messages, cursor.failed, cursor.done, message) {
		if uint64(offset) == cursor.end {
			cursor.complete()
		}
	}
}

func ensureReplayTopology(environment rabbitEnvironment, request rabbitstream.ReplayRequest) error {
	if request.SuperStream == "" {
		return nil
	}
	partitions, err := environment.QueryPartitions(request.SuperStream)
	if err != nil {
		return classifyBrokerError(err)
	}
	if len(partitions) != len(request.ExpectedPartitions) ||
		!validSuperStreamPartitionCount(len(partitions)) {
		return rabbitstream.ErrPartitionUnavailable
	}
	for index, partition := range partitions {
		if partition != request.ExpectedPartitions[index] {
			return rabbitstream.ErrPartitionUnavailable
		}
	}
	return nil
}

func replayTarget(request rabbitstream.ReplayRequest) string {
	if request.Partition != "" {
		return request.Partition
	}
	return request.Stream
}

type replayCursor struct {
	environment rabbitEnvironment
	consumer    rabbitConsumer
	target      string
	superStream string
	end         uint64
	limits      rabbitstream.Limits
	messages    chan rabbitstream.Message
	done        chan struct{}
	completed   chan struct{}
	failed      chan struct{}

	completeOnce sync.Once
	failureOnce  sync.Once
	failure      error
	closeOnce    sync.Once
	closeErr     error
}

func (cursor *replayCursor) complete() {
	cursor.completeOnce.Do(func() { close(cursor.completed) })
}

func (cursor *replayCursor) reportFailure(err error) {
	cursor.failureOnce.Do(func() {
		if err == nil {
			err = rabbitstream.ErrConnection
		}
		cursor.failure = err
		close(cursor.failed)
	})
}

// Next returns one retained delivery or the cursor's bounded terminal state.
func (cursor *replayCursor) Next(ctx context.Context) (rabbitstream.Message, error) {
	select {
	case message := <-cursor.messages:
		return message, nil
	default:
	}
	select {
	case message := <-cursor.messages:
		return message, nil
	case <-cursor.completed:
		return cursor.nextCompleted()
	case <-cursor.failed:
		return rabbitstream.Message{}, cursor.failure
	case <-cursor.done:
		return rabbitstream.Message{}, rabbitstream.ErrClosed
	case <-ctx.Done():
		return rabbitstream.Message{}, ctx.Err()
	}
}

func (cursor *replayCursor) nextCompleted() (rabbitstream.Message, error) {
	select {
	case message := <-cursor.messages:
		return message, nil
	default:
		return rabbitstream.Message{}, io.EOF
	}
}

// Close cancels cursor work and releases consumer and environment resources.
func (cursor *replayCursor) Close() error {
	cursor.closeOnce.Do(func() {
		close(cursor.done)
		if cursor.consumer != nil {
			if err := cursor.consumer.Close(); err != nil && !errors.Is(err, stream.AlreadyClosed) {
				cursor.closeErr = err
			}
		}
		if err := cursor.environment.Close(); err != nil && cursor.closeErr == nil {
			cursor.closeErr = err
		}
	})
	return cursor.closeErr
}

func openFreshEnvironment(
	ctx context.Context,
	connection rabbitstream.ConnectionConfig,
) (producerEnvironment, error) {
	return openFreshEnvironmentWith(ctx, connection, func(options *stream.EnvironmentOptions) (producerEnvironment, error) {
		upstream, err := stream.NewEnvironment(options)
		return wrapStreamEnvironment(upstream, err)
	})
}

func wrapStreamEnvironment(upstream *stream.Environment, err error) (producerEnvironment, error) {
	if upstream == nil {
		return nil, err
	}
	return &streamEnvironment{environment: upstream}, err
}

func openFreshEnvironmentWith(
	ctx context.Context,
	connection rabbitstream.ConnectionConfig,
	opener func(*stream.EnvironmentOptions) (producerEnvironment, error),
) (producerEnvironment, error) {
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
				return nil, operationCtx.Err()
			}
			backoff = nextReconnectBackoff(backoff, connection.MaxReconnectBackoff)
		}
		safeObserve(connection.Observer, rabbitstream.Observation{
			Kind: rabbitstream.ObservationConnectionConnecting, Count: 1, Value: connectionAttemptNumber(attempt),
		})
		credentials, err := connection.Credentials.Credentials(operationCtx)
		if err != nil {
			if operationCtx.Err() != nil {
				return nil, operationCtx.Err()
			}
			safeObserve(connection.Observer, rabbitstream.Observation{
				Kind: rabbitstream.ObservationAuthenticationError, Count: 1,
				Category: rabbitstream.CategoryAuthentication,
			})
			return nil, rabbitstream.ErrAuthentication
		}
		if credentials.Username == "" || len(credentials.Password) == 0 ||
			len(credentials.Username) > 255 || len(credentials.Password) > 4096 {
			return nil, rabbitstream.ErrInvalidConfiguration
		}
		endpoint := connection.Endpoints[attempt%len(connection.Endpoints)]
		remaining := time.Until(operationDeadline(operationCtx, connection.ConnectTimeout))
		attemptRPCTimeout := boundedAttemptTimeout(
			remaining, connection.MaxReconnectAttempts, attempt, connection.RPCTimeout,
		)
		if attemptRPCTimeout <= 0 {
			if err := operationCtx.Err(); err != nil {
				return nil, err
			}
			return nil, context.DeadlineExceeded
		}
		result := make(chan struct {
			environment producerEnvironment
			err         error
		}, 1)
		go func() {
			environment, openErr := opener(environmentOptions(connection, endpoint, credentials, attemptRPCTimeout))
			result <- struct {
				environment producerEnvironment
				err         error
			}{environment: environment, err: openErr}
		}()
		select {
		case opened := <-result:
			if opened.err == nil {
				safeObserve(connection.Observer, rabbitstream.Observation{
					Kind: rabbitstream.ObservationConnectionReady, Count: 1,
				})
				return opened.environment, nil
			}
			lastErr = classifyBrokerError(opened.err)
			if brokerErrorCategory(opened.err) == rabbitstream.CategoryAuthentication {
				safeObserve(connection.Observer, rabbitstream.Observation{
					Kind: rabbitstream.ObservationAuthenticationError, Count: 1,
					Category: rabbitstream.CategoryAuthentication,
				})
			}
		case <-operationCtx.Done():
			go func() {
				opened := <-result
				closeLateEnvironment(opened.environment)
			}()
			return nil, operationCtx.Err()
		}
	}
	if lastErr != nil {
		return nil, lastErr
	}
	return nil, rabbitstream.ErrConnection
}

func connectionAttemptNumber(attempt int) uint64 {
	return uint64(attempt + 1)
}

func boundedAttemptTimeout(
	remaining time.Duration,
	maxAttempts int,
	attempt int,
	configured time.Duration,
) time.Duration {
	budget := remaining / time.Duration(maxAttempts-attempt)
	return min(budget, configured)
}

func closeLateEnvironment(environment producerEnvironment) {
	if environment != nil {
		_ = environment.Close()
	}
}

func shouldWaitBeforeConnectionAttempt(attempt int) bool {
	return attempt > 0
}

func nextReconnectBackoff(current time.Duration, maximum time.Duration) time.Duration {
	return min(current*2, maximum)
}

func stopAndDrainTimer(timer *time.Timer) {
	drainStoppedTimer(timer.Stop(), timer.C)
}

func drainStoppedTimer(stopped bool, timerChannel <-chan time.Time) {
	if !stopped {
		select {
		case <-timerChannel:
		default:
		}
	}
}

func classifyBrokerError(err error) error {
	category := brokerErrorCategory(err)
	return &rabbitstream.OperationError{
		Operation: rabbitstream.OperationConnect,
		Category:  category,
		Cause:     err,
	}
}

func brokerErrorCategory(err error) rabbitstream.ErrorCategory {
	switch {
	case errors.Is(err, rabbitstream.ErrAuthentication),
		errors.Is(err, stream.AuthenticationFailure),
		errors.Is(err, stream.AuthenticationFailureLoopbackError):
		return rabbitstream.CategoryAuthentication
	case errors.Is(err, rabbitstream.ErrAuthorization),
		errors.Is(err, stream.VirtualHostAccessFailure),
		errors.Is(err, stream.CodeAccessRefused):
		return rabbitstream.CategoryAuthorization
	case errors.Is(err, rabbitstream.ErrInvalidConfiguration):
		return rabbitstream.CategoryInvalidConfiguration
	case errors.Is(err, rabbitstream.ErrStreamUnavailable),
		errors.Is(err, stream.StreamDoesNotExist),
		errors.Is(err, stream.StreamNotAvailable):
		return rabbitstream.CategoryStreamUnavailable
	case errors.Is(err, rabbitstream.ErrMessageTooLarge), errors.Is(err, stream.FrameTooLarge):
		return rabbitstream.CategoryMessageTooLarge
	case errors.Is(err, context.Canceled):
		return rabbitstream.CategoryCanceled
	case errors.Is(err, context.DeadlineExceeded):
		return rabbitstream.CategoryTimeout
	default:
		return rabbitstream.CategoryConnection
	}
}

func environmentOptions(
	connection rabbitstream.ConnectionConfig,
	endpoint rabbitstream.Endpoint,
	credentials rabbitstream.Credentials,
	rpcTimeout time.Duration,
) *stream.EnvironmentOptions {
	options := stream.NewEnvironmentOptions().
		SetHost(endpoint.Host).
		SetPort(int(endpoint.Port)).
		SetUser(credentials.Username).
		SetPassword(string(credentials.Password)).
		SetRequestedHeartbeat(connection.Heartbeat).
		SetRPCTimeout(rpcTimeout)
	if connection.VirtualHost != "" {
		options = options.SetVHost(connection.VirtualHost)
	}
	if connection.Security.Mode == rabbitstream.SecurityTLS {
		tlsConfig := connection.Security.TLS.Clone()
		if tlsConfig.ServerName == "" {
			tlsConfig.ServerName = endpoint.Host
		}
		options = options.IsTLS(true).SetTLSConfig(tlsConfig)
	}
	return options
}

func operationDeadline(ctx context.Context, fallback time.Duration) time.Time {
	if deadline, ok := ctx.Deadline(); ok {
		return deadline
	}
	return time.Now().Add(fallback)
}
