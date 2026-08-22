package rabbitmq

import (
	"context"
	"errors"
	"time"

	"github.com/faustbrian/golib/pkg/rabbitstream"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"
)

// Inspector uses a fresh bounded connection for every read-only request so a
// stale locator cannot enter the upstream client's unbounded reconnect loop.
type Inspector struct {
	connection      rabbitstream.ConnectionConfig
	limits          rabbitstream.Limits
	openEnvironment func(context.Context) (rabbitEnvironment, error)
}

// NewInspector validates read-only inspection policy without connecting.
func NewInspector(
	connection rabbitstream.ConnectionConfig,
	limits rabbitstream.Limits,
) (*Inspector, error) {
	normalized, err := connection.Normalized()
	if err != nil {
		return nil, err
	}
	if limits == (rabbitstream.Limits{}) {
		limits = rabbitstream.DefaultLimits()
	}
	if err := (rabbitstream.InspectionRequest{Stream: "x"}).Validate(limits); err != nil {
		return nil, err
	}
	return &Inspector{
		connection: normalized,
		limits:     limits,
		openEnvironment: func(ctx context.Context) (rabbitEnvironment, error) {
			return openFreshEnvironment(ctx, normalized)
		},
	}, nil
}

// Inspect returns bounded topology, retained-start, committed-chunk, and
// optional stored-offset facts without mutating broker state.
func (inspector *Inspector) Inspect(
	ctx context.Context,
	request rabbitstream.InspectionRequest,
) (rabbitstream.InspectionResult, error) {
	if ctx == nil {
		return rabbitstream.InspectionResult{}, &rabbitstream.OperationError{
			Operation: rabbitstream.OperationInspect,
			Category:  rabbitstream.CategoryInvalidConfiguration,
		}
	}
	if err := request.Validate(inspector.limits); err != nil {
		return rabbitstream.InspectionResult{}, err
	}
	environment, err := inspector.openEnvironment(ctx)
	if err != nil {
		return rabbitstream.InspectionResult{}, inspectError(err)
	}
	defer func() { _ = environment.Close() }()
	targets := []string{request.Stream}
	if request.SuperStream != "" {
		targets, err = environment.QueryPartitions(request.SuperStream)
		if err != nil {
			return rabbitstream.InspectionResult{}, inspectError(err)
		}
		if !validSuperStreamPartitions(targets, inspector.limits) {
			return rabbitstream.InspectionResult{}, &rabbitstream.OperationError{
				Operation: rabbitstream.OperationInspect,
				Category:  rabbitstream.CategoryPartitionUnavailable,
			}
		}
	}
	result := rabbitstream.InspectionResult{
		SuperStream: request.SuperStream,
		Partitions:  make([]rabbitstream.StreamInspection, 0, len(targets)),
		ObservedAt:  time.Now().UTC(),
	}
	for _, target := range targets {
		partition, err := inspectStream(ctx, environment, target, request.ConsumerName)
		if err != nil {
			return rabbitstream.InspectionResult{}, inspectError(err)
		}
		result.Partitions = append(result.Partitions, partition)
		if partition.LastOffset != nil {
			safeObserve(inspector.connection.Observer, rabbitstream.Observation{
				Kind: rabbitstream.ObservationStreamEndOffset, Count: 1, Value: *partition.LastOffset,
			})
		}
		if partition.Lag != nil {
			safeObserve(inspector.connection.Observer, rabbitstream.Observation{
				Kind: rabbitstream.ObservationConsumerLag, Count: 1, Value: *partition.Lag,
			})
		}
	}
	return result, nil
}

// StoredOffset returns the broker-stored consumer offset without opening a
// temporary end-of-stream consumer or inspecting retained-range metadata.
// A nil offset means that the consumer has not stored a position.
func (inspector *Inspector) StoredOffset(
	ctx context.Context,
	streamName string,
	consumerName string,
) (*uint64, error) {
	if ctx == nil {
		return nil, &rabbitstream.OperationError{
			Operation: rabbitstream.OperationInspect,
			Category:  rabbitstream.CategoryInvalidConfiguration,
		}
	}
	request := rabbitstream.InspectionRequest{
		Stream: streamName, ConsumerName: consumerName,
	}
	if err := request.Validate(inspector.limits); err != nil {
		return nil, err
	}
	environment, err := inspector.openEnvironment(ctx)
	if err != nil {
		return nil, inspectError(err)
	}
	defer func() { _ = environment.Close() }()
	stored, err := environment.QueryOffset(consumerName, streamName)
	if errors.Is(err, stream.OffsetNotFoundError) {
		return nil, nil
	}
	if err != nil || stored < 0 {
		if err == nil {
			err = rabbitstream.ErrOffset
		}
		return nil, inspectError(err)
	}
	offset := uint64(stored)
	return &offset, nil
}

func inspectStream(
	ctx context.Context,
	environment rabbitEnvironment,
	target string,
	consumerName string,
) (rabbitstream.StreamInspection, error) {
	inspection := rabbitstream.StreamInspection{Stream: target}
	exists, err := environment.StreamExists(target)
	if err != nil || !exists {
		inspection.Exists = exists
		return inspection, err
	}
	inspection.Exists = true
	stats, err := environment.StreamStats(target)
	if err != nil {
		return rabbitstream.StreamInspection{}, err
	}
	return inspectStreamStats(ctx, environment, inspection, consumerName, stats)
}

type streamStatistics interface {
	// FirstOffset returns the earliest retained offset.
	FirstOffset() (int64, error)
	// CommittedChunkId returns broker chunk metadata, not an exact end offset.
	CommittedChunkId() (int64, error)
}

func inspectStreamStats(
	ctx context.Context,
	environment rabbitEnvironment,
	inspection rabbitstream.StreamInspection,
	consumerName string,
	stats streamStatistics,
) (rabbitstream.StreamInspection, error) {
	target := inspection.Stream
	if first, firstErr := stats.FirstOffset(); firstErr == nil && nonNegativeBrokerOffset(first) {
		value := uint64(first)
		inspection.FirstOffset = &value
		last, lastErr := snapshotLastOffset(ctx, environment, target)
		if lastErr != nil {
			return rabbitstream.StreamInspection{}, lastErr
		}
		inspection.LastOffset = &last
	}
	if committed, committedErr := stats.CommittedChunkId(); committedErr == nil && nonNegativeBrokerOffset(committed) {
		value := uint64(committed)
		inspection.CommittedChunkID = &value
	}
	if consumerName != "" {
		stored, storedErr := environment.QueryOffset(consumerName, target)
		if err := applyStoredOffset(&inspection, stored, storedErr); err != nil {
			return rabbitstream.StreamInspection{}, storedErr
		}
	}
	return inspection, nil
}

func nonNegativeBrokerOffset(offset int64) bool {
	return offset >= 0
}

func applyStoredOffset(inspection *rabbitstream.StreamInspection, stored int64, storedErr error) error {
	if storedErr != nil {
		if errors.Is(storedErr, stream.OffsetNotFoundError) {
			return nil
		}
		return storedErr
	}
	if stored < 0 {
		return nil
	}
	value := uint64(stored)
	inspection.StoredOffset = &value
	if inspection.LastOffset != nil && value <= *inspection.LastOffset {
		lag := *inspection.LastOffset - value
		inspection.Lag = &lag
	}
	return nil
}

// Health checks only RabbitMQ dependency connectivity and authentication. It
// is not process liveness and does not inspect or mutate topology.
func (inspector *Inspector) Health(ctx context.Context) rabbitstream.DependencyHealth {
	health := rabbitstream.DependencyHealth{ObservedAt: time.Now().UTC()}
	if ctx == nil {
		health.State = rabbitstream.DependencyUnavailable
		health.Category = rabbitstream.CategoryInvalidConfiguration
		return health
	}
	environment, err := inspector.openEnvironment(ctx)
	if err == nil {
		_ = environment.Close()
		health.State = rabbitstream.DependencyHealthy
		return health
	}
	health.State = rabbitstream.DependencyUnavailable
	health.Category = brokerErrorCategory(err)
	return health
}

func inspectError(err error) error {
	category := brokerErrorCategory(err)
	return &rabbitstream.OperationError{
		Operation: rabbitstream.OperationInspect, Category: category, Cause: err,
	}
}
