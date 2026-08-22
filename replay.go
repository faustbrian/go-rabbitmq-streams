package rabbitstream

import (
	"context"
	"errors"
	"io"
)

// RetainedRange is an exact snapshot of offsets available when replay opens.
// Implementations must not substitute committed chunk IDs or estimates for
// LastOffset. Empty distinguishes an empty stream from offset zero.
type RetainedRange struct {
	// FirstOffset is the earliest retained message offset when Empty is false.
	FirstOffset uint64
	// LastOffset is the latest retained message offset when Empty is false.
	LastOffset uint64
	// Empty distinguishes no retained messages from a range beginning at zero.
	Empty bool
}

// ReplayRequest identifies an isolated replay. Super Stream replay is always
// partition-specific because no global cross-partition order exists.
type ReplayRequest struct {
	// Stream selects one direct stream when SuperStream is empty.
	Stream string
	// SuperStream selects a logical partitioned stream when Stream is empty.
	SuperStream string
	// Partition is the single backing stream replayed for a Super Stream.
	Partition string
	// ExpectedPartitions is the ordered Super Stream topology the caller
	// approved for this replay. Super Stream replay rejects a different live
	// topology because routing and ordering assumptions may have changed.
	ExpectedPartitions []string
	// Start selects the first requested retained message.
	Start StartPosition
	// EndOffset optionally requires an inclusive exact terminal offset.
	EndOffset *uint64
	// Checkpoint optionally selects a caller-owned exact replay start. Callers
	// storing the last completed offset must advance it before reuse.
	Checkpoint *uint64
	// AllowSideEffects makes application side-effect authority explicit to handlers.
	AllowSideEffects bool
}

// ReplayDelivery makes the caller's explicit side-effect policy visible to the
// handler. Replayers never mutate a live consumer's stored offset.
type ReplayDelivery struct {
	// Message is the retained delivery in partition offset order.
	Message Message
	// SideEffectsAllowed repeats the caller's explicit replay authority.
	SideEffectsAllowed bool
}

// ReplayHandler processes one retained message in partition offset order.
type ReplayHandler func(context.Context, ReplayDelivery) error

// ReplayCursor is an isolated, non-offset-storing retained-message cursor.
type ReplayCursor interface {
	// Next returns the next retained message or io.EOF after the requested range.
	Next(context.Context) (Message, error)
	// Close cancels cursor work and releases all owned resources.
	Close() error
}

// ReplaySource supplies exact retained-range inspection and isolated cursors.
type ReplaySource interface {
	// RetainedRange returns exact currently available offsets for the request.
	RetainedRange(context.Context, ReplayRequest) (RetainedRange, error)
	// Open creates an isolated cursor that never stores live-consumer progress.
	Open(context.Context, ReplayRequest) (ReplayCursor, error)
}

// Replayer validates exact retention boundaries before invoking application
// code and never owns or advances normal consumer progress.
type Replayer struct {
	limits   Limits
	source   ReplaySource
	observer Observer
}

// NewReplayer validates finite message bounds and a replay source.
func NewReplayer(limits Limits, source ReplaySource, observer Observer) (*Replayer, error) {
	if err := limits.validate(); err != nil {
		return nil, invalidConfiguration(err)
	}
	if source == nil {
		return nil, invalidConfiguration(errors.New("replay source is required"))
	}
	return &Replayer{limits: limits, source: source, observer: observer}, nil
}

// Inspect returns the exact currently retained range without opening a cursor.
func (replayer *Replayer) Inspect(ctx context.Context, request ReplayRequest) (RetainedRange, error) {
	request = request.owned()
	if err := replayer.validateRequest(ctx, request); err != nil {
		return RetainedRange{}, err
	}
	retained, err := replayer.source.RetainedRange(ctx, request)
	if err != nil {
		return RetainedRange{}, &OperationError{
			Operation: OperationInspect, Category: categoryForError(err, CategoryConnection), Cause: err,
		}
	}
	if !retained.Empty && retained.FirstOffset > retained.LastOffset {
		return RetainedRange{}, &OperationError{
			Operation: OperationInspect, Category: CategoryReplayRange,
		}
	}
	return retained, nil
}

// Run replays an exact retained range. Explicit missing starts are retention
// gaps; missing requested ends are incomplete replay ranges.
func (replayer *Replayer) Run(
	ctx context.Context,
	request ReplayRequest,
	handler ReplayHandler,
) (runErr error) {
	request = request.owned()
	if handler == nil {
		return &OperationError{Operation: OperationReplay, Category: CategoryValidation}
	}
	retained, err := replayer.Inspect(ctx, request)
	if err != nil {
		return err
	}
	startOffset, hasExactStart := requestedStartOffset(request)
	if retained.Empty {
		if request.EndOffset != nil || hasExactStart {
			return &OperationError{Operation: OperationReplay, Category: CategoryRetentionGap}
		}
		return nil
	}
	if hasExactStart && startOffset < retained.FirstOffset {
		return &OperationError{Operation: OperationReplay, Category: CategoryRetentionGap}
	}
	if request.Start.Kind == OffsetStartBeginning {
		startOffset = retained.FirstOffset
		hasExactStart = true
	}
	if hasExactStart && startOffset > retained.LastOffset {
		return &OperationError{Operation: OperationReplay, Category: CategoryReplayRange}
	}
	if request.EndOffset != nil && *request.EndOffset > retained.LastOffset {
		return &OperationError{Operation: OperationReplay, Category: CategoryReplayRange}
	}
	effectiveEnd := retained.LastOffset
	if request.EndOffset != nil {
		effectiveEnd = *request.EndOffset
	}
	openRequest := request
	if request.Start.Kind == OffsetStartBeginning {
		openRequest.Start = StartPosition{Kind: OffsetStartExplicit, Offset: startOffset}
	}
	openRequest.EndOffset = &effectiveEnd

	cursor, err := replayer.source.Open(ctx, openRequest)
	if err != nil {
		return &OperationError{
			Operation: OperationReplay, Category: categoryForError(err, CategoryConnection), Cause: err,
		}
	}
	defer func() {
		if closeErr := cursor.Close(); closeErr != nil && runErr == nil {
			runErr = &OperationError{
				Operation: OperationReplay,
				Category:  categoryForError(closeErr, CategoryConnection),
				Cause:     closeErr,
			}
		}
	}()

	seen := false
	for {
		message, nextErr := cursor.Next(ctx)
		if nextErr != nil {
			if errors.Is(nextErr, io.EOF) {
				return &OperationError{Operation: OperationReplay, Category: CategoryReplayRange}
			}
			if ctx.Err() != nil {
				return &OperationError{Operation: OperationReplay, Category: CategoryCanceled, Cause: ctx.Err()}
			}
			return &OperationError{
				Operation: OperationReplay,
				Category:  categoryForError(nextErr, CategoryConnection),
				Cause:     nextErr,
			}
		}
		if !message.HasOffset || message.Partition == "" || message.Partition != message.Stream {
			return &OperationError{Operation: OperationReplay, Category: CategoryValidation}
		}
		if !seen && hasExactStart && message.Offset > startOffset {
			return &OperationError{Operation: OperationReplay, Category: CategoryRetentionGap}
		}
		if hasExactStart && message.Offset < startOffset {
			continue
		}
		if message.Offset > effectiveEnd {
			return &OperationError{Operation: OperationReplay, Category: CategoryReplayRange}
		}
		if err := callReplayHandler(ctx, handler, ReplayDelivery{
			Message: message.Retain(), SideEffectsAllowed: request.AllowSideEffects,
		}); err != nil {
			if ctx.Err() != nil {
				return &OperationError{Operation: OperationReplay, Category: CategoryCanceled, Cause: ctx.Err()}
			}
			return &OperationError{Operation: OperationReplay, Category: CategoryHandler, Cause: err}
		}
		observe(replayer.observer, Observation{
			Kind: ObservationReplayProgress, Count: 1, Value: message.Offset,
		})
		seen = true
		if message.Offset == effectiveEnd {
			return nil
		}
	}
}

func (replayer *Replayer) validateRequest(ctx context.Context, request ReplayRequest) error {
	if ctx == nil {
		return &OperationError{Operation: OperationReplay, Category: CategoryValidation}
	}
	if err := ctx.Err(); err != nil {
		return &OperationError{Operation: OperationReplay, Category: CategoryCanceled, Cause: err}
	}
	if (request.Stream == "") == (request.SuperStream == "") ||
		(request.Stream != "" && request.Partition != "") ||
		(request.SuperStream != "" && request.Partition == "") ||
		(request.Stream != "" && len(request.ExpectedPartitions) != 0) ||
		(request.SuperStream != "" && len(request.ExpectedPartitions) == 0) ||
		len(request.ExpectedPartitions) > MaxSuperStreamPartitions ||
		(request.Stream != "" && invalidIdentifier(request.Stream, replayer.limits.MaxStreamNameBytes)) ||
		(request.SuperStream != "" && invalidIdentifier(request.SuperStream, replayer.limits.MaxStreamNameBytes)) ||
		(request.Partition != "" && invalidIdentifier(request.Partition, replayer.limits.MaxStreamNameBytes)) {
		return &OperationError{Operation: OperationReplay, Category: CategoryValidation}
	}
	if request.SuperStream != "" {
		seen := make(map[string]struct{}, len(request.ExpectedPartitions))
		partitionFound := false
		for _, partition := range request.ExpectedPartitions {
			if invalidIdentifier(partition, replayer.limits.MaxStreamNameBytes) {
				return &OperationError{Operation: OperationReplay, Category: CategoryValidation}
			}
			if _, duplicate := seen[partition]; duplicate {
				return &OperationError{Operation: OperationReplay, Category: CategoryValidation}
			}
			seen[partition] = struct{}{}
			partitionFound = partitionFound || partition == request.Partition
		}
		if !partitionFound {
			return &OperationError{Operation: OperationReplay, Category: CategoryValidation}
		}
	}
	if request.Start.Kind != OffsetStartBeginning &&
		request.Start.Kind != OffsetStartExplicit &&
		request.Start.Kind != OffsetStartTimestamp {
		return &OperationError{Operation: OperationReplay, Category: CategoryValidation}
	}
	if request.Start.Kind == OffsetStartTimestamp && request.Start.Timestamp.IsZero() {
		return &OperationError{Operation: OperationReplay, Category: CategoryValidation}
	}
	start, exact := requestedStartOffset(request)
	if exact && request.EndOffset != nil && *request.EndOffset < start {
		return &OperationError{Operation: OperationReplay, Category: CategoryReplayRange}
	}
	return nil
}

func (request ReplayRequest) owned() ReplayRequest {
	request.ExpectedPartitions = append([]string(nil), request.ExpectedPartitions...)
	return request
}

func requestedStartOffset(request ReplayRequest) (uint64, bool) {
	start := request.Start.Offset
	exact := request.Start.Kind == OffsetStartExplicit
	if request.Checkpoint != nil {
		if exact {
			start = max(start, *request.Checkpoint)
		} else {
			start = *request.Checkpoint
		}
		exact = true
	}
	return start, exact
}

func callReplayHandler(ctx context.Context, handler ReplayHandler, delivery ReplayDelivery) (err error) {
	defer func() {
		if recover() != nil {
			err = errors.New("replay handler panicked")
		}
	}()
	return handler(ctx, delivery)
}
