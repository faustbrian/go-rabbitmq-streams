package rabbitstream

import (
	"context"
	"errors"
	"io"
	"testing"
)

func TestReplayRejectsAnOffsetThatRetentionAlreadyRemoved(t *testing.T) {
	t.Parallel()

	source := &fakeReplaySource{retained: RetainedRange{FirstOffset: 100, LastOffset: 200}}
	replayer, err := NewReplayer(DefaultLimits(), source, nil)
	if err != nil {
		t.Fatalf("NewReplayer() error = %v", err)
	}
	err = replayer.Run(context.Background(), ReplayRequest{
		Stream: "tracking.events", Start: StartPosition{Kind: OffsetStartExplicit, Offset: 99},
	}, func(context.Context, ReplayDelivery) error { return nil })
	if !errors.Is(err, ErrRetentionGap) || source.opened {
		t.Fatalf("Run() = %v; source opened = %t", err, source.opened)
	}
}

func TestReplayRequiresAndOwnsExpectedSuperStreamTopology(t *testing.T) {
	t.Parallel()

	invalid := []ReplayRequest{
		{
			SuperStream: "tracking", Partition: "tracking-0",
			Start: StartPosition{Kind: OffsetStartBeginning},
		},
		{
			SuperStream: "tracking", Partition: "tracking-2",
			ExpectedPartitions: []string{"tracking-0", "tracking-1"},
			Start:              StartPosition{Kind: OffsetStartBeginning},
		},
		{
			SuperStream: "tracking", Partition: "tracking-0",
			ExpectedPartitions: []string{"tracking-0", "tracking-0"},
			Start:              StartPosition{Kind: OffsetStartBeginning},
		},
		{
			Stream: "tracking.events", ExpectedPartitions: []string{"tracking.events"},
			Start: StartPosition{Kind: OffsetStartBeginning},
		},
	}
	for _, request := range invalid {
		source := &fakeReplaySource{retained: RetainedRange{Empty: true}}
		replayer, err := NewReplayer(DefaultLimits(), source, nil)
		if err != nil {
			t.Fatalf("NewReplayer() error = %v", err)
		}
		if _, err := replayer.Inspect(context.Background(), request); !errors.Is(err, ErrValidation) {
			t.Fatalf("Inspect(%#v) error = %v", request, err)
		}
	}

	partitions := []string{"tracking-0", "tracking-1"}
	source := &fakeReplaySource{retained: RetainedRange{Empty: true}}
	replayer, err := NewReplayer(DefaultLimits(), source, nil)
	if err != nil {
		t.Fatalf("NewReplayer() error = %v", err)
	}
	_, err = replayer.Inspect(context.Background(), ReplayRequest{
		SuperStream: "tracking", Partition: "tracking-0", ExpectedPartitions: partitions,
		Start: StartPosition{Kind: OffsetStartBeginning},
	})
	if err != nil {
		t.Fatalf("Inspect() error = %v", err)
	}
	partitions[0] = "mutated"
	if source.lastRequest.ExpectedPartitions[0] != "tracking-0" {
		t.Fatalf("source topology changed with caller slice: %#v", source.lastRequest.ExpectedPartitions)
	}
}

func TestReplayProcessesAnExactBoundedRangeWithoutConsumerOffsetMutation(t *testing.T) {
	t.Parallel()

	source := &fakeReplaySource{
		retained: RetainedRange{FirstOffset: 40, LastOffset: 50},
		messages: []Message{
			{Stream: "tracking.events", Partition: "tracking.events", Offset: 41, HasOffset: true},
			{Stream: "tracking.events", Partition: "tracking.events", Offset: 42, HasOffset: true},
		},
	}
	replayer, err := NewReplayer(DefaultLimits(), source, nil)
	if err != nil {
		t.Fatalf("NewReplayer() error = %v", err)
	}
	end := uint64(42)
	var offsets []uint64
	err = replayer.Run(context.Background(), ReplayRequest{
		Stream: "tracking.events", Start: StartPosition{Kind: OffsetStartExplicit, Offset: 41},
		EndOffset: &end, AllowSideEffects: true,
	}, func(_ context.Context, delivery ReplayDelivery) error {
		if !delivery.SideEffectsAllowed {
			t.Fatal("side-effect opt-in was not propagated")
		}
		offsets = append(offsets, delivery.Message.Offset)
		return nil
	})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if len(offsets) != 2 || offsets[0] != 41 || offsets[1] != 42 || !source.closed {
		t.Fatalf("replay offsets = %#v; closed = %t", offsets, source.closed)
	}
	if source.lastRequest.Start.Kind != OffsetStartExplicit || source.lastRequest.Start.Offset != 41 ||
		source.lastRequest.EndOffset == nil || *source.lastRequest.EndOffset != 42 {
		t.Fatalf("opened request = %#v", source.lastRequest)
	}
}

func TestReplayBeginningOpensAtFirstRetainedOffsetThroughExactLastOffset(t *testing.T) {
	t.Parallel()

	source := &fakeReplaySource{
		retained: RetainedRange{FirstOffset: 4, LastOffset: 4},
		messages: []Message{{Stream: "stream", Partition: "stream", Offset: 4, HasOffset: true}},
	}
	replayer, err := NewReplayer(DefaultLimits(), source, nil)
	if err != nil {
		t.Fatalf("NewReplayer() error = %v", err)
	}
	if err := replayer.Run(context.Background(), ReplayRequest{
		Stream: "stream", Start: StartPosition{Kind: OffsetStartBeginning},
	}, func(context.Context, ReplayDelivery) error { return nil }); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if source.lastRequest.Start != (StartPosition{Kind: OffsetStartExplicit, Offset: 4}) ||
		source.lastRequest.EndOffset == nil || *source.lastRequest.EndOffset != 4 {
		t.Fatalf("opened request = %#v", source.lastRequest)
	}
}

func TestReplayAcceptsAnExplicitEndAtTheExactRetainedLastOffset(t *testing.T) {
	t.Parallel()

	end := uint64(4)
	source := &fakeReplaySource{
		retained: RetainedRange{FirstOffset: 4, LastOffset: 4},
		messages: []Message{{Stream: "stream", Partition: "stream", Offset: 4, HasOffset: true}},
	}
	replayer, _ := NewReplayer(DefaultLimits(), source, nil)
	if err := replayer.Run(context.Background(), ReplayRequest{
		Stream: "stream", Start: StartPosition{Kind: OffsetStartExplicit, Offset: 4}, EndOffset: &end,
	}, func(context.Context, ReplayDelivery) error { return nil }); err != nil {
		t.Fatalf("Run() exact retained end error = %v", err)
	}
}

func TestReplayMessageIdentityChecksAreIndependent(t *testing.T) {
	t.Parallel()

	valid := Message{Stream: "stream", Partition: "stream", Offset: 1, HasOffset: true}
	for name, mutate := range map[string]func(*Message){
		"missing offset":     func(message *Message) { message.HasOffset = false },
		"missing partition":  func(message *Message) { message.Partition = "" },
		"partition mismatch": func(message *Message) { message.Partition = "other" },
	} {
		t.Run(name, func(t *testing.T) {
			message := valid
			mutate(&message)
			source := &fakeReplaySource{retained: RetainedRange{FirstOffset: 1, LastOffset: 1}, messages: []Message{message}}
			replayer, _ := NewReplayer(DefaultLimits(), source, nil)
			if err := replayer.Run(context.Background(), ReplayRequest{
				Stream: "stream", Start: StartPosition{Kind: OffsetStartBeginning},
			}, func(context.Context, ReplayDelivery) error { return nil }); !errors.Is(err, ErrValidation) {
				t.Fatalf("Run() error = %v", err)
			}
		})
	}
}

func TestReplayRequestTargetAndRangeBoundariesAreIndependent(t *testing.T) {
	t.Parallel()

	limits := DefaultLimits()
	replayer, _ := NewReplayer(limits, &fakeReplaySource{retained: RetainedRange{Empty: true}}, nil)
	end := uint64(1)
	validEqualRange := ReplayRequest{
		Stream: "stream", Start: StartPosition{Kind: OffsetStartExplicit, Offset: 1}, EndOffset: &end,
	}
	if _, err := replayer.Inspect(context.Background(), validEqualRange); err != nil {
		t.Fatalf("Inspect(equal range) error = %v", err)
	}
	invalid := []ReplayRequest{
		{Start: StartPosition{Kind: OffsetStartBeginning}},
		{Stream: "stream", SuperStream: "super", Start: StartPosition{Kind: OffsetStartBeginning}},
		{Stream: "stream", Partition: "partition", Start: StartPosition{Kind: OffsetStartBeginning}},
	}
	for _, request := range invalid {
		if _, err := replayer.Inspect(context.Background(), request); !errors.Is(err, ErrValidation) {
			t.Fatalf("Inspect(%#v) error = %v", request, err)
		}
	}
	partitions := make([]string, MaxSuperStreamPartitions)
	for index := range partitions {
		partitions[index] = "p" + string(rune(0x100+index))
	}
	maximum := ReplayRequest{
		SuperStream: "super", Partition: partitions[0], ExpectedPartitions: partitions,
		Start: StartPosition{Kind: OffsetStartBeginning},
	}
	if _, err := replayer.Inspect(context.Background(), maximum); err != nil {
		t.Fatalf("Inspect(maximum partitions) error = %v", err)
	}
	maximum.ExpectedPartitions = append(maximum.ExpectedPartitions, "excess")
	if _, err := replayer.Inspect(context.Background(), maximum); !errors.Is(err, ErrValidation) {
		t.Fatalf("Inspect(excess partitions) error = %v", err)
	}
}

func TestReplayCheckpointSelectsOnlyTheLaterExactStart(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name       string
		start      StartPosition
		checkpoint uint64
		want       uint64
	}{
		{name: "checkpoint after explicit", start: StartPosition{Kind: OffsetStartExplicit, Offset: 5}, checkpoint: 6, want: 6},
		{name: "checkpoint equal explicit", start: StartPosition{Kind: OffsetStartExplicit, Offset: 5}, checkpoint: 5, want: 5},
		{name: "checkpoint before explicit", start: StartPosition{Kind: OffsetStartExplicit, Offset: 5}, checkpoint: 4, want: 5},
		{name: "checkpoint supplies timestamp offset", start: StartPosition{Kind: OffsetStartTimestamp}, checkpoint: 7, want: 7},
	} {
		t.Run(test.name, func(t *testing.T) {
			request := ReplayRequest{Start: test.start, Checkpoint: &test.checkpoint}
			if got, exact := requestedStartOffset(request); !exact || got != test.want {
				t.Fatalf("requestedStartOffset() = %d, %t; want %d, true", got, exact, test.want)
			}
		})
	}
}

func TestReplayReportsBoundedProgressAfterEachSuccessfulHandler(t *testing.T) {
	t.Parallel()

	source := &fakeReplaySource{
		retained: RetainedRange{FirstOffset: 40, LastOffset: 41},
		messages: []Message{
			{Stream: "tracking.events", Partition: "tracking.events", Offset: 40, HasOffset: true},
			{Stream: "tracking.events", Partition: "tracking.events", Offset: 41, HasOffset: true},
		},
	}
	observer := &recordingObserver{}
	replayer, err := NewReplayer(DefaultLimits(), source, observer)
	if err != nil {
		t.Fatalf("NewReplayer() error = %v", err)
	}
	if err := replayer.Run(context.Background(), ReplayRequest{
		Stream: "tracking.events", Start: StartPosition{Kind: OffsetStartBeginning},
	}, func(context.Context, ReplayDelivery) error { return nil }); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if len(observer.observations) != 2 ||
		observer.observations[0] != (Observation{Kind: ObservationReplayProgress, Count: 1, Value: 40}) ||
		observer.observations[1] != (Observation{Kind: ObservationReplayProgress, Count: 1, Value: 41}) {
		t.Fatalf("progress observations = %#v", observer.observations)
	}
}

func TestReplayFailsWhenTheRequestedRangeIsIncomplete(t *testing.T) {
	t.Parallel()

	source := &fakeReplaySource{
		retained: RetainedRange{FirstOffset: 40, LastOffset: 50},
		messages: []Message{{
			Stream: "tracking.events", Partition: "tracking.events", Offset: 41, HasOffset: true,
		}},
	}
	replayer, err := NewReplayer(DefaultLimits(), source, nil)
	if err != nil {
		t.Fatalf("NewReplayer() error = %v", err)
	}
	end := uint64(42)
	err = replayer.Run(context.Background(), ReplayRequest{
		Stream: "tracking.events", Start: StartPosition{Kind: OffsetStartExplicit, Offset: 41},
		EndOffset: &end,
	}, func(context.Context, ReplayDelivery) error { return nil })
	if !errors.Is(err, ErrReplayRange) {
		t.Fatalf("Run() error = %v", err)
	}
}

func TestReplayPreservesStableSourceSecurityCategories(t *testing.T) {
	t.Parallel()

	t.Run("inspection authentication", func(t *testing.T) {
		source := &fakeReplaySource{retainedErr: ErrAuthentication}
		replayer, err := NewReplayer(DefaultLimits(), source, nil)
		if err != nil {
			t.Fatalf("NewReplayer() error = %v", err)
		}
		_, err = replayer.Inspect(context.Background(), ReplayRequest{
			Stream: "tracking.events", Start: StartPosition{Kind: OffsetStartBeginning},
		})
		var operationError *OperationError
		if !errors.Is(err, ErrAuthentication) || !errors.As(err, &operationError) ||
			operationError.Category != CategoryAuthentication {
			t.Fatalf("Inspect() error = %v", err)
		}
	})

	t.Run("cursor authorization", func(t *testing.T) {
		source := &fakeReplaySource{
			retained: RetainedRange{FirstOffset: 40, LastOffset: 41},
			openErr:  ErrAuthorization,
		}
		replayer, err := NewReplayer(DefaultLimits(), source, nil)
		if err != nil {
			t.Fatalf("NewReplayer() error = %v", err)
		}
		err = replayer.Run(context.Background(), ReplayRequest{
			Stream: "tracking.events", Start: StartPosition{Kind: OffsetStartBeginning},
		}, func(context.Context, ReplayDelivery) error { return nil })
		var operationError *OperationError
		if !errors.Is(err, ErrAuthorization) || !errors.As(err, &operationError) ||
			operationError.Category != CategoryAuthorization {
			t.Fatalf("Run() error = %v", err)
		}
	})
}

type fakeReplaySource struct {
	retained    RetainedRange
	messages    []Message
	opened      bool
	closed      bool
	retainedErr error
	openErr     error
	nextErr     error
	nextHook    func()
	closeErr    error
	lastRequest ReplayRequest
}

func (source *fakeReplaySource) RetainedRange(_ context.Context, request ReplayRequest) (RetainedRange, error) {
	source.lastRequest = request
	return source.retained, source.retainedErr
}

func (source *fakeReplaySource) Open(_ context.Context, request ReplayRequest) (ReplayCursor, error) {
	source.opened = true
	source.lastRequest = request
	if source.openErr != nil {
		return nil, source.openErr
	}
	return &fakeReplayCursor{source: source}, nil
}

type fakeReplayCursor struct {
	source *fakeReplaySource
	index  int
}

type recordingObserver struct {
	observations []Observation
}

func (observer *recordingObserver) Observe(observation Observation) {
	observer.observations = append(observer.observations, observation)
}

func (cursor *fakeReplayCursor) Next(context.Context) (Message, error) {
	if cursor.source.nextHook != nil {
		cursor.source.nextHook()
	}
	if cursor.source.nextErr != nil {
		return Message{}, cursor.source.nextErr
	}
	if cursor.index >= len(cursor.source.messages) {
		return Message{}, io.EOF
	}
	message := cursor.source.messages[cursor.index]
	cursor.index++
	return message, nil
}

func (cursor *fakeReplayCursor) Close() error {
	cursor.source.closed = true
	return cursor.source.closeErr
}
