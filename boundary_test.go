package rabbitstream

import (
	"context"
	"crypto/tls"
	"errors"
	"strings"
	"testing"
)

var absentContext context.Context

func TestStableErrorsPreserveOnlySafeRenderedCategories(t *testing.T) {
	t.Parallel()

	if got := (categoryError{category: CategoryFatal}).Error(); got != "fatal" {
		t.Fatalf("category error = %q", got)
	}
	var nilOperation *OperationError
	if nilOperation.Error() != "<nil>" || nilOperation.Unwrap() != nil || nilOperation.Is(ErrFatal) {
		t.Fatal("nil operation error contract changed")
	}
	cause := errors.New("sensitive detail")
	operation := &OperationError{Operation: OperationConsume, Category: CategoryHandler, Cause: cause}
	if operation.Error() != "rabbitstream consume failed: handler" || !errors.Is(operation, ErrHandler) ||
		!errors.Is(operation, cause) || operation.Is(errors.New("handler")) || operation.Is(ErrConnection) {
		t.Fatalf("operation error contract = %v", operation)
	}

	categories := []struct {
		err  error
		want ErrorCategory
	}{
		{ErrInvalidConfiguration, CategoryInvalidConfiguration}, {ErrValidation, CategoryValidation},
		{ErrClosed, CategoryClosed}, {context.Canceled, CategoryCanceled},
		{context.DeadlineExceeded, CategoryTimeout}, {ErrAuthentication, CategoryAuthentication},
		{ErrAuthorization, CategoryAuthorization}, {ErrStreamUnavailable, CategoryStreamUnavailable},
		{ErrPartitionUnavailable, CategoryPartitionUnavailable}, {ErrBrokerRejected, CategoryBrokerRejected},
		{ErrMessageTooLarge, CategoryMessageTooLarge}, {ErrPublishAmbiguous, CategoryPublishAmbiguous},
		{ErrConfirmation, CategoryConfirmation}, {ErrRetentionGap, CategoryRetentionGap},
		{ErrReplayRange, CategoryReplayRange}, {ErrOffset, CategoryOffset},
		{ErrHandler, CategoryHandler}, {ErrFatal, CategoryFatal}, {ErrConnection, CategoryConnection},
		{errors.New("unclassified"), CategoryTimeout},
	}
	for _, test := range categories {
		if got := categoryForError(test.err, CategoryTimeout); got != test.want {
			t.Fatalf("categoryForError(%v) = %q, want %q", test.err, got, test.want)
		}
	}
}

func TestConfigurationAndMessageBoundariesRejectEveryUnboundedDimension(t *testing.T) {
	t.Parallel()

	provider := StaticCredentials("track", []byte("secret"))
	if _, err := provider.Credentials(absentContext); !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("nil credential context error = %v", err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := provider.Credentials(canceled); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled credential context error = %v", err)
	}

	baseConnection := ConnectionConfig{
		Endpoints:   []Endpoint{{Host: "rabbitmq.internal", Port: 5551}},
		Credentials: provider,
	}
	invalidConnections := []ConnectionConfig{
		{Endpoints: baseConnection.Endpoints, Credentials: provider, VirtualHost: " bad"},
		{Endpoints: baseConnection.Endpoints, Credentials: provider, Security: SecurityConfig{Mode: SecurityMode(255)}},
		{Endpoints: baseConnection.Endpoints, Credentials: provider, Security: SecurityConfig{TLS: &tlsConfigBelow12}},
	}
	for _, config := range invalidConnections {
		if err := config.Validate(); !errors.Is(err, ErrInvalidConfiguration) {
			t.Fatalf("ConnectionConfig.Validate() = %v", err)
		}
	}

	limits := DefaultLimits()
	invalidLimits := limits
	invalidLimits.MaxBatchBytes = 0
	if err := invalidLimits.validate(); !errors.Is(err, ErrValidation) {
		t.Fatalf("limits.validate() = %v", err)
	}
	messageCases := []Message{
		{},
		{Stream: " bad"},
		{SuperStream: " bad"},
		{Stream: "stream", RoutingKey: " bad"},
		{Stream: "stream", Payload: make([]byte, limits.MaxPayloadBytes+1)},
		{Stream: "stream", Headers: make([]MetadataEntry, limits.MaxMetadataEntries+1)},
		{Stream: "stream", ContentType: strings.Repeat("x", limits.MaxMetadataValueBytes+1)},
		{Stream: "stream", ContentType: strings.Repeat("x", limits.MaxMetadataBytes+1)},
		{Stream: "stream", Headers: []MetadataEntry{{Key: RoutingKeyMetadata}}},
		{Stream: "stream", Headers: []MetadataEntry{{Key: strings.Repeat("x", limits.MaxMetadataKeyBytes+1)}}},
		{Stream: "stream", Headers: []MetadataEntry{{Key: "x", Value: make([]byte, limits.MaxMetadataValueBytes+1)}}},
		{Stream: "stream", Headers: []MetadataEntry{{Key: "x", Value: make([]byte, limits.MaxMetadataBytes)}}},
	}
	for index, message := range messageCases {
		if err := message.Validate(limits); err == nil {
			t.Fatalf("Message.Validate() case %d succeeded", index)
		}
	}
	if err := (Message{Stream: "stream"}).Validate(invalidLimits); err == nil {
		t.Fatal("Message.Validate() accepted invalid limits")
	}
	tightMetadata := limits
	tightMetadata.MaxMetadataBytes = 2
	if err := (Message{Stream: "stream", ContentType: "a", MessageID: "b", CorrelationID: "c"}).Validate(tightMetadata); err == nil {
		t.Fatal("Message.Validate() accepted excessive aggregate standard metadata")
	}
	if err := (Message{Stream: "stream", Headers: []MetadataEntry{{Key: "ab", Value: []byte("c")}}}).Validate(tightMetadata); err == nil {
		t.Fatal("Message.Validate() accepted excessive aggregate entry metadata")
	}
	batchCases := [][]Message{
		nil,
		make([]Message, limits.MaxBatchMessages+1),
		{{}},
		{{Stream: "stream", Payload: []byte("xx")}},
	}
	batchLimits := limits
	batchLimits.MaxBatchBytes = 1
	for index, messages := range batchCases {
		useLimits := limits
		if index == len(batchCases)-1 {
			useLimits = batchLimits
		}
		if err := ValidateBatch(messages, useLimits); err == nil {
			t.Fatalf("ValidateBatch() case %d succeeded", index)
		}
	}
	if err := ValidateBatch([]Message{{Stream: "stream"}}, invalidLimits); err == nil {
		t.Fatal("ValidateBatch() accepted invalid limits")
	}
	batchWithEveryMetadataGroup := []Message{{
		Stream: "stream", Headers: []MetadataEntry{{Key: "h"}},
		Properties: []MetadataEntry{{Key: "p"}}, BrokerMetadata: []MetadataEntry{{Key: "b"}},
	}}
	if err := ValidateBatch(batchWithEveryMetadataGroup, limits); err != nil {
		t.Fatalf("ValidateBatch() complete metadata groups error = %v", err)
	}
}

var tlsConfigBelow12 = tls.Config{MinVersion: tls.VersionTLS11}

func TestInspectionValidationCoversInvalidLimits(t *testing.T) {
	t.Parallel()

	limits := DefaultLimits()
	limits.MaxStreamNameBytes = 0
	if err := (InspectionRequest{Stream: "stream"}).Validate(limits); !errors.Is(err, ErrValidation) {
		t.Fatalf("InspectionRequest.Validate() error = %v", err)
	}
}

func TestReplayFailureBoundariesAreExplicit(t *testing.T) {
	t.Parallel()

	if _, err := NewReplayer(Limits{}, &fakeReplaySource{}, nil); err == nil {
		t.Fatal("NewReplayer() accepted invalid limits")
	}
	if _, err := NewReplayer(DefaultLimits(), nil, nil); err == nil {
		t.Fatal("NewReplayer() accepted nil source")
	}

	valid := ReplayRequest{Stream: "stream", Start: StartPosition{Kind: OffsetStartBeginning}}
	cases := []struct {
		name    string
		source  *fakeReplaySource
		request ReplayRequest
		handler ReplayHandler
		want    error
	}{
		{"nil handler", &fakeReplaySource{}, valid, nil, ErrValidation},
		{"empty", &fakeReplaySource{retained: RetainedRange{Empty: true}}, valid, func(context.Context, ReplayDelivery) error { return nil }, nil},
		{"empty exact", &fakeReplaySource{retained: RetainedRange{Empty: true}}, ReplayRequest{Stream: "stream", Start: StartPosition{Kind: OffsetStartExplicit}}, func(context.Context, ReplayDelivery) error { return nil }, ErrRetentionGap},
		{"start beyond end", &fakeReplaySource{retained: RetainedRange{FirstOffset: 1, LastOffset: 2}}, ReplayRequest{Stream: "stream", Start: StartPosition{Kind: OffsetStartExplicit, Offset: 3}}, func(context.Context, ReplayDelivery) error { return nil }, ErrReplayRange},
		{"end beyond range", &fakeReplaySource{retained: RetainedRange{FirstOffset: 1, LastOffset: 2}}, replayWithEnd(1, 3), func(context.Context, ReplayDelivery) error { return nil }, ErrReplayRange},
		{"invalid retained range", &fakeReplaySource{retained: RetainedRange{FirstOffset: 2, LastOffset: 1}}, valid, func(context.Context, ReplayDelivery) error { return nil }, ErrReplayRange},
		{"open connection", &fakeReplaySource{retained: RetainedRange{FirstOffset: 1, LastOffset: 1}, openErr: ErrConnection}, valid, func(context.Context, ReplayDelivery) error { return nil }, ErrConnection},
		{"empty cursor", &fakeReplaySource{retained: RetainedRange{FirstOffset: 1, LastOffset: 1}}, valid, func(context.Context, ReplayDelivery) error { return nil }, ErrReplayRange},
		{"cursor connection", &fakeReplaySource{retained: RetainedRange{FirstOffset: 1, LastOffset: 1}, nextErr: ErrConnection}, valid, func(context.Context, ReplayDelivery) error { return nil }, ErrConnection},
		{"missing metadata", &fakeReplaySource{retained: RetainedRange{FirstOffset: 1, LastOffset: 1}, messages: []Message{{Offset: 1}}}, valid, func(context.Context, ReplayDelivery) error { return nil }, ErrValidation},
		{"retention jump", &fakeReplaySource{retained: RetainedRange{FirstOffset: 1, LastOffset: 3}, messages: []Message{{Stream: "stream", Partition: "stream", Offset: 2, HasOffset: true}}}, replayWithEnd(1, 2), func(context.Context, ReplayDelivery) error { return nil }, ErrRetentionGap},
		{"offset beyond end", &fakeReplaySource{retained: RetainedRange{FirstOffset: 1, LastOffset: 3}, messages: []Message{{Stream: "stream", Partition: "stream", Offset: 1, HasOffset: true}, {Stream: "stream", Partition: "stream", Offset: 3, HasOffset: true}}}, replayWithEnd(1, 2), func(context.Context, ReplayDelivery) error { return nil }, ErrReplayRange},
		{"handler error", &fakeReplaySource{retained: RetainedRange{FirstOffset: 1, LastOffset: 1}, messages: []Message{{Stream: "stream", Partition: "stream", Offset: 1, HasOffset: true}}}, valid, func(context.Context, ReplayDelivery) error { return ErrFatal }, ErrHandler},
		{"handler panic", &fakeReplaySource{retained: RetainedRange{FirstOffset: 1, LastOffset: 1}, messages: []Message{{Stream: "stream", Partition: "stream", Offset: 1, HasOffset: true}}}, valid, func(context.Context, ReplayDelivery) error { panic("secret") }, ErrHandler},
		{"close error", &fakeReplaySource{retained: RetainedRange{FirstOffset: 1, LastOffset: 1}, messages: []Message{{Stream: "stream", Partition: "stream", Offset: 1, HasOffset: true}}, closeErr: ErrConnection}, valid, func(context.Context, ReplayDelivery) error { return nil }, ErrConnection},
	}
	canceledDuringCursor, cancelCursor := context.WithCancel(context.Background())
	cursorSource := &fakeReplaySource{
		retained: RetainedRange{FirstOffset: 1, LastOffset: 1}, nextErr: ErrConnection,
		nextHook: cancelCursor,
	}
	replayer, _ := NewReplayer(DefaultLimits(), cursorSource, nil)
	if err := replayer.Run(canceledDuringCursor, valid, func(context.Context, ReplayDelivery) error { return nil }); !errors.Is(err, ErrCanceled) {
		t.Fatalf("canceled cursor Run() error = %v", err)
	}
	handlerCanceled, cancelHandler := context.WithCancel(context.Background())
	handlerSource := &fakeReplaySource{retained: RetainedRange{FirstOffset: 1, LastOffset: 1}, messages: []Message{{Stream: "stream", Partition: "stream", Offset: 1, HasOffset: true}}}
	replayer, _ = NewReplayer(DefaultLimits(), handlerSource, nil)
	if err := replayer.Run(handlerCanceled, valid, func(context.Context, ReplayDelivery) error {
		cancelHandler()
		return errors.New("stopped")
	}); !errors.Is(err, ErrCanceled) {
		t.Fatalf("canceled handler Run() error = %v", err)
	}
	for _, test := range cases {
		replayer, err := NewReplayer(DefaultLimits(), test.source, nil)
		if err != nil {
			t.Fatalf("%s: NewReplayer() error = %v", test.name, err)
		}
		err = replayer.Run(context.Background(), test.request, test.handler)
		if !errors.Is(err, test.want) {
			t.Fatalf("%s: Run() error = %v, want %v", test.name, err, test.want)
		}
	}

	invalidRequests := []ReplayRequest{
		{Stream: "stream", Start: StartPosition{Kind: OffsetStartKind(255)}},
		{Stream: "stream", Start: StartPosition{Kind: OffsetStartTimestamp}},
		replayWithEnd(2, 1),
		{SuperStream: "super", Partition: "p0", ExpectedPartitions: []string{" bad"}, Start: StartPosition{Kind: OffsetStartBeginning}},
	}
	replayer, _ = NewReplayer(DefaultLimits(), &fakeReplaySource{}, nil)
	if _, err := replayer.Inspect(absentContext, valid); !errors.Is(err, ErrValidation) {
		t.Fatalf("Inspect(nil) error = %v", err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := replayer.Inspect(canceled, valid); !errors.Is(err, ErrCanceled) {
		t.Fatalf("Inspect(canceled) error = %v", err)
	}
	for _, request := range invalidRequests {
		if _, err := replayer.Inspect(context.Background(), request); err == nil {
			t.Fatalf("Inspect(%#v) succeeded", request)
		}
	}

	skipSource := &fakeReplaySource{
		retained: RetainedRange{FirstOffset: 1, LastOffset: 2},
		messages: []Message{
			{Stream: "stream", Partition: "stream", Offset: 1, HasOffset: true},
			{Stream: "stream", Partition: "stream", Offset: 2, HasOffset: true},
		},
	}
	replayer, _ = NewReplayer(DefaultLimits(), skipSource, nil)
	checkpoint := uint64(2)
	if err := replayer.Run(context.Background(), ReplayRequest{
		Stream: "stream", Start: StartPosition{Kind: OffsetStartExplicit, Offset: 1}, Checkpoint: &checkpoint,
	}, func(context.Context, ReplayDelivery) error { return nil }); err != nil {
		t.Fatalf("checkpoint replay error = %v", err)
	}
}

func replayWithEnd(start, end uint64) ReplayRequest {
	return ReplayRequest{
		Stream: "stream", Start: StartPosition{Kind: OffsetStartExplicit, Offset: start}, EndOffset: &end,
	}
}
