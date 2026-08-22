package rabbitstream

import (
	"errors"
	"testing"
)

func TestMessageRetainOwnsPayloadAndOrderedMetadata(t *testing.T) {
	t.Parallel()

	source := Message{
		Stream:        "tracking.events",
		RoutingKey:    "tracking-123",
		ContentType:   "application/octet-stream",
		MessageID:     "event-123",
		CorrelationID: "tracking-123",
		Payload:       []byte("payload"),
		Headers: []MetadataEntry{
			{Key: "traceparent", Value: []byte("00-abc-def-01")},
			{Key: "x-duplicate", Value: []byte("first")},
			{Key: "x-duplicate", Value: []byte("second")},
		},
		Properties: []MetadataEntry{{Key: "content-type", Value: []byte("application/octet-stream")}},
	}

	owned := source.Retain()
	source.Payload[0] = 'X'
	source.Headers[0].Value[0] = 'X'
	source.Headers[0].Key = "changed"
	source.Properties[0].Value[0] = 'X'

	if got := string(owned.Payload); got != "payload" {
		t.Fatalf("payload = %q", got)
	}
	if got := owned.Headers[0].Key; got != "traceparent" {
		t.Fatalf("header key = %q", got)
	}
	if got := string(owned.Headers[0].Value); got != "00-abc-def-01" {
		t.Fatalf("header value = %q", got)
	}
	if got := string(owned.Headers[1].Value) + "," + string(owned.Headers[2].Value); got != "first,second" {
		t.Fatalf("ordered duplicate headers = %q", got)
	}
	if got := string(owned.Properties[0].Value); got != "application/octet-stream" {
		t.Fatalf("property value = %q", got)
	}
}

func TestMessageValidationBoundsEveryRetainedDimension(t *testing.T) {
	t.Parallel()

	limits := DefaultLimits()
	valid := Message{Stream: "tracking.events", Payload: []byte("payload")}

	tests := map[string]func(*Message){
		"missing target":       func(message *Message) { message.Stream = "" },
		"two targets":          func(message *Message) { message.SuperStream = "tracking" },
		"stream too long":      func(message *Message) { message.Stream = repeatByte('s', limits.MaxStreamNameBytes+1) },
		"routing key too long": func(message *Message) { message.RoutingKey = repeatByte('k', limits.MaxRoutingKeyBytes+1) },
		"payload too large":    func(message *Message) { message.Payload = make([]byte, limits.MaxPayloadBytes+1) },
		"too many headers":     func(message *Message) { message.Headers = make([]MetadataEntry, limits.MaxMetadataEntries+1) },
		"header key too long": func(message *Message) {
			message.Headers = []MetadataEntry{{Key: repeatByte('k', limits.MaxMetadataKeyBytes+1)}}
		},
		"header value too long": func(message *Message) {
			message.Headers = []MetadataEntry{{Key: "key", Value: make([]byte, limits.MaxMetadataValueBytes+1)}}
		},
		"aggregate metadata too large": func(message *Message) {
			message.Headers = []MetadataEntry{{Key: "first", Value: make([]byte, limits.MaxMetadataBytes)}}
		},
		"control character": func(message *Message) { message.Stream = "tracking\x00events" },
		"content type too long": func(message *Message) {
			message.ContentType = repeatByte('c', limits.MaxMetadataValueBytes+1)
		},
		"message id too long": func(message *Message) {
			message.MessageID = repeatByte('m', limits.MaxMetadataValueBytes+1)
		},
		"correlation id too long": func(message *Message) {
			message.CorrelationID = repeatByte('c', limits.MaxMetadataValueBytes+1)
		},
		"reserved routing annotation": func(message *Message) {
			message.Headers = []MetadataEntry{{Key: RoutingKeyMetadata, Value: []byte("caller")}}
		},
	}

	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			message := valid
			mutate(&message)
			if err := message.Validate(limits); !errors.Is(err, ErrValidation) {
				t.Fatalf("Validate() error = %v, want ErrValidation", err)
			}
		})
	}

	if err := valid.Validate(limits); err != nil {
		t.Fatalf("valid message error = %v", err)
	}

	oversized := valid
	oversized.Payload = make([]byte, limits.MaxPayloadBytes+1)
	if err := oversized.Validate(limits); !errors.Is(err, ErrValidation) ||
		!errors.Is(err, ErrMessageTooLarge) {
		t.Fatalf("oversized error = %v, want validation and message-too-large", err)
	}
}

func TestDeliveryValidationAcceptsDirectAndSuperStreamBrokerShapes(t *testing.T) {
	t.Parallel()

	limits := DefaultLimits()
	direct := Message{Stream: "tracking.events", Partition: "tracking.events", HasOffset: true}
	superStream := direct
	superStream.Stream = "tracking-0"
	superStream.Partition = "tracking-0"
	superStream.SuperStream = "tracking"
	for name, message := range map[string]Message{"direct": direct, "super stream": superStream} {
		if err := message.ValidateDelivery(limits); err != nil {
			t.Fatalf("%s ValidateDelivery() error = %v", name, err)
		}
	}

	for name, mutate := range map[string]func(*Message){
		"missing partition": func(message *Message) { message.Partition = "" },
		"wrong partition":   func(message *Message) { message.Partition = "tracking-1" },
		"missing offset":    func(message *Message) { message.HasOffset = false },
		"invalid super":     func(message *Message) { message.SuperStream = " bad" },
		"oversized payload": func(message *Message) { message.Payload = make([]byte, limits.MaxPayloadBytes+1) },
	} {
		message := superStream
		mutate(&message)
		err := message.ValidateDelivery(limits)
		var operationErr *OperationError
		if !errors.Is(err, ErrValidation) || !errors.As(err, &operationErr) || operationErr.Operation != OperationConsume {
			t.Fatalf("%s ValidateDelivery() error = %#v", name, err)
		}
	}
}

func TestLimitsRequireEachDimensionAndExposeExactDefaults(t *testing.T) {
	t.Parallel()

	want := Limits{255, 255, 1 << 20, 64, 128, 8 << 10, 64 << 10, 256, 8 << 20, 1024}
	if got := DefaultLimits(); got != want {
		t.Fatalf("DefaultLimits() = %#v, want %#v", got, want)
	}

	for name, zero := range map[string]func(*Limits){
		"stream":         func(limits *Limits) { limits.MaxStreamNameBytes = 0 },
		"routing":        func(limits *Limits) { limits.MaxRoutingKeyBytes = 0 },
		"payload":        func(limits *Limits) { limits.MaxPayloadBytes = 0 },
		"entries":        func(limits *Limits) { limits.MaxMetadataEntries = 0 },
		"key":            func(limits *Limits) { limits.MaxMetadataKeyBytes = 0 },
		"value":          func(limits *Limits) { limits.MaxMetadataValueBytes = 0 },
		"metadata":       func(limits *Limits) { limits.MaxMetadataBytes = 0 },
		"batch messages": func(limits *Limits) { limits.MaxBatchMessages = 0 },
		"batch bytes":    func(limits *Limits) { limits.MaxBatchBytes = 0 },
		"buffered":       func(limits *Limits) { limits.MaxBufferedMessages = 0 },
	} {
		t.Run(name, func(t *testing.T) {
			limits := DefaultLimits()
			zero(&limits)
			if err := limits.validate(); !errors.Is(err, ErrValidation) {
				t.Fatalf("validate() error = %v", err)
			}
		})
	}
}

func TestMessageValidationAcceptsExactLimitsAndCountsAllMetadata(t *testing.T) {
	t.Parallel()

	limits := DefaultLimits()
	limits.MaxStreamNameBytes = 3
	limits.MaxRoutingKeyBytes = 3
	limits.MaxPayloadBytes = 3
	limits.MaxMetadataEntries = 3
	limits.MaxMetadataKeyBytes = 2
	limits.MaxMetadataValueBytes = 2
	limits.MaxMetadataBytes = 14
	message := Message{
		Stream: "a b", RoutingKey: "k k", Payload: []byte("123"),
		ContentType: "ct", MessageID: "id", CorrelationID: "co",
		Headers:        []MetadataEntry{{Key: "h1", Value: []byte("v1")}},
		Properties:     []MetadataEntry{{Key: "p1"}},
		BrokerMetadata: []MetadataEntry{{Key: "b1"}},
	}
	if err := message.Validate(limits); err != nil {
		t.Fatalf("Validate() exact limits error = %v", err)
	}

	cases := map[string]func(*Message){
		"stream":  func(message *Message) { message.Stream += "x" },
		"routing": func(message *Message) { message.RoutingKey += "x" },
		"payload": func(message *Message) { message.Payload = append(message.Payload, 'x') },
		"entries": func(message *Message) {
			message.BrokerMetadata = append(message.BrokerMetadata, MetadataEntry{Key: "x"})
		},
		"content type":   func(message *Message) { message.ContentType = "xxx" },
		"message id":     func(message *Message) { message.MessageID = "xxx" },
		"correlation id": func(message *Message) { message.CorrelationID = "xxx" },
		"header key":     func(message *Message) { message.Headers[0].Key = "xxx" },
		"header value":   func(message *Message) { message.Headers[0].Value = []byte("xxx") },
		"aggregate":      func(message *Message) { message.BrokerMetadata[0].Value = []byte("x") },
	}
	for name, exceed := range cases {
		t.Run(name, func(t *testing.T) {
			candidate := message.Retain()
			exceed(&candidate)
			if err := candidate.Validate(limits); !errors.Is(err, ErrValidation) {
				t.Fatalf("Validate() error = %v", err)
			}
		})
	}
}

func TestMessageMetadataEntryArithmeticAndStandardAggregateAreExact(t *testing.T) {
	t.Parallel()

	limits := DefaultLimits()
	limits.MaxMetadataEntries = 2
	message := Message{
		Stream: "stream", Headers: []MetadataEntry{{Key: "h"}},
		Properties: []MetadataEntry{{Key: "p"}}, BrokerMetadata: []MetadataEntry{{Key: "b"}},
	}
	if err := message.Validate(limits); !errors.Is(err, ErrValidation) {
		t.Fatalf("Validate(three metadata groups) error = %v", err)
	}

	limits = DefaultLimits()
	limits.MaxMetadataBytes = 6
	message = Message{Stream: "stream", ContentType: "ct", MessageID: "id", CorrelationID: "co"}
	if err := message.Validate(limits); err != nil {
		t.Fatalf("Validate(exact standard metadata) error = %v", err)
	}
	message.CorrelationID += "x"
	if err := message.Validate(limits); !errors.Is(err, ErrValidation) {
		t.Fatalf("Validate(excess standard metadata) error = %v", err)
	}
}

func TestBatchValidationUsesCountAndAggregateByteBounds(t *testing.T) {
	t.Parallel()

	limits := DefaultLimits()
	message := Message{Stream: "tracking.events", Payload: []byte("payload")}

	tooMany := make([]Message, limits.MaxBatchMessages+1)
	if err := ValidateBatch(tooMany, limits); !errors.Is(err, ErrValidation) {
		t.Fatalf("too-many error = %v", err)
	}

	limits.MaxBatchBytes = len(message.Payload)
	if err := ValidateBatch([]Message{message, message}, limits); !errors.Is(err, ErrValidation) {
		t.Fatalf("aggregate error = %v", err)
	}

	if err := ValidateBatch([]Message{message}, limits); err != nil {
		t.Fatalf("valid batch error = %v", err)
	}
}

func TestBatchValidationCountsEveryByteDimensionExactly(t *testing.T) {
	t.Parallel()

	message := Message{
		Stream: "stream", Payload: []byte("p"), ContentType: "c", MessageID: "m", CorrelationID: "i",
		Headers:        []MetadataEntry{{Key: "h", Value: []byte("1")}},
		Properties:     []MetadataEntry{{Key: "p", Value: []byte("2")}},
		BrokerMetadata: []MetadataEntry{{Key: "b", Value: []byte("3")}},
	}
	const exactBytes = 10
	limits := DefaultLimits()
	limits.MaxBatchMessages = 1
	limits.MaxBatchBytes = exactBytes
	if err := ValidateBatch([]Message{message}, limits); err != nil {
		t.Fatalf("ValidateBatch() exact bytes error = %v", err)
	}
	limits.MaxBatchBytes--
	if err := ValidateBatch([]Message{message}, limits); !errors.Is(err, ErrValidation) {
		t.Fatalf("ValidateBatch() oversized error = %v", err)
	}
}

func repeatByte(value byte, count int) string {
	result := make([]byte, count)
	for index := range result {
		result[index] = value
	}
	return string(result)
}
