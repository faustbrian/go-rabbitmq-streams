package rabbitmq

import (
	"errors"
	"runtime"
	"strings"
	"testing"

	"github.com/faustbrian/go-rabbitmq-streams"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"
)

func TestWireConversionRejectsOversizedDecodedInput(t *testing.T) {
	limits := rabbitstream.DefaultLimits()
	value := strings.Repeat("x", limits.MaxMetadataValueBytes+1)
	cases := map[string]*amqp.Message{
		"payload":        {Data: [][]byte{make([]byte, limits.MaxPayloadBytes+1)}},
		"content type":   {Properties: &amqp.MessageProperties{ContentType: value}},
		"message ID":     {Properties: &amqp.MessageProperties{MessageID: []byte(value)}},
		"correlation ID": {Properties: &amqp.MessageProperties{CorrelationID: []byte(value)}},
		"header value":   {Annotations: amqp.Annotations{"header": []byte(value)}},
		"property value": {ApplicationProperties: map[string]any{"property": value}},
		"header key":     {Annotations: amqp.Annotations{strings.Repeat("k", limits.MaxMetadataKeyBytes+1): "v"}},
		"property key":   {ApplicationProperties: map[string]any{strings.Repeat("k", limits.MaxMetadataKeyBytes+1): "v"}},
		"routing key":    {Annotations: amqp.Annotations{routingKeyAnnotation: strings.Repeat("r", limits.MaxRoutingKeyBytes+1)}},
	}
	for name, wire := range cases {
		t.Run(name, func(t *testing.T) {
			delivery, err := fromWireMessage(limits, "", "tracking.events", 0, wire)
			if !errors.Is(err, rabbitstream.ErrValidation) || delivery.HasOffset {
				t.Fatalf("oversized input retained: err=%v, hasOffset=%v", err, delivery.HasOffset)
			}
			if name == "payload" && !errors.Is(err, rabbitstream.ErrMessageTooLarge) {
				t.Fatalf("payload error lost classification: %v", err)
			}
		})
	}
}

func TestWireConversionHonorsConfiguredBoundariesAndOwnership(t *testing.T) {
	limits := rabbitstream.DefaultLimits()
	limits.MaxPayloadBytes = 3
	limits.MaxMetadataEntries = 2
	limits.MaxMetadataKeyBytes = 2
	limits.MaxMetadataValueBytes = 2
	limits.MaxMetadataBytes = 11
	limits.MaxRoutingKeyBytes = 3
	wire := &amqp.Message{
		Data:                  [][]byte{[]byte("abc")},
		Properties:            &amqp.MessageProperties{ContentType: "c", MessageID: []byte("a"), CorrelationID: []byte("b")},
		Annotations:           amqp.Annotations{"hh": []byte("vv"), routingKeyAnnotation: "key"},
		ApplicationProperties: map[string]any{"pp": []byte("vv")},
	}
	for name, lower := range map[string]func(*rabbitstream.Limits){
		"payload":         func(l *rabbitstream.Limits) { l.MaxPayloadBytes-- },
		"entry count":     func(l *rabbitstream.Limits) { l.MaxMetadataEntries-- },
		"key bytes":       func(l *rabbitstream.Limits) { l.MaxMetadataKeyBytes-- },
		"value bytes":     func(l *rabbitstream.Limits) { l.MaxMetadataValueBytes-- },
		"aggregate bytes": func(l *rabbitstream.Limits) { l.MaxMetadataBytes-- },
		"routing bytes":   func(l *rabbitstream.Limits) { l.MaxRoutingKeyBytes-- },
	} {
		t.Run(name, func(t *testing.T) {
			lowered := limits
			lower(&lowered)
			delivery, err := fromWireMessage(lowered, "", "events", 0, wire)
			if !errors.Is(err, rabbitstream.ErrValidation) || delivery.HasOffset {
				t.Fatalf("lowered limit accepted: %v, offset=%v", err, delivery.HasOffset)
			}
		})
	}
	delivery, err := fromWireMessage(limits, "", "events", 0, wire)
	if err != nil || !delivery.HasOffset || delivery.Offset != 0 || delivery.RoutingKey != "key" {
		t.Fatalf("exact boundary rejected: %v", err)
	}
	wire.Data[0][0] = 'X'
	wire.Properties.MessageID.([]byte)[0] = 'X'
	wire.Properties.CorrelationID.([]byte)[0] = 'X'
	wire.Annotations["hh"].([]byte)[0] = 'X'
	wire.ApplicationProperties["pp"].([]byte)[0] = 'X'
	if string(delivery.Payload) != "abc" || delivery.MessageID != "a" || delivery.CorrelationID != "b" ||
		string(delivery.Headers[0].Value) != "vv" || string(delivery.Properties[0].Value) != "vv" {
		t.Fatal("accepted delivery retained borrowed wire bytes")
	}
}

func TestWireRejectionDoesNotAllocateInputSizedCopies(t *testing.T) {
	// A generous budget detects an owned copy of the finite 8 MiB fixture, not
	// implementation-specific small allocations or exact performance numbers.
	large := make([]byte, 8<<20)
	for name, wire := range map[string]*amqp.Message{
		"payload":              {Data: [][]byte{large}},
		"standard property":    {Properties: &amqp.MessageProperties{MessageID: large}},
		"application property": {ApplicationProperties: map[string]any{"value": large}},
	} {
		t.Run(name, func(t *testing.T) {
			var before, after runtime.MemStats
			runtime.ReadMemStats(&before)
			_, err := fromWireMessage(rabbitstream.DefaultLimits(), "", "events", 0, wire)
			runtime.ReadMemStats(&after)
			if !errors.Is(err, rabbitstream.ErrValidation) {
				t.Fatalf("oversized input accepted: %v", err)
			}
			if after.TotalAlloc-before.TotalAlloc >= uint64(len(large)/2) {
				t.Fatal("rejection allocated an input-sized copy")
			}
		})
	}
}
