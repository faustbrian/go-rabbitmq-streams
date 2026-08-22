package rabbitstream

import (
	"context"
	"strings"
	"testing"
	"time"
)

func FuzzMessageValidationAndOwnership(f *testing.F) {
	f.Add("tracking.events", "tracking-123", "traceparent", uint16(128))
	f.Add("", "", "", uint16(0))
	f.Fuzz(func(t *testing.T, streamName, routingKey, metadataKey string, payloadSize uint16) {
		limits := DefaultLimits()
		size := int(payloadSize) % (limits.MaxPayloadBytes + 2)
		message := Message{
			Stream: streamName, RoutingKey: routingKey, Payload: make([]byte, size),
			Headers: []MetadataEntry{{Key: metadataKey, Value: []byte("bounded")}},
		}
		_ = message.Validate(limits)
		owned := message.Retain()
		if len(message.Payload) > 0 {
			message.Payload[0] = 1
			if owned.Payload[0] != 0 {
				t.Fatal("Retain() aliased payload bytes")
			}
		}
	})
}

func FuzzConnectionConfigNormalization(f *testing.F) {
	f.Add("rabbitmq.internal", uint16(5552), int64(time.Second), uint8(2))
	f.Add("bad\nhost", uint16(0), int64(-1), uint8(0))
	f.Fuzz(func(t *testing.T, host string, port uint16, timeoutNanos int64, attempts uint8) {
		config := ConnectionConfig{
			Endpoints:            []Endpoint{{Host: host, Port: port}},
			Credentials:          fuzzCredentialProvider{},
			Security:             DevelopmentPlaintextSecurity(),
			ConnectTimeout:       time.Duration(timeoutNanos),
			MaxReconnectAttempts: int(attempts),
		}
		normalized, err := config.Normalized()
		if err == nil {
			if len(normalized.Endpoints) != 1 || normalized.Endpoints[0].Port == 0 ||
				normalized.ConnectTimeout <= 0 || normalized.MaxReconnectAttempts <= 0 {
				t.Fatalf("invalid normalized connection = %#v", normalized)
			}
			if strings.ContainsAny(normalized.Endpoints[0].Host, "\r\n\x00") {
				t.Fatal("normalized endpoint retained control characters")
			}
		}
	})
}

func FuzzReplayRequestValidation(f *testing.F) {
	f.Add(uint8(OffsetStartBeginning), uint64(0), int64(0), uint64(10), true)
	f.Add(uint8(255), ^uint64(0), int64(-1), uint64(0), false)
	f.Fuzz(func(t *testing.T, kind uint8, offset uint64, timestampMillis int64, end uint64, checkpointed bool) {
		request := ReplayRequest{
			Stream: "tracking.events",
			Start:  StartPosition{Kind: OffsetStartKind(kind), Offset: offset},
		}
		if OffsetStartKind(kind) == OffsetStartTimestamp {
			request.Start.Offset = 0
			request.Start.Timestamp = time.UnixMilli(timestampMillis)
		}
		request.EndOffset = &end
		if checkpointed {
			request.Checkpoint = &offset
		}
		replayer := &Replayer{limits: DefaultLimits()}
		_ = replayer.validateRequest(context.Background(), request)
	})
}

type fuzzCredentialProvider struct{}

func (fuzzCredentialProvider) Credentials(context.Context) (Credentials, error) {
	return Credentials{}, nil
}
