package rabbitmq

import (
	"bytes"
	"errors"
	"math"
	"testing"

	"github.com/faustbrian/go-rabbitmq-streams/adapters/rabbitmq/v2/internal/rabbitmqstream/amqp"
	"github.com/faustbrian/go-rabbitmq-streams/v2"
)

func TestTransportRejectsUnrepresentableProtocolNamePolicy(t *testing.T) {
	limits := rabbitstream.DefaultLimits()
	limits.MaxStreamNameBytes = math.MaxUint16 + 1
	if _, err := transportDecoderLimits(limits); !errors.Is(err, rabbitstream.ErrInvalidConfiguration) {
		t.Fatal("transport accepted a name budget outside its protocol string width")
	}
	limits.MaxStreamNameBytes = math.MaxUint16
	if _, err := transportDecoderLimits(limits); err != nil {
		t.Fatal("transport rejected its representable name budget:", err)
	}
}

func TestTransportDecoderAdmitsSupportedMessageEnvelope(t *testing.T) {
	limits := rabbitstream.DefaultLimits()
	limits.MaxPayloadBytes = 3
	limits.MaxMetadataBytes = 3
	limits.MaxMetadataEntries = 1
	policy, err := transportDecoderLimits(limits)
	if err != nil {
		t.Fatal(err)
	}
	wire, err := toWireMessage(rabbitstream.Message{
		Payload: []byte("abc"),
		Headers: []rabbitstream.MetadataEntry{{Key: "k", Value: []byte("vv")}},
	}).MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	var decoded amqp.Message
	if err := decoded.UnmarshalBinaryWithLimits(wire, policy.AMQP); err != nil {
		t.Fatal("supported message envelope was charged as payload only:", err)
	}
	if len(decoded.Data) != 1 || !bytes.Equal(decoded.Data[0], []byte("abc")) {
		t.Fatal("bounded decoding changed the message payload")
	}
	policy.AMQP.MaxMessageBytes = len(wire) - 1
	if err := decoded.UnmarshalBinaryWithLimits(wire, policy.AMQP); err == nil {
		t.Fatal("decoder ignored encoded-message admission")
	}
}

func TestTransportDecoderRejectsOverflowBeforeWireAllocation(t *testing.T) {
	for _, field := range []string{"payload", "metadata", "entries"} {
		t.Run(field, func(t *testing.T) {
			limits := rabbitstream.DefaultLimits()
			maximum := int(^uint(0) >> 1)
			switch field {
			case "payload":
				limits.MaxPayloadBytes = maximum
			case "metadata":
				limits.MaxMetadataBytes = maximum
			case "entries":
				limits.MaxMetadataEntries = maximum
			}
			if _, err := transportDecoderLimits(limits); !errors.Is(err, rabbitstream.ErrInvalidConfiguration) {
				t.Fatalf("unrepresentable wire budget was accepted: %v", err)
			}
		})
	}
}
