package rabbitmq

import (
	"errors"
	"reflect"
	"testing"

	"github.com/faustbrian/go-rabbitmq-streams/adapters/rabbitmq/v2/internal/rabbitmqstream/amqp"
	rabbitstream "github.com/faustbrian/go-rabbitmq-streams/v2"
)

func TestInboundAMQPSectionsRespectSelectedPublicProfile(t *testing.T) {
	// AMQP 1.0 Messaging section descriptors: header 0x70, delivery annotations
	// 0x71, data 0x75, value 0x77, footer 0x78. These tiny literal encodings are
	// independent of the owning encoder and exercise the actual bounded decoder.
	data := []byte{0x00, 0x53, 0x75, 0xa0, 0x00}
	annotation := []byte{0xc1, 0x07, 0x02, 0xa3, 0x01, 'x', 0xa1, 0x01, 'y'}
	for name, raw := range map[string][]byte{
		"no data":                    {},
		"value":                      {0x00, 0x53, 0x77, 0xa1, 0x01, 'x'},
		"null value":                 {0x00, 0x53, 0x77, 0x40},
		"data plus value":            append(append([]byte{}, data...), 0x00, 0x53, 0x77, 0xa1, 0x01, 'x'),
		"data plus null value":       append(append([]byte{}, data...), 0x00, 0x53, 0x77, 0x40),
		"empty footer":               append(append([]byte{}, data...), 0x00, 0x53, 0x78, 0xc1, 0x01, 0x00),
		"empty delivery annotations": append([]byte{0x00, 0x53, 0x71, 0xc1, 0x01, 0x00}, data...),
		"header":                     append([]byte{0x00, 0x53, 0x70, 0x45}, data...),
		"delivery annotations":       append(append([]byte{0x00, 0x53, 0x71}, annotation...), data...),
		"footer":                     append(append(append([]byte{}, data...), 0x00, 0x53, 0x78), annotation...),
	} {
		t.Run(name, func(t *testing.T) {
			for _, native := range []string{"decoded message", "stream wrapper"} {
				t.Run(native, func(t *testing.T) {
					var wire any
					if native == "decoded message" {
						message := &amqp.Message{}
						if err := message.UnmarshalBinaryWithLimits(raw, amqp.DefaultDecoderLimits()); err != nil {
							t.Fatalf("valid codec fixture: %v", err)
						}
						wire = message
					} else {
						message := amqp.NewMessage(nil)
						if err := message.UnmarshalBinary(raw); err != nil {
							t.Fatalf("valid wrapper fixture: %v", err)
						}
						wire = message
					}
					delivery, err := fromWireMessage(rabbitstream.DefaultLimits(), "", "tracking.events", 0, wire)
					if !errors.Is(err, rabbitstream.ErrValidation) || !reflect.DeepEqual(delivery, rabbitstream.Message{}) {
						t.Fatal("unsupported section profile published a delivery or lost validation refusal")
					}
				})
			}
		})
	}
	for name, raw := range map[string][]byte{"null data": {0x00, 0x53, 0x75, 0x40}, "empty data": data} {
		t.Run(name, func(t *testing.T) {
			decoded := &amqp.Message{}
			if err := decoded.UnmarshalBinary(raw); err != nil {
				t.Fatal(err)
			}
			wrapper := amqp.NewMessage(nil)
			if err := wrapper.UnmarshalBinary(raw); err != nil {
				t.Fatal(err)
			}
			for _, wire := range []any{decoded, wrapper} {
				got, err := fromWireMessage(rabbitstream.DefaultLimits(), "", "tracking.events", 0, wire)
				if err != nil || !got.HasOffset || got.Offset != 0 || len(got.Payload) != 0 {
					t.Fatalf("legitimate decoded empty/null data lost admission: %v", err)
				}
			}
		})
	}

	for _, payload := range [][]byte{nil, []byte("ordinary")} {
		wire := amqp.NewMessage(payload)
		delivery, err := fromWireMessage(rabbitstream.DefaultLimits(), "", "tracking.events", 0, wire)
		if err != nil || !delivery.HasOffset || delivery.Offset != 0 || string(delivery.Payload) != string(payload) {
			t.Fatalf("one data section including empty payload lost admission: %v", err)
		}
	}
}
