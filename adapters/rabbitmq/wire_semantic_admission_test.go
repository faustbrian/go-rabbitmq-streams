package rabbitmq

import (
	"errors"
	"reflect"
	"testing"

	rabbitstream "github.com/faustbrian/go-rabbitmq-streams"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"
)

// Byte and type admission must not replace semantic identifier validation.
func TestWireDeliveryRejectsInvalidIdentifiersAfterByteAdmission(t *testing.T) {
	limits := rabbitstream.DefaultLimits()
	tests := map[string]*amqp.Message{
		"empty header key":       {Annotations: amqp.Annotations{"": "value"}},
		"whitespace header key":  {Annotations: amqp.Annotations{" key": "value"}},
		"control property key":   {ApplicationProperties: map[string]any{"key\n": "value"}},
		"empty property key":     {ApplicationProperties: map[string]any{"": "value"}},
		"whitespace routing key": {Annotations: amqp.Annotations{routingKeyAnnotation: " key"}},
	}
	for name, wire := range tests {
		t.Run(name, func(t *testing.T) {
			if err := admitWireSizes(limits, wire.Data, wire.Properties, wire.Annotations, wire.ApplicationProperties); err != nil {
				t.Fatalf("fixture must pass byte admission: %v", err)
			}
			delivery, err := fromWireMessage(limits, "", "tracking.events", 1, wire)
			if !errors.Is(err, rabbitstream.ErrValidation) || !reflect.DeepEqual(delivery, rabbitstream.Message{}) {
				t.Fatal("invalid identifier must return validation refusal without a delivery")
			}
		})
	}
	wire := &amqp.Message{Annotations: amqp.Annotations{"key": "value"}}
	delivery, err := fromWireMessage(limits, "", "tracking.events", 1, wire)
	if err != nil || len(delivery.Headers) != 1 || delivery.Headers[0].Key != "key" || string(delivery.Headers[0].Value) != "value" {
		t.Fatal("valid identifier must retain the admitted header")
	}
}
