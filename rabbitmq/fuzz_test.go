package rabbitmq

import (
	"testing"

	"github.com/faustbrian/golib/pkg/rabbitstream"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"
)

func FuzzWireMessageRoundTrip(f *testing.F) {
	f.Add("tracking-123", "application/octet-stream", "event-123", "payload")
	f.Add("", "", "", "")
	f.Fuzz(func(t *testing.T, routingKey, contentType, messageID, payload string) {
		outbound := rabbitstream.Message{
			Stream: "tracking.events", RoutingKey: routingKey, ContentType: contentType,
			MessageID: messageID, Payload: []byte(payload),
		}
		delivery, err := fromWireMessage("", "tracking.events", 41, toWireMessage(outbound))
		if err != nil {
			t.Fatalf("wire round trip: %v", err)
		}
		if delivery.RoutingKey != routingKey || delivery.ContentType != contentType ||
			delivery.MessageID != messageID || string(delivery.Payload) != payload {
			t.Fatalf("wire delivery = %#v", delivery)
		}
	})
}

func FuzzHashPartition(f *testing.F) {
	f.Add("tracking-123", uint8(3))
	f.Add("", uint8(0))
	f.Fuzz(func(t *testing.T, routingKey string, count uint8) {
		partitionCount := int(count % 17)
		partitions := make([]string, partitionCount)
		for index := range partitions {
			partitions[index] = "tracking-" + string(rune('a'+index))
		}
		partition, err := hashPartition(routingKey, partitions)
		if partitionCount == 0 {
			if err == nil {
				t.Fatal("empty topology selected a partition")
			}
			return
		}
		if err != nil || partition == "" {
			t.Fatalf("hashPartition() = %q, %v", partition, err)
		}
	})
}

func FuzzConfirmationClassification(f *testing.F) {
	f.Add(true, uint8(0), uint64(1))
	f.Add(false, uint8(1), uint64(2))
	f.Fuzz(func(t *testing.T, confirmed bool, causeKind uint8, publishingID uint64) {
		var cause error
		switch causeKind % 3 {
		case 1:
			cause = stream.ConfirmationTimoutError
		case 2:
			cause = stream.CodeAccessRefused
		}
		result := classifyConfirmation(confirmed, cause, publishingID)
		if result.Confirmed && (result.BrokerRejected || result.Ambiguous) {
			t.Fatalf("contradictory confirmation = %#v", result)
		}
		if result.BrokerRejected && result.Ambiguous {
			t.Fatalf("contradictory failure = %#v", result)
		}
	})
}
