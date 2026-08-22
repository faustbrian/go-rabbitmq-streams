package rabbitmq

import (
	"fmt"
	"testing"
	"time"

	"github.com/faustbrian/golib/pkg/rabbitstream"
)

func BenchmarkWireMessagePolicy(b *testing.B) {
	for _, payloadBytes := range []int{128, 1 << 10, 64 << 10} {
		outbound := benchmarkWireMessage(payloadBytes)
		b.Run(fmt.Sprintf("encode/%dB", payloadBytes), func(b *testing.B) {
			b.SetBytes(int64(payloadBytes))
			b.ReportAllocs()
			for b.Loop() {
				wire := toWireMessage(outbound)
				if len(wire.GetData()) != 1 || len(wire.GetData()[0]) != payloadBytes {
					b.Fatal("wire payload changed")
				}
			}
		})

		wire := toWireMessage(outbound)
		b.Run(fmt.Sprintf("decode/%dB", payloadBytes), func(b *testing.B) {
			b.SetBytes(int64(payloadBytes))
			b.ReportAllocs()
			for b.Loop() {
				delivery, err := fromWireMessage("", "tracking.events", 1, wire)
				if err != nil || len(delivery.Payload) != payloadBytes {
					b.Fatalf("fromWireMessage() = %d bytes, %v", len(delivery.Payload), err)
				}
			}
		})
	}
}

func BenchmarkSuperStreamHashRouting(b *testing.B) {
	for _, partitionCount := range []int{3, 32, 256} {
		partitions := make([]string, partitionCount)
		for index := range partitions {
			partitions[index] = fmt.Sprintf("tracking.events-%04d", index)
		}
		b.Run(fmt.Sprintf("%d-partitions", partitionCount), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				partition, err := hashPartition("tracked-item-1", partitions)
				if err != nil || partition == "" {
					b.Fatalf("hashPartition() = %q, %v", partition, err)
				}
			}
		})
	}
}

func benchmarkWireMessage(payloadBytes int) rabbitstream.Message {
	return rabbitstream.Message{
		Stream: "tracking.events", RoutingKey: "tracked-item-1",
		PublishingID: 42, HasPublishingID: true,
		Timestamp:   time.Unix(1_700_000_000, 0).UTC(),
		ContentType: "application/octet-stream", MessageID: "message-1",
		CorrelationID: "correlation-1", Payload: make([]byte, payloadBytes),
		Headers:    []rabbitstream.MetadataEntry{{Key: "traceparent", Value: []byte("00-00000000000000000000000000000001-0000000000000001-01")}},
		Properties: []rabbitstream.MetadataEntry{{Key: "schema-version", Value: []byte("1")}},
	}
}
