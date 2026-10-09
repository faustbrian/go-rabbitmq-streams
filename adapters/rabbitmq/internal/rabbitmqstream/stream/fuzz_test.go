package stream

import "testing"

func FuzzBoundedDeliveryDecoder(f *testing.F) {
	for _, codec := range []byte{255, None, GZIP, SNAPPY, LZ4, ZSTD} {
		f.Add(deliveryFixture(f, codec))
	}
	f.Add([]byte{})
	limits := DefaultDecoderLimits()
	limits.MaxFrameBytes = 4096
	limits.MaxMessageBytes = 4096
	limits.MaxChunkBytes = 4096
	limits.MaxChunkValues = 128
	limits.MaxRecords = 32
	limits.MaxProtocolItems = 32
	limits.AMQP.MaxMessageBytes = 4096
	limits.AMQP.MaxValueBytes = 4096
	limits.AMQP.MaxContainerValues = 128
	limits.AMQP.MaxValues = 128
	limits.AMQP.MaxDepth = 8
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > limits.MaxFrameBytes {
			return
		}
		_, chunk, records, _, _, err := decodeDelivery(data, limits)
		if err != nil {
			return
		}
		if records > uint32(limits.MaxRecords) || len(chunk.offsetMessages) != int(records) {
			t.Fatal("successful chunk decode escaped its admitted record count")
		}
		for _, message := range chunk.offsetMessages {
			if message == nil || message.message == nil {
				t.Fatal("successful chunk decode retained an absent message")
			}
		}
	})
}
