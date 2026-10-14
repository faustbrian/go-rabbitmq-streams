package stream

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"testing"
)

func TestProtocolSubentrySeparatesPhysicalEntriesAndLogicalOffsets(t *testing.T) {
	// RabbitMQ's pinned Deliver-v1 schema distinguishes NumEntries (uint16)
	// from NumRecords (uint32). One entry here contains two literal AMQP Data
	// messages: each five-byte message is prefixed by its four-byte length.
	// gzip uses the standard-library producer, not the owner's codec/encoder.
	records := []byte{0, 0, 0, 5, 0, 0x53, 0x75, 0xa0, 0, 0, 0, 0, 5, 0, 0x53, 0x75, 0xa0, 0}
	for _, codec := range []byte{0, 1} {
		compressed := records
		if codec == 1 {
			var buffer bytes.Buffer
			writer := gzip.NewWriter(&buffer)
			if _, err := writer.Write(records); err != nil {
				t.Fatal(err)
			}
			if err := writer.Close(); err != nil {
				t.Fatal(err)
			}
			compressed = buffer.Bytes()
		}
		// One subentry: flags, record count, raw byte count, compressed byte count.
		entry := make([]byte, 11)
		entry[0] = 0x80 | codec<<4
		binary.BigEndian.PutUint16(entry[1:3], 2)
		binary.BigEndian.PutUint32(entry[3:7], uint32(len(records)))
		binary.BigEndian.PutUint32(entry[7:11], uint32(len(compressed)))
		entry = append(entry, compressed...)
		// Subscription byte followed by the 48-byte Osiris chunk header.
		frame := make([]byte, 49)
		frame[0], frame[1] = 1, 0x51
		binary.BigEndian.PutUint16(frame[3:5], 1)
		binary.BigEndian.PutUint32(frame[5:9], 2)
		binary.BigEndian.PutUint64(frame[25:33], 5)
		binary.BigEndian.PutUint32(frame[37:41], uint32(len(entry)))
		frame = append(frame, entry...)
		_, chunk, count, _, _, err := decodeDelivery(frame, DefaultDecoderLimits())
		if err != nil {
			t.Fatalf("codec %d: %v", codec, err)
		}
		if count != 2 || len(chunk.offsetMessages) != 2 || chunk.offsetMessages[0].offset != 5 || chunk.offsetMessages[1].offset != 6 {
			t.Fatal("one physical entry lost the two distinct logical offsets")
		}
		context := ConsumerContext{chunkInfo: &chunk}
		if context.GetRecordsCount() != 2 {
			t.Fatal("logical record API lost compressed subentry records")
		}
		if context.GetEntriesCount() != 1 {
			t.Fatal("physical entry API was redefined as a logical count")
		}
	}
}
