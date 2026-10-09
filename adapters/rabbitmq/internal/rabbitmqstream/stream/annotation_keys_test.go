package stream

import (
	"encoding/binary"
	"testing"
)

func TestChunkDecoderRejectsUnsupportedAnnotationKeyWithoutPanic(t *testing.T) {
	wire := []byte{0, 0x53, 0x72, 0xc1, 5, 2, 0xa0, 1, 1, 0x40}
	body := append([]byte(nil), deliveryFixture(t, 255)[:49]...)
	binary.BigEndian.PutUint16(body[3:5], 1)
	binary.BigEndian.PutUint32(body[5:9], 1)
	binary.BigEndian.PutUint32(body[37:41], uint32(len(wire)+4))
	entry := make([]byte, 4)
	binary.BigEndian.PutUint32(entry, uint32(len(wire)))
	body = append(body, entry...)
	body = append(body, wire...)
	_, chunk, _, _, _, err := decodeDelivery(body, DefaultDecoderLimits())
	if err == nil || len(chunk.offsetMessages) != 0 {
		t.Fatalf("annotation chunk accepted: count=%d err=%v", len(chunk.offsetMessages), err)
	}
}
