package amqp

import "testing"

func TestAnnotationDecoderRejectsUnsupportedKeysWithoutPanic(t *testing.T) {
	for _, section := range []byte{0x71, 0x72, 0x78} {
		for _, key := range [][]byte{{0xa0, 1, 1}, {0x45}, {0xc1, 1, 0}, {0xe0, 2, 0, 0x43}, {0x40}, {0x41}} {
			wire := []byte{0, 0x53, section, 0xc1, byte(len(key) + 2), 2}
			wire = append(wire, key...)
			wire = append(wire, 0x40)
			var message Message
			if err := message.UnmarshalBinary(wire); err == nil {
				t.Fatalf("section %x accepted unsupported key %x", section, key[0])
			}
		}
	}
}

func TestAnnotationDecoderPreservesProtocolAndLegacyScalarKeys(t *testing.T) {
	for _, key := range [][]byte{{0xa3, 1, 'k'}, {0xa1, 1, 'k'}, {0x53, 1}, {0x55, 1}} {
		wire := []byte{0, 0x53, 0x72, 0xc1, byte(len(key) + 2), 2}
		wire = append(wire, key...)
		wire = append(wire, 0x40)
		var message Message
		if err := message.UnmarshalBinary(wire); err != nil {
			t.Fatalf("scalar key %x: %v", key[0], err)
		}
		if len(message.Annotations) != 1 {
			t.Fatal("annotation lost")
		}
	}
}
