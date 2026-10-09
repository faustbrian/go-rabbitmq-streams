package amqp

import "testing"

func TestValueMapRejectsNonComparableDescribedKeysWithoutPanic(t *testing.T) {
	for _, value := range [][]byte{{0xa0, 1, 1}, {0xc0, 2, 1, 0x40}} {
		t.Run(string(rune(value[0])), func(t *testing.T) {
			defer func() {
				if recover() != nil {
					t.Error("bounded value map decode panicked on a non-comparable described key")
				}
			}()
			key := append([]byte{0, 0x53, 1}, value...)
			wire := []byte{0, 0x53, 0x77, 0xc1, byte(len(key) + 2), 2}
			wire = append(wire, key...)
			wire = append(wire, 0x40)
			var message Message
			if err := message.UnmarshalBinary(wire); err == nil {
				t.Fatal("non-comparable described key was accepted")
			}
		})
	}
}

func TestValueMapPreservesComparableKeys(t *testing.T) {
	for _, key := range [][]byte{{0x40}, {0xa1, 1, 'k'}, {0, 0x53, 1, 0xa1, 1, 'k'}} {
		wire := []byte{0, 0x53, 0x77, 0xc1, byte(len(key) + 2), 2}
		wire = append(wire, key...)
		wire = append(wire, 0x40)
		var message Message
		if err := message.UnmarshalBinary(wire); err != nil {
			t.Fatal("comparable map key was rejected:", err)
		}
		switch got := message.Value.(type) {
		case map[any]any:
			if len(got) != 1 {
				t.Fatal("comparable map entry was lost")
			}
		case map[string]any:
			if len(got) != 1 || got["k"] != nil {
				t.Fatal("string map entry changed")
			}
		default:
			t.Fatalf("comparable map entry was not retained: %T", message.Value)
		}
	}
}
