package amqp

import "testing"

func FuzzBoundedMessageDecoder(f *testing.F) {
	wire, err := NewMessage([]byte("bounded")).MarshalBinary()
	if err != nil {
		f.Fatal(err)
	}
	f.Add(wire)
	f.Add([]byte{})
	f.Add([]byte{0, 0x53, 0x77, 0xc1, 2, 1, 0x40})
	limits := DecoderLimits{MaxMessageBytes: 4096, MaxValueBytes: 4096, MaxContainerValues: 128, MaxValues: 128, MaxDepth: 8}
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > limits.MaxMessageBytes {
			return
		}
		var decoded Message
		err := decoded.UnmarshalBinaryWithLimits(data, limits)
		remaining := limits.MaxValues
		message, budgetErr := DecodeMessageWithBudget(data, limits, &remaining)
		if (err == nil) != (budgetErr == nil) {
			t.Fatal("single-message and aggregate admission disagree")
		}
		if remaining < 0 || remaining > limits.MaxValues {
			t.Fatal("aggregate value accounting escaped its initial budget")
		}
		if budgetErr == nil && message == nil {
			t.Fatal("successful bounded decode returned no message")
		}
	})
}
