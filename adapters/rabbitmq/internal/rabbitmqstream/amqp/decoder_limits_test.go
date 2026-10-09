package amqp

import (
	"bytes"
	"testing"
)

func TestDecoderRejectsListCrossingDeclaredBoundary(t *testing.T) {
	// A list declaring only its count cannot borrow the following null value.
	var m Message
	if err := m.UnmarshalBinary([]byte{0, 0x53, 0x77, 0xc0, 1, 1, 0x40}); err == nil {
		t.Fatal("list consumed a value outside its declared body")
	}
}

func TestDecoderAMQPFiniteCountsDepthAndVariableBytes(t *testing.T) {
	cases := [][]byte{
		{0, 0x53, 0x77, 0xe0, 2, 3, 0x43},                   // three zero-width uints exceed a two-value array budget
		{0, 0x53, 0x77, 0xc1, 2, 1, 0x40},                   // odd map count
		{0, 0x53, 0x77, 0xa1, 3, 'a', 'b', 'c'},             // variable byte budget
		{0, 0x53, 0x77, 0xc0, 7, 1, 0xc0, 4, 1, 0xc0, 1, 0}, // valid nested lists exceed depth
		{0, 0x53, 0x77, 0xb0, 0xff, 0xff, 0xff, 0xff},       // missing binary body
	}
	for i, wire := range cases {
		l := DefaultDecoderLimits()
		l.MaxContainerValues = 2
		l.MaxValueBytes = 2
		l.MaxDepth = 2
		var m Message
		if err := m.UnmarshalBinaryWithLimits(wire, l); err == nil {
			t.Fatalf("case %d accepted malformed or over-budget value", i)
		}
	}
}

func TestDecoderAMQPExactBoundariesAndSections(t *testing.T) {
	array := []byte{0, 0x53, 0x77, 0xe0, 2, 2, 0x43}
	limits := DefaultDecoderLimits()
	limits.MaxContainerValues = 2
	limits.MaxMessageBytes = len(array)
	var got Message
	if e := got.UnmarshalBinaryWithLimits(array, limits); e != nil {
		t.Fatal(e)
	}
	nested := []byte{0, 0x53, 0x77, 0xc0, 7, 1, 0xc0, 4, 1, 0xc0, 1, 0}
	if e := got.UnmarshalBinary(nested); e != nil {
		t.Fatal(e)
	}
	message := &Message{Header: &MessageHeader{Durable: true}, Properties: &MessageProperties{MessageID: "id", Subject: "subject"}, ApplicationProperties: map[string]any{"key": "value"}, Data: [][]byte{{1, 2}, {3}}}
	wire, e := message.MarshalBinary()
	if e != nil {
		t.Fatal(e)
	}
	if e = got.UnmarshalBinary(wire); e != nil {
		t.Fatal(e)
	}
	if got.Properties.Subject != "subject" || got.ApplicationProperties["key"] != "value" || !got.Header.Durable {
		t.Fatal("message sections changed")
	}
}

func TestDecoderAMQPPreservesDataValuesAndFullDescriptor(t *testing.T) {
	for _, value := range []any{nil, true, uint32(0), "hello", []byte{1, 2}, []uint32{0, 1}, []any{"a", uint64(2)}, map[string]any{"a": int32(1)}} {
		m := &Message{Value: value}
		wire, e := m.MarshalBinary()
		if e != nil {
			t.Fatal(e)
		}
		var got Message
		if e = got.UnmarshalBinary(wire); e != nil {
			t.Fatalf("%T: %v", value, e)
		}
	}
	wire := []byte{0, 0x80, 0, 0, 0, 0, 0, 0, 0, 0x75, 0xa0, 2, 1, 2}
	var got Message
	if e := got.UnmarshalBinary(wire); e != nil {
		t.Fatal(e)
	}
	if len(got.Data) != 1 || !bytes.Equal(got.Data[0], []byte{1, 2}) {
		t.Fatal("full descriptor lost data")
	}
}
