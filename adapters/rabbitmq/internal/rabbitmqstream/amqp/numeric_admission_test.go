package amqp

import (
	"encoding/binary"
	"math"
	"testing"
	"time"
)

func TestCompositeDescriptorRejectsNarrowing(t *testing.T) {
	for _, descriptor := range []uint64{uint64(typeCodeMessageHeader) + 256, math.MaxUint64} {
		wire := binary.BigEndian.AppendUint64([]byte{0, byte(typeCodeUlong)}, descriptor)
		wire = append(wire, byte(typeCodeList0))
		if _, _, err := readCompositeHeader(&buffer{b: wire}); err == nil {
			t.Fatal("composite descriptor narrowed into the supported type space")
		}
	}
}

func TestIntegerDecodeRejectsOutOfRangeUnsignedValue(t *testing.T) {
	wire := binary.BigEndian.AppendUint64([]byte{byte(typeCodeUlong)}, uint64(math.MaxInt)+1)
	if _, err := readInt(&buffer{b: wire}); err == nil {
		t.Fatal("unsigned value outside int range was accepted")
	}
}

func TestMillisecondsRejectsOverflowBeforeConversion(t *testing.T) {
	for _, value := range []uint64{uint64(math.MaxUint32) + 1, math.MaxUint64} {
		wire := binary.BigEndian.AppendUint64([]byte{byte(typeCodeUlong)}, value)
		var ttl milliseconds
		if err := ttl.unmarshal(&buffer{b: wire}); err == nil {
			t.Fatal("TTL outside protocol uint range was accepted")
		}
	}
}

func TestMillisecondsRejectsInvalidEncodeRange(t *testing.T) {
	for _, test := range []struct {
		name  string
		value milliseconds
	}{{"negative", -1}, {"oversized", milliseconds(math.MaxInt64)}} {
		t.Run(test.name, func(t *testing.T) {
			var wire buffer
			if err := test.value.marshal(&wire); err == nil {
				t.Fatal("TTL outside protocol uint range was encoded")
			}
		})
	}
}

func TestMillisecondsPreservesProtocolMaximum(t *testing.T) {
	want := milliseconds(time.Duration(math.MaxUint32) * time.Millisecond)
	var wire buffer
	if err := want.marshal(&wire); err != nil {
		t.Fatal(err)
	}
	var got milliseconds
	if err := got.unmarshal(&wire); err != nil || got != want {
		t.Fatalf("protocol maximum TTL: got %v, error %v", got, err)
	}
}
