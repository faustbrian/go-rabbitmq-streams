package amqp

import (
	"bytes"
	"encoding/binary"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestEncoderSizeAndCountBounds(t *testing.T) {
	for _, n := range []int{-1, math.MaxInt} {
		if _, err := checkedWireSize(n); err == nil {
			t.Errorf("accepted size %d", n)
		}
	}
	for _, n := range []int{-1, math.MaxUint32/2 + 1, math.MaxInt} {
		if _, err := mapWireCount(n); err == nil {
			t.Errorf("accepted map pair count %d", n)
		}
	}
	if got, err := checkedWireSize(math.MaxUint32); err != nil || got != math.MaxUint32 {
		t.Fatalf("maximum wire size: %d, %v", got, err)
	}
	if got, err := mapWireCount(math.MaxUint32 / 2); err != nil || got != math.MaxUint32-1 {
		t.Fatalf("maximum map pair count: %d, %v", got, err)
	}
}

func TestArrayHeadersPreserveWireWidthBoundaries(t *testing.T) {
	for _, n := range []int{0, 253, 254, math.MaxUint32 - array32TLSize} {
		var wire buffer
		if err := writeArrayHeader(&wire, n, 1, typeCodeByte); err != nil {
			t.Fatal(err)
		}
		if n <= 253 {
			if got := wire.bytes(); !reflect.DeepEqual(got, []byte{byte(typeCodeArray8), byte(n + 2), byte(n), byte(typeCodeByte)}) {
				t.Fatalf("small header: %x", got)
			}
		} else {
			got := wire.bytes()
			if len(got) != 10 || got[0] != byte(typeCodeArray32) || binary.BigEndian.Uint32(got[1:5]) != uint32(n+5) || binary.BigEndian.Uint32(got[5:9]) != uint32(n) {
				t.Fatalf("large header: %x", got)
			}
		}
	}
}

func TestArrayHeaderRejectsUnrepresentableArithmetic(t *testing.T) {
	for _, test := range []struct{ length, width int }{
		{-1, 1}, {1, -1}, {math.MaxInt, 16}, {math.MaxInt, 0}, {math.MaxUint32/16 + 1, 16},
	} {
		var wire buffer
		if err := writeArrayHeader(&wire, test.length, test.width, typeCodeLong); err == nil {
			t.Errorf("invalid header (%d, %d) returned no error", test.length, test.width)
		}
		if wire.len() != 0 {
			t.Errorf("invalid header (%d, %d) emitted %x", test.length, test.width, wire.bytes())
		}
	}
}

func TestVariableArrayHeaderRejectsUnrepresentableArithmetic(t *testing.T) {
	for _, test := range []struct{ length, total int }{
		{-1, 1}, {1, -1}, {math.MaxInt, 1}, {1, math.MaxInt}, {1, math.MaxUint32 - 5},
	} {
		var wire buffer
		if err := writeVariableArrayHeader(&wire, test.length, test.total, typeCodeStr32); err == nil {
			t.Errorf("invalid header (%d, %d) returned no error", test.length, test.total)
		}
		if wire.len() != 0 {
			t.Errorf("invalid header (%d, %d) emitted %x", test.length, test.total, wire.bytes())
		}
	}
}

func TestSignedWireRoundTrip(t *testing.T) {
	for _, want := range []any{
		int8(-128), int16(-32768), int32(-1), int32(-128), int32(math.MinInt32),
		int64(-1), int64(-128), int64(math.MinInt64),
		[]int8{-128, -1, 0, 127}, []int16{-32768, -1, 0, 32767},
		[]int32{-128, -1, 127}, []int32{math.MinInt32, -129}, []int32{math.MaxInt32},
		[]int64{-128, -1, 127}, []int64{math.MinInt64, -129}, []int64{128, 255}, []int64{math.MaxInt64},
		time.Unix(-1, 123000000).UTC(), []time.Time{time.Unix(-1, 123000000).UTC()},
	} {
		var wire buffer
		if err := marshal(&wire, want); err != nil {
			t.Fatal(err)
		}
		got, err := readAny(&wire)
		if err != nil || !reflect.DeepEqual(got, want) {
			t.Errorf("%T: got %#v, want %#v, error %v", want, got, want, err)
		}
	}
}

func TestUnsignedArrayWidthRoundTrip(t *testing.T) {
	for _, want := range []any{[]uint32{0, 255}, []uint32{256, math.MaxUint32}, []uint64{0, 255}, []uint64{256, math.MaxUint64}} {
		var wire buffer
		if err := marshal(&wire, want); err != nil {
			t.Fatal(err)
		}
		got, err := readAny(&wire)
		if err != nil || !reflect.DeepEqual(got, want) {
			t.Fatalf("got %#v, want %#v, error %v", got, want, err)
		}
	}
}

func TestVariableArrayElementWidthRoundTrip(t *testing.T) {
	for _, size := range []int{255, 256} {
		str := strings.Repeat("a", size)
		for _, want := range []any{[]string{str, "b"}, []symbol{symbol(str), "b"}, [][]byte{bytes.Repeat([]byte{'a'}, size), {'b'}}} {
			var wire buffer
			if err := marshal(&wire, want); err != nil {
				t.Fatal(err)
			}
			got, err := readAny(&wire)
			if err != nil || !reflect.DeepEqual(got, want) {
				t.Fatalf("size %d: got %#v, want %#v, error %v", size, got, want, err)
			}
		}
	}
}
