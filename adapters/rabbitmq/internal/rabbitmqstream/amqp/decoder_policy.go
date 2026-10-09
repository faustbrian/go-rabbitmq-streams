package amqp

import (
	"encoding/binary"
	"fmt"
	"unicode/utf8"
)

// DecoderLimits bounds encoded bytes and work independently of payload size.
// A zero limits value selects finite defaults; partially zero policies are invalid.
type DecoderLimits struct {
	MaxMessageBytes    int
	MaxValueBytes      int
	MaxContainerValues int
	MaxValues          int
	MaxDepth           int
}

func DefaultDecoderLimits() DecoderLimits {
	return DecoderLimits{2 << 20, 2 << 20, 4096, 65536, 32}
}

func (l DecoderLimits) Validate() error {
	for _, n := range []int{l.MaxMessageBytes, l.MaxValueBytes, l.MaxContainerValues, l.MaxValues, l.MaxDepth} {
		if n <= 0 || n > 1<<30 {
			return fmt.Errorf("invalid AMQP decoder limit")
		}
	}
	if l.MaxDepth > 128 {
		return fmt.Errorf("AMQP decoder depth exceeds 128")
	}
	return nil
}

// UnmarshalBinaryWithLimits admits the complete wire structure before the existing
// semantic decoder can allocate count-driven containers or copy variable values.
func (m *Message) UnmarshalBinaryWithLimits(data []byte, limits DecoderLimits) error {
	if _, err := admitMessage(data, limits); err != nil {
		return err
	}
	return m.unmarshal(&buffer{b: data})
}

// DecodeMessageWithBudget charges the actual admitted structural value count to
// a caller-owned aggregate budget before allocating a message or its containers.
// The budget must be exclusively owned by this decoding operation. On any error
// the caller discards the aggregate; already charged values are not refunded.
func DecodeMessageWithBudget(data []byte, limits DecoderLimits, remaining *int) (*Message, error) {
	if remaining == nil || *remaining < 0 {
		return nil, fmt.Errorf("invalid aggregate AMQP value budget")
	}
	if limits == (DecoderLimits{}) {
		limits = DefaultDecoderLimits()
	}
	if *remaining < limits.MaxValues {
		limits.MaxValues = *remaining
	}
	if limits.MaxValues == 0 {
		return nil, fmt.Errorf("aggregate AMQP value budget exhausted")
	}
	count, err := admitMessage(data, limits)
	if err != nil {
		return nil, err
	}
	*remaining -= count
	m := &Message{}
	if err = m.unmarshal(&buffer{b: data}); err != nil {
		return nil, err
	}
	return m, nil
}

func admitMessage(data []byte, limits DecoderLimits) (int, error) {
	if limits == (DecoderLimits{}) {
		limits = DefaultDecoderLimits()
	}
	if err := limits.Validate(); err != nil {
		return 0, err
	}
	if len(data) > limits.MaxMessageBytes {
		return 0, fmt.Errorf("AMQP message byte limit exceeded")
	}
	s := wireScanner{data: data, limits: limits}
	for s.pos < len(data) {
		if err := s.value(0); err != nil {
			return 0, err
		}
	}
	return s.values, nil
}

// wireScanner performs structural admission only; the attributed AMQP codec
// remains the authority for message sections and semantic types.
type wireScanner struct {
	data        []byte
	pos, values int
	limits      DecoderLimits
}

func (s *wireScanner) take(n int) ([]byte, error) {
	if n < 0 || n > len(s.data)-s.pos {
		return nil, fmt.Errorf("truncated AMQP value")
	}
	b := s.data[s.pos : s.pos+n]
	s.pos += n
	return b, nil
}
func (s *wireScanner) number(width int) (int, error) {
	b, e := s.take(width)
	if e != nil {
		return 0, e
	}
	if width == 1 {
		return int(b[0]), nil
	}
	n := binary.BigEndian.Uint32(b)
	if uint64(n) > uint64(^uint(0)>>1) {
		return 0, fmt.Errorf("AMQP length exceeds integer range")
	}
	return int(n), nil
}
func (s *wireScanner) value(depth int) error {
	b, e := s.take(1)
	if e != nil {
		return e
	}
	return s.payload(b[0], depth)
}
func (s *wireScanner) payload(code byte, depth int) error {
	s.values++
	if depth > s.limits.MaxDepth || s.values > s.limits.MaxValues {
		return fmt.Errorf("AMQP value work limit exceeded")
	}
	width := 0
	switch code {
	case 0x00:
		if err := s.value(depth + 1); err != nil {
			return err
		}
		return s.value(depth + 1)
	case 0x40, 0x41, 0x42, 0x43, 0x44, 0x45:
		return nil
	case 0x50, 0x51, 0x52, 0x53, 0x54, 0x55, 0x56:
		width = 1
	case 0x60, 0x61:
		width = 2
	case 0x70, 0x71, 0x72, 0x73, 0x74:
		width = 4
	case 0x80, 0x81, 0x82, 0x83, 0x84:
		width = 8
	case 0x94, 0x98:
		width = 16
	case 0xa0, 0xa1, 0xa3, 0xb0, 0xb1, 0xb3:
		w := 1
		if code >= 0xb0 {
			w = 4
		}
		n, e := s.number(w)
		if e != nil {
			return e
		}
		if n > s.limits.MaxValueBytes {
			return fmt.Errorf("AMQP variable value byte limit exceeded")
		}
		b, e := s.take(n)
		if e != nil {
			return e
		}
		if (code == 0xa1 || code == 0xb1) && !utf8.Valid(b) {
			return fmt.Errorf("invalid AMQP UTF-8")
		}
		if code == 0xa3 || code == 0xb3 {
			for _, v := range b {
				if v > 127 {
					return fmt.Errorf("invalid AMQP symbol")
				}
			}
		}
		return nil
	case 0xc0, 0xc1, 0xd0, 0xd1, 0xe0, 0xf0:
		w := 1
		if code == 0xd0 || code == 0xd1 || code == 0xf0 {
			w = 4
		}
		n, e := s.number(w)
		if e != nil {
			return e
		}
		body, e := s.take(n)
		if e != nil {
			return e
		}
		child := wireScanner{data: body, limits: s.limits, values: s.values}
		count, e := child.number(w)
		if e != nil {
			return e
		}
		if count > s.limits.MaxContainerValues || count > s.limits.MaxValues-s.values {
			return fmt.Errorf("AMQP container count limit exceeded")
		}
		if (code == 0xc1 || code == 0xd1) && count%2 != 0 {
			return fmt.Errorf("odd AMQP map count")
		}
		array := code == 0xe0 || code == 0xf0
		var constructor byte
		if array {
			b, e := child.take(1)
			if e != nil {
				return e
			}
			constructor = b[0]
			// Described array constructors carry one descriptor, not one per item.
			if constructor == 0 {
				if e = child.value(depth + 1); e != nil {
					return e
				}
				b, e = child.take(1)
				if e != nil {
					return e
				}
				constructor = b[0]
			}
			if constructor == 0x40 || constructor == 0 {
				return fmt.Errorf("invalid AMQP array constructor")
			}
		}
		for i := 0; i < count; i++ {
			if array {
				e = child.payload(constructor, depth+1)
			} else {
				e = child.value(depth + 1)
			}
			if e != nil {
				return e
			}
		}
		if child.pos != len(body) {
			return fmt.Errorf("AMQP container size/count mismatch")
		}
		s.values = child.values
		return nil
	default:
		return fmt.Errorf("unsupported AMQP constructor")
	}
	_, e := s.take(width)
	return e
}
