package stream

import (
	"encoding/binary"
	"fmt"
	"math"

	"github.com/faustbrian/go-rabbitmq-streams/adapters/rabbitmq/v2/internal/rabbitmqstream/amqp"
)

// decodeDelivery uses frame-local slices. Counts and encoded lengths are
// admitted before allocation, then decoded output consumes one aggregate budget.
func decodeDelivery(body []byte, limits DecoderLimits) (byte, chunkInfo, uint32, uint32, []byte, error) {
	var chunk chunkInfo
	fail := func(e error) (byte, chunkInfo, uint32, uint32, []byte, error) { return 0, chunk, 0, 0, nil, e }
	if err := limits.Validate(); err != nil {
		return fail(err)
	}
	r := frameCursor{b: body, limits: limits}
	id := r.take(1)
	magic := r.take(1)
	kind := r.take(1)
	entries := r.u16()
	records := r.u32()
	r.take(16)
	off := r.take(8)
	crc := r.u32()
	size := r.u32()
	trailer := r.u32()
	bloomAndReserved := r.take(4)
	if r.err != nil {
		return fail(r.err)
	}
	if (magic[0] != 0x50 && magic[0] != 0x51) || kind[0] != 0 {
		return fail(fmt.Errorf("invalid chunk header"))
	}
	if int64(records) > int64(limits.MaxRecords) || int64(records)*4 > int64(limits.MaxChunkBytes) || int(entries) > limits.MaxRecords || entries == 0 && records != 0 || uint32(entries) > records {
		return fail(ErrDecoderLimit)
	}
	bloomSize := int(bloomAndReserved[0])
	remainingBytes := int64(len(body) - r.i)
	if int64(size) > int64(limits.MaxChunkBytes) {
		return fail(ErrDecoderLimit)
	}
	// Osiris offset/user_data sendfile strips bloom and trailer bytes but keeps
	// the on-disk header. Also accept complete chunk data without trusting either
	// metadata length to escape this frame's actual boundary.
	switch remainingBytes {
	case int64(size):
		trailer = 0
	case int64(size) + int64(trailer):
	case int64(size) + int64(trailer) + int64(bloomSize):
		r.take(bloomSize)
	default:
		return fail(fmt.Errorf("chunk data length mismatch"))
	}
	data := r.take(int(size))
	r.take(int(trailer))
	if r.err != nil {
		return fail(r.err)
	}
	rawOffset := binary.BigEndian.Uint64(off)
	if rawOffset > math.MaxInt64 {
		return fail(fmt.Errorf("chunk offset overflow"))
	}
	offset := int64(rawOffset)
	if int64(records) > int64(math.MaxInt64-offset) {
		return fail(fmt.Errorf("chunk offset overflow"))
	}
	chunk.numEntries = entries
	// Capacity is admitted by MaxRecords, independently of compressed byte size.
	chunk.offsetMessages = make(offsetMessages, 0, int(records))
	d := frameCursor{b: data, limits: limits}
	remaining := records
	decodedBytes := 0
	remainingValues := limits.MaxChunkValues
	decode := func(messages []byte, count uint32) error {
		m := frameCursor{b: messages, limits: limits}
		for i := uint32(0); i < count; i++ {
			n := m.u32()
			if m.err != nil {
				return m.err
			}
			if int64(n) > int64(limits.MaxMessageBytes) {
				return ErrDecoderLimit
			}
			wire := m.take(int(n))
			if m.err != nil {
				return m.err
			}
			message, e := amqp.DecodeMessageWithBudget(wire, limits.AMQP, &remainingValues)
			if e != nil {
				return e
			}
			chunk.offsetMessages = append(chunk.offsetMessages, &offsetMessage{message: message, offset: offset})
			offset++
		}
		if m.i != len(messages) {
			return fmt.Errorf("chunk message count/size mismatch")
		}
		return nil
	}
	for entry := 0; entry < int(entries); entry++ {
		if remaining == 0 {
			return fail(fmt.Errorf("chunk entry/record mismatch"))
		}
		if d.i >= len(data) {
			return fail(fmt.Errorf("truncated chunk entry"))
		}
		if data[d.i]&0x80 == 0 {
			start := d.i
			n := d.u32()
			if d.err != nil {
				return fail(d.err)
			}
			if int64(n) > int64(limits.MaxMessageBytes) || int(n) > limits.MaxChunkBytes-decodedBytes-4 {
				return fail(ErrDecoderLimit)
			}
			d.take(int(n))
			if d.err != nil {
				return fail(d.err)
			}
			decodedBytes += int(n) + 4
			if e := decode(data[start:d.i], 1); e != nil {
				return fail(e)
			}
			remaining--
		} else {
			flags := d.take(1)[0]
			count := uint32(d.u16())
			uncompressed := d.u32()
			compressed := d.u32()
			if d.err != nil {
				return fail(d.err)
			}
			if flags&0x0f != 0 || count == 0 || count > remaining {
				return fail(fmt.Errorf("invalid sub-entry count/flags"))
			}
			if int64(count)*4 > int64(uncompressed) {
				return fail(fmt.Errorf("sub-entry cannot contain declared records"))
			}
			if int64(uncompressed) > int64(limits.MaxChunkBytes-decodedBytes) || int64(compressed) > int64(len(data)-d.i) {
				return fail(ErrDecoderLimit)
			}
			wire := d.take(int(compressed))
			if d.err != nil {
				return fail(d.err)
			}
			messages, e := decodeCompressed(wire, (flags&0x70)>>4, int(uncompressed), limits.MaxChunkBytes-decodedBytes)
			if e != nil {
				return fail(e)
			}
			decodedBytes += len(messages)
			if e = decode(messages, count); e != nil {
				return fail(e)
			}
			remaining -= count
		}
	}
	if remaining != 0 || d.i != len(data) {
		return fail(fmt.Errorf("chunk record/entry size mismatch"))
	}
	return id[0], chunk, records, crc, data, nil
}
