package stream

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/faustbrian/go-rabbitmq-streams/adapters/rabbitmq/v2/internal/rabbitmqstream/amqp"
)

var ErrDecoderLimit = errors.New("stream decoder limit exceeded")

// DecoderLimits are independent admission budgets. MaxMessageBytes includes
// the AMQP wire envelope, not just payload. Chunk limits apply to aggregates.
// Configure before opening an environment; do not mutate a live policy.
type DecoderLimits struct {
	MaxFrameBytes    int
	MaxMessageBytes  int
	MaxChunkBytes    int
	MaxChunkValues   int
	MaxRecords       int
	MaxProtocolItems int
	AMQP             amqp.DecoderLimits
}

func DefaultDecoderLimits() DecoderLimits {
	return DecoderLimits{MaxFrameBytes: 16 << 20, MaxMessageBytes: 2 << 20, MaxChunkBytes: 16 << 20, MaxChunkValues: 1 << 20, MaxRecords: 65536, MaxProtocolItems: 4096, AMQP: amqp.DefaultDecoderLimits()}
}
func (l DecoderLimits) Validate() error {
	for _, n := range []int{l.MaxFrameBytes, l.MaxMessageBytes, l.MaxChunkBytes, l.MaxChunkValues, l.MaxRecords, l.MaxProtocolItems} {
		if n <= 0 || n > 1<<30 {
			return fmt.Errorf("invalid stream decoder limit")
		}
	}
	if l.MaxFrameBytes < 8 {
		return fmt.Errorf("frame budget is smaller than protocol header")
	}
	return l.AMQP.Validate()
}

// SetDecoderLimits copies a finite validated policy into the environment options.
// A zero value is not accepted here; DefaultDecoderLimits supplies defaults.
func (o *EnvironmentOptions) SetDecoderLimits(l DecoderLimits) error {
	if err := l.Validate(); err != nil {
		return err
	}
	if o.TCPParameters == nil {
		o.TCPParameters = newTCPParameterDefault()
	}
	p := *o.TCPParameters
	p.DecoderLimits = l
	o.TCPParameters = &p
	return nil
}
func (c *Client) decoderLimits() DecoderLimits {
	if c.tcpParameters == nil || c.tcpParameters.DecoderLimits == (DecoderLimits{}) {
		return DefaultDecoderLimits()
	}
	return c.tcpParameters.DecoderLimits
}

// readInboundFrame admits length before allocation and returns only this frame.
func readInboundFrame(r io.Reader, max int) ([]byte, error) {
	if max < 8 || max > 1<<30 {
		return nil, fmt.Errorf("invalid frame byte budget")
	}
	n, e := readUInt(r)
	if e != nil {
		return nil, e
	}
	if n < 4 {
		return nil, fmt.Errorf("short stream frame")
	}
	if uint64(n)+4 > uint64(max) {
		return nil, ErrDecoderLimit
	}
	b := make([]byte, int(n))
	_, e = io.ReadFull(r, b)
	return b, e
}

type frameCursor struct {
	b        []byte
	i, items int
	limits   DecoderLimits
	err      error
}

func (r *frameCursor) take(n int) []byte {
	if r.err != nil {
		return nil
	}
	if n < 0 || n > len(r.b)-r.i {
		r.err = io.ErrUnexpectedEOF
		return nil
	}
	b := r.b[r.i : r.i+n]
	r.i += n
	return b
}
func (r *frameCursor) u16() uint16 {
	b := r.take(2)
	if b == nil {
		return 0
	}
	return binary.BigEndian.Uint16(b)
}
func (r *frameCursor) u32() uint32 {
	b := r.take(4)
	if b == nil {
		return 0
	}
	return binary.BigEndian.Uint32(b)
}
func (r *frameCursor) str() { r.take(int(r.u16())) }
func (r *frameCursor) count(width int) int {
	n := r.u32()
	if r.err != nil {
		return 0
	}
	if int64(n) > int64(r.limits.MaxProtocolItems-r.items) || width > 0 && int64(n) > int64((len(r.b)-r.i)/width) {
		r.err = ErrDecoderLimit
		return 0
	}
	r.items += int(n)
	return int(n)
}

// Preflight all response schemas before attributed handlers allocate or loop.
// The cursor and handlers see the same admitted, immutable frame-local bytes.
func validateInboundFrame(frame []byte, l DecoderLimits) error {
	if err := l.Validate(); err != nil {
		return err
	}
	r := frameCursor{b: frame, limits: l}
	key := r.u16()
	cmd := uShortExtractResponseCode(key)
	version := r.u16()
	if version != version1 {
		return fmt.Errorf("unsupported incoming frame version")
	}
	rpc := func() { r.take(6) }
	pairs := func() {
		for n := r.count(4); n > 0; n-- {
			r.str()
			r.str()
		}
	}
	strings := func() {
		for n := r.count(2); n > 0; n-- {
			r.str()
		}
	}
	switch cmd {
	case commandPeerProperties:
		rpc()
		pairs()
	case commandOpen:
		rpc()
		// The pinned server omits the count itself for an empty property map.
		if r.i < len(frame) {
			pairs()
		}
	case commandSaslHandshake:
		rpc()
		strings()
	case commandTune:
		r.take(8)
	case commandDeclarePublisher, CommandDeletePublisher, commandDeleteStream, commandCreateStream, commandSubscribe, CommandUnsubscribe, commandCreateSuperStream, commandDeleteSuperStream:
		rpc()
	case commandSaslAuthenticate:
		rpc()
		if r.i < len(frame) {
			r.str()
		}
	case commandQueryPublisherSequence, CommandQueryOffset:
		rpc()
		r.take(8)
	case commandQueryPartition, commandQueryRoute:
		rpc()
		strings()
	case commandPublishConfirm:
		r.take(1)
		r.take(r.count(8) * 8)
	case commandPublishError:
		r.take(1)
		r.take(r.count(10) * 10)
	case commandCredit:
		r.take(3)
	case commandHeartbeat:
	case CommandMetadataUpdate:
		r.take(2)
		r.str()
	case CommandClose:
		rpc()
		if key&0x8000 == 0 {
			r.str()
		}
	case consumerUpdateQueryResponse:
		r.take(6)
	case commandExchangeVersion:
		rpc()
		r.take(r.count(6) * 6)
	case commandStreamStatus:
		rpc()
		for n := r.count(10); n > 0; n-- {
			r.str()
			r.take(8)
		}
	case commandMetadata:
		r.take(4)
		for n := r.count(8); n > 0; n-- {
			r.take(2)
			r.str()
			r.take(4)
		}
		for n := r.count(10); n > 0; n-- {
			r.str()
			r.take(4)
			r.take(r.count(2) * 2)
		}
	case commandDeliver:
		// Complete chunk/message structural checks are performed before allocation
		// by decodeDelivery; framing still isolates all delivery bytes here.
		r.take(len(r.b) - r.i)
	default:
		return fmt.Errorf("unsupported incoming command")
	}
	if r.err != nil {
		return r.err
	}
	if r.i != len(frame) {
		return fmt.Errorf("stream frame size mismatch")
	}
	return nil
}

// Response channels are one-slot mailboxes owned by the waiting operation.
// Duplicate/full delivery is protocol-invalid, not a reason to stall the reader.
func (c *Client) deliverCode(res *Response, v Code) {
	select {
	case res.code <- v:
	default:
		_ = c.socket.abort()
	}
}
func (c *Client) deliverData(res *Response, v any) {
	select {
	case res.data <- v:
	default:
		_ = c.socket.abort()
	}
}

func (c *Client) dispatchChunk(consumer *Consumer, chunk chunkInfo) bool {
	select {
	case <-c.socket.done:
		return false
	case <-consumer.closeCh:
		return false
	default:
	}
	select {
	case <-c.socket.done:
		return false
	case <-consumer.closeCh:
		return false
	case consumer.chunkForConsumer <- chunk:
		return true
	}
}

func finiteDecodedReader(r io.Reader, limit int) ([]byte, error) {
	b, e := io.ReadAll(io.LimitReader(r, int64(limit)+1))
	if e != nil {
		return nil, e
	}
	if len(b) > limit {
		return nil, ErrDecoderLimit
	}
	return b, nil
}
