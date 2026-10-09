package stream

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/faustbrian/go-rabbitmq-streams/adapters/rabbitmq/v2/internal/rabbitmqstream/amqp"
)

func TestDecoderDuplicateResponseCannotBlockReader(t *testing.T) {
	c, _, _ := pipeContextClient(t)
	resp, allocationErr := c.coordinator.NewResponse(commandPeerProperties)
	if allocationErr != nil {
		t.Fatal(allocationErr)
	}
	resp.code <- Code{id: responseCodeOk}
	resp.data <- "already delivered"
	var body bytes.Buffer
	writeUInt(&body, uint32(resp.correlationid))
	writeUShort(&body, responseCodeOk)
	writeUInt(&body, 0)
	done := make(chan struct{})
	go func() { c.handlePeerProperties(&ReaderProtocol{}, bufio.NewReader(&body)); close(done) }()
	select {
	case <-done:
	case <-time.After(100 * time.Millisecond):
		// Retire the legacy blocked sends so the failing test owns no goroutine.
		<-resp.code
		<-resp.data
		<-done
		t.Fatal("duplicate response blocked the native reader")
	}
}

func TestDecoderFrameAdmissionAndIsolation(t *testing.T) {
	var wire bytes.Buffer
	writeUInt(&wire, 4)
	writeUShort(&wire, commandHeartbeat)
	writeUShort(&wire, version1)
	writeUInt(&wire, 4)
	writeUShort(&wire, commandHeartbeat)
	writeUShort(&wire, version1)
	frame, e := readInboundFrame(&wire, 8)
	if e != nil {
		t.Fatal(e)
	}
	if len(frame) != 4 || wire.Len() != 8 {
		t.Fatal("frame consumed following frame")
	}
	if e = validateInboundFrame(frame, DefaultDecoderLimits()); e != nil {
		t.Fatal(e)
	}
	if _, e = readInboundFrame(bytes.NewReader([]byte{0, 0, 0, 17}), 16); !errors.Is(e, ErrDecoderLimit) {
		t.Fatalf("length admitted before body: %v", e)
	}
	var malformed bytes.Buffer
	writeUShort(&malformed, commandPeerProperties)
	writeUShort(&malformed, version1)
	writeUInt(&malformed, 1)
	writeUShort(&malformed, responseCodeOk)
	writeUInt(&malformed, 2)
	if e = validateInboundFrame(malformed.Bytes(), DefaultDecoderLimits()); e == nil {
		t.Fatal("truncated metadata accepted")
	}
}

type countedReader struct{ n int }

func (r *countedReader) Read(p []byte) (int, error) {
	if r.n >= 8 {
		return 0, io.EOF
	}
	n := copy(p, []byte{1, 2, 3, 4, 5, 6, 7, 8}[r.n:])
	r.n += n
	return n, nil
}
func TestDecoderActualOutputBudget(t *testing.T) {
	r := &countedReader{}
	_, e := finiteDecodedReader(r, 4)
	if !errors.Is(e, ErrDecoderLimit) || r.n > 5 {
		t.Fatalf("output read=%d err=%v", r.n, e)
	}
}

func deliveryFixture(t testing.TB, codec byte) []byte {
	t.Helper()
	wire, e := amqp.NewMessage([]byte{1, 2}).MarshalBinary()
	if e != nil {
		t.Fatal(e)
	}
	var data bytes.Buffer
	if codec == 255 {
		writeUInt(&data, uint32(len(wire)))
		data.Write(wire)
	} else {
		entries := &subEntries{items: []*subEntry{{messages: []*messageSequence{{messageBytes: wire}, {messageBytes: wire}}}}}
		if e = compressByValue(codec).Compress(entries); e != nil {
			t.Fatal(e)
		}
		data.WriteByte(0x80 | codec<<4)
		writeUShort(&data, 2)
		writeUInt(&data, uint32(2*(len(wire)+4)))
		writeUInt(&data, uint32(len(entries.items[0].dataInBytes)))
		data.Write(entries.items[0].dataInBytes)
	}
	var b bytes.Buffer
	b.Write([]byte{1, 0x51, 0})
	writeUShort(&b, 1)
	count := uint32(2)
	if codec == 255 {
		count = 1
	}
	writeUInt(&b, count)
	writeLong(&b, 0)
	writeLong(&b, 0)
	writeLong(&b, 5)
	writeUInt(&b, 0)
	writeUInt(&b, uint32(data.Len()))
	writeUInt(&b, 0)
	writeUInt(&b, 0)
	b.Write(data.Bytes())
	return b.Bytes()
}

func TestDecoderChunksPreserveAllCodecsAndRejectMalformedCounts(t *testing.T) {
	for _, codec := range []byte{255, None, GZIP, SNAPPY, LZ4, ZSTD} {
		wire := deliveryFixture(t, codec)
		_, chunk, records, _, _, e := decodeDelivery(wire, DefaultDecoderLimits())
		if e != nil {
			t.Fatalf("codec %d: %v", codec, e)
		}
		if len(chunk.offsetMessages) != int(records) || chunk.offsetMessages[0].offset != 5 {
			t.Fatal("records or offsets changed")
		}
		if !bytes.Equal(chunk.offsetMessages[0].message.Data[0], []byte{1, 2}) {
			t.Fatal("payload changed")
		}
		bad := append([]byte(nil), wire...)
		binary.BigEndian.PutUint32(bad[5:9], 0)
		if _, _, _, _, _, e = decodeDelivery(bad, DefaultDecoderLimits()); e == nil {
			t.Fatalf("codec %d accepted inconsistent records", codec)
		}
		if codec != 255 {
			bad = append([]byte(nil), wire...)
			binary.BigEndian.PutUint16(bad[50:52], 3)
			if _, _, _, _, _, e = decodeDelivery(bad, DefaultDecoderLimits()); e == nil {
				t.Fatal("batch record subtraction underflow admitted")
			}
		}
	}
}

func TestDecoderCodecsBoundActualRatherThanAdvertisedOutput(t *testing.T) {
	for _, codec := range []byte{None, GZIP, SNAPPY, LZ4, ZSTD} {
		wire := deliveryFixture(t, codec)
		// Delivery fixed header is 49 bytes. A sub-entry has an 11-byte header.
		size := int(binary.BigEndian.Uint32(wire[56:60]))
		data := wire[60 : 60+size]
		if _, e := decodeCompressed(data, codec, 1, 1); e == nil {
			t.Fatalf("codec %d accepted output beyond actual budget", codec)
		}
	}
}

func TestDecoderChunkDispatchStopsOnSocketAbort(t *testing.T) {
	c, _, _ := pipeContextClient(t)
	consumer := &Consumer{closeCh: make(chan struct{}), chunkForConsumer: make(chan chunkInfo)}
	_ = c.socket.abort()
	if c.dispatchChunk(consumer, chunkInfo{}) {
		t.Fatal("closed socket admitted chunk")
	}
}

func TestDecoderNegativeRPCResponsesPreserveClassification(t *testing.T) {
	c, _, _ := pipeContextClient(t)
	resp, allocationErr := c.coordinator.NewResponse(CommandQueryOffset)
	if allocationErr != nil {
		t.Fatal(allocationErr)
	}
	var wire bytes.Buffer
	writeUShort(&wire, uShortEncodeResponseCode(CommandQueryOffset))
	writeUShort(&wire, version1)
	writeUInt(&wire, uint32(resp.correlationid))
	writeUShort(&wire, responseCodeNoOffset)
	writeLong(&wire, 0)
	if e := validateInboundFrame(wire.Bytes(), DefaultDecoderLimits()); e != nil {
		t.Fatal(e)
	}
	c.queryOffsetFrameHandler(&ReaderProtocol{}, bufio.NewReader(bytes.NewReader(wire.Bytes()[4:])))
	if code := <-resp.code; code.id != responseCodeNoOffset {
		t.Fatal("no-offset classification lost")
	}
	for _, cmd := range []uint16{commandOpen, commandSaslAuthenticate} {
		var b bytes.Buffer
		writeUShort(&b, uShortEncodeResponseCode(cmd))
		writeUShort(&b, version1)
		writeUInt(&b, 1)
		writeUShort(&b, 8)
		if e := validateInboundFrame(b.Bytes(), DefaultDecoderLimits()); e != nil {
			t.Fatalf("negative cmd %d: %v", cmd, e)
		}
	}
}

func TestDecoderAggregatesHaveSeparateMessageAndChunkBudgets(t *testing.T) {
	wire := deliveryFixture(t, GZIP)
	limits := DefaultDecoderLimits()
	limits.MaxMessageBytes = 16
	limits.AMQP.MaxMessageBytes = 16
	if _, chunk, _, _, _, e := decodeDelivery(wire, limits); e != nil || len(chunk.offsetMessages) != 2 {
		t.Fatalf("aggregate inherited message limit: %v", e)
	}
	// Normal Stream sendfile strips on-disk bloom and trailer, not their header.
	wire[45] = 8
	binary.BigEndian.PutUint32(wire[41:45], 7)
	if _, _, _, _, _, e := decodeDelivery(wire, limits); e != nil {
		t.Fatalf("stripped metadata: %v", e)
	}
}

func TestDecoderReaderRejectsMalformedMetadataBeforeResponse(t *testing.T) {
	c := newClient(connectionParameters{})
	_, peer := attachPipeSocket(t, &c.socket)
	resp, allocationErr := c.coordinator.NewResponse(commandPeerProperties)
	if allocationErr != nil {
		t.Fatal(allocationErr)
	}
	var payload bytes.Buffer
	writeUShort(&payload, uShortEncodeResponseCode(commandPeerProperties))
	writeUShort(&payload, version1)
	writeUInt(&payload, uint32(resp.correlationid))
	writeUShort(&payload, responseCodeOk)
	writeUInt(&payload, 2)
	var frame bytes.Buffer
	writeUInt(&frame, uint32(payload.Len()))
	frame.Write(payload.Bytes())
	done := make(chan struct{})
	go func() { c.handleResponse(); close(done) }()
	if _, e := peer.Write(frame.Bytes()); e != nil {
		t.Fatal(e)
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		_ = peer.Close()
		<-done
		t.Fatal("malformed frame did not stop reader")
	}
	select {
	case <-resp.code:
		t.Fatal("malformed metadata emitted success")
	default:
	}
	select {
	case <-resp.data:
		t.Fatal("malformed metadata emitted data")
	default:
	}
}

func TestDecoderUnknownOpenPropertiesDoNotDesynchronize(t *testing.T) {
	c, _, _ := pipeContextClient(t)
	resp, allocationErr := c.coordinator.NewResponse(commandOpen)
	if allocationErr != nil {
		t.Fatal(allocationErr)
	}
	var b bytes.Buffer
	writeUInt(&b, uint32(resp.correlationid))
	writeUShort(&b, responseCodeOk)
	writeUInt(&b, 2)
	for _, value := range []string{"other", "ignored", "advertised_host", "broker"} {
		if err := writeString(&b, value); err != nil {
			t.Fatal(err)
		}
	}
	c.commandOpen(&ReaderProtocol{}, bufio.NewReader(&b))
	if got := (<-resp.data).(ConnectionProperties); got.host != "broker" {
		t.Fatal("unknown property value consumed as next key")
	}
}
