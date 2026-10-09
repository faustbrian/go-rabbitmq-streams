package stream

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"math"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/faustbrian/go-rabbitmq-streams/adapters/rabbitmq/v2/internal/rabbitmqstream/amqp"
	"github.com/faustbrian/go-rabbitmq-streams/adapters/rabbitmq/v2/internal/rabbitmqstream/message"
)

func TestProtocolCountRejectsUnrepresentableBeforeMutation(t *testing.T) {
	if strconv.IntSize < 64 {
		t.Skip("uint32 overflow fixture needs 64-bit int")
	}
	overflow := uint64(math.MaxUint32) + 1
	for _, n := range []int{-1, int(overflow)} {
		var plain, output bytes.Buffer
		if err := writeInt(&plain, n); !errors.Is(err, ErrProtocolNumberRange) {
			t.Errorf("plain count %d error: %v", n, err)
		}
		if plain.Len() != 0 {
			t.Errorf("plain count %d encoded %d malformed bytes", n, plain.Len())
		}
		writer := bufio.NewWriter(&output)
		if err := writeBInt(writer, n); err == nil {
			t.Errorf("buffered count %d accepted", n)
		}
		if writer.Buffered() != 0 || output.Len() != 0 {
			t.Errorf("buffered count %d mutated writer", n)
		}
		if b, err := newProtocolBuffer(n); b != nil || !errors.Is(err, ErrProtocolNumberRange) {
			t.Fatalf("invalid frame allocation: %v %v", b, err)
		}
		var header bytes.Buffer
		if err := writeProtocolHeader(&header, n, commandPublish); !errors.Is(err, ErrProtocolNumberRange) || header.Len() != 0 {
			t.Fatal("invalid header length mutated frame")
		}
	}
}

func TestNativeTuneBrokerZeroRetainsFiniteRequestedValues(t *testing.T) {
	p := newTCPParameterDefault()
	if err := p.validateTune(); err != nil {
		t.Fatal(err)
	}
	c := &Client{coordinator: NewCoordinator(), tuneState: TuneState{requestedMaxFrameSize: p.RequestedMaxFrameSize, requestedHeartbeat: int(p.RequestedHeartbeat / time.Second)}}
	response := c.coordinator.NewResponseWithName("tune")
	var body bytes.Buffer
	writeUInt(&body, 0)
	writeUInt(&body, 0)
	got, ok := c.handleTune(bufio.NewReader(&body)).(tuneResponse)
	if !ok || got.maxFrameSize != p.RequestedMaxFrameSize || got.heartbeat != int(p.RequestedHeartbeat/time.Second) {
		t.Fatalf("zero broker tune lost finite request: %+v", got)
	}
	select {
	case delivered := <-response.data:
		d, ok := delivered.(tuneResponse)
		if !ok || d.maxFrameSize != got.maxFrameSize || d.heartbeat != got.heartbeat || !bytes.Equal(d.frame, got.frame) {
			t.Fatal("tune response changed")
		}
	default:
		t.Fatal("tune response not delivered")
	}
}

func TestCorrelationConcurrentWrapAllocatesDistinctOwners(t *testing.T) {
	c := NewCoordinator()
	c.counter = math.MaxUint32 - 3
	results := make(chan *Response, 8)
	errors := make(chan error, 8)
	var workers sync.WaitGroup
	defer workers.Wait()
	for range 8 {
		workers.Add(1)
		go func() { defer workers.Done(); r, err := c.NewResponse(CommandQueryOffset); results <- r; errors <- err }()
	}
	seen := make(map[uint32]bool)
	for range 8 {
		r := <-results
		if err := <-errors; err != nil {
			t.Fatal(err)
		}
		if seen[r.correlationid] {
			t.Fatal("duplicate live correlation owner")
		}
		seen[r.correlationid] = true
		if got, err := c.GetResponseById(r.correlationid); err != nil || got != r {
			t.Fatal("correlation owner not registered atomically")
		}
	}
}

func TestProtocolCountMaxUint32Preserved(t *testing.T) {
	if strconv.IntSize < 64 {
		t.Skip("uint32 boundary fixture needs 64-bit int")
	}
	var b bytes.Buffer
	boundary := uint64(math.MaxUint32)
	if err := writeInt(&b, int(boundary)); err != nil {
		t.Fatal(err)
	}
	if b.Len() != 4 || binary.BigEndian.Uint32(b.Bytes()) != math.MaxUint32 {
		t.Fatal("uint32 boundary encoding changed")
	}
}

func TestCorrelationWrapPreservesLiveResponseOwner(t *testing.T) {
	c := NewCoordinator()
	original, allocationErr := c.NewResponse(CommandQueryOffset)
	if allocationErr != nil {
		t.Fatal(allocationErr)
	}
	c.counter = math.MaxUint32 - 1
	last, allocationErr := c.NewResponse(CommandQueryOffset)
	if allocationErr != nil {
		t.Fatal(allocationErr)
	}
	next, allocationErr := c.NewResponse(CommandQueryOffset)
	if allocationErr != nil {
		t.Fatal(allocationErr)
	}
	if last.correlationid != math.MaxUint32 || next.correlationid != 0 {
		t.Fatalf("unexpected wire wrap IDs: %d, %d", last.correlationid, next.correlationid)
	}
	wrapped, allocationErr := c.NewResponse(CommandQueryOffset)
	if allocationErr != nil {
		t.Fatal(allocationErr)
	}
	if wrapped.correlationid != 2 {
		t.Fatalf("occupied wire ID not skipped: %d", wrapped.correlationid)
	}
	got, err := c.GetResponseById(1)
	if err != nil || got != original {
		t.Fatal("wrap replaced live response owner")
	}
	if err := c.RemoveResponseById(1); err != nil {
		t.Fatal(err)
	}
	c.counter = 0
	reused, allocationErr := c.NewResponse(CommandQueryOffset)
	if allocationErr != nil {
		t.Fatal(allocationErr)
	}
	if reused.correlationid != 1 {
		t.Fatal("retired correlation ID could not be reused")
	}
	c.retireResponse(original)
	if got, err := c.GetResponseById(1); err != nil || got != reused {
		t.Fatal("late retirement removed replacement owner")
	}
}

func TestNativeTuneConfigurationRejectedBeforeDial(t *testing.T) {
	for name, edit := range map[string]func(*TCPParameters){
		"zero-frame":     func(p *TCPParameters) { p.RequestedMaxFrameSize = 0 },
		"negative-frame": func(p *TCPParameters) { p.RequestedMaxFrameSize = -1 },
		"oversize-frame": func(p *TCPParameters) {
			overflow := uint64(math.MaxUint32) + 1
			p.RequestedMaxFrameSize = int(overflow)
		},
		"oversize-heartbeat": func(p *TCPParameters) { p.RequestedHeartbeat = (time.Duration(math.MaxUint32) + 1) * time.Second },
	} {
		t.Run(name, func(t *testing.T) {
			o := NewEnvironmentOptions()
			edit(o.TCPParameters)
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			env, err := NewEnvironmentContext(ctx, o)
			if env != nil {
				_ = env.Close()
			}
			if err == nil || errors.Is(err, context.Canceled) {
				t.Fatalf("invalid tune config reached connection operation: %v", err)
			}
		})
	}
}

func TestHashRouteRejectsEmptyPartitions(t *testing.T) {
	r := NewHashRoutingStrategy(func(_ message.StreamMessage) string { return "key" })
	if _, err := r.Route(amqp.NewMessage([]byte{1}), nil); err == nil {
		t.Fatal("empty route accepted")
	}
}

func TestSignedProtocolBitsAndHashRoutePreserved(t *testing.T) {
	var b bytes.Buffer
	writeLong(&b, -1)
	writeShort(&b, -1)
	if binary.BigEndian.Uint64(b.Bytes()) != math.MaxUint64 || binary.BigEndian.Uint16(b.Bytes()[8:]) != math.MaxUint16 {
		t.Fatal("signed wire bit representation changed")
	}
	r := NewHashRoutingStrategy(func(_ message.StreamMessage) string { return "key" })
	partitions := []string{"a", "b", "c"}
	first, err := r.Route(amqp.NewMessage(nil), partitions)
	if err != nil || len(first) != 1 {
		t.Fatalf("valid route: %v %v", first, err)
	}
	second, err := r.Route(amqp.NewMessage(nil), partitions)
	if err != nil || len(second) != 1 || first[0] != second[0] {
		t.Fatal("hash route not deterministic")
	}
}
