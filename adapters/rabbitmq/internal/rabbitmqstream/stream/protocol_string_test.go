package stream

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/faustbrian/go-rabbitmq-streams/adapters/rabbitmq/v2/internal/rabbitmqstream/amqp"
	"github.com/faustbrian/go-rabbitmq-streams/adapters/rabbitmq/v2/internal/rabbitmqstream/message"
)

func TestProtocolStringWritersRejectOverflowBeforeMutation(t *testing.T) {
	value := strings.Repeat("x", 1<<16)
	for _, write := range []func(*bytes.Buffer) error{func(b *bytes.Buffer) error { return writeString(b, value) }, func(b *bytes.Buffer) error { return writeStringArray(b, []string{"valid", value}) }, func(b *bytes.Buffer) error { return writeMapStringString(b, map[string]string{"key": value}) }} {
		var buffer bytes.Buffer
		if err := write(&buffer); !errors.Is(err, ErrProtocolStringTooLong) {
			t.Fatalf("wire overflow error: %v", err)
		}
		if buffer.Len() != 0 {
			t.Fatalf("oversized string mutated encoding: %d bytes", buffer.Len())
		}
	}
	var output bytes.Buffer
	writer := bufio.NewWriterSize(&output, 8)
	if err := writeBString(writer, value); err == nil {
		t.Fatal("buffered string accepted overflowing length")
	}
	if writer.Buffered() != 0 || output.Len() != 0 {
		t.Fatal("buffered string attempted partial malformed encoding")
	}
}

func TestProtocolStringMaxUint16EncodingPreserved(t *testing.T) {
	value := strings.Repeat("x", (1<<16)-1)
	var plain, buffered bytes.Buffer
	if err := writeString(&plain, value); err != nil {
		t.Fatal(err)
	}
	writer := bufio.NewWriter(&buffered)
	if err := writeBString(writer, value); err != nil {
		t.Fatal(err)
	}
	if err := writer.Flush(); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(plain.Bytes(), buffered.Bytes()) || plain.Len() != len(value)+2 || binary.BigEndian.Uint16(plain.Bytes()) != 65535 {
		t.Fatal("max protocol string encoding changed")
	}
}

func TestNativeProtocolStringOverflowDoesNotAttemptFrame(t *testing.T) {
	value := strings.Repeat("x", 1<<16)
	cases := map[string]func(context.Context, *Client) error{
		"properties": func(ctx context.Context, c *Client) error {
			c.clientProperties.items = map[string]string{"connection_name": value}
			_, e := c.peerPropertiesContext(ctx)
			return e
		},
		"consumer-offset": func(ctx context.Context, c *Client) error {
			options := NewConsumerOptions()
			options.ConsumerName = value
			options.streamName = "stream"
			consumer, e := c.coordinator.NewConsumer(func(ConsumerContext, *amqp.Message) {}, options, nil)
			if e != nil {
				return e
			}
			consumer.client = c
			defer consumer.close(Event{Reason: SocketClosed})
			return consumer.writeOffsetToSocketContext(ctx, 1)
		},
		"store": func(ctx context.Context, c *Client) error { return c.StoreOffsetContext(ctx, "consumer", value, 1) },
		"query": func(ctx context.Context, c *Client) error {
			_, e := c.queryOffsetContext(ctx, "consumer", value)
			return e
		},
		"metadata": func(ctx context.Context, c *Client) error {
			_, e := c.queryMetadataContext(ctx, "valid", value)
			return e
		},
		"open":       func(ctx context.Context, c *Client) error { return c.openContext(ctx, value) },
		"sasl":       func(ctx context.Context, c *Client) error { return c.sendSaslAuthenticateContext(ctx, value, nil) },
		"partitions": func(ctx context.Context, c *Client) error { _, e := c.QueryPartitionsContext(ctx, value); return e },
		"route": func(ctx context.Context, c *Client) error {
			_, e := c.queryRouteContext(ctx, "stream", value)
			return e
		},
		"sequence": func(ctx context.Context, c *Client) error {
			_, e := c.queryPublisherSequenceContext(ctx, value, "stream")
			return e
		},
	}
	for name, run := range cases {
		t.Run(name, func(t *testing.T) {
			c, observed, _ := pipeContextClient(t)
			ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
			defer cancel()
			err := run(ctx, c)
			var admitted *WriteAdmittedError
			if !errors.Is(err, ErrProtocolStringTooLong) || errors.As(err, &admitted) {
				t.Fatalf("overflow not refused before I/O: %v", err)
			}
			select {
			case <-observed.started:
				t.Fatal("overflow attempted native frame")
			default:
			}
			c.coordinator.mutex.Lock()
			remaining := len(c.coordinator.responses)
			c.coordinator.mutex.Unlock()
			if remaining != 0 {
				t.Fatalf("overflow retained RPC response: %d", remaining)
			}
		})
	}
}

func TestNativeFilterStringOverflowIsPreWrite(t *testing.T) {
	c, observed, _ := pipeContextClient(t)
	options := NewProducerOptions()
	options.Filter = NewProducerFilter(func(_ message.StreamMessage) string { return strings.Repeat("x", 1<<16) })
	p, err := c.coordinator.NewProducer(options, nil)
	if err != nil {
		t.Fatal(err)
	}
	p.client = c
	t.Cleanup(func() { p.signalStop(Event{Reason: SocketClosed}); c.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	err = p.SendContext(ctx, amqp.NewMessage([]byte{1}))
	var admitted *WriteAdmittedError
	if err == nil || errors.As(err, &admitted) || errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("filter string overflow outcome: %v", err)
	}
	select {
	case <-observed.started:
		t.Fatal("filter overflow attempted native write")
	default:
	}
	if p.unConfirmed.size() != 0 {
		t.Fatal("filter overflow retained confirmation admission")
	}
}
