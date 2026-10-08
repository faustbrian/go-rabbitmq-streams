package rabbitstream

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"
)

func TestRunBatchSplitsBeforeAggregateByteAllowance(t *testing.T) {
	for name, addBytes := range map[string]func(*Message){
		"payload":         func(m *Message) { m.Payload = []byte("ab") },
		"content type":    func(m *Message) { m.ContentType = "ab" },
		"message ID":      func(m *Message) { m.MessageID = "ab" },
		"correlation ID":  func(m *Message) { m.CorrelationID = "ab" },
		"headers":         func(m *Message) { m.Headers = []MetadataEntry{{Key: "a", Value: []byte("b")}} },
		"properties":      func(m *Message) { m.Properties = []MetadataEntry{{Key: "a", Value: []byte("b")}} },
		"broker metadata": func(m *Message) { m.BrokerMetadata = []MetadataEntry{{Key: "a", Value: []byte("b")}} },
	} {
		t.Run(name, func(t *testing.T) {
			messages := make([]Message, 4)
			for i := range messages {
				messages[i] = Message{Stream: "stream", Partition: "stream", Offset: uint64(i + 1), HasOffset: true}
				addBytes(&messages[i])
			}
			transport := newFakeConsumerTransport(messages...)
			limits := DefaultLimits()
			limits.MaxBatchBytes = 4
			consumer, err := NewConsumer(ConsumerConfig{Stream: "stream", ConsumerName: "consumer", Limits: limits}, transport)
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			batches := make(chan []uint64, 4)
			done := make(chan error, 1)
			go func() {
				done <- consumer.RunBatch(ctx, BatchPolicy{MaxMessages: 4, MaxWait: time.Minute}, func(_ context.Context, batch []Message) error {
					offsets := make([]uint64, len(batch))
					for i, message := range batch {
						offsets[i] = message.Offset
					}
					batches <- offsets
					return nil
				})
			}()
			var stored []uint64
			for {
				offset := receiveTest(t, transport.stored)
				stored = append(stored, offset.offset)
				if offset.offset == 4 {
					break
				}
			}
			cancel()
			if err := receiveTest(t, done); !errors.Is(err, ErrCanceled) {
				t.Fatalf("RunBatch: %v", err)
			}
			var got [][]uint64
			for len(batches) > 0 {
				got = append(got, <-batches)
			}
			if !reflect.DeepEqual(got, [][]uint64{{1, 2}, {3, 4}}) || !reflect.DeepEqual(stored, []uint64{2, 4}) {
				t.Fatalf("byte-bounded batches=%v stored=%v", got, stored)
			}
		})
	}
}

func TestRunBatchRefusesOneOverBudgetMessageBeforeHandler(t *testing.T) {
	for name, addBytes := range map[string]func(*Message){
		"payload":        func(m *Message) { m.Payload = []byte("abc") },
		"metadata key":   func(m *Message) { m.Headers = []MetadataEntry{{Key: "abc"}} },
		"metadata value": func(m *Message) { m.Headers = []MetadataEntry{{Key: "a", Value: []byte("bc")}} },
	} {
		t.Run(name, func(t *testing.T) {
			limits := DefaultLimits()
			limits.MaxBatchBytes = 2
			message := Message{Stream: "stream", Partition: "stream", Offset: 1, HasOffset: true}
			addBytes(&message)
			transport := newFakeConsumerTransport(message)
			consumer, err := NewConsumer(ConsumerConfig{Stream: "stream", ConsumerName: "consumer", Limits: limits}, transport)
			if err != nil {
				t.Fatal(err)
			}
			called := false
			err = consumer.RunBatch(boundedTestContext(), BatchPolicy{MaxMessages: 1}, func(context.Context, []Message) error {
				called = true
				return ErrFatal
			})
			if !errors.Is(err, ErrValidation) || called || len(transport.stored) != 0 {
				t.Fatalf("over-budget delivery: error=%v handler=%t stored=%d", err, called, len(transport.stored))
			}
		})
	}
}

func TestBatchWorkerIndependentlyRefusesOverBudgetQueueRecord(t *testing.T) {
	limits := DefaultLimits()
	limits.MaxBatchBytes = 2
	transport := newFakeConsumerTransport()
	consumer, err := NewConsumer(ConsumerConfig{Stream: "stream", ConsumerName: "consumer", Limits: limits}, transport)
	if err != nil {
		t.Fatal(err)
	}
	queue := make(chan Message, 1)
	queue <- Message{Stream: "stream", Partition: "stream", Offset: 1, HasOffset: true, Payload: []byte("abc")}
	called := false
	err = consumer.runBatchWorker(boundedTestContext(), queue, BatchPolicy{MaxMessages: 1, MaxWait: time.Minute}, func(context.Context, []Message) error {
		called = true
		return ErrFatal
	})
	if !errors.Is(err, ErrValidation) || called || len(transport.stored) != 0 {
		t.Fatalf("worker byte admission: error=%v handler=%t stored=%d", err, called, len(transport.stored))
	}
}

func TestRunBatchCarriesOverflowRecordIntoNextBoundedBatch(t *testing.T) {
	transport := newFakeConsumerTransport(
		Message{Stream: "stream", Partition: "stream", Offset: 1, HasOffset: true, Payload: []byte("abc")},
		Message{Stream: "stream", Partition: "stream", Offset: 2, HasOffset: true, Payload: []byte("ab")},
		Message{Stream: "stream", Partition: "stream", Offset: 3, HasOffset: true, Payload: []byte("ab")},
	)
	limits := DefaultLimits()
	limits.MaxBatchBytes = 4
	consumer, err := NewConsumer(ConsumerConfig{Stream: "stream", ConsumerName: "consumer", Limits: limits}, transport)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	batches := make(chan []uint64, 3)
	done := make(chan error, 1)
	go func() {
		done <- consumer.RunBatch(ctx, BatchPolicy{MaxMessages: 3, MaxWait: time.Minute}, func(_ context.Context, batch []Message) error {
			offsets := make([]uint64, len(batch))
			for i, message := range batch {
				offsets[i] = message.Offset
			}
			batches <- offsets
			return nil
		})
	}()
	var stored []uint64
	for {
		offset := receiveTest(t, transport.stored)
		stored = append(stored, offset.offset)
		if offset.offset == 3 {
			break
		}
	}
	cancel()
	if err := receiveTest(t, done); !errors.Is(err, ErrCanceled) {
		t.Fatalf("RunBatch: %v", err)
	}
	var got [][]uint64
	for len(batches) > 0 {
		got = append(got, <-batches)
	}
	if !reflect.DeepEqual(got, [][]uint64{{1}, {2, 3}}) || !reflect.DeepEqual(stored, []uint64{1, 3}) {
		t.Fatalf("overflow batches=%v stored=%v", got, stored)
	}
}

func TestRunBatchOverflowFlushFailureLeavesBothRecordsUnstored(t *testing.T) {
	transport := newFakeConsumerTransport(
		Message{Stream: "stream", Partition: "stream", Offset: 1, HasOffset: true, Payload: []byte("abc")},
		Message{Stream: "stream", Partition: "stream", Offset: 2, HasOffset: true, Payload: []byte("ab")},
	)
	limits := DefaultLimits()
	limits.MaxBatchBytes = 4
	consumer, err := NewConsumer(ConsumerConfig{Stream: "stream", ConsumerName: "consumer", Limits: limits}, transport)
	if err != nil {
		t.Fatal(err)
	}
	var handled []uint64
	err = consumer.RunBatch(boundedTestContext(), BatchPolicy{MaxMessages: 2, MaxWait: time.Minute}, func(_ context.Context, batch []Message) error {
		for _, message := range batch {
			handled = append(handled, message.Offset)
		}
		return ErrFatal
	})
	if !errors.Is(err, ErrHandler) || !reflect.DeepEqual(handled, []uint64{1}) || len(transport.stored) != 0 {
		t.Fatalf("failed overflow flush: error=%v handled=%v stored=%d", err, handled, len(transport.stored))
	}
}
