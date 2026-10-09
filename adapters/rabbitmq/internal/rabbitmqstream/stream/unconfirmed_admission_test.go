package stream

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/faustbrian/go-rabbitmq-streams/adapters/rabbitmq/v2/internal/rabbitmqstream/amqp"
	"github.com/faustbrian/go-rabbitmq-streams/adapters/rabbitmq/v2/internal/rabbitmqstream/message"
)

func saturatedNativeProducer(t *testing.T, pending int) *Producer {
	t.Helper()
	c, _, _ := pipeContextClient(t)
	p, err := c.coordinator.NewProducer(NewProducerOptions(), nil)
	if err != nil {
		t.Fatal(err)
	}
	p.client = c
	p.unConfirmed = newUnConfirmed(1)
	// Tiny existing pending entries. The old implementation admits beyond its
	// nominal capacity, so also cover that reachable pre-fix saturated state.
	for i := range pending {
		id := int64(-i - 1)
		p.unConfirmed.messages[id] = &ConfirmationStatus{publishingId: id, inserted: time.Now()}
	}
	t.Cleanup(func() { p.signalStop(Event{Reason: SocketClosed}); c.Close() })
	return p
}

func TestNativeConfirmationCapacityCancellationIsPreWrite(t *testing.T) {
	p := saturatedNativeProducer(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	err := p.SendContext(ctx, amqp.NewMessage([]byte{1}))
	var admitted *WriteAdmittedError
	if !errors.Is(err, context.DeadlineExceeded) || errors.As(err, &admitted) {
		t.Fatalf("saturated admission attempted write or lost cancellation: %v", err)
	}
	if n := p.unConfirmed.size(); n != 1 {
		t.Fatalf("capacity ownership changed on refusal: %d", n)
	}
}

func TestNativeConfirmationSaturationCloseReleasesAndJoins(t *testing.T) {
	p := saturatedNativeProducer(t, 2)
	result := make(chan error, 1)
	go func() { result <- p.SendContext(context.Background(), amqp.NewMessage([]byte{1})) }()
	// Cancel the producer while admission is pending; Wait must join the actual
	// Send rather than merely returning because root-level capacity was freed.
	timeLimit := time.Now().Add(time.Second)
	for {
		p.tasks.mutex.Lock()
		active := p.tasks.active
		p.tasks.mutex.Unlock()
		if active > 0 {
			break
		}
		if time.Now().After(timeLimit) {
			t.Fatal("send did not register")
		}
		time.Sleep(time.Millisecond)
	}
	p.signalStop(Event{Reason: SocketClosed})
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	err := p.WaitContext(ctx)
	if err != nil {
		// Release the pre-fix actual condition waiter, then join it before failing.
		p.unConfirmed.extractWithErrors([]int64{-1, -2}, entityClosed)
		cleanup, stop := context.WithTimeout(context.Background(), time.Second)
		defer stop()
		if joinErr := p.WaitContext(cleanup); joinErr != nil {
			t.Fatalf("fixture failed to release native waiter: %v", joinErr)
		}
		<-result
		t.Fatalf("producer stop left confirmation admission blocked: %v", err)
	}
	select {
	case err = <-result:
		var admitted *WriteAdmittedError
		if err == nil || errors.As(err, &admitted) {
			t.Fatalf("closed admission outcome: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("send survived native join")
	}
}

func TestNativePendingPublishingIDPreservesOriginalOwnership(t *testing.T) {
	p := saturatedNativeProducer(t, 0)
	p.unConfirmed = newUnConfirmed(2)
	original := &messageSequence{publishingId: 41, sourceMsg: amqp.NewMessage([]byte{1})}
	if err := p.unConfirmed.addFromSequencesContext(context.Background(), []*messageSequence{original}, p.id, p.client.socket.done); err != nil {
		t.Fatal(err)
	}
	record := p.unConfirmed.messages[41]
	retry := amqp.NewMessage([]byte{2})
	retry.SetPublishingId(41)
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	err := p.SendContext(ctx, retry)
	var admitted *WriteAdmittedError
	if !errors.Is(err, ErrPendingPublishingID) || errors.As(err, &admitted) {
		t.Fatalf("duplicate pending ID outcome: %v", err)
	}
	select {
	case <-p.client.socket.connection.(*observedWriteConn).started:
		t.Fatal("duplicate pending ID attempted native write")
	default:
	}
	if p.unConfirmed.messages[41] != record || record.message != original.sourceMsg {
		t.Fatal("original confirmation ownership replaced")
	}
	confirms := p.unConfirmed.extractWithConfirms([]int64{41})
	if len(confirms) != 1 || confirms[0] != record || !record.IsConfirmed() {
		t.Fatal("original confirmation lost")
	}
	replacement := &messageSequence{publishingId: 41, sourceMsg: retry}
	if err = p.unConfirmed.addFromSequencesContext(ctx, []*messageSequence{replacement}, p.id, p.client.socket.done); err != nil {
		t.Fatalf("completed ID cannot be reused: %v", err)
	}
	if len(p.unConfirmed.removeUnsent([]*messageSequence{original})) != 0 || p.unConfirmed.messages[41].admission != replacement.admission {
		t.Fatal("old lease removed replacement ownership")
	}
}

func TestNativeConfirmationAdmissionIsAtomicAndBatchBounded(t *testing.T) {
	p := saturatedNativeProducer(t, 0)
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	err := p.BatchSendContext(ctx, []message.StreamMessage{amqp.NewMessage([]byte{1}), amqp.NewMessage([]byte{2})})
	if !errors.Is(err, ErrUnconfirmedCapacity) || p.unConfirmed.size() != 0 {
		t.Fatalf("oversized native batch admitted: %v", err)
	}
	select {
	case <-p.client.socket.connection.(*observedWriteConn).started:
		t.Fatal("oversized batch attempted native write")
	default:
	}
	result := make(chan error, 2)
	for _, id := range []int64{10, 11} {
		go func() {
			result <- p.unConfirmed.addFromSequencesContext(ctx, []*messageSequence{{publishingId: id}}, p.id, p.client.socket.done)
		}()
	}
	accepted, refused := 0, 0
	for range 2 {
		select {
		case err := <-result:
			if err == nil {
				accepted++
			} else if errors.Is(err, context.DeadlineExceeded) {
				refused++
			} else {
				t.Fatal(err)
			}
		case <-time.After(time.Second):
			t.Fatal("atomic admission survived cancellation")
		}
	}
	if accepted != 1 || refused != 1 || p.unConfirmed.size() != 1 {
		t.Fatalf("capacity was not atomic: accepted=%d refused=%d count=%d", accepted, refused, p.unConfirmed.size())
	}
}

func TestNativeConfirmationPreWriteFailureReleasesOnlyOwnAdmission(t *testing.T) {
	p := saturatedNativeProducer(t, 0)
	p.client.socket.initWriteGate()
	<-p.client.socket.writeGate
	defer func() { p.client.socket.writeGate <- struct{}{} }()
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	err := p.SendContext(ctx, amqp.NewMessage([]byte{1}))
	var admitted *WriteAdmittedError
	if !errors.Is(err, context.DeadlineExceeded) || errors.As(err, &admitted) {
		t.Fatalf("write admission outcome: %v", err)
	}
	if p.unConfirmed.size() != 0 || !p.client.socket.isOpen() {
		t.Fatal("pre-write refusal retained native ownership or closed unrelated writer")
	}
}

func TestNativeQueuedConfirmationAdmissionStopsWithProducer(t *testing.T) {
	p := saturatedNativeProducer(t, 1)
	if !p.processPendingSequencesQueue() {
		t.Fatal("queue worker admission")
	}
	if err := p.Send(amqp.NewMessage([]byte{1})); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	for p.pendingSequencesQueue.Size() != 0 {
		select {
		case <-ctx.Done():
			t.Fatal("worker did not consume publication")
		case <-time.After(time.Millisecond):
		}
	}
	p.signalStop(Event{Reason: SocketClosed})
	if err := p.WaitContext(ctx); err != nil {
		t.Fatalf("queued confirmation admission survived stop: %v", err)
	}
	select {
	case <-p.client.socket.connection.(*observedWriteConn).started:
		t.Fatal("saturated queue worker attempted write")
	default:
	}
}
