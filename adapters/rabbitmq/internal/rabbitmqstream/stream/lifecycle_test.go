package stream

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"github.com/faustbrian/go-rabbitmq-streams/adapters/rabbitmq/v2/internal/rabbitmqstream/amqp"
	"io"
	"runtime"
	"sync"
	"testing"
	"time"
)

func TestChunkValuesBudgetChargesActualZeroWidthValues(t *testing.T) {
	wire := []byte{0, 0x53, 0x77, 0xe0, 2, 2, 0x43} // five admitted structural values
	var data bytes.Buffer
	for range 2 {
		writeUInt(&data, uint32(len(wire)))
		data.Write(wire)
	}
	body := append([]byte(nil), deliveryFixture(t, 255)[:49]...)
	binary.BigEndian.PutUint16(body[3:5], 2)
	binary.BigEndian.PutUint32(body[5:9], 2)
	binary.BigEndian.PutUint32(body[37:41], uint32(data.Len()))
	body = append(body, data.Bytes()...)
	limits := DefaultDecoderLimits()
	limits.MaxChunkValues = 10
	if _, chunk, _, _, _, e := decodeDelivery(body, limits); e != nil || len(chunk.offsetMessages) != 2 {
		t.Fatalf("actual values were overcharged: %v", e)
	}
	limits.MaxChunkValues = 9
	_, chunk, _, _, _, e := decodeDelivery(body, limits)
	if e == nil || len(chunk.offsetMessages) != 1 {
		t.Fatalf("aggregate admitted second message: count=%d err=%v", len(chunk.offsetMessages), e)
	}
}

func TestProducerConfirmationCloseUnblocksPendingNotification(t *testing.T) {
	p, e := NewCoordinator().NewProducer(NewProducerOptions(), nil)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() {
		p.confirmationTimeoutTicker.Stop()
		p.pendingSequencesQueue.Stop()
		p.pendingSequencesQueue.Close()
	})
	ch := p.NotifyPublishConfirmation()
	ch <- nil
	sent := make(chan struct{})
	started := make(chan struct{})
	go func() { close(started); p.sendConfirmationStatus(nil); close(sent) }()
	<-started
	runtime.Gosched()
	closed := make(chan struct{})
	go func() { p.closeConfirmationStatus(); close(closed) }()
	select {
	case <-closed:
	case <-time.After(100 * time.Millisecond):
		<-ch
		<-sent
		<-closed
		t.Fatal("confirmation closure waited behind unread notification")
	}
	select {
	case <-sent:
	case <-time.After(time.Second):
		t.Fatal("notification sender survived closure")
	}
}

func TestCoordinatorCloseRetainsReaderOwnedResponseChannels(t *testing.T) {
	c := NewCoordinator()
	r, allocationErr := c.NewResponse(CommandQueryOffset)
	if allocationErr != nil {
		t.Fatal(allocationErr)
	}
	c.Close()
	defer func() {
		if recover() != nil {
			t.Fatal("coordinator close destroyed reader-owned response channels")
		}
	}()
	r.code <- Code{id: responseCodeOk}
	r.data <- int64(1)
}

func TestConsumerCloseDoesNotBlockOnFullTerminalMailbox(t *testing.T) {
	c, e := NewCoordinator().NewConsumer(func(ConsumerContext, *amqp.Message) {}, NewConsumerOptions(), nil)
	if e != nil {
		t.Fatal(e)
	}
	ch := c.NotifyClose()
	c.closeHandler <- Event{}
	done := make(chan struct{})
	go func() { c.close(Event{Reason: SocketClosed}); close(done) }()
	select {
	case <-done:
	case <-time.After(100 * time.Millisecond):
		<-ch
		<-done
		t.Fatal("consumer close blocked on full terminal mailbox")
	}
}

func TestNativeOwnerJoinWaitsAndCanResumeAfterDeadline(t *testing.T) {
	c1, _, _ := pipeContextClient(t)
	c2, _, _ := pipeContextClient(t)
	owner := &taskOwner{}
	tcp := &TCPParameters{lifecycle: owner}
	c1.tcpParameters, c2.tcpParameters = tcp, tcp
	env := &Environment{options: &EnvironmentOptions{TCPParameters: tcp}, locator: newLocator(c1), producers: newProducersEnvironment(1, nil), consumers: newConsumersEnvironment(1, nil)}
	stopped1, stopped2, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
		_ = env.Abort()
		c2.Close()
	})
	c1.startTask(nil, func() { <-c1.socket.done; close(stopped1); <-release })
	c2.startTask(nil, func() { <-c2.socket.done; close(stopped2); <-release })
	coordinator := &environmentCoordinator{mutex: &sync.Mutex{}}
	coordinator.clientsPerContext.Store(1, c2)
	env.producers.producersCoordinator["peer"] = coordinator
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	if err := env.CloseContext(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("unjoined cleanup reported %v", err)
	}
	for _, stopped := range []chan struct{}{stopped1, stopped2} {
		select {
		case <-stopped:
		case <-time.After(time.Second):
			t.Fatal("owner joined before signaling sockets")
		}
	}
	close(release)
	joined, cancelJoin := context.WithTimeout(context.Background(), time.Second)
	defer cancelJoin()
	if err := env.WaitContext(joined); err != nil {
		t.Fatalf("resumed join: %v", err)
	}
}

func TestNativeOwnerRetainsRetiredClientTasks(t *testing.T) {
	c, _, _ := pipeContextClient(t)
	owner := &taskOwner{}
	c.tcpParameters = &TCPParameters{lifecycle: owner}
	release := make(chan struct{})
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
	})
	c.startTask(nil, func() { <-c.socket.done; <-release })
	c.Close() // pool lookup can now retire this client, but owner registration remains.
	owner.stop()
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	if err := owner.wait(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("retired client lost join ownership: %v", err)
	}
	close(release)
	joined, cancelJoin := context.WithTimeout(context.Background(), time.Second)
	defer cancelJoin()
	if err := owner.wait(joined); err != nil {
		t.Fatal(err)
	}
}

func TestNativeReaderCloseDoesNotJoinItself(t *testing.T) {
	c, _, peer := pipeContextClient(t)
	readerReturned := make(chan struct{})
	if !c.startTask(nil, func() {
		c.handleResponse()
		close(readerReturned)
	}) {
		t.Fatal("reader admission")
	}
	_ = peer.Close()
	select {
	case <-readerReturned:
	case <-time.After(time.Second):
		t.Fatal("reader failed to return from terminal stop")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if e := c.WaitContext(ctx); e != nil {
		t.Fatalf("reader self-join: %v", e)
	}
}

func TestNativeProducerCloseCancelsAdmittedWriteAndJoins(t *testing.T) {
	c, _, peer := pipeContextClient(t)
	p, e := c.coordinator.NewProducer(NewProducerOptions(), nil)
	if e != nil {
		t.Fatal(e)
	}
	p.client = c
	result := make(chan error, 1)
	go func() { result <- p.SendContext(context.Background(), amqp.NewMessage([]byte{1})) }()
	if _, e = io.ReadFull(peer, make([]byte, 1)); e != nil {
		t.Fatal(e)
	}
	_ = p.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if e = p.WaitContext(ctx); e != nil {
		t.Fatal(e)
	}
	select {
	case e = <-result:
		var admitted *WriteAdmittedError
		if !errors.As(e, &admitted) {
			t.Fatalf("lost ambiguity: %v", e)
		}
	case <-ctx.Done():
		t.Fatal("native write survived close")
	}
}

func TestNativeConsumerCallbackCanRequestClose(t *testing.T) {
	c, _, peer := pipeContextClient(t)
	options := NewConsumerOptions()
	called := make(chan struct{})
	consumer, e := c.coordinator.NewConsumer(func(cc ConsumerContext, _ *amqp.Message) {
		_ = cc.Consumer.Close()
		close(called)
	}, options, nil)
	if e != nil {
		t.Fatal(e)
	}
	consumer.client = c
	serverDone := make(chan error, 1)
	go func() {
		_, err := readContextTestFrame(peer)
		if err == nil {
			c.coordinator.mutex.Lock()
			var response *Response
			for _, candidate := range c.coordinator.responses {
				response = candidate
			}
			c.coordinator.mutex.Unlock()
			if response == nil {
				err = errors.New("missing unsubscribe response")
			} else {
				response.code <- Code{id: responseCodeOk}
			}
		}
		serverDone <- err
	}()
	if !c.startConsumerDispatch(consumer, options, "stream") {
		t.Fatal("dispatch admission")
	}
	consumer.sendChunk(chunkInfo{offsetMessages: []*offsetMessage{{offset: 1, message: &amqp.Message{}}}})
	select {
	case <-called:
	case <-time.After(time.Second):
		c.Close()
		t.Fatal("callback self-close blocked")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if e = consumer.WaitContext(ctx); e != nil {
		t.Fatalf("callback join: %v", e)
	}
	select {
	case err := <-serverDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("unsubscribe peer survived callback close")
	}
}

func TestNativeLateCloseSubscriberObservesTerminalState(t *testing.T) {
	p, e := NewCoordinator().NewProducer(NewProducerOptions(), nil)
	if e != nil {
		t.Fatal(e)
	}
	p.signalStop(Event{Reason: SocketClosed})
	consumer, e := NewCoordinator().NewConsumer(func(ConsumerContext, *amqp.Message) {}, NewConsumerOptions(), nil)
	if e != nil {
		t.Fatal(e)
	}
	consumer.signalStop(Event{Reason: SocketClosed})
	for _, ch := range []ChannelClose{p.NotifyClose(), consumer.NotifyClose()} {
		select {
		case event, ok := <-ch:
			if !ok || event.Reason != SocketClosed {
				t.Fatalf("lost terminal event: %#v %v", event, ok)
			}
		case <-time.After(25 * time.Millisecond):
			t.Fatal("late subscriber missed terminal resource state")
		}
	}
}
