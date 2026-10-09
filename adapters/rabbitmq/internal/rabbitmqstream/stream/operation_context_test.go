package stream

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/faustbrian/go-rabbitmq-streams/adapters/rabbitmq/v2/internal/rabbitmqstream/amqp"
)

func TestRPCContextCancellationEndsResponseWait(t *testing.T) {
	c, _, peer := pipeContextClient(t)
	resp, allocationErr := c.coordinator.NewResponse(CommandQueryOffset)
	if allocationErr != nil {
		t.Fatal(allocationErr)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan responseError, 1)
	go func() { result <- c.handleWriteContext(ctx, []byte{1}, resp) }()
	if _, err := io.ReadFull(peer, make([]byte, 1)); err != nil {
		t.Fatal(err)
	}
	cancel()
	select {
	case got := <-result:
		if !errors.Is(got.Err, context.Canceled) {
			t.Fatalf("response wait: %v", got.Err)
		}
	case <-time.After(100 * time.Millisecond):
		resp.code <- Code{id: responseCodeOk}
		<-result
		t.Fatal("caller cancellation did not end RPC response wait")
	}
}

func TestProducerSendContextPreservesPartialWriteAmbiguity(t *testing.T) {
	c, _, peer := pipeContextClient(t)
	producer, err := c.coordinator.NewProducer(NewProducerOptions(), nil)
	if err != nil {
		t.Fatal(err)
	}
	producer.client = c
	t.Cleanup(func() {
		producer.confirmationTimeoutTicker.Stop()
		producer.pendingSequencesQueue.Stop()
		producer.pendingSequencesQueue.Close()
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan error, 1)
	go func() { result <- producer.SendContext(ctx, amqp.NewMessage([]byte{1})) }()
	if _, err := io.ReadFull(peer, make([]byte, 1)); err != nil {
		t.Fatal(err)
	}
	cancel()
	select {
	case err := <-result:
		var admitted *WriteAdmittedError
		if !errors.Is(err, context.Canceled) || !errors.As(err, &admitted) {
			t.Fatalf("producer partial write: %v", err)
		}
		if producer.unConfirmed.size() != 1 {
			t.Fatal("admitted partial write lost confirmation ownership")
		}
	case <-time.After(time.Second):
		t.Fatal("producer native send retained cancelled caller")
	}
}

func TestRPCDataContextCancellationEndsWait(t *testing.T) {
	c, _, peer := pipeContextClient(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan error, 1)
	go func() { _, err := c.queryOffsetContext(ctx, "consumer", "stream"); result <- err }()
	if _, err := readContextTestFrame(peer); err != nil {
		t.Fatal(err)
	}
	c.coordinator.mutex.Lock()
	var resp *Response
	for _, response := range c.coordinator.responses {
		resp = response
	}
	c.coordinator.mutex.Unlock()
	if resp == nil {
		t.Fatal("missing query response")
	}
	resp.code <- Code{id: responseCodeOk}
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("data wait: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("data wait retained cancelled caller")
	}
}

func TestBlockingQueueContextAdmissionCancelsWithoutEnqueue(t *testing.T) {
	queue := NewBlockingQueue[int](1)
	t.Cleanup(func() { queue.Stop(); queue.Close() })
	if err := queue.Enqueue(1); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if err := queue.EnqueueContext(ctx, 2); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("admission: %v", err)
	}
	if queue.Size() != 1 || <-queue.GetChannel() != 1 {
		t.Fatal("cancelled item entered queue")
	}
}

func TestBlockingQueueStopReleasesWaitingAdmission(t *testing.T) {
	queue := NewBlockingQueue[int](0)
	result := make(chan error, 1)
	go func() { result <- queue.EnqueueContext(context.Background(), 1) }()
	queue.Stop()
	queue.Close()
	queue.Close()
	select {
	case err := <-result:
		if !errors.Is(err, ErrBlockingQueueStopped) {
			t.Fatalf("stopped admission: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("stop retained waiting sender")
	}
}

func TestConsumerOffsetCancellationDoesNotAdvanceLocalOffset(t *testing.T) {
	c, _, _ := pipeContextClient(t)
	consumer := &Consumer{client: c, mutex: &sync.RWMutex{}, lastStoredOffset: 1,
		options: &ConsumerOptions{streamName: "stream", ConsumerName: "consumer"}}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	err := consumer.StoreCustomOffsetContext(ctx, 2)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("offset write: %v", err)
	}
	if consumer.GetLastStoredOffset() != 1 {
		t.Fatal("failed offset write advanced local checkpoint")
	}
}

func pipeContextClient(t *testing.T) (*Client, *observedWriteConn, net.Conn) {
	t.Helper()
	c := &Client{coordinator: NewCoordinator(), socketCallTimeout: time.Second}
	observed, peer := attachPipeSocket(t, &c.socket)
	return c, observed, peer
}

func TestConnectContextRejectsCancelledDial(t *testing.T) {
	c := newClient(connectionParameters{broker: &Broker{Host: "127.0.0.1", Port: "1", Scheme: "rabbitmq-stream"}})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := c.connectContext(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("dial: %v", err)
	}
}

func TestConnectContextCancelsTLSHandshake(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	accepted := make(chan struct{})
	serverDone := make(chan error, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			serverDone <- err
			return
		}
		defer func() { _ = conn.Close() }()
		_ = conn.SetDeadline(time.Now().Add(2 * time.Second))
		close(accepted)
		_, err = io.Copy(io.Discard, conn)
		serverDone <- err
	}()
	host, port, _ := net.SplitHostPort(listener.Addr().String())
	c := newClient(connectionParameters{broker: &Broker{Host: host, Port: port, Scheme: "rabbitmq-stream+tls"}})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan error, 1)
	go func() { result <- c.connectContext(ctx) }()
	select {
	case <-accepted:
	case <-time.After(time.Second):
		t.Fatal("TLS peer was not reached")
	}
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("TLS handshake: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("TLS handshake retained cancelled caller")
	}
	select {
	case err := <-serverDone:
		if err != nil {
			t.Fatalf("TLS peer was not closed: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("TLS native connection remained open")
	}
}

// This tiny protocol fixture only handles the four opening commands; it does
// not implement a broker, publications, or adversarial framing.
func readContextTestFrame(conn net.Conn) ([]byte, error) {
	header := make([]byte, 4)
	if _, err := io.ReadFull(conn, header); err != nil {
		return nil, err
	}
	size := binary.BigEndian.Uint32(header)
	if size < 4 || size > 4096 {
		return nil, fmt.Errorf("fixture frame size out of bounds")
	}
	body := make([]byte, size)
	_, err := io.ReadFull(conn, body)
	return body, err
}

func contextTestReply(conn net.Conn, command uint16, correlation uint32, extra func(*bytes.Buffer)) error {
	body := &bytes.Buffer{}
	writeUShort(body, uShortEncodeResponseCode(command))
	writeShort(body, version1)
	if command != commandTune {
		writeUInt(body, correlation)
		writeUShort(body, responseCodeOk)
	}
	if extra != nil {
		extra(body)
	}
	frame := &bytes.Buffer{}
	if encodingErr := writeInt(frame, body.Len()); encodingErr != nil {
		return encodingErr
	}
	frame.Write(body.Bytes())
	_, err := conn.Write(frame.Bytes())
	return err
}

func serveContextTestOpening(conn net.Conn) error {
	for _, expected := range []uint16{commandPeerProperties, commandSaslHandshake, commandSaslAuthenticate, commandTune, commandOpen} {
		frame, err := readContextTestFrame(conn)
		if err != nil {
			return err
		}
		command := uShortExtractResponseCode(binary.BigEndian.Uint16(frame[:2]))
		if command != expected {
			return fmt.Errorf("unexpected opening command")
		}
		if command == commandTune {
			continue
		}
		if len(frame) < 8 {
			return fmt.Errorf("fixture correlation absent")
		}
		correlation := binary.BigEndian.Uint32(frame[4:8])
		var extra func(*bytes.Buffer)
		switch command {
		case commandPeerProperties, commandOpen:
			var encoded bytes.Buffer
			if err := writeInt(&encoded, 0); err != nil {
				return err
			}
			extra = func(b *bytes.Buffer) { b.Write(encoded.Bytes()) }
		case commandSaslHandshake:
			var encoded bytes.Buffer
			if encodingErr := writeInt(&encoded, 1); encodingErr != nil {
				return encodingErr
			}
			if err := writeString(&encoded, SaslConfigurationPlain); err != nil {
				return err
			}
			extra = func(b *bytes.Buffer) { b.Write(encoded.Bytes()) }
		}
		if err := contextTestReply(conn, command, correlation, extra); err != nil {
			return err
		}
		if command == commandSaslAuthenticate {
			if err := contextTestReply(conn, commandTune, 0, func(b *bytes.Buffer) { writeUInt(b, 4096); writeUInt(b, 60) }); err != nil {
				return err
			}
		}
	}
	return nil
}

func TestEnvironmentRetryUsesFreshSocketAndOpeningContextDetaches(t *testing.T) {
	failed, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	success, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		_ = failed.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = failed.Close(); _ = success.Close() })
	failedDone := make(chan struct{})
	go func() {
		defer close(failedDone)
		conn, err := failed.Accept()
		if err == nil {
			_ = conn.Close()
		}
	}()
	serverDone := make(chan error, 1)
	serverRelease := make(chan struct{})
	serverStopped := make(chan struct{})
	t.Cleanup(func() {
		_ = success.Close()
		close(serverRelease)
		select {
		case <-serverStopped:
		case <-time.After(3 * time.Second):
			t.Error("fixture server did not stop")
		}
	})
	go func() {
		defer close(serverStopped)
		conn, err := success.Accept()
		if err != nil {
			serverDone <- err
			return
		}
		defer func() { _ = conn.Close() }()
		_ = conn.SetDeadline(time.Now().Add(2 * time.Second))
		if err := serveContextTestOpening(conn); err != nil {
			serverDone <- err
			return
		}
		marker := make([]byte, 1)
		_, err = io.ReadFull(conn, marker)
		if err == nil && marker[0] != 7 {
			err = fmt.Errorf("fixture marker differs")
		}
		serverDone <- err
		// A peer close here races the writer's final deadline reset, which is
		// unrelated to detachment from the caller's opening context.
		<-serverRelease
	}()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	options := NewEnvironmentOptions().SetUris([]string{"rabbitmq-stream://guest:guest@" + failed.Addr().String(), "rabbitmq-stream://guest:guest@" + success.Addr().String()}).SetRPCTimeout(300 * time.Millisecond)
	env, err := NewEnvironmentContext(ctx, options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = env.Abort() })
	cancel()
	if err := env.locator.current().socket.writeAndFlushContext(context.Background(), []byte{7}); err != nil {
		t.Fatalf("opening context retained lifetime: %v", err)
	}
	select {
	case err := <-serverDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("fixture did not complete")
	}
	select {
	case <-failedDone:
	case <-time.After(time.Second):
		t.Fatal("failed attempt not released")
	}
}

func TestResponseRetirementDoesNotCloseReaderOwnedChannels(t *testing.T) {
	c := NewCoordinator()
	resp, allocationErr := c.NewResponse(CommandQueryOffset)
	if allocationErr != nil {
		t.Fatal(allocationErr)
	}
	if err := c.RemoveResponseById(resp.correlationid); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if recover() != nil {
			t.Fatal("late reader delivery panicked after response retirement")
		}
	}()
	resp.code <- Code{id: responseCodeOk}
	resp.data <- int64(1)
}
