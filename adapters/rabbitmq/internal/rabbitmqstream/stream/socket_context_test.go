package stream

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"
)

type observedWriteConn struct {
	net.Conn
	started chan struct{}
	once    sync.Once
}

func TestSocketContextCancelsNativeWrite(t *testing.T) {
	s, c, _ := pipeSocket(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan error, 1)
	go func() { result <- s.writeAndFlushContext(ctx, []byte{1}) }()
	select {
	case <-c.started:
	case <-time.After(time.Second):
		t.Fatal("write did not start")
	}
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("got %v, want cancellation", err)
		}
	case <-time.After(time.Second):
		t.Fatal("cancel did not release native I/O")
	}
	if s.isOpen() {
		t.Fatal("cancelled admitted frame left connection reusable")
	}
}

func TestSocketContextDeadlineBoundsNativeWrite(t *testing.T) {
	s, _, _ := pipeSocket(t)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := s.writeAndFlushContext(ctx, []byte{1}); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("got %v, want deadline", err)
	}
}

func TestSocketWriteAdmissionCancellationDoesNotInterruptOwner(t *testing.T) {
	s, c, peer := pipeSocket(t)
	ownerCtx, ownerCancel := context.WithTimeout(context.Background(), time.Second)
	defer ownerCancel()
	owner := make(chan error, 1)
	go func() { owner <- s.writeAndFlushContext(ownerCtx, []byte{1}) }()
	select {
	case <-c.started:
	case <-time.After(time.Second):
		t.Fatal("write did not start")
	}
	waitCtx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if err := s.writeAndFlushContext(waitCtx, []byte{2}); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("got %v, want admission deadline", err)
	}
	if !s.isOpen() {
		t.Fatal("waiting cancellation aborted writer's connection")
	}
	got := make([]byte, 1)
	if _, err := io.ReadFull(peer, got); err != nil || got[0] != 1 {
		t.Fatalf("owner frame: %v %v", got, err)
	}
	if err := <-owner; err != nil {
		t.Fatalf("owner write: %v", err)
	}
	second := make(chan error, 1)
	go func() { second <- s.writeAndFlushContext(ownerCtx, []byte{3}) }()
	if _, err := io.ReadFull(peer, got); err != nil || got[0] != 3 {
		t.Fatalf("subsequent frame: %v %v", got, err)
	}
	if err := <-second; err != nil {
		t.Fatalf("subsequent write inherited cancelled deadline: %v", err)
	}
}

func TestSocketAbortInterruptsRead(t *testing.T) {
	s, c, _ := pipeSocket(t)
	result := make(chan error, 1)
	go func() { _, err := c.Read(make([]byte, 1)); result <- err }()
	if err := s.abort(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-result:
		if err == nil {
			t.Fatal("read succeeded after abort")
		}
	case <-time.After(time.Second):
		t.Fatal("abort did not release reader")
	}
}

func TestSocketPartialWriteCancellationInvalidatesConnection(t *testing.T) {
	s, _, peer := pipeSocket(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan error, 1)
	go func() { result <- s.writeAndFlushContext(ctx, []byte{1, 2}) }()
	first := make([]byte, 1)
	if _, err := io.ReadFull(peer, first); err != nil || first[0] != 1 {
		t.Fatalf("partial frame: %v %v", first, err)
	}
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("partial write: %v", err)
		}
		var admitted *WriteAdmittedError
		if !errors.As(err, &admitted) {
			t.Fatalf("partial frame was classified as not admitted: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("partial write did not cancel")
	}
	if err := s.writeAndFlushContext(context.Background(), []byte{3}); !errors.Is(err, net.ErrClosed) {
		t.Fatalf("partial connection reused: %v", err)
	}
}

func TestSocketCancelledAdmissionIsNotWriteAdmitted(t *testing.T) {
	s, _, _ := pipeSocket(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := s.writeAndFlushContext(ctx, []byte{1})
	var admitted *WriteAdmittedError
	if !errors.Is(err, context.Canceled) || errors.As(err, &admitted) {
		t.Fatalf("cancelled admission: %v", err)
	}
	if !s.isOpen() {
		t.Fatal("pre-admission cancellation closed the connection")
	}
}

func TestSocketCompletedWriteDoesNotLeaveCancellationCallback(t *testing.T) {
	s, _, peer := pipeSocket(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan error, 1)
	go func() { result <- s.writeAndFlushContext(ctx, []byte{1}) }()
	got := make([]byte, 1)
	if _, err := io.ReadFull(peer, got); err != nil {
		t.Fatal(err)
	}
	if err := <-result; err != nil {
		t.Fatal(err)
	}
	cancel()
	go func() { result <- s.writeAndFlushContext(context.Background(), []byte{2}) }()
	if _, err := io.ReadFull(peer, got); err != nil || got[0] != 2 {
		t.Fatalf("next frame: %v %v", got, err)
	}
	if err := <-result; err != nil {
		t.Fatalf("completed context affected next owner: %v", err)
	}
}

func (c *observedWriteConn) Write(p []byte) (int, error) {
	c.once.Do(func() { close(c.started) })
	return c.Conn.Write(p)
}

func pipeSocket(t *testing.T) (*socket, *observedWriteConn, net.Conn) {
	s := &socket{}
	c, peer := attachPipeSocket(t, s)
	return s, c, peer
}

func attachPipeSocket(t *testing.T, s *socket) (*observedWriteConn, net.Conn) {
	t.Helper()
	local, peer := net.Pipe()
	if err := peer.SetDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	c := &observedWriteConn{Conn: local, started: make(chan struct{})}
	s.connection = c
	s.writer = bufio.NewWriter(c)
	s.mutex = &sync.Mutex{}
	s.destructor = &sync.Once{}
	s.setOpen()
	t.Cleanup(func() { _ = local.Close(); _ = peer.Close() })
	return c, peer
}

func TestSocketShutdownInterruptsBlockedWrite(t *testing.T) {
	s, c, _ := pipeSocket(t)
	written := make(chan error, 1)
	go func() { written <- s.writeAndFlush([]byte{1}) }()
	select {
	case <-c.started:
	case <-time.After(time.Second):
		t.Fatal("write did not start")
	}
	closed := make(chan struct{})
	go func() { s.shutdown(nil); close(closed) }()
	select {
	case <-closed:
	case <-time.After(100 * time.Millisecond):
		_ = c.Close()
		<-written
		<-closed
		t.Fatal("shutdown waited behind blocked network write")
	}
	select {
	case err := <-written:
		if err == nil {
			t.Fatal("blocked write succeeded after abort")
		}
	case <-time.After(time.Second):
		t.Fatal("abort did not release writer")
	}
}
