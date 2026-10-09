package stream

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"sync"
	"time"
)

type socket struct {
	connection net.Conn
	writer     *bufio.Writer
	mutex      *sync.Mutex
	closed     int32
	destructor *sync.Once
	// mutex owns connection and closed state only, never network I/O.
	writeOnce sync.Once
	writeGate chan struct{}
	done      chan struct{}
}

const defaultSocketWriteTimeout = 10 * time.Second

// WriteAdmittedError means native Write was attempted, so the broker may have
// received part or all of a publication. It must not be classified as not sent.
// Unwrap preserves cancellation and network error classification.
type WriteAdmittedError struct{ Err error }

func (e *WriteAdmittedError) Error() string { return "native write outcome is uncertain" }
func (e *WriteAdmittedError) Unwrap() error { return e.Err }

type socketAttemptWriter struct {
	connection net.Conn
	attempted  *bool
}

func (w socketAttemptWriter) Write(p []byte) (int, error) {
	*w.attempted = true
	return w.connection.Write(p)
}

func (sck *socket) initWriteGate() {
	sck.writeOnce.Do(func() {
		sck.writeGate = make(chan struct{}, 1)
		sck.writeGate <- struct{}{}
		sck.done = make(chan struct{})
	})
}

func (sck *socket) setOpen() {
	sck.mutex.Lock()
	defer sck.mutex.Unlock()
	sck.closed = 1
}

func (sck *socket) isOpen() bool {
	sck.mutex.Lock()
	defer sck.mutex.Unlock()
	return sck.closed == 1
}
func (sck *socket) shutdown(_ error) {
	_ = sck.abort()
}

// abort closes native I/O without waiting for write admission. Unlike graceful
// shutdown it remains usable while a peer is not reading a pending frame.
func (sck *socket) abort() error {
	sck.initWriteGate()
	var err error
	sck.destructor.Do(func() {
		sck.mutex.Lock()
		sck.closed = 0
		conn := sck.connection
		sck.mutex.Unlock()
		close(sck.done)
		if conn != nil {
			err = conn.Close()
		}
	})
	return err
}

// acquireWrite transfers sole writer/deadline ownership; the gate is not a
// state lock and waiting for it is cancellable. The returned release must run
// after all cancellation callbacks have finished touching this connection.
func (sck *socket) acquireWrite(ctx context.Context) (func(), error) {
	sck.initWriteGate()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-sck.done:
		return nil, net.ErrClosed
	case <-sck.writeGate:
	}
	release := func() { sck.writeGate <- struct{}{} }
	if err := ctx.Err(); err != nil {
		release()
		return nil, err
	}
	select {
	case <-sck.done:
		release()
		return nil, net.ErrClosed
	default:
	}
	return release, nil
}

func (sck *socket) writeAndFlush(buffer []byte) error {
	ctx, cancel := context.WithTimeout(context.Background(), defaultSocketWriteTimeout)
	defer cancel()
	return sck.writeAndFlushContext(ctx, buffer)
}

// writeAndFlushContext bounds both admission and native I/O. A failed admitted
// write invalidates the stream: a frame may have been partially transmitted.
func (sck *socket) writeAndFlushContext(ctx context.Context, buffer []byte) error {
	return sck.withWriteContext(ctx, func() error {
		if _, err := sck.writer.Write(buffer); err != nil {
			return err
		}
		return sck.writer.Flush()
	})
}

func (sck *socket) withWriteContext(ctx context.Context, write func() error) error {
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, defaultSocketWriteTimeout)
		defer cancel()
	}
	release, err := sck.acquireWrite(ctx)
	if err != nil {
		return err
	}
	defer release()
	sck.mutex.Lock()
	conn := sck.connection
	sck.mutex.Unlock()
	if conn == nil {
		return net.ErrClosed
	}
	deadline, _ := ctx.Deadline()
	if err := conn.SetWriteDeadline(deadline); err != nil {
		_ = sck.abort()
		return err
	}
	callbackDone := make(chan struct{})
	stop := context.AfterFunc(ctx, func() {
		_ = conn.SetWriteDeadline(time.Now())
		close(callbackDone)
	})
	attempted := false
	// Every previous successful operation flushed its whole frame; failed
	// operations are terminal. Reset establishes this operation's I/O observer.
	sck.writer.Reset(socketAttemptWriter{connection: conn, attempted: &attempted})
	err = write()
	if !stop() {
		<-callbackDone
	}
	if err != nil {
		_ = sck.abort()
		if ctx.Err() != nil {
			err = ctx.Err()
		} else if !time.Now().Before(deadline) {
			err = context.DeadlineExceeded
		}
		if attempted {
			return &WriteAdmittedError{Err: err}
		}
		return err
	}
	if err := conn.SetWriteDeadline(time.Time{}); err != nil {
		_ = sck.abort()
		if attempted {
			return &WriteAdmittedError{Err: err}
		}
		return err
	}
	return nil
}

func (c *Client) handleWrite(buffer []byte, response *Response) responseError {
	return c.handleWriteContext(context.Background(), buffer, response)
}

func (c *Client) handleWriteContext(ctx context.Context, buffer []byte, response *Response) responseError {
	return c.handleWriteWithResponseContext(ctx, buffer, response, true)
}

func (c *Client) handleWriteWithResponseContext(ctx context.Context, buffer []byte, response *Response, removeResponse bool) responseError {
	ctx, cancel := boundedOperationContext(ctx, c.socketCallTimeout)
	defer cancel()
	// Fail fast: an over-sized frame makes the broker close the connection and we
	// would block until timeout. 0 = no limit.
	if fm := c.maxFrameSize(); fm > 0 && len(buffer) > fm {
		c.coordinator.retireResponse(response)
		return newResponseError(
			fmt.Errorf("%w: frame size %d exceeds the maximum %d negotiated with the server, operation: %s",
				FrameTooLarge, len(buffer), fm, response.commandDescription), false)
	}

	if err := c.socket.writeAndFlushContext(ctx, buffer); err != nil {
		c.coordinator.retireResponse(response)
		return newResponseError(err, ctx.Err() != nil)
	}
	result := c.waitResponseCode(ctx, response)
	if removeResponse || result.Err != nil {
		c.coordinator.retireResponse(response)
	}
	return result
}
