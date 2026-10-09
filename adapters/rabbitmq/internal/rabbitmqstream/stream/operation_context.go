package stream

import (
	"context"
	"net"
	"sync"
	"time"
)

// contextGate owns operation admission, not mutable connection state. Unlike a
// mutex, a caller can abandon its place while a previous operation owns I/O.
type contextGate struct {
	once  sync.Once
	token chan struct{}
}

func (g *contextGate) acquire(ctx context.Context) (func(), error) {
	g.once.Do(func() { g.token = make(chan struct{}, 1); g.token <- struct{}{} })
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-g.token:
	}
	release := func() { g.token <- struct{}{} }
	if err := ctx.Err(); err != nil {
		release()
		return nil, err
	}
	return release, nil
}

func boundedOperationContext(ctx context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	if timeout <= 0 {
		timeout = defaultSocketCallTimeout
	}
	return context.WithTimeout(ctx, timeout)
}

func ensureOperationContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if _, ok := ctx.Deadline(); ok {
		return ctx, func() {}
	}
	return boundedOperationContext(ctx, defaultSocketCallTimeout)
}

func (env *Environment) operationContext(ctx context.Context) (context.Context, context.CancelFunc) {
	ctx, end := ensureOperationContext(ctx)
	operation, cancel := context.WithCancel(ctx)
	if env.lifetime == nil {
		return operation, func() { cancel(); end() }
	}
	stop := context.AfterFunc(env.lifetime, cancel)
	if env.IsClosed() {
		cancel()
	}
	return operation, func() { stop(); cancel(); end() }
}

func (l *locator) current() *Client {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	return l.client
}

func (env *Environment) setLocatorClient(c *Client) error {
	env.locator.mutex.Lock()
	defer env.locator.mutex.Unlock()
	if env.IsClosed() {
		_ = c.socket.abort()
		return net.ErrClosed
	}
	env.locator.client = c
	return nil
}

// Abort closes actual connections without waiting for producer/consumer
// callbacks, graceful RPCs, or operation admission. It is terminal.
func (env *Environment) Abort() error {
	env.closed.Store(true)
	if env.stopLifetime != nil {
		env.stopLifetime()
	}
	if env.options != nil && env.options.TCPParameters != nil && env.options.TCPParameters.lifecycle != nil {
		env.options.TCPParameters.lifecycle.stop()
	}
	clients := env.nativeClients()
	// Signal every socket before resource cleanup or any external-owner join.
	for _, c := range clients {
		_ = c.socket.abort()
		c.tasks.stop()
	}
	for _, c := range clients {
		c.Close()
	}
	return nil
}

func sleepContext(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (c *Client) waitResponseData(ctx context.Context, response *Response) (any, error) {
	c.socket.initWriteGate()
	select {
	case <-ctx.Done():
		_ = c.socket.abort()
		return nil, ctx.Err()
	case <-c.socket.done:
		return nil, net.ErrClosed
	case data, ok := <-response.data:
		if !ok {
			return nil, net.ErrClosed
		}
		return data, nil
	}
}

func (c *Client) waitResponseCode(ctx context.Context, response *Response) responseError {
	c.socket.initWriteGate()
	select {
	case <-ctx.Done():
		_ = c.socket.abort()
		return newResponseError(ctx.Err(), true)
	case <-c.socket.done:
		return newResponseError(net.ErrClosed, false)
	case code, ok := <-response.code:
		if !ok {
			return newResponseError(net.ErrClosed, false)
		}
		if code.id != responseCodeOk {
			return newResponseError(lookErrorCode(code.id), false)
		}
		return newResponseError(nil, false)
	}
}
