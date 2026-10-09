package stream

import (
	"context"
	"fmt"
	"net"
	"sync"
)

// taskOwner owns registration and draining, not task work. Admission is stopped
// before any join, so Wait cannot race a new task starting after it sees zero.
// No waiter goroutine is created: cancellation selects the actual drain signal.
type taskOwner struct {
	mutex   sync.Mutex
	stopped bool
	active  int
	drained chan struct{}
}

func (o *taskOwner) begin() (func(), bool) {
	o.mutex.Lock()
	if o.stopped {
		o.mutex.Unlock()
		return nil, false
	}
	if o.active == 0 {
		o.drained = make(chan struct{})
	}
	o.active++
	o.mutex.Unlock()
	return sync.OnceFunc(func() {
		o.mutex.Lock()
		o.active--
		if o.active == 0 {
			close(o.drained)
		}
		o.mutex.Unlock()
	}), true
}
func (o *taskOwner) stop() { o.mutex.Lock(); o.stopped = true; o.mutex.Unlock() }
func (o *taskOwner) wait(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	o.mutex.Lock()
	stopped, active, drained := o.stopped, o.active, o.drained
	o.mutex.Unlock()
	if !stopped {
		return fmt.Errorf("native owner has not been stopped")
	}
	if active == 0 {
		return nil
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-drained:
		return nil
	}
}

func (c *Client) beginTask(resource *taskOwner) (func(), bool) {
	c.socket.initWriteGate()
	owners := []*taskOwner{&c.tasks}
	if c.tcpParameters != nil && c.tcpParameters.lifecycle != nil {
		owners = append(owners, c.tcpParameters.lifecycle)
	}
	if resource != nil {
		owners = append(owners, resource)
	}
	dones := make([]func(), 0, len(owners))
	for _, owner := range owners {
		done, ok := owner.begin()
		if !ok {
			for _, release := range dones {
				release()
			}
			return nil, false
		}
		dones = append(dones, done)
	}
	return func() {
		for i := len(dones) - 1; i >= 0; i-- {
			dones[i]()
		}
	}, true
}
func (c *Client) startTask(resource *taskOwner, run func()) bool {
	done, ok := c.beginTask(resource)
	if !ok {
		return false
	}
	go func() { defer done(); run() }()
	return true
}

// WaitContext is an external-owner join after Close. It must not be called by
// this client's reader, heartbeat or message callback (those request Close only).
func (c *Client) WaitContext(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	return c.tasks.wait(ctx)
}
func (p *Producer) WaitContext(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	return p.tasks.wait(ctx)
}
func (c *Consumer) WaitContext(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	return c.tasks.wait(ctx)
}

// CloseContext is terminal abort-and-join for the environment's external owner.
// The owner must first release callback backpressure (for example session.done).
// A context error is an explicit unjoined result, never a successful cleanup.
func (env *Environment) CloseContext(ctx context.Context) error {
	ctx, cancel := ensureOperationContext(ctx)
	defer cancel()
	_ = env.Abort()
	return env.WaitContext(ctx)
}

// WaitContext joins every admitted reachable native task, including clients no
// longer present in a pool lookup. Call only from outside those tasks after Abort.
func (env *Environment) WaitContext(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if env.options != nil && env.options.TCPParameters != nil && env.options.TCPParameters.lifecycle != nil {
		return env.options.TCPParameters.lifecycle.wait(ctx)
	}
	return nil
}

func (env *Environment) nativeClients() []*Client {
	seen := map[*Client]bool{}
	clients := []*Client{}
	add := func(c *Client) {
		if c != nil && !seen[c] {
			seen[c] = true
			clients = append(clients, c)
		}
	}
	if env.locator != nil {
		add(env.locator.current())
	}
	var sets []map[string]*environmentCoordinator
	if env.producers != nil {
		sets = append(sets, env.producers.getCoordinators())
	}
	if env.consumers != nil {
		sets = append(sets, env.consumers.getCoordinators())
	}
	for _, set := range sets {
		for _, coordinator := range set {
			coordinator.clientsPerContext.Range(func(_, v any) bool { add(v.(*Client)); return true })
		}
	}
	return clients
}

func (p *Producer) operationContext(ctx context.Context) (context.Context, func(), error) {
	ctx, end := ensureOperationContext(ctx)
	done, ok := p.client.beginTask(&p.tasks)
	if !ok {
		end()
		return nil, nil, net.ErrClosed
	}
	op, cancel := context.WithCancel(ctx)
	cancelled := make(chan struct{})
	stop := context.AfterFunc(p.lifetime, func() { defer close(cancelled); cancel() })
	if p.getStatus() == closed {
		cancel()
	}
	return op, func() {
		if !stop() {
			<-cancelled
		}
		cancel()
		end()
		done()
	}, nil
}
