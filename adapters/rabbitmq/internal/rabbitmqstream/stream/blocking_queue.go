package stream

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/faustbrian/go-rabbitmq-streams/adapters/rabbitmq/v2/internal/rabbitmqstream/logs"
)

// ErrBlockingQueueStopped reports that stop has fenced further native queue
// admission.
var ErrBlockingQueueStopped = errors.New("blocking queue stopped")

// BlockingQueue owns a finite channel and sender admission gate. Stop joins
// admitted senders and drains pending items; Close owns channel closure only
// after stop. Direct channel operations bypass this gate.
type BlockingQueue[T any] struct {
	queue     chan T
	status    int32
	mutex     sync.Mutex
	active    sync.WaitGroup
	stopped   chan struct{}
	stopOnce  sync.Once
	closeOnce sync.Once
}

// NewBlockingQueue initializes a new BlockingQueue with the given capacity
func NewBlockingQueue[T any](capacity int) *BlockingQueue[T] {
	return &BlockingQueue[T]{
		queue:   make(chan T, capacity),
		status:  0,
		stopped: make(chan struct{}),
	}
}

// Enqueue delegates to EnqueueContext using the native fallback operation
// budget. Full-queue admission can block until that budget or Stop intervenes.
func (bq *BlockingQueue[T]) Enqueue(item T) error {
	ctx, cancel := ensureOperationContext(context.Background())
	defer cancel()
	return bq.EnqueueContext(ctx, item)
}

// EnqueueContext bounds admission and cannot race channel destruction: Stop
// rejects new admissions and joins existing senders before Close owns closure.
func (bq *BlockingQueue[T]) EnqueueContext(ctx context.Context, item T) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	bq.mutex.Lock()
	if bq.IsStopped() {
		bq.mutex.Unlock()
		return ErrBlockingQueueStopped
	}
	bq.active.Add(1)
	bq.mutex.Unlock()
	defer bq.active.Done()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-bq.stopped:
		return ErrBlockingQueueStopped
	case bq.queue <- item:
		return nil
	}
}

// GetChannel exposes the owned mutable channel for native receiving.
// Sending/closing directly bypasses admission ownership and is unsupported.
func (bq *BlockingQueue[T]) GetChannel() chan T {
	return bq.queue
}

// Size observes buffered items at this instant; it does not count admitted
// blocked senders.
func (bq *BlockingQueue[T]) Size() int {
	return len(bq.queue)
}

// IsEmpty observes current channel length, not quiescence or completed sender
// cleanup.
func (bq *BlockingQueue[T]) IsEmpty() bool {
	return len(bq.queue) == 0
}

// Stop fences new admissions, joins active senders and returns drained pending
// items after a 10 ms receive-idle interval. Concurrent receivers can take some
// items themselves; Stop does not process application effects or close the channel.
func (bq *BlockingQueue[T]) Stop() []T {
	bq.stopOnce.Do(func() {
		bq.mutex.Lock()
		atomic.StoreInt32(&bq.status, 1)
		close(bq.stopped)
		bq.mutex.Unlock()
	})
	bq.active.Wait()
	// drain the queue. To be sure there are not pending messages
	// in the queue and return to the caller the remaining pending messages
	msgInQueue := make([]T, 0, len(bq.queue))
outer:
	for {
		select {
		case msg, ok := <-bq.queue:
			if !ok {
				break outer
			}
			msgInQueue = append(msgInQueue, msg)
		case <-time.After(10 * time.Millisecond):
			break outer
		}
	}
	logs.LogDebug("BlockingQueue stopped")
	return msgInQueue
}

// Close closes the native-owned channel once only after Stop; calling it before
// Stop leaves the channel open.
func (bq *BlockingQueue[T]) Close() {
	if bq.IsStopped() {
		bq.active.Wait()
		bq.closeOnce.Do(func() { atomic.StoreInt32(&bq.status, 2); close(bq.queue) })
	}
}

// IsStopped reports the atomic stop-or-closed gate, not completion of all
// cleanup.
func (bq *BlockingQueue[T]) IsStopped() bool {
	return atomic.LoadInt32(&bq.status) == 1 || atomic.LoadInt32(&bq.status) == 2
}
