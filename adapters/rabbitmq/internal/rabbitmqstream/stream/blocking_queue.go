package stream

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/faustbrian/go-rabbitmq-streams/adapters/rabbitmq/v2/internal/rabbitmqstream/logs"
)

var ErrBlockingQueueStopped = errors.New("blocking queue stopped")

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

// Enqueue adds an item to the queue, blocking if the queue is full
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

func (bq *BlockingQueue[T]) GetChannel() chan T {
	return bq.queue
}

func (bq *BlockingQueue[T]) Size() int {
	return len(bq.queue)
}

func (bq *BlockingQueue[T]) IsEmpty() bool {
	return len(bq.queue) == 0
}

// Stop stops the queue from accepting new items
// but allows some pending items.
// Stop is different from Close in that it allows the
// existing items to be processed.
// Drain the queue to be sure there are not pending messages
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

func (bq *BlockingQueue[T]) Close() {
	if bq.IsStopped() {
		bq.active.Wait()
		bq.closeOnce.Do(func() { atomic.StoreInt32(&bq.status, 2); close(bq.queue) })
	}
}

func (bq *BlockingQueue[T]) IsStopped() bool {
	return atomic.LoadInt32(&bq.status) == 1 || atomic.LoadInt32(&bq.status) == 2
}
