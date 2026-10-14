package stream

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"sync"
	"time"

	"github.com/faustbrian/go-rabbitmq-streams/adapters/rabbitmq/v2/internal/rabbitmqstream/amqp"
)

// Coordinator owns native producer/consumer lookups and response correlation
// admission. Correlation IDs are allocated under its mutex without replacing
// live owners; returned resources/maps are borrowed and zero state requires
// NewCoordinator.
type Coordinator struct {
	counter          uint32
	producers        *sync.Map
	consumers        *sync.Map
	responses        map[any]*Response
	nextItemProducer uint8
	nextItemConsumer uint8
	mutex            *sync.Mutex
}

// Code is a native protocol-response code delivered through an owned response
// mailbox.
type Code struct {
	id uint16
}

type offsetMessage struct {
	message *amqp.Message
	offset  int64
}

type offsetMessages = []*offsetMessage
type chunkInfo struct {
	offsetMessages offsetMessages
	numEntries     uint16
	numRecords     uint32
}

// Response is an operation-owned buffered code/data mailbox. Retirement removes
// lookup ownership without destroying channels already owned by the reader.
type Response struct {
	code               chan Code
	data               chan any
	commandDescription string
	correlationid      uint32
}

// NewCoordinator initializes independent resource maps and response admission state.
func NewCoordinator() *Coordinator {
	return &Coordinator{mutex: &sync.Mutex{},
		producers: &sync.Map{},
		consumers: &sync.Map{},
		responses: make(map[any]*Response)}
}

// NewProducer allocates a native ID, confirmation ticker and pending queues under the
// coordinator mutex. Options are borrowed and must already be validated; cleanup
// is retained for resource close, not called while registering.
// producersEnvironment
func (coordinator *Coordinator) NewProducer(
	parameters *ProducerOptions, cleanUp func()) (*Producer, error) {
	coordinator.mutex.Lock()
	defer coordinator.mutex.Unlock()
	dynSize := 10000
	queueSize := defaultQueuePublisherSize
	tickerTime := defaultConfirmationTimeOut
	if parameters != nil {
		dynSize = parameters.BatchSize
		tickerTime = parameters.ConfirmationTimeOut
		queueSize = parameters.QueueSize
	}

	var lastId, err = coordinator.getNextProducerItem()
	if err != nil {
		return nil, err
	}
	lifetime, stopLifetime := context.WithCancel(context.Background())
	var producer = &Producer{
		lifetime: lifetime, stopLifetime: stopLifetime, confirmationDone: make(chan struct{}),
		options:                   parameters,
		mutex:                     &sync.RWMutex{},
		unConfirmed:               newUnConfirmed(queueSize),
		confirmationTimeoutTicker: time.NewTicker(tickerTime),
		doneTimeoutTicker:         make(chan struct{}, 1),
		pendingSequencesQueue:     NewBlockingQueue[*messageSequence](dynSize),
		confirmMutex:              &sync.Mutex{},
		status:                    open,
		onClose:                   cleanUp,
	}

	producer.setID(lastId)
	coordinator.producers.Store(lastId, producer)
	return producer, err
}

// RemoveConsumerById extracts lookup ownership and requests close without joining dispatch.
func (coordinator *Coordinator) RemoveConsumerById(id any, reason Event) error {
	consumer, err := coordinator.ExtractConsumerById(id)
	if err != nil {
		return err
	}
	consumer.close(reason)
	return nil
}

// Consumers returns the live borrowed concurrent lookup map, not a snapshot.
func (coordinator *Coordinator) Consumers() *sync.Map {
	return coordinator.consumers
}

// RemoveProducerById extracts lookup ownership and requests close without joining all tasks.
func (coordinator *Coordinator) RemoveProducerById(id uint8, reason Event) error {
	producer, err := coordinator.ExtractProducerById(id)
	if err != nil {
		return err
	}
	return producer.close(reason)
}

// RemoveResponseById retires the formatted numeric lookup key without closing reader-owned mailboxes.
func (coordinator *Coordinator) RemoveResponseById(id any) error {
	key := fmt.Sprintf("%d", id)
	coordinator.mutex.Lock()
	_, ok := coordinator.responses[key]
	if !ok {
		coordinator.mutex.Unlock()
		return fmt.Errorf("response %q not found", key)
	}
	delete(coordinator.responses, key)
	coordinator.mutex.Unlock()

	// The response reader may already own the response. Retirement removes lookup
	// ownership; its buffered delivery channels must remain safe for that reader.
	return nil
}

// retireResponse releases only the operation's original owner, even if an
// earlier explicit retirement allowed the numeric ID to be reused after wrap.
func (coordinator *Coordinator) retireResponse(response *Response) {
	key := strconv.FormatUint(uint64(response.correlationid), 10)
	coordinator.mutex.Lock()
	if coordinator.responses[key] == response {
		delete(coordinator.responses, key)
	}
	coordinator.mutex.Unlock()
}

// ProducersCount counts one traversal of the live producer map, not a quiescent total.
func (coordinator *Coordinator) ProducersCount() int {
	return coordinator.countSyncMap(coordinator.producers)
}

// response
func newResponse(commandDescription string) *Response {
	res := &Response{}
	res.commandDescription = commandDescription
	res.code = make(chan Code, 1)
	res.data = make(chan any, 1)
	return res
}

// NewResponseWithName registers a handshake mailbox, replacing an existing named entry.
// Callers own distinct handshake names; this is not numeric correlation admission.
func (coordinator *Coordinator) NewResponseWithName(id string) *Response {
	coordinator.mutex.Lock()
	res := newResponse(id)
	coordinator.responses[id] = res
	coordinator.mutex.Unlock()
	return res
}

// NewResponse atomically reserves an unused uint32 correlation ID and mailbox.
// Diagnostic info is retained verbatim; callers must avoid secrets.
func (coordinator *Coordinator) NewResponse(commandId uint16, info ...string) (*Response, error) {
	description := lookUpCommand(commandId)
	if len(info) > 0 {
		description = fmt.Sprintf("%s, - %s", description, info[0])
	}

	coordinator.mutex.Lock()
	defer coordinator.mutex.Unlock()
	id, err := coordinator.nextCorrelationIDLocked()
	if err != nil {
		return nil, err
	}
	res := newResponse(description)
	res.correlationid = id
	coordinator.responses[strconv.FormatUint(uint64(id), 10)] = res
	return res, nil
}

// The response map mutex owns ID allocation and registration atomically. There
// are at most len(responses) occupied candidates; one further candidate is free
// unless the entire uint32 namespace is occupied. Wrapping never replaces a live
// owner. Named handshake responses do not consume a wire correlation ID.
func (coordinator *Coordinator) nextCorrelationIDLocked() (uint32, error) {
	for attempts := uint64(0); attempts <= uint64(len(coordinator.responses)) && attempts <= math.MaxUint32; attempts++ {
		coordinator.counter++
		if _, occupied := coordinator.responses[strconv.FormatUint(uint64(coordinator.counter), 10)]; !occupied {
			return coordinator.counter, nil
		}
	}
	return 0, fmt.Errorf("all protocol correlation IDs are pending")
}

// GetResponseByName borrows a registered mailbox or returns a diagnostic containing its key.
func (coordinator *Coordinator) GetResponseByName(id string) (*Response, error) {
	coordinator.mutex.Lock()
	defer coordinator.mutex.Unlock()
	c, ok := coordinator.responses[id]
	if !ok {
		return nil, fmt.Errorf("response %q not found", id)
	}

	return c, nil
}

// RemoveResponseByName retires a named lookup without destroying borrowed mailbox channels.
func (coordinator *Coordinator) RemoveResponseByName(id string) error {
	coordinator.mutex.Lock()
	defer coordinator.mutex.Unlock()
	if _, ok := coordinator.responses[id]; !ok {
		return fmt.Errorf("response %q not found", id)
	}
	delete(coordinator.responses, id)
	return nil
}

// NewConsumer allocates a native ID and bounded chunk mailbox. Parameters must be
// non-nil and validated; callback/options are borrowed and may delay dispatch.
// Consumer functions
func (coordinator *Coordinator) NewConsumer(
	messagesHandler MessagesHandler,
	parameters *ConsumerOptions,
	cleanUp func(),
) (*Consumer, error) {
	coordinator.mutex.Lock()
	defer coordinator.mutex.Unlock()
	lastId, err := coordinator.getNextConsumerItem()
	if err != nil {
		return nil, err
	}
	var consumer = &Consumer{
		options:              parameters,
		response:             newResponse(lookUpCommand(commandSubscribe)),
		status:               open,
		mutex:                &sync.RWMutex{},
		MessagesHandler:      messagesHandler,
		currentOffset:        -1, // currentOffset has to equal lastStoredOffset as the currentOffset 0 may otherwise be flushed to the server when the consumer is closed and auto commit is enabled
		lastStoredOffset:     -1, // because 0 is a valid value for the offset
		isPromotedAsActive:   true,
		lastAutoCommitStored: time.Now(),
		// The +5 provides a small buffer to prevent the consumer loop from blocking.
		chunkForConsumer: make(chan chunkInfo, parameters.initialCredits+5),
		closeCh:          make(chan struct{}),
		onClose:          cleanUp,
	}
	consumer.setID(lastId)
	coordinator.consumers.Store(lastId, consumer)
	return consumer, nil
}

// GetConsumerById borrows a registered consumer; absence returns a key-bearing diagnostic.
func (coordinator *Coordinator) GetConsumerById(id any) (*Consumer, error) {
	if consumer, exists := coordinator.consumers.Load(id); exists {
		return consumer.(*Consumer), nil
	}

	return nil, fmt.Errorf("consumer %v not found", id)
}

// ExtractConsumerById atomically removes and transfers lookup ownership without closing the consumer.
func (coordinator *Coordinator) ExtractConsumerById(id any) (*Consumer, error) {
	if consumer, exists := coordinator.consumers.LoadAndDelete(id); exists {
		return consumer.(*Consumer), nil
	}

	return nil, fmt.Errorf("consumer %v not found", id)
}

// GetResponseById borrows the live operation mailbox for a numeric correlation ID.
func (coordinator *Coordinator) GetResponseById(id uint32) (*Response, error) {
	v, err := coordinator.getById(fmt.Sprintf("%d", id), coordinator.responses)
	if err != nil {
		return nil, err
	}
	return v.(*Response), nil
}

// ConsumersCount counts one traversal of the live consumer map, not a quiescent total.
func (coordinator *Coordinator) ConsumersCount() int {
	return coordinator.countSyncMap(coordinator.consumers)
}

// GetProducerById borrows a registered producer; absence returns a key-bearing diagnostic.
func (coordinator *Coordinator) GetProducerById(id any) (*Producer, error) {
	if producer, exists := coordinator.producers.Load(id); exists {
		return producer.(*Producer), nil
	}

	return nil, fmt.Errorf("producer %v not found", id)
}

// ExtractProducerById atomically removes and transfers lookup ownership without closing the producer.
func (coordinator *Coordinator) ExtractProducerById(id any) (*Producer, error) {
	if producer, exists := coordinator.producers.LoadAndDelete(id); exists {
		return producer.(*Producer), nil
	}

	return nil, fmt.Errorf("producer %v not found", id)
}

// general functions
func (coordinator *Coordinator) getById(id any, refmap map[any]*Response) (any, error) {
	coordinator.mutex.Lock()
	defer coordinator.mutex.Unlock()
	v, ok := refmap[id]
	if !ok {
		return nil, fmt.Errorf("item %v not found", id)
	}
	return v, nil
}

func (coordinator *Coordinator) countSyncMap(refmap *sync.Map) int {
	count := 0
	refmap.Range(func(_, _ any) bool {
		count++
		return true
	})

	return count
}

func (coordinator *Coordinator) getNextProducerItem() (uint8, error) {
	if coordinator.nextItemProducer >= ^uint8(0) {
		return coordinator.reuseFreeId(coordinator.producers)
	}
	res := coordinator.nextItemProducer
	coordinator.nextItemProducer++
	return res, nil
}

func (coordinator *Coordinator) getNextConsumerItem() (uint8, error) {
	if coordinator.nextItemConsumer >= ^uint8(0) {
		return coordinator.reuseFreeId(coordinator.consumers)
	}
	res := coordinator.nextItemConsumer
	coordinator.nextItemConsumer++
	return res, nil
}

func (coordinator *Coordinator) reuseFreeId(refMap *sync.Map) (byte, error) {
	for i := range int(^uint8(0)) {
		if _, exists := refMap.Load(byte(i)); !exists {
			return byte(i), nil
		}
	}
	return 0, fmt.Errorf("no more items available")
}

// Producers returns the live borrowed concurrent lookup map, not a snapshot.
func (coordinator *Coordinator) Producers() *sync.Map {
	return coordinator.producers
}

// Close requests resource closes and clears response lookups. It is not a task
// join; callback/native close cooperation is still required.
func (coordinator *Coordinator) Close() {
	coordinator.producers.Range(func(_, producer any) bool {
		_ = producer.(*Producer).Close()

		return true
	})

	coordinator.consumers.Range(func(_, consumer any) bool {
		_ = consumer.(*Consumer).Close()

		return true
	})

	coordinator.mutex.Lock()
	coordinator.responses = make(map[any]*Response)
	coordinator.mutex.Unlock()
}
