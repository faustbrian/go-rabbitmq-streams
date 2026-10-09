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

type Coordinator struct {
	counter          uint32
	producers        *sync.Map
	consumers        *sync.Map
	responses        map[any]*Response
	nextItemProducer uint8
	nextItemConsumer uint8
	mutex            *sync.Mutex
}

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
}

type Response struct {
	code               chan Code
	data               chan any
	commandDescription string
	correlationid      uint32
}

func NewCoordinator() *Coordinator {
	return &Coordinator{mutex: &sync.Mutex{},
		producers: &sync.Map{},
		consumers: &sync.Map{},
		responses: make(map[any]*Response)}
}

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

func (coordinator *Coordinator) RemoveConsumerById(id any, reason Event) error {
	consumer, err := coordinator.ExtractConsumerById(id)
	if err != nil {
		return err
	}
	consumer.close(reason)
	return nil
}
func (coordinator *Coordinator) Consumers() *sync.Map {
	return coordinator.consumers
}

func (coordinator *Coordinator) RemoveProducerById(id uint8, reason Event) error {
	producer, err := coordinator.ExtractProducerById(id)
	if err != nil {
		return err
	}
	return producer.close(reason)
}

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

func (coordinator *Coordinator) NewResponseWithName(id string) *Response {
	coordinator.mutex.Lock()
	res := newResponse(id)
	coordinator.responses[id] = res
	coordinator.mutex.Unlock()
	return res
}

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

func (coordinator *Coordinator) GetResponseByName(id string) (*Response, error) {
	coordinator.mutex.Lock()
	defer coordinator.mutex.Unlock()
	c, ok := coordinator.responses[id]
	if !ok {
		return nil, fmt.Errorf("response %q not found", id)
	}

	return c, nil
}

func (coordinator *Coordinator) RemoveResponseByName(id string) error {
	coordinator.mutex.Lock()
	defer coordinator.mutex.Unlock()
	if _, ok := coordinator.responses[id]; !ok {
		return fmt.Errorf("response %q not found", id)
	}
	delete(coordinator.responses, id)
	return nil
}

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

func (coordinator *Coordinator) GetConsumerById(id any) (*Consumer, error) {
	if consumer, exists := coordinator.consumers.Load(id); exists {
		return consumer.(*Consumer), nil
	}

	return nil, fmt.Errorf("consumer %v not found", id)
}

func (coordinator *Coordinator) ExtractConsumerById(id any) (*Consumer, error) {
	if consumer, exists := coordinator.consumers.LoadAndDelete(id); exists {
		return consumer.(*Consumer), nil
	}

	return nil, fmt.Errorf("consumer %v not found", id)
}

func (coordinator *Coordinator) GetResponseById(id uint32) (*Response, error) {
	v, err := coordinator.getById(fmt.Sprintf("%d", id), coordinator.responses)
	if err != nil {
		return nil, err
	}
	return v.(*Response), nil
}

func (coordinator *Coordinator) ConsumersCount() int {
	return coordinator.countSyncMap(coordinator.consumers)
}

func (coordinator *Coordinator) GetProducerById(id any) (*Producer, error) {
	if producer, exists := coordinator.producers.Load(id); exists {
		return producer.(*Producer), nil
	}

	return nil, fmt.Errorf("producer %v not found", id)
}

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

func (coordinator *Coordinator) Producers() *sync.Map {
	return coordinator.producers
}

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
