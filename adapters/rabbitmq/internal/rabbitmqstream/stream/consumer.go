package stream

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/faustbrian/go-rabbitmq-streams/adapters/rabbitmq/v2/internal/rabbitmqstream/amqp"
	logs "github.com/faustbrian/go-rabbitmq-streams/adapters/rabbitmq/v2/internal/rabbitmqstream/logs"
)

// Consumer owns one native subscription, chunk dispatch and local offset state.
// Close stops rather than joins callback work; an external owner uses
// WaitContext. Getter offsets are local observations, not durable broker
// receipts.
type Consumer struct {
	terminalEvent Event
	tasks         taskOwner
	offsetGate    contextGate
	client        *Client
	// ID is the native subscription identifier. Prefer GetID; direct mutation can break lookup ownership.
	//
	// Deprecated: ConsumerID is deprecated. Use GetID() instead.
	ID               uint8 // also the SubscriptionId
	response         *Response
	options          *ConsumerOptions
	onClose          func()
	mutex            *sync.RWMutex
	chunkForConsumer chan chunkInfo
	// closeCh is closed when the consumer is shutting down.
	closeCh chan struct{}
	// closeOnce ensures close() is idempotent even if called concurrently.
	closeOnce sync.Once
	// MessagesHandler is borrowed synchronous dispatch code; blocked work delays external joins.
	MessagesHandler MessagesHandler
	// different form ConsumerOptions.offset. ConsumerOptions.offset is just the configuration
	// and won't change. currentOffset is the status of the offset
	currentOffset int64

	// Remembers the last stored offset (manual or automatic) to avoid to store always the same values
	lastStoredOffset int64

	closeHandler chan Event
	// see autocommit strategy
	// it is needed to trigger the
	// auto-commit after messageCountBeforeStorage
	messageCountBeforeStorage int

	status int

	// Single Active consumer. The consumer can be running
	// but not active. This flag is used to know if the consumer
	// is in waiting mode or not.
	// in normal mode, the consumer is always isPromotedAsActive==true
	isPromotedAsActive bool

	// lastAutoCommitStored tracks when the offset was last flushed
	lastAutoCommitStored time.Time
}

func (consumer *Consumer) getStatus() int {
	consumer.mutex.Lock()
	defer consumer.mutex.Unlock()
	return consumer.status
}

func (consumer *Consumer) isZombie() bool {
	consumer.mutex.Lock()
	defer consumer.mutex.Unlock()
	return consumer.status == open && !consumer.client.socket.isOpen()
}

// GetStreamName observes the configured native stream name under its mutex, not
// a topology snapshot.
func (consumer *Consumer) GetStreamName() string {
	if consumer.options == nil {
		return ""
	}
	return consumer.options.streamName
}

// GetName observes the configured broker offset-tracking reference; it is not a
// principal or durable checkpoint.
func (consumer *Consumer) GetName() string {
	if consumer.options == nil {
		return ""
	}
	return consumer.options.ConsumerName
}

func (consumer *Consumer) setCurrentOffset(offset int64) {
	consumer.mutex.Lock()
	defer consumer.mutex.Unlock()
	consumer.currentOffset = offset
}

// GetOffset returns the locally observed current delivery offset; it is not
// broker-stored progress.
func (consumer *Consumer) GetOffset() int64 {
	consumer.mutex.Lock()
	defer consumer.mutex.Unlock()
	return consumer.currentOffset
}

// GetID returns the native subscription ID, not a delivery offset or
// application fence.
func (consumer *Consumer) GetID() uint8 {
	consumer.mutex.RLock()
	defer consumer.mutex.RUnlock()
	return consumer.ID
}

func (consumer *Consumer) setID(id uint8) {
	consumer.mutex.Lock()
	defer consumer.mutex.Unlock()
	consumer.ID = id
}

// isActive returns true if the consumer is promoted as active
// used for Single Active Consumer. Always true in other cases
func (consumer *Consumer) isActive() bool {
	consumer.mutex.Lock()
	defer consumer.mutex.Unlock()
	return consumer.isPromotedAsActive
}

func (consumer *Consumer) setPromotedAsActive(promoted bool) {
	consumer.mutex.Lock()
	defer consumer.mutex.Unlock()
	consumer.isPromotedAsActive = promoted
}

// GetLastStoredOffset returns the locally accepted store offset, not a
// confirmed durable broker checkpoint.
//
// Deprecated: The method name may be misleading.
// The method does not indicate the last message stored, but the last stored in memory.
// The method was added to avoid to query the offset from the server, but it created confusion.
// Use `QueryOffset` instead.:
//
//	     	offset, err := consumer.QueryOffset()
//		// or:
//			offset, err := env.QueryOffset(consumerName, streamName)
//		 // check the error
//		 ....
//		 SetOffset(stream.OffsetSpecification{}.Offset(offset)).
//
// There is an edge case in which multiple clients use the same consumer name,
// and the last stored offset in memory is not the one the user expects.
// So, to avoid confusion, it is better to use QueryOffset, which always gets the value from the server.
func (consumer *Consumer) GetLastStoredOffset() int64 {
	consumer.mutex.Lock()
	defer consumer.mutex.Unlock()
	return consumer.lastStoredOffset
}

// GetCloseHandler returns a borrowed native-owned terminal channel; callers
// must not close or replace its ownership.
func (consumer *Consumer) GetCloseHandler() chan Event {
	consumer.mutex.Lock()
	defer consumer.mutex.Unlock()
	return consumer.closeHandler
}

// NotifyClose returns a native-owned single terminal event channel, including
// an already closed terminal snapshot.
func (consumer *Consumer) NotifyClose() ChannelClose {
	consumer.mutex.Lock()
	defer consumer.mutex.Unlock()
	if consumer.closeHandler != nil {
		return consumer.closeHandler
	}
	ch := make(chan Event, 1)
	if consumer.status == closed {
		ch <- consumer.terminalEvent
		close(ch)
		return ch
	}
	consumer.closeHandler = ch
	return ch
}

// Credit requests positive manual chunk credits only in manual mode; nil return
// is local command submission, not a delivery acknowledgement.
func (consumer *Consumer) Credit(credits int16) error {
	if consumer.options.CreditStrategy != ManualCreditStrategy {
		return fmt.Errorf("credit can only be called when CreditStrategy is ManualCreditStrategy")
	}
	if credits <= 0 {
		return fmt.Errorf("credits must be a positive number")
	}
	consumer.client.credit(consumer.ID, credits)
	return nil
}

// ConsumerContext exposes a borrowed native subscription and current chunk
// metadata during its callback, not a durable replay checkpoint.
type ConsumerContext struct {
	// Consumer borrows the dispatching owner; do not wait for its own task from a callback.
	Consumer  *Consumer
	chunkInfo *chunkInfo
}

// GetEntriesCount returns the current chunk entry count; subentries can contain
// multiple messages, so it is not a general record-count oracle.
func (cc ConsumerContext) GetEntriesCount() uint16 {
	return cc.chunkInfo.numEntries
}

// GetRecordsCount returns the original logical record count of the delivered
// chunk, including records preceding an explicit subscription offset.
func (cc ConsumerContext) GetRecordsCount() uint32 {
	return cc.chunkInfo.numRecords
}

// MessagesHandler is the function that will be called when a message is received. This function
// is used to process incoming messages. The consumer context allows access to the consumer handling
// the message and some chunk information.
//
// When manual credit strategy is used, the consumer context allows to send credits to the broker.
type MessagesHandler func(consumerContext ConsumerContext, message *amqp.Message)

// AutoCommitStrategy controls native local offset-store count/time cadence, not
// transactional or acknowledged broker storage. Configure it before
// subscription.
type AutoCommitStrategy struct {
	messageCountBeforeStorage int
	flushInterval             time.Duration
}

// SetCountBeforeStorage stores native local offset-command cadence; this does
// not bound durable crash redelivery.
func (ac *AutoCommitStrategy) SetCountBeforeStorage(messageCountBeforeStorage int) *AutoCommitStrategy {
	ac.messageCountBeforeStorage = messageCountBeforeStorage
	return ac
}

// SetFlushInterval stores the native auto-store interval; configure before
// consuming and do not treat it as an acknowledged receipt.
func (ac *AutoCommitStrategy) SetFlushInterval(flushInterval time.Duration) *AutoCommitStrategy {
	ac.flushInterval = flushInterval
	return ac
}

// NewAutoCommitStrategy returns native defaults of 10000 messages and five
// seconds; the public adapter selects manual offset storage.
func NewAutoCommitStrategy() *AutoCommitStrategy {
	return &AutoCommitStrategy{
		messageCountBeforeStorage: 10_000,
		flushInterval:             5 * time.Second,
	}
}

// PostFilter is an inline borrowed decoded-message predicate; callers own
// prompt completion and synchronization.
type PostFilter func(message *amqp.Message) bool

// ConsumerFilter borrows filter values and a post-filter callback. Broker
// filtering reduces deliveries but is not authentication or authorization.
type ConsumerFilter struct {
	// Values borrows broker filtering values; initialization validates native limits.
	Values []string
	// MatchUnfiltered requests records without a broker filter value too.
	MatchUnfiltered bool
	// PostFilter is an optional borrowed local predicate, not broker authorization.
	PostFilter PostFilter
}

// NewConsumerFilter borrows values and predicate without copying; configure
// exclusively before opening.
func NewConsumerFilter(values []string, matchUnfiltered bool, postFilter PostFilter) *ConsumerFilter {
	return &ConsumerFilter{
		Values:          values,
		MatchUnfiltered: matchUnfiltered,
		PostFilter:      postFilter,
	}
}

// ConsumerUpdate is an inline native promotion callback returning the requested
// subscription offset; it is not an application-effect fence.
type ConsumerUpdate func(streamName string, isActive bool) OffsetSpecification

// SingleActiveConsumer holds native activation/offset policy; callbacks remain
// borrowed and must not join their own subscription.
type SingleActiveConsumer struct {
	// Enabled requests broker promotion coordination, not application fencing.
	Enabled bool
	// ConsumerUpdate is borrowed inline code invoked for both active and inactive
	// broker notifications. The boolean reports current promotion and the result
	// chooses a requested subscription offset, not an external-effect fence.
	ConsumerUpdate ConsumerUpdate
	// This offset is the one form the user that decides from the ConsumerUpdate function
	// nothing to do with: ConsumerOptions.Offset
	// the ConsumerOptions.Offset is the initial offset and in case of SingleActiveConsumer
	// is not used because the consumer will be promoted and the offset will be set by the ConsumerUpdate
	// This is needed to filter the messages during the promotion where needed
	offsetSpecification OffsetSpecification

	// SingleActiveConsumer can be used with the super stream consumer
	// in this case we need to pass the super stream name
	superStream string
}

// NewSingleActiveConsumer enables native promotion handling with a borrowed
// callback; it is not an application-effect fence.
func NewSingleActiveConsumer(consumerUpdate ConsumerUpdate) *SingleActiveConsumer {
	return &SingleActiveConsumer{
		Enabled:        true,
		ConsumerUpdate: consumerUpdate,
	}
}

func newSingleActiveConsumerWithAllParameters(
	consumerUpdate ConsumerUpdate, isEnabled bool, superStream string) *SingleActiveConsumer {
	return &SingleActiveConsumer{
		Enabled:        isEnabled,
		ConsumerUpdate: consumerUpdate,
		superStream:    superStream,
	}
}

// SetEnabled mutates native single-active-consumer policy before opening, not
// application effect fencing.
func (s *SingleActiveConsumer) SetEnabled(enabled bool) *SingleActiveConsumer {
	s.Enabled = enabled
	return s
}

// CreditStrategy selects native chunk flow-control ownership, not a per-message
// acknowledgement.
type CreditStrategy int

const (
	// AutomaticCreditStrategy replenishes one native chunk credit during dispatch, not a per-message acknowledgment.
	AutomaticCreditStrategy CreditStrategy = iota // Default, sends 1 credit per chunk
	// ManualCreditStrategy leaves chunk credit replenishment to the caller.
	ManualCreditStrategy // User manages credits
)

// ConsumerOptions for a consumer
type ConsumerOptions struct {
	// ConsumerName selects named offset storage; reuse shares a broker progress reference.
	ConsumerName       string
	streamName         string
	autocommit         bool
	autoCommitStrategy *AutoCommitStrategy
	// Offset selects native starting position and does not itself store progress.
	Offset OffsetSpecification
	// CRCCheck checks chunk corruption; it is not authenticity verification.
	CRCCheck       bool
	initialCredits int16
	// ClientProvidedName is a broker-visible diagnostic identifier.
	ClientProvidedName string
	// Filter borrows optional native broker/local filtering configuration.
	Filter *ConsumerFilter
	// SingleActiveConsumer borrows promotion policy; external effects still need fencing.
	SingleActiveConsumer *SingleActiveConsumer
	// CreditStrategy selects chunk credit replenishment, not per-message acknowledgment.
	CreditStrategy CreditStrategy
}

// NewConsumerOptions returns a new ConsumerOptions instance
func NewConsumerOptions() *ConsumerOptions {
	return &ConsumerOptions{
		Offset:             OffsetSpecification{}.Last(),
		autocommit:         false,
		autoCommitStrategy: NewAutoCommitStrategy(),
		CRCCheck:           true,
		CreditStrategy:     AutomaticCreditStrategy,
		initialCredits:     10,
		ClientProvidedName: "go-stream-consumer",
		Filter:             nil,
	}
}

// SetConsumerName sets the broker offset-tracking identity; using the same name
// from concurrent owners can change restart progress.
func (c *ConsumerOptions) SetConsumerName(consumerName string) *ConsumerOptions {
	c.ConsumerName = consumerName
	return c
}

// SetCRCCheck selects native chunk integrity checking, not sender
// authentication.
func (c *ConsumerOptions) SetCRCCheck(crcCheck bool) *ConsumerOptions {
	c.CRCCheck = crcCheck
	return c
}

// SetInitialCredits stores raw chunk credits; opening validates native credit
// policy, not this setter.
func (c *ConsumerOptions) SetInitialCredits(initialCredits int16) *ConsumerOptions {
	c.initialCredits = initialCredits
	return c
}

// SetAutoCommit enables native count/time-driven offset submission; a nil
// strategy selects defaults and storage is not atomic with effects.
func (c *ConsumerOptions) SetAutoCommit(autoCommitStrategy *AutoCommitStrategy) *ConsumerOptions {
	c.autocommit = true
	if autoCommitStrategy == nil {
		c.autoCommitStrategy = NewAutoCommitStrategy()
	} else {
		c.autoCommitStrategy = autoCommitStrategy
	}
	return c
}

// SetManualCommit disables native automatic stores so the caller owns progress
// after handling.
func (c *ConsumerOptions) SetManualCommit() *ConsumerOptions {
	c.autocommit = false
	return c
}

// SetOffset stores the native starting selector without querying retained
// availability.
func (c *ConsumerOptions) SetOffset(offset OffsetSpecification) *ConsumerOptions {
	c.Offset = offset
	return c
}

// SetClientProvidedName stores a broker-visible diagnostic name that may be
// sensitive; it is not authentication.
func (c *ConsumerOptions) SetClientProvidedName(clientProvidedName string) *ConsumerOptions {
	c.ClientProvidedName = clientProvidedName
	return c
}

// GetClientProvidedName selects the fallback only for a nil options pointer;
// a present empty name is returned unchanged.
// The result can be displayed by broker management tooling.
func (c *ConsumerOptions) GetClientProvidedName(defaultClientProvidedName string) string {
	if c == nil {
		return defaultClientProvidedName
	}
	return c.ClientProvidedName
}

// SetFilter borrows the filter object/callback; configure it before
// publication/subscription.
func (c *ConsumerOptions) SetFilter(filter *ConsumerFilter) *ConsumerOptions {
	c.Filter = filter
	return c
}

// SetSingleActiveConsumer borrows promotion policy/callbacks; do not mutate
// them during a subscription.
func (c *ConsumerOptions) SetSingleActiveConsumer(singleActiveConsumer *SingleActiveConsumer) *ConsumerOptions {
	c.SingleActiveConsumer = singleActiveConsumer
	return c
}

// SetCreditStrategy sets the credit strategy for the consumer. Available strategies are:
//   - AutomaticCreditStrategy: 1 credit per chunk.
//   - ManualCreditStrategy: the number of credits specified in the initialCredits field.
//
// The credit is a flow control mechanism that allows consumers to control how RabbitMQ sends messages to them.
//
// The AutomaticCreditStrategy delegates the credit management to the library. The library will send 1 credit after receiving a chunk.
//
// The ManualCreditStrategy lets the user send credits to the broker. When ManualCreditStrategy is used, it is critically important
// to send credits to keep receiving messages. The consumer context in the message handler can be used to send credits.
func (c *ConsumerOptions) SetCreditStrategy(creditStrategy CreditStrategy) *ConsumerOptions {
	c.CreditStrategy = creditStrategy
	return c
}

// IsSingleActiveConsumerEnabled reports the borrowed option flag, not current
// broker promotion or an external-effect fencing token.
func (c *ConsumerOptions) IsSingleActiveConsumerEnabled() bool {
	return c.SingleActiveConsumer != nil && c.SingleActiveConsumer.Enabled
}

// IsFilterEnabled reports whether filter options are present, not broker support.
func (c *ConsumerOptions) IsFilterEnabled() bool {
	return c.Filter != nil
}

func (c *Client) credit(subscriptionId byte, credit int16) {
	length := 2 + 2 + 1 + 2
	b, bufferErr := newProtocolBuffer(length)
	if bufferErr != nil {
		_ = c.socket.abort()
		return
	}
	if encodingErr := writeProtocolHeader(b, length, commandCredit); encodingErr != nil {
		_ = c.socket.abort()
		return
	}
	writeByte(b, subscriptionId)
	writeShort(b, credit)
	err := c.socket.writeAndFlush(b.Bytes())
	if err != nil {
		logs.LogWarn("credit error:%s", err)
	}
}

// Close requests terminal native stop/cleanup without joining its message
// callback. External owners release backpressure and then use WaitContext.
func (consumer *Consumer) Close() error {
	if consumer.getStatus() == closed {
		return AlreadyClosed
	}

	consumer.close(Event{
		Command:    CommandUnsubscribe,
		StreamName: consumer.GetStreamName(),
		Name:       consumer.GetName(),
		Reason:     UnSubscribe,
		Err:        nil,
	})

	return nil
}

// sendChunk delivers a chunk to the dispatch goroutine. It returns false
// (dropping the chunk) if the consumer is concurrently closing, avoiding a
// panic from sending on a closed channel.
func (consumer *Consumer) sendChunk(chunk chunkInfo) bool {
	select {
	case <-consumer.closeCh:
		return false
	default:
	}
	select {
	case consumer.chunkForConsumer <- chunk:
		return true
	case <-consumer.closeCh:
		return false
	}
}

func (consumer *Consumer) signalStop(reason Event) bool {
	stopped := false
	consumer.closeOnce.Do(func() {
		stopped = true
		consumer.mutex.Lock()
		consumer.status = closed
		reason.StreamName, reason.Name = consumer.GetStreamName(), consumer.GetName()
		consumer.terminalEvent = reason
		ch := consumer.closeHandler
		consumer.closeHandler = nil
		consumer.mutex.Unlock()
		consumer.tasks.stop()
		close(consumer.closeCh)
		if ch != nil {
			select {
			case ch <- reason:
			default:
			}
			close(ch)
		}
	})
	return stopped
}
func (consumer *Consumer) close(reason Event) {
	if !consumer.signalStop(reason) {
		return
	}
	if consumer.client != nil {
		if reason.Reason == UnSubscribe && consumer.client.socket.isOpen() {
			consumer.cacheStoreOffset()
			length := 2 + 2 + 4 + 1
			resp, allocationErr := consumer.client.coordinator.NewResponse(CommandUnsubscribe)
			if allocationErr == nil {
				b, encodingErr := newProtocolBuffer(length)
				if encodingErr == nil {
					encodingErr = writeProtocolHeader(b, length, CommandUnsubscribe, resp.correlationid)
				}
				if encodingErr == nil {
					writeByte(b, consumer.ID)
					_ = consumer.client.handleWrite(b.Bytes(), resp)
				}
				consumer.client.coordinator.retireResponse(resp)
				logErrorCommand(encodingErr, "unsubscribe encoding")
			} else {
				logErrorCommand(allocationErr, "unsubscribe response allocation")
			}
		}
		_, _ = consumer.client.coordinator.ExtractConsumerById(consumer.ID)
		if consumer.client.coordinator.ConsumersCount() == 0 && consumer.client.coordinator.ProducersCount() == 0 {
			consumer.client.Close()
		}
	}
	if consumer.onClose != nil {
		consumer.onClose()
	}
}

func (consumer *Consumer) cacheStoreOffset() {
	if consumer.options.autocommit {
		consumer.mutex.Lock()
		consumer.lastAutoCommitStored = time.Now()
		consumer.messageCountBeforeStorage = 0
		consumer.mutex.Unlock() // internalStoreOffset also takes the consumer state mutex.

		err := consumer.internalStoreOffset()
		if err != nil {
			logs.LogError("cache Store Offset error : %s", err)
		}
	}
}

func (consumer *Consumer) increaseMessageCountBeforeStorage() int {
	consumer.mutex.Lock()
	defer consumer.mutex.Unlock()
	consumer.messageCountBeforeStorage += 1
	return consumer.messageCountBeforeStorage
}

func (consumer *Consumer) getLastAutoCommitStored() time.Time {
	consumer.mutex.Lock()
	defer consumer.mutex.Unlock()
	return consumer.lastAutoCommitStored
}

// StoreOffset stores the current offset for this consumer given its name and stream
func (consumer *Consumer) StoreOffset() error {
	return consumer.StoreOffsetContext(context.Background())
}

// StoreOffsetContext bounds writing the consumer's currently handled offset.
func (consumer *Consumer) StoreOffsetContext(ctx context.Context) error {
	ctx, endOperation := ensureOperationContext(ctx)
	defer endOperation()
	return consumer.internalStoreOffsetContext(ctx)
}

// StoreCustomOffset stores a custom offset for this consumer given its name and stream
func (consumer *Consumer) StoreCustomOffset(offset int64) error {
	return consumer.StoreCustomOffsetContext(context.Background(), offset)
}

// StoreCustomOffsetContext serializes monotonic offset writes with cancellable
// admission and updates the local checkpoint only after native write success.
func (consumer *Consumer) StoreCustomOffsetContext(ctx context.Context, offset int64) error {
	ctx, endOperation := ensureOperationContext(ctx)
	defer endOperation()
	release, err := consumer.offsetGate.acquire(ctx)
	if err != nil {
		return err
	}
	defer release()
	consumer.mutex.Lock()
	alreadyStored := consumer.lastStoredOffset >= offset
	consumer.mutex.Unlock()
	if alreadyStored {
		return nil
	}
	if err := consumer.writeOffsetToSocketContext(ctx, offset); err != nil {
		return err
	}
	consumer.mutex.Lock()
	consumer.lastStoredOffset = offset
	consumer.mutex.Unlock()
	return nil
}
func (consumer *Consumer) internalStoreOffset() error {
	return consumer.internalStoreOffsetContext(context.Background())
}

func (consumer *Consumer) internalStoreOffsetContext(ctx context.Context) error {
	ctx, endOperation := ensureOperationContext(ctx)
	defer endOperation()
	if consumer.options.streamName == "" {
		return fmt.Errorf("stream Name can't be empty")
	}

	return consumer.StoreCustomOffsetContext(ctx, consumer.GetOffset())
}

func (consumer *Consumer) writeOffsetToSocketContext(ctx context.Context, offset int64) error {
	if err := validateProtocolStrings(consumer.options.ConsumerName, consumer.options.streamName); err != nil {
		return err
	}
	ctx, endOperation := ensureOperationContext(ctx)
	defer endOperation()
	length := 2 + 2 + 2 + len(consumer.options.ConsumerName) + 2 +
		len(consumer.options.streamName) + 8
	b, bufferErr := newProtocolBuffer(length)
	if bufferErr != nil {
		return bufferErr
	}
	if encodingErr := writeProtocolHeader(b, length, commandStoreOffset); encodingErr != nil {
		return encodingErr
	}

	if err := writeString(b, consumer.options.ConsumerName); err != nil {
		return err
	}
	if err := writeString(b, consumer.options.streamName); err != nil {
		return err
	}

	writeLong(b, offset)
	return consumer.client.socket.writeAndFlushContext(ctx, b.Bytes())
}

func (consumer *Consumer) writeConsumeUpdateOffsetToSocket(correlationID uint32, offsetSpec OffsetSpecification) error {
	length := 2 + 2 + 4 + 2 + 2
	if offsetSpec.isOffset() ||
		offsetSpec.isTimestamp() {
		length += 8
	}

	b, bufferErr := newProtocolBuffer(length)
	if bufferErr != nil {
		return bufferErr
	}
	if encodingErr := writeProtocolHeader(b, length, commandConsumerUpdate); encodingErr != nil {
		return encodingErr
	}

	writeUInt(b, correlationID)
	writeUShort(b, responseCodeOk)

	writeShort(b, offsetSpec.typeOfs)

	if offsetSpec.isOffset() ||
		offsetSpec.isTimestamp() {
		writeLong(b, offsetSpec.offset)
	}
	return consumer.client.socket.writeAndFlush(b.Bytes())
}

// QueryOffset returns the last stored offset for this consumer given its name and stream
func (consumer *Consumer) QueryOffset() (int64, error) {
	return consumer.QueryOffsetContext(context.Background())
}

// QueryOffsetContext bounds lookup of this consumer's named broker offset.
func (consumer *Consumer) QueryOffsetContext(ctx context.Context) (int64, error) {
	ctx, endOperation := ensureOperationContext(ctx)
	defer endOperation()
	if (consumer.options == nil) || (consumer.client == nil) || (consumer.options.ConsumerName == "") || (consumer.options.streamName == "") {
		return -1, fmt.Errorf("offset query error: consumer not properly initialized")
	}
	return consumer.client.queryOffsetContext(ctx, consumer.options.ConsumerName, consumer.options.streamName)
}

/*
SetOffset constants
*/
const (
	typeFirst     = int16(1)
	typeLast      = int16(2)
	typeNext      = int16(3)
	typeOffset    = int16(4)
	typeTimestamp = int16(5)
	//
	// Deprecated: see LastConsumed()
	typeLastConsumed = int16(6)
)

// OffsetSpecification is a by-value native subscription selector. Last is the
// last chunk, Next is live intake; neither is an exact retained end-offset
// query.
type OffsetSpecification struct {
	typeOfs int16
	offset  int64
}

// First returns a retained-beginning selector without querying current
// retention.
func (o OffsetSpecification) First() OffsetSpecification {
	o.typeOfs = typeFirst
	return o
}

// Last returns a last-chunk selector, not the last exact message offset.
func (o OffsetSpecification) Last() OffsetSpecification {
	o.typeOfs = typeLast
	return o
}

// Next returns a live-tail selector for messages arriving after attachment.
func (o OffsetSpecification) Next() OffsetSpecification {
	o.typeOfs = typeNext
	return o
}

// Offset returns an exact signed native selector; raw values need validation
// before wire use.
func (o OffsetSpecification) Offset(offset int64) OffsetSpecification {
	o.typeOfs = typeOffset
	o.offset = offset
	return o
}

// Timestamp returns a timestamp selector in Unix milliseconds without
// rounding/validation.
func (o OffsetSpecification) Timestamp(offset int64) OffsetSpecification {
	o.typeOfs = typeTimestamp
	o.offset = offset
	return o
}

func (o OffsetSpecification) isOffset() bool {
	return o.typeOfs == typeOffset || o.typeOfs == typeLastConsumed
}

// Deprecated: see LastConsumed()
func (o OffsetSpecification) isLastConsumed() bool {
	return o.typeOfs == typeLastConsumed
}
func (o OffsetSpecification) isTimestamp() bool {
	return o.typeOfs == typeTimestamp
}

// LastConsumed selects the legacy named-offset resume marker; it is not proof
// of application effects and is not root replay checkpoint authority.
//
// Deprecated: The method name may be misleading.
// The method does not indicate the last message consumed of the stream but the last stored offset.
// The method was added to help the user, but it created confusion.
// Use `QueryOffset` instead.:
//
//		offset, err := env.QueryOffset(consumerName, streamName)
//	 // check the error
//	 ....
//	 SetOffset(stream.OffsetSpecification{}.Offset(offset)).
//
// So in this way it possible to start from the last offset stored and customize the behavior
func (o OffsetSpecification) LastConsumed() OffsetSpecification {
	o.typeOfs = typeLastConsumed
	o.offset = -1
	return o
}

// String renders the native selection for diagnosis, without querying broker
// state.
func (o OffsetSpecification) String() string {
	switch o.typeOfs {
	case typeFirst:
		return "first"
	case typeNext:
		return "next"
	case typeLast:
		return "last"
	case typeLastConsumed:
		return "last consumed"
	case typeOffset:
		return fmt.Sprintf("%s, value: %d", "offset", o.offset)
	case typeTimestamp:
		return fmt.Sprintf("%s, value: %d", "time-stamp", o.offset)
	}
	return ""
}
