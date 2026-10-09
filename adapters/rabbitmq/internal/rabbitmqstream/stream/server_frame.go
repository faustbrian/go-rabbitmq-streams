package stream

import (
	"bufio"
	"bytes"
	"context"
	"hash/crc32"
	"time"

	"github.com/faustbrian/go-rabbitmq-streams/adapters/rabbitmq/v2/internal/rabbitmqstream/logs"
)

type ReaderProtocol struct {
	FrameLen          uint32
	CommandID         uint16
	Key               uint16
	Version           uint16
	CorrelationId     uint32
	ResponseCode      uint16
	PublishID         uint8
	PublishingIdCount uint64
}

func logErrorCommand(error error, details string) {
	if error != nil {
		logs.LogError("Error handling command response: %s - details: %s", error, details)
	}
}

func (c *Client) handleResponse() {
	network := bufio.NewReader(c.socket.connection)

	for {
		readerProtocol := &ReaderProtocol{}
		policy := c.decoderLimits()
		if err := policy.Validate(); err != nil {
			_ = c.socket.abort()
			c.Close()
			return
		}
		limit := policy.MaxFrameBytes
		if negotiated := c.maxFrameSize(); negotiated > 0 && negotiated < limit {
			limit = negotiated
		}
		frame, err := readInboundFrame(network, limit)
		if err == nil {
			err = validateInboundFrame(frame, policy)
		}
		if err != nil {
			logs.LogDebug("Read connection failed: %s", err)
			_ = c.socket.abort()
			c.Close()
			break
		}

		c.setLastHeartBeat(time.Now())
		buffer := bufio.NewReader(bytes.NewReader(frame))
		readerProtocol.FrameLen = uint32(len(frame)) // #nosec G115 -- readInboundFrame admitted n+4 within a validated <=1<<30 byte budget.
		readerProtocol.Key = readUShort(buffer)
		readerProtocol.CommandID = uShortExtractResponseCode(readerProtocol.Key)
		readerProtocol.Version = readUShort(buffer)

		switch readerProtocol.CommandID {
		case commandPeerProperties:
			{
				c.handlePeerProperties(readerProtocol, buffer)
			}
		case commandSaslHandshake:
			{
				c.handleSaslHandshakeResponse(readerProtocol, buffer)
			}
		case commandTune:
			{
				c.handleTune(buffer)
			}
		case commandDeclarePublisher,
			CommandDeletePublisher, commandDeleteStream,
			commandCreateStream, commandSaslAuthenticate, commandSubscribe,
			CommandUnsubscribe, commandCreateSuperStream, commandDeleteSuperStream:
			{
				c.handleGenericResponse(readerProtocol, buffer)
			}
		case commandOpen:
			{
				c.commandOpen(readerProtocol, buffer)
			}
		case commandPublishError:
			{
				c.handlePublishError(buffer)
			}
		case commandPublishConfirm:
			{
				c.handleConfirm(readerProtocol, buffer)
			}
		case commandDeliver:
			{
				c.handleDeliver(buffer)
			}
		case commandQueryPublisherSequence:
			{
				c.queryPublisherSequenceFrameHandler(readerProtocol, buffer)
			}
		case CommandMetadataUpdate:
			{
				c.metadataUpdateFrameHandler(buffer)
			}
		case commandCredit:
			{
				c.creditNotificationFrameHandler(readerProtocol, buffer)
			}
		case commandHeartbeat:
			{
				c.handleHeartbeat()
			}
		case CommandQueryOffset:
			{
				c.queryOffsetFrameHandler(readerProtocol, buffer)
			}
		case commandStreamStatus:
			{
				c.streamStatusFrameHandler(readerProtocol, buffer)
			}
		case commandMetadata:
			{
				c.metadataFrameHandler(readerProtocol, buffer)
			}
		case CommandClose:
			{
				if readerProtocol.Key&0x8000 != 0 {
					c.handleGenericResponse(readerProtocol, buffer)
				} else {
					c.closeFrameHandler(readerProtocol, buffer)
				}
			}
		case commandExchangeVersion:
			{
				c.handleExchangeVersionResponse(readerProtocol, buffer)
			}
		case consumerUpdateQueryResponse:
			{
				c.handleConsumerUpdate(readerProtocol, buffer)
			}
		case commandQueryPartition:
			{
				c.handleQueryPartitions(readerProtocol, buffer)
			}
		case commandQueryRoute:
			{
				c.handleQueryRoute(readerProtocol, buffer)
			}
		default:
			{
				logs.LogWarn("Command not implemented %d buff:%d \n", readerProtocol.CommandID, buffer.Buffered())
				break
			}
		}
	}
}

func (c *Client) handleSaslHandshakeResponse(streamingRes *ReaderProtocol, r *bufio.Reader) {
	streamingRes.CorrelationId, _ = readUInt(r)
	streamingRes.ResponseCode = uShortExtractResponseCode(readUShort(r))
	mechanismsCount, _ := readUInt(r)
	mechanisms := make([]string, mechanismsCount)
	for i := range int(mechanismsCount) {
		mechanism := readString(r)
		mechanisms[i] = mechanism
	}

	res, err := c.coordinator.GetResponseById(streamingRes.CorrelationId)
	if err != nil {
		logErrorCommand(err, "handleSaslHandshakeResponse")
		return
	}

	c.deliverData(res, mechanisms)
}

func (c *Client) handlePeerProperties(readProtocol *ReaderProtocol, r *bufio.Reader) {
	readProtocol.CorrelationId, _ = readUInt(r)
	readProtocol.ResponseCode = uShortExtractResponseCode(readUShort(r))
	serverPropertiesCount, _ := readUInt(r)
	serverProperties := make(map[string]string)

	for i := 0; i < int(serverPropertiesCount); i++ {
		key := readString(r)
		value := readString(r)
		serverProperties[key] = value
	}
	res, err := c.coordinator.GetResponseById(readProtocol.CorrelationId)
	if err != nil {
		logErrorCommand(err, "handlePeerProperties")
		return
	}

	c.deliverCode(res, Code{id: readProtocol.ResponseCode})
	c.deliverData(res, serverProperties)
}

func (c *Client) handleTune(r *bufio.Reader) any {
	serverMaxFrameSize, _ := readUInt(r)
	serverHeartbeat, _ := readUInt(r)

	maxFrameSize := negotiatedMaxValue(c.tuneState.requestedMaxFrameSize, int(serverMaxFrameSize))
	heartbeat := negotiatedMaxValue(c.tuneState.requestedHeartbeat, int(serverHeartbeat))
	if maxFrameSize < 8 || heartbeat <= 0 || validateProtocolCount(maxFrameSize) != nil || validateProtocolCount(heartbeat) != nil {
		_ = c.socket.abort()
		return nil
	}

	length := 2 + 2 + 4 + 4
	b, bufferErr := newProtocolBuffer(length)
	if bufferErr != nil {
		_ = c.socket.abort()
		return nil
	}
	if encodingErr := writeInt(b, length); encodingErr != nil {
		_ = c.socket.abort()
		return nil
	}
	writeUShort(b, uShortEncodeResponseCode(commandTune))
	writeShort(b, version1)
	writeUInt(b, uint32(maxFrameSize)) // #nosec G115 -- Positive negotiated value explicitly admitted by validateProtocolCount above.
	writeUInt(b, uint32(heartbeat))    // #nosec G115 -- Positive negotiated value explicitly admitted by validateProtocolCount above.
	res, err := c.coordinator.GetResponseByName("tune")
	logErrorCommand(err, "handleTune")
	if err != nil {
		return nil
	}
	resp := tuneResponse{frame: b.Bytes(), maxFrameSize: maxFrameSize, heartbeat: heartbeat}
	c.deliverData(res, resp)
	return resp
}

func (c *Client) handleGenericResponse(readProtocol *ReaderProtocol, r *bufio.Reader) {
	readProtocol.CorrelationId, _ = readUInt(r)
	readProtocol.ResponseCode = uShortExtractResponseCode(readUShort(r))
	res, err := c.coordinator.GetResponseById(readProtocol.CorrelationId)
	if err != nil {
		logErrorCommand(err, "handleGenericResponse")
		return
	}

	c.deliverCode(res, Code{id: readProtocol.ResponseCode})
}

func (c *Client) commandOpen(readProtocol *ReaderProtocol, r *bufio.Reader) {
	readProtocol.CorrelationId, _ = readUInt(r)
	readProtocol.ResponseCode = uShortExtractResponseCode(readUShort(r))
	clientProperties := ConnectionProperties{}
	connectionPropertiesCount, _ := readUInt(r)
	for i := 0; i < int(connectionPropertiesCount); i++ {
		v := readString(r)
		value := readString(r)
		switch v {
		case "advertised_host":
			{
				clientProperties.host = value
			}
		case "advertised_port":
			{
				clientProperties.port = value
			}
		}
	}

	res, err := c.coordinator.GetResponseById(readProtocol.CorrelationId)
	if err != nil {
		logErrorCommand(err, "commandOpen")
		return
	}

	c.deliverCode(res, Code{id: readProtocol.ResponseCode})
	c.deliverData(res, clientProperties)
}

func (c *Client) handleConfirm(readProtocol *ReaderProtocol, r *bufio.Reader) any {
	producerFound := false
	readProtocol.PublishID = readByte(r)
	publishingIdCount, _ := readUInt(r)
	producer, err := c.coordinator.GetProducerById(readProtocol.PublishID)
	producerFound = err == nil
	if err != nil {
		logs.LogWarn("can't find the producer during confirmation: %s. Id %d", err, readProtocol.PublishID)
	}

	// even the producer is not found we need to read the publishingId
	// to empty the buffer.
	// The producer here could not exist because the producer is closed before the confirmations are received

	arraySeq := make([]int64, 0, publishingIdCount)
	for publishingIdCount != 0 {
		seq := readInt64(r)
		arraySeq = append(arraySeq, seq)
		publishingIdCount--
	}

	if producerFound {
		confirms := producer.unConfirmed.extractWithConfirms(arraySeq)
		c.metrics.confirmed(context.Background(), int64(len(confirms)), c.otelAttributesForConfirm())
		producer.sendConfirmationStatus(confirms)
	}

	return 0
}

func (c *Client) queryPublisherSequenceFrameHandler(readProtocol *ReaderProtocol, r *bufio.Reader) {
	readProtocol.CorrelationId, _ = readUInt(r)
	readProtocol.ResponseCode = uShortExtractResponseCode(readUShort(r))
	sequence := readInt64(r)
	res, err := c.coordinator.GetResponseById(readProtocol.CorrelationId)
	if err != nil {
		logErrorCommand(err, "queryPublisherSequenceFrameHandler")
		return
	}

	c.deliverCode(res, Code{id: readProtocol.ResponseCode})
	c.deliverData(res, sequence)
}

func (c *Client) handleDeliver(r *bufio.Reader) {
	body, err := finiteDecodedReader(r, c.decoderLimits().MaxFrameBytes)
	if err != nil {
		_ = c.socket.abort()
		return
	}
	subscriptionID, chunk, records, crc, data, err := decodeDelivery(body, c.decoderLimits())
	if err != nil {
		_ = c.socket.abort()
		return
	}
	consumer, err := c.coordinator.GetConsumerById(subscriptionID)
	if err != nil {
		return
	}
	if consumer.options.CRCCheck && crc32.ChecksumIEEE(data) != crc {
		_ = c.socket.abort()
		return
	}
	var offsetLimit int64 = -1
	if consumer.options.IsSingleActiveConsumerEnabled() {
		if consumer.options.SingleActiveConsumer.offsetSpecification.isOffset() {
			offsetLimit = consumer.options.SingleActiveConsumer.offsetSpecification.offset
		}
	} else if consumer.options.Offset.isOffset() {
		offsetLimit = consumer.GetOffset()
	}
	if offsetLimit != -1 {
		kept := chunk.offsetMessages[:0]
		for _, message := range chunk.offsetMessages {
			if message.offset >= offsetLimit {
				kept = append(kept, message)
			}
		}
		chunk.offsetMessages = kept
	}
	if !c.dispatchChunk(consumer, chunk) {
		return
	}
	c.metrics.consumed(context.Background(), int64(records), c.otelAttributesForConsumer(consumer.GetStreamName()))
	c.metrics.chunkReceived(context.Background(), int64(records), c.otelAttributesForConsumer(consumer.GetStreamName()))
}

func (c *Client) creditNotificationFrameHandler(readProtocol *ReaderProtocol,
	r *bufio.Reader) {
	readProtocol.ResponseCode = uShortExtractResponseCode(readUShort(r))
	subscriptionId := readByte(r)
	consumer, err := c.coordinator.GetConsumerById(subscriptionId)
	if err != nil {
		logs.LogWarn("received a credit for an unknown subscriptionId: %d", subscriptionId)
		return
	}

	if consumer != nil && consumer.getStatus() == closed {
		logs.LogDebug("received a credit for a closed consumer %d", subscriptionId)
		return
	}
}

func (c *Client) queryOffsetFrameHandler(readProtocol *ReaderProtocol,
	r *bufio.Reader) {
	c.handleGenericResponse(readProtocol, r)
	offset := readInt64(r)
	res, err := c.coordinator.GetResponseById(readProtocol.CorrelationId)
	if err != nil {
		logErrorCommand(err, "queryOffsetFrameHandler")
		return
	}

	c.deliverData(res, offset)
}

func (c *Client) handlePublishError(buffer *bufio.Reader) {
	publisherId := readByte(buffer)

	publishingErrorCount, _ := readUInt(buffer)
	// setting error to nil because we emit the metric once, and the error code is specific to each message.
	// the error value will use the default fallback value in OTEL "_OTHER".
	c.metrics.errored(context.Background(), int64(publishingErrorCount), c.otelAttributesWithError(nil))
	var publishingId int64
	var code uint16
	for publishingErrorCount != 0 {
		publishingId = readInt64(buffer)
		code = readUShort(buffer)
		producer, err := c.coordinator.GetProducerById(publisherId)
		if err != nil {
			logs.LogWarn("producer id %d not found, publish error :%s", publisherId, lookErrorCode(code))
			producer = &Producer{unConfirmed: newUnConfirmed(defaultQueuePublisherSize)}
		} else {
			unConfirmedMessage := producer.unConfirmed.extractWithError(publishingId, code)

			if unConfirmedMessage != nil {
				producer.sendConfirmationStatus([]*ConfirmationStatus{unConfirmedMessage})
			}
		}
		publishingErrorCount--
	}
}

func (c *Client) metadataUpdateFrameHandler(buffer *bufio.Reader) {
	code := readUShort(buffer)
	if code == responseCodeStreamNotAvailable {
		stream := readString(buffer)
		logs.LogDebug("stream %s is no longer available", stream)
		c.maybeCleanProducers(stream)
		c.maybeCleanConsumers(stream)
	} else {
		// TODO handle the error, see the java code
		logs.LogWarn("unsupported metadata update code %d", code)
	}
}

func (c *Client) streamStatusFrameHandler(readProtocol *ReaderProtocol,
	r *bufio.Reader) {
	c.handleGenericResponse(readProtocol, r)

	count, _ := readUInt(r)
	streamStatus := make(map[string]int64)

	for i := 0; i < int(count); i++ {
		key := readString(r)
		value := readInt64(r)
		streamStatus[key] = value
	}
	res, err := c.coordinator.GetResponseById(readProtocol.CorrelationId)
	if err != nil {
		logErrorCommand(err, "streamStatusFrameHandler")
		return
	}

	c.deliverData(res, streamStatus)
}

func (c *Client) metadataFrameHandler(readProtocol *ReaderProtocol,
	r *bufio.Reader) {
	readProtocol.CorrelationId, _ = readUInt(r)
	readProtocol.ResponseCode = responseCodeOk
	brokers := newBrokers()
	brokersCount, _ := readUInt(r)
	for i := 0; i < int(brokersCount); i++ {
		brokerReference := readShort(r)
		host := readString(r)
		port, _ := readUInt(r)
		brokers.Add(brokerReference, host, port)
	}

	streamsMetadata := StreamsMetadata{}.New()
	streamsCount, _ := readUInt(r)
	for i := 0; i < int(streamsCount); i++ {
		stream := readString(r)
		responseCode := readUShort(r)
		var leader *Broker
		var replicas []*Broker
		leaderReference := readShort(r)
		leader = brokers.Get(leaderReference)
		replicasCount, _ := readUInt(r)
		for i := 0; i < int(replicasCount); i++ {
			replicaReference := readShort(r)
			replicas = append(replicas, brokers.Get(replicaReference))
		}
		streamsMetadata.Add(stream, responseCode, leader, replicas)
	}

	res, err := c.coordinator.GetResponseById(readProtocol.CorrelationId)
	if err != nil {
		logErrorCommand(err, "metadataFrameHandler")
		return
	}

	c.deliverCode(res, Code{id: readProtocol.ResponseCode})
	c.deliverData(res, streamsMetadata)
}

func (c *Client) closeFrameHandler(readProtocol *ReaderProtocol,
	r *bufio.Reader) {
	readProtocol.CorrelationId, _ = readUInt(r)
	readProtocol.ResponseCode = uShortExtractResponseCode(readUShort(r))
	closeReason := readString(r)
	logs.LogDebug("Received close from server, reason: %s %s %d", lookErrorCode(readProtocol.ResponseCode),
		closeReason, readProtocol.ResponseCode)

	length := 2 + 2 + 4 + 2
	b, bufferErr := newProtocolBuffer(length)
	if bufferErr != nil {
		_ = c.socket.abort()
		return
	}
	if encodingErr := writeProtocolHeader(b, length, uShortEncodeResponseCode(CommandClose),
		readProtocol.CorrelationId); encodingErr != nil {
		_ = c.socket.abort()
		return
	}
	writeUShort(b, responseCodeOk)

	err := c.socket.writeAndFlush(b.Bytes())
	logErrorCommand(err, "Socket write buffer closeFrameHandler")
}

func (c *Client) handleConsumerUpdate(readProtocol *ReaderProtocol, r *bufio.Reader) {
	readProtocol.CorrelationId, _ = readUInt(r)
	subscriptionId := readByte(r)
	isActive := readByte(r)
	consumer, err := c.coordinator.GetConsumerById(subscriptionId)

	logErrorCommand(err, "handleConsumerUpdate")
	if consumer == nil {
		logs.LogWarn("consumer not found %d. The consumer maybe removed before the update", subscriptionId)
		return
	}
	consumer.setPromotedAsActive(isActive == 1)
	responseOff := consumer.options.SingleActiveConsumer.ConsumerUpdate(consumer.GetStreamName(),
		isActive == 1)
	consumer.options.SingleActiveConsumer.offsetSpecification = responseOff

	if isActive == 1 && responseOff.isOffset() {
		consumer.setCurrentOffset(responseOff.offset)
	}

	err = consumer.writeConsumeUpdateOffsetToSocket(readProtocol.CorrelationId, responseOff)
	logErrorCommand(err, "handleConsumerUpdate writeConsumeUpdateOffsetToSocket")
}

func (c *Client) handleQueryPartitions(readProtocol *ReaderProtocol, r *bufio.Reader) {
	readProtocol.CorrelationId, _ = readUInt(r)
	readProtocol.ResponseCode = uShortExtractResponseCode(readUShort(r))
	partitions := make([]string, 0)
	partitionsCount, _ := readUInt(r)
	for i := 0; i < int(partitionsCount); i++ {
		partition := readString(r)
		partitions = append(partitions, partition)
	}
	res, err := c.coordinator.GetResponseById(readProtocol.CorrelationId)
	if err != nil {
		logErrorCommand(err, "handleQueryPartitions")
		return
	}

	c.deliverCode(res, Code{id: readProtocol.ResponseCode})
	c.deliverData(res, partitions)
}

func (c *Client) handleQueryRoute(readProtocol *ReaderProtocol, r *bufio.Reader) {
	readProtocol.CorrelationId, _ = readUInt(r)
	readProtocol.ResponseCode = uShortExtractResponseCode(readUShort(r))
	numStreams, _ := readUInt(r)

	routes := make([]string, 0)
	for i := 0; i < int(numStreams); i++ {
		route := readString(r)
		routes = append(routes, route)
	}

	res, err := c.coordinator.GetResponseById(readProtocol.CorrelationId)
	if err != nil {
		logErrorCommand(err, "handleQueryRoute")
		return
	}

	c.deliverCode(res, Code{id: readProtocol.ResponseCode})
	c.deliverData(res, routes)
}

func (c *Client) handleExchangeVersionResponse(readProtocol *ReaderProtocol, r *bufio.Reader) {
	readProtocol.CorrelationId, _ = readUInt(r)
	readProtocol.ResponseCode = uShortExtractResponseCode(readUShort(r))
	commandsSize, _ := readUInt(r)
	commands := make([]commandVersion, 0)
	for i := 0; i < int(commandsSize); i++ {
		commandKey := readUShort(r)
		minVersion := readUShort(r)
		maxVersion := readUShort(r)
		commands = append(commands, newCommandVersionResponse(minVersion, maxVersion, commandKey))
	}
	res, err := c.coordinator.GetResponseById(readProtocol.CorrelationId)
	if err != nil {
		logErrorCommand(err, "handleExchangeVersionResponse")
		return
	}

	c.deliverCode(res, Code{id: readProtocol.ResponseCode})
	c.deliverData(res, commands)
}

func (c *Client) handleHeartbeat() {
	logs.LogDebug("Heart beat received at %s", time.Now())
	c.setLastHeartBeat(time.Now())
}
