package stream

import (
	"bufio"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"math/rand"
	"net"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/faustbrian/go-rabbitmq-streams/adapters/rabbitmq/v2/internal/rabbitmqstream/logs"
	"go.opentelemetry.io/otel/attribute"
	semconv "go.opentelemetry.io/otel/semconv/v1.39.0"
)

// SaslConfiguration see
//
//	SaslConfigurationPlain    = "PLAIN"
//	SaslConfigurationExternal = "EXTERNAL"
//

type SaslConfiguration struct {
	Mechanism string
}

func newSaslConfigurationDefault() *SaslConfiguration {
	return &SaslConfiguration{
		Mechanism: SaslConfigurationPlain,
	}
}

type TuneState struct {
	requestedMaxFrameSize int
	requestedHeartbeat    int
}

// tuneResponse carries the TUNE negotiation result back to the connect goroutine.
type tuneResponse struct {
	frame        []byte
	maxFrameSize int
	heartbeat    int
}

// negotiatedMaxValue picks the smaller value, or the larger when either is 0 ("no limit").
func negotiatedMaxValue(clientValue, serverValue int) int {
	if clientValue == 0 || serverValue == 0 {
		return max(clientValue, serverValue)
	}
	return min(clientValue, serverValue)
}

type ClientProperties struct {
	items map[string]string
}

type ConnectionProperties struct {
	host string
	port string
}

type HeartBeat struct {
	mutex sync.Mutex
	value time.Time
}

type Client struct {
	tasks                taskOwner
	stopOnce             sync.Once
	heartbeatStop        sync.Once
	connectGate          contextGate
	socket               socket
	destructor           *sync.Once
	clientProperties     ClientProperties
	connectionProperties ConnectionProperties
	tuneState            TuneState
	// frameMax is the negotiated frame size (0 = no limit), atomic so the write
	// path reads it lock-free (connect owns connectGate during the handshake).
	frameMax          atomic.Int64
	coordinator       *Coordinator
	broker            *Broker
	tcpParameters     *TCPParameters
	saslConfiguration *SaslConfiguration

	mutex             *sync.Mutex
	lastHeartBeat     HeartBeat
	socketCallTimeout time.Duration
	availableFeatures *availableFeatures
	serverProperties  map[string]string

	doneTimeoutTicker chan struct{}
	metrics           *streamMetrics
}

func newClient(parameters connectionParameters) *Client {
	var clientBroker = parameters.broker
	if parameters.broker == nil {
		clientBroker = newBrokerDefault()
	}
	if parameters.tcpParameters == nil {
		parameters.tcpParameters = newTCPParameterDefault()
	}

	if parameters.saslConfiguration == nil {
		parameters.saslConfiguration = newSaslConfigurationDefault()
	}

	c := &Client{
		coordinator:          NewCoordinator(),
		broker:               clientBroker,
		tcpParameters:        parameters.tcpParameters,
		saslConfiguration:    parameters.saslConfiguration,
		destructor:           &sync.Once{},
		mutex:                &sync.Mutex{},
		clientProperties:     ClientProperties{items: make(map[string]string)},
		connectionProperties: ConnectionProperties{},
		lastHeartBeat: HeartBeat{
			value: time.Now(),
		},
		socket: socket{
			mutex:      &sync.Mutex{},
			destructor: &sync.Once{},
		},
		socketCallTimeout: parameters.rpcTimeout,
		availableFeatures: newAvailableFeatures(),
		doneTimeoutTicker: make(chan struct{}, 1),
		metrics:           parameters.metrics,
	}
	c.setConnectionName(parameters.connectionName)
	return c
}

func (c *Client) setSocketConnection(connection net.Conn) {
	c.socket.mutex.Lock()
	defer c.socket.mutex.Unlock()
	c.socket.connection = connection
	c.socket.writer = bufio.NewWriter(connection)
}

// maxFrameSize returns the negotiated frame size, or 0 ("no limit") before TUNE.
func (c *Client) maxFrameSize() int {
	return int(c.frameMax.Load())
}

func (c *Client) getLastHeartBeat() time.Time {
	c.lastHeartBeat.mutex.Lock()
	defer c.lastHeartBeat.mutex.Unlock()
	return c.lastHeartBeat.value
}

func (c *Client) setLastHeartBeat(value time.Time) {
	c.lastHeartBeat.mutex.Lock()
	defer c.lastHeartBeat.mutex.Unlock()
	c.lastHeartBeat.value = value
}

func (c *Client) connectContext(ctx context.Context) (result error) {
	if err := c.tcpParameters.validateTune(); err != nil {
		return err
	}
	ctx, endOperation := ensureOperationContext(ctx)
	defer endOperation()
	release, err := c.connectGate.acquire(ctx)
	if err != nil {
		return err
	}
	defer release()
	done, admitted := c.beginTask(nil)
	if !admitted {
		return net.ErrClosed
	}
	defer done()
	defer func() {
		if result != nil {
			c.Close()
		}
	}()
	if !c.socket.isOpen() {
		u, err := url.Parse(c.broker.GetUri())
		if err != nil {
			return err
		}
		host, port := u.Hostname(), u.Port()
		c.tuneState.requestedMaxFrameSize = c.tcpParameters.RequestedMaxFrameSize
		c.tuneState.requestedHeartbeat = int(c.tcpParameters.RequestedHeartbeat / time.Second)

		servAddr := net.JoinHostPort(host, port)
		rawConnection, errorConnection := (&net.Dialer{}).DialContext(ctx, "tcp", servAddr)
		if errorConnection != nil {
			logs.LogDebug("%s", errorConnection)
			return errorConnection
		}
		connection := rawConnection.(*net.TCPConn)
		connected := false
		defer func() {
			if !connected {
				_ = rawConnection.Close()
				_ = c.socket.abort()
			}
		}()

		if c.tcpParameters.WriteBuffer > 0 {
			if err = connection.SetWriteBuffer(c.tcpParameters.WriteBuffer); err != nil {
				logs.LogError("Failed to SetWriteBuffer to %d due to %v", c.tcpParameters.WriteBuffer, err)
				return err
			}
		}

		if c.tcpParameters.ReadBuffer > 0 {
			if err = connection.SetReadBuffer(c.tcpParameters.ReadBuffer); err != nil {
				logs.LogError("Failed to SetReadBuffer to %d due to %v", c.tcpParameters.ReadBuffer, err)
				return err
			}
		}

		if err = connection.SetNoDelay(c.tcpParameters.NoDelay); err != nil {
			logs.LogError("Failed to SetNoDelay to %v due to %v", c.tcpParameters.NoDelay, err)
			return err
		}

		if c.broker.isTLS() {
			conf := &tls.Config{
				MinVersion: tls.VersionTLS12,
			}
			if c.tcpParameters.tlsConfig != nil {
				conf = c.tcpParameters.tlsConfig
			}
			conf = conf.Clone()
			if conf.ServerName == "" {
				conf.ServerName = host
			}
			tlsConnection := tls.Client(connection, conf)
			if err := tlsConnection.HandshakeContext(ctx); err != nil {
				return err
			}
			c.setSocketConnection(tlsConnection)
		} else {
			c.setSocketConnection(connection)
		}

		c.socket.setOpen()
		// Opening cancellation owns only the handshake. Stop and join this
		// callback before success so caller cleanup cannot kill an open resource.
		callbackDone := make(chan struct{})
		stop := context.AfterFunc(ctx, func() { _ = c.socket.abort(); close(callbackDone) })
		stopOpening := sync.OnceFunc(func() {
			if !stop() {
				<-callbackDone
			}
		})
		defer stopOpening()

		if !c.startTask(nil, c.handleResponse) {
			return net.ErrClosed
		}
		serverProperties, err2 := c.peerPropertiesContext(ctx)
		c.serverProperties = serverProperties
		if err2 != nil {
			logs.LogError("Can't set the peer-properties. Check if the stream server is running/reachable")
			return err2
		}

		pwd, _ := u.User.Password()
		err2 = c.authenticateContext(ctx, u.User.Username(), pwd)
		if err2 != nil {
			logs.LogDebug("User:%s, %s", u.User.Username(), err2)
			return err2
		}
		vhost := "/"
		if len(u.Path) > 1 {
			vhost, _ = url.QueryUnescape(u.Path[1:])
		}
		err2 = c.openContext(ctx, vhost)
		if err2 != nil {
			logs.LogDebug("%s", err2)
			return err2
		}

		// Increase the connection counter
		c.metrics.connectionOpened(context.Background(), c.otelAttributesForClient())

		err = c.availableFeatures.SetVersion(serverProperties["version"])
		if err != nil {
			logs.LogWarn("Error checking server version: %s", err)
		}

		if serverProperties["version"] == "" || !c.availableFeatures.Is311OrMore() {
			logs.LogDebug(
				"Server version is less than 3.11.0, skipping command version exchange")
		} else {
			err := c.exchangeVersionContext(ctx, c.serverProperties["version"])
			if err != nil {
				return err
			}
			logs.LogDebug("available features: %s", c.availableFeatures)
		}

		c.heartBeat()
		logs.LogDebug("User %s, connected to: %s, vhost:%s", u.User.Username(),
			net.JoinHostPort(host, port),
			vhost)
		stopOpening()
		if err := ctx.Err(); err != nil {
			return err
		}
		connected = true
	}
	return nil
}

func (c *Client) setConnectionName(connectionName string) {
	c.clientProperties.items["connection_name"] = connectionName
}

func (c *Client) peerPropertiesContext(ctx context.Context) (map[string]string, error) {
	ctx, endOperation := ensureOperationContext(ctx)
	defer endOperation()
	clientPropertiesSize := 4 // size of the map, always there

	c.clientProperties.items["product"] = "RabbitMQ Stream"
	c.clientProperties.items["copyright"] = "Copyright (c) 2021 VMware, Inc. or its affiliates."
	c.clientProperties.items["information"] = "Licensed under the MPL 2.0. See https://www.rabbitmq.com/"
	c.clientProperties.items["version"] = ClientVersion
	c.clientProperties.items["platform"] = "Golang"
	for key, element := range c.clientProperties.items {
		if err := validateProtocolStrings(key, element); err != nil {
			return nil, err
		}
		clientPropertiesSize = clientPropertiesSize + 2 + len(key) + 2 + len(element)
	}

	length := 2 + 2 + 4 + clientPropertiesSize
	resp, allocationErr := c.coordinator.NewResponse(commandPeerProperties)
	if allocationErr != nil {
		return nil, allocationErr
	}
	defer c.coordinator.retireResponse(resp)
	correlationId := resp.correlationid
	b, bufferErr := newProtocolBuffer(length)
	if bufferErr != nil {
		return nil, bufferErr
	}
	if encodingErr := writeProtocolHeader(b, length, commandPeerProperties,
		correlationId); encodingErr != nil {
		return nil, encodingErr
	}
	if encodingErr := writeInt(b, len(c.clientProperties.items)); encodingErr != nil {
		return nil, encodingErr
	}

	for key, element := range c.clientProperties.items {
		if err := writeString(b, key); err != nil {
			c.coordinator.retireResponse(resp)
			return nil, err
		}
		if err := writeString(b, element); err != nil {
			c.coordinator.retireResponse(resp)
			return nil, err
		}
	}

	err := c.handleWriteWithResponseContext(ctx, b.Bytes(), resp, false)
	if err.Err != nil {
		return nil, err.Err
	}

	serverProperties, dataErr := c.waitResponseData(ctx, resp)
	if dataErr != nil {
		c.coordinator.retireResponse(resp)
		return nil, dataErr
	}
	c.coordinator.retireResponse(resp)
	return serverProperties.(map[string]string), nil
}

func (c *Client) authenticateContext(ctx context.Context, user string, password string) error {
	ctx, endOperation := ensureOperationContext(ctx)
	defer endOperation()
	saslMechanisms, err := c.getSaslMechanismsContext(ctx)
	if err != nil {
		return err
	}
	saslMechanism := ""
	for i := range saslMechanisms {
		if saslMechanisms[i] == c.saslConfiguration.Mechanism {
			saslMechanism = c.saslConfiguration.Mechanism
			break
		}
	}
	if saslMechanism == "" {
		return fmt.Errorf("no matching mechanism found")
	}

	response := unicodeNull + user + unicodeNull + password
	saslResponse := []byte(response)
	return c.sendSaslAuthenticateContext(ctx, saslMechanism, saslResponse)
}

func (c *Client) getSaslMechanismsContext(ctx context.Context) ([]string, error) {
	ctx, endOperation := ensureOperationContext(ctx)
	defer endOperation()
	length := 2 + 2 + 4
	resp, allocationErr := c.coordinator.NewResponse(commandSaslHandshake)
	if allocationErr != nil {
		return nil, allocationErr
	}
	defer c.coordinator.retireResponse(resp)
	correlationId := resp.correlationid
	b, bufferErr := newProtocolBuffer(length)
	if bufferErr != nil {
		return nil, bufferErr
	}
	if encodingErr := writeProtocolHeader(b, length, commandSaslHandshake,
		correlationId); encodingErr != nil {
		return nil, encodingErr
	}

	if errWrite := c.socket.writeAndFlushContext(ctx, b.Bytes()); errWrite != nil {
		c.coordinator.retireResponse(resp)
		return nil, errWrite
	}
	data, dataErr := c.waitResponseData(ctx, resp)
	if dataErr != nil {
		c.coordinator.retireResponse(resp)
		return nil, dataErr
	}
	c.coordinator.retireResponse(resp)
	return data.([]string), nil
}

func (c *Client) sendSaslAuthenticateContext(ctx context.Context, saslMechanism string, challengeResponse []byte) error {
	if err := validateProtocolStrings(saslMechanism); err != nil {
		return err
	}
	ctx, endOperation := ensureOperationContext(ctx)
	defer endOperation()
	length := 2 + 2 + 4 + 2 + len(saslMechanism) + 4 + len(challengeResponse)
	resp, allocationErr := c.coordinator.NewResponse(commandSaslAuthenticate)
	if allocationErr != nil {
		return allocationErr
	}
	defer c.coordinator.retireResponse(resp)
	respTune := c.coordinator.NewResponseWithName("tune")
	defer func() { _ = c.coordinator.RemoveResponseByName("tune") }()
	correlationId := resp.correlationid
	b, bufferErr := newProtocolBuffer(length)
	if bufferErr != nil {
		return bufferErr
	}
	if encodingErr := writeProtocolHeader(b, length, commandSaslAuthenticate,
		correlationId); encodingErr != nil {
		return encodingErr
	}

	if err := writeString(b, saslMechanism); err != nil {
		c.coordinator.retireResponse(resp)
		return err
	}
	if encodingErr := writeInt(b, len(challengeResponse)); encodingErr != nil {
		return encodingErr
	}
	b.Write(challengeResponse)
	err := c.handleWriteContext(ctx, b.Bytes(), resp)
	if err.Err != nil {
		return err.Err
	}
	// double read for TUNE
	tuneData, dataErr := c.waitResponseData(ctx, respTune)
	if dataErr != nil {
		return dataErr
	}
	errR := c.coordinator.RemoveResponseByName("tune")
	if errR != nil {
		return errR
	}

	tuneResp := tuneData.(tuneResponse)
	c.frameMax.Store(int64(tuneResp.maxFrameSize))
	// Store the negotiated heartbeat so the ticker uses min(requested, broker)
	// rather than the originally configured value (0 = no limit).
	c.tuneState.requestedHeartbeat = tuneResp.heartbeat

	return c.socket.writeAndFlushContext(ctx, tuneResp.frame)
}

func (c *Client) exchangeVersionContext(ctx context.Context, serverVersion string) error {
	ctx, endOperation := ensureOperationContext(ctx)
	defer endOperation()
	_ = c.availableFeatures.SetVersion(serverVersion)

	commands := c.availableFeatures.GetCommands()

	length := 2 + 2 + 4 +
		4 + // commands size
		len(commands)*(2+2+2)
	resp, allocationErr := c.coordinator.NewResponse(commandExchangeVersion)
	if allocationErr != nil {
		return allocationErr
	}
	defer c.coordinator.retireResponse(resp)
	correlationId := resp.correlationid
	b, bufferErr := newProtocolBuffer(length)
	if bufferErr != nil {
		return bufferErr
	}
	if encodingErr := writeProtocolHeader(b, length, commandExchangeVersion,
		correlationId); encodingErr != nil {
		return encodingErr
	}

	if encodingErr := writeInt(b, len(commands)); encodingErr != nil {
		return encodingErr
	}

	for _, command := range commands {
		writeUShort(b, command.GetCommandKey())
		writeUShort(b, command.GetMinVersion())
		writeUShort(b, command.GetMaxVersion())
	}

	err := c.handleWriteWithResponseContext(ctx, b.Bytes(), resp, false)
	if err.Err != nil {
		return err.Err
	}

	commandsResponse, dataErr := c.waitResponseData(ctx, resp)
	if dataErr != nil {
		c.coordinator.retireResponse(resp)
		return dataErr
	}
	c.coordinator.retireResponse(resp)
	c.availableFeatures.ParseCommandVersions(commandsResponse.([]commandVersion))
	return nil
}

func (c *Client) openContext(ctx context.Context, virtualHost string) error {
	if err := validateProtocolStrings(virtualHost); err != nil {
		return err
	}
	ctx, endOperation := ensureOperationContext(ctx)
	defer endOperation()
	length := 2 + 2 + 4 + 2 + len(virtualHost)
	resp, allocationErr := c.coordinator.NewResponse(commandOpen, virtualHost)
	if allocationErr != nil {
		return allocationErr
	}
	defer c.coordinator.retireResponse(resp)
	correlationId := resp.correlationid
	b, bufferErr := newProtocolBuffer(length)
	if bufferErr != nil {
		return bufferErr
	}
	if encodingErr := writeProtocolHeader(b, length, commandOpen,
		correlationId); encodingErr != nil {
		return encodingErr
	}
	if err := writeString(b, virtualHost); err != nil {
		c.coordinator.retireResponse(resp)
		return err
	}
	err := c.handleWriteWithResponseContext(ctx, b.Bytes(), resp, false)
	if err.Err != nil {
		return err.Err
	}

	advHostPort, dataErr := c.waitResponseData(ctx, resp)
	if dataErr != nil {
		c.coordinator.retireResponse(resp)
		return dataErr
	}
	c.connectionProperties.host = advHostPort.(ConnectionProperties).host
	c.connectionProperties.port = advHostPort.(ConnectionProperties).port

	c.coordinator.retireResponse(resp)
	return nil
}

func (c *Client) DeleteStream(streamName string) error {
	if err := validateProtocolStrings(streamName); err != nil {
		return err
	}
	length := 2 + 2 + 4 + 2 + len(streamName)
	resp, allocationErr := c.coordinator.NewResponse(commandDeleteStream, streamName)
	if allocationErr != nil {
		return allocationErr
	}
	defer c.coordinator.retireResponse(resp)
	correlationId := resp.correlationid
	b, bufferErr := newProtocolBuffer(length)
	if bufferErr != nil {
		return bufferErr
	}
	if encodingErr := writeProtocolHeader(b, length, commandDeleteStream,
		correlationId); encodingErr != nil {
		return encodingErr
	}

	if err := writeString(b, streamName); err != nil {
		c.coordinator.retireResponse(resp)
		return err
	}
	return c.handleWrite(b.Bytes(), resp).Err
}

func (c *Client) heartBeat() {
	// A negotiated heartbeat of 0 means "no limit": heartbeats are disabled.
	// time.NewTicker also panics on a non-positive duration, so guard here.
	if c.tuneState.requestedHeartbeat <= 0 {
		return
	}

	var heartBeatMissed int32

	c.startTask(nil, func() {
		tickerHeartbeat := time.NewTicker(time.Duration(c.tuneState.requestedHeartbeat) * time.Second)
		defer tickerHeartbeat.Stop()
		for {
			select {
			case <-c.socket.done:
				tickerHeartbeat.Stop()
				return
			case <-c.doneTimeoutTicker:
				tickerHeartbeat.Stop()
				return
			case <-tickerHeartbeat.C:
				if c.socket.isOpen() {
					logs.LogDebug("Heartbeat ticker is open, sending heartbeat")
					c.sendHeartbeat()
					if time.Since(c.getLastHeartBeat()) > time.Duration(c.tuneState.requestedHeartbeat)*time.Second {
						v := atomic.AddInt32(&heartBeatMissed, 1)
						logs.LogWarn("Missing heart beat: %d", v)
						if v >= 2 {
							logs.LogWarn("Too many heartbeat missing: %d", v)
							c.Close()
						}
					} else {
						atomic.StoreInt32(&heartBeatMissed, 0)
					}
				} else {
					logs.LogDebug("Socket Heartbeat ticker is closed. Closing ticker")
					tickerHeartbeat.Stop()
					return
				}
			}
		}
	})
}

func (c *Client) sendHeartbeat() {
	length := 4
	b, bufferErr := newProtocolBuffer(length)
	if bufferErr != nil {
		_ = c.socket.abort()
		return
	}
	if encodingErr := writeProtocolHeader(b, length, commandHeartbeat); encodingErr != nil {
		_ = c.socket.abort()
		return
	}
	_ = c.socket.writeAndFlush(b.Bytes())
}

func (c *Client) closeHeartBeat() {
	c.heartbeatStop.Do(func() {
		if c.doneTimeoutTicker == nil {
			return
		}
		close(c.doneTimeoutTicker)
	})
}

// Close requests terminal stop, without joining a possibly calling reader.
func (c *Client) Close() {
	first := false
	c.stopOnce.Do(func() { first = true })
	if !first {
		return
	}
	{
		wasOpen := c.socket.isOpen()
		_ = c.socket.abort()
		c.tasks.stop()
		c.closeHeartBeat()
		if wasOpen && c.metrics != nil {
			c.metrics.connectionClosed(context.Background(), c.otelAttributesForClient())
		}
		c.coordinator.Producers().Range(func(_, value any) bool {
			p := value.(*Producer)
			if p.signalStop(Event{Reason: SocketClosed}) && p.onClose != nil {
				p.onClose()
			}
			return true
		})
		c.coordinator.Consumers().Range(func(_, value any) bool {
			consumer := value.(*Consumer)
			if consumer.signalStop(Event{Reason: SocketClosed}) && consumer.onClose != nil {
				consumer.onClose()
			}
			return true
		})
	}
}

func (c *Client) DeclarePublisher(streamName string, options *ProducerOptions) (*Producer, error) {
	return c.DeclarePublisherContext(context.Background(), streamName, options)
}

func (c *Client) DeclarePublisherContext(ctx context.Context, streamName string, options *ProducerOptions) (*Producer, error) {
	ctx, endOperation := ensureOperationContext(ctx)
	defer endOperation()
	return c.declarePublisherContext(ctx, streamName, options, nil)
}

func (c *Client) declarePublisherContext(ctx context.Context, streamName string, options *ProducerOptions, cleanUp func()) (*Producer, error) {
	ctx, endOperation := ensureOperationContext(ctx)
	defer endOperation()
	if options == nil {
		options = NewProducerOptions()
	}

	if options.IsFilterEnabled() && !c.availableFeatures.BrokerFilterEnabled() {
		return nil, FilterNotSupported
	}

	if options.isSubEntriesBatching() && options.IsFilterEnabled() {
		return nil, fmt.Errorf("sub-entry batching can't be enabled with filter")
	}
	if options.QueueSize < minQueuePublisherSize || options.QueueSize > maxQueuePublisherSize {
		return nil, fmt.Errorf("QueueSize values must be between %d and %d",
			minQueuePublisherSize, maxQueuePublisherSize)
	}

	if options.BatchSize < minBatchSize || options.BatchSize > maxBatchSize {
		return nil, fmt.Errorf("BatchSize values must be between %d and %d",
			minBatchSize, maxBatchSize)
	}

	if options.BatchPublishingDelay < minBatchPublishingDelay || options.BatchPublishingDelay > maxBatchPublishingDelay {
		return nil, fmt.Errorf("BatchPublishingDelay values must be between %d and %d",
			minBatchPublishingDelay, maxBatchPublishingDelay)
	}

	if options.SubEntrySize < minSubEntrySize || options.SubEntrySize > maxSubEntrySize {
		return nil, fmt.Errorf("SubEntrySize values must be between %d and %d",
			minSubEntrySize, maxSubEntrySize)
	}

	if !options.isSubEntriesBatching() {
		if options.Compression.enabled {
			return nil, fmt.Errorf("sub-entry batching must be enabled to enable compression")
		}
	}

	if !options.isSubEntriesBatching() {
		if options.Compression.value != None && options.Compression.value != GZIP {
			return nil, fmt.Errorf("compression values valid are: %d (None) %d (Gzip)", None, GZIP)
		}
	}

	producer, err := c.coordinator.NewProducer(&ProducerOptions{
		streamName:           streamName,
		Name:                 options.Name,
		QueueSize:            options.QueueSize,
		BatchSize:            options.BatchSize,
		BatchPublishingDelay: options.BatchPublishingDelay,
		SubEntrySize:         options.SubEntrySize,
		Compression:          options.Compression,
		ConfirmationTimeOut:  options.ConfirmationTimeOut,
		ClientProvidedName:   options.ClientProvidedName,
		Filter:               options.Filter,
	}, cleanUp)

	if err != nil {
		return nil, err
	}
	producer.client = c
	res := c.internalDeclarePublisherContext(ctx, streamName, producer)
	if res.Err == nil {
		if !producer.startUnconfirmedMessagesTimeOutTask() || !producer.processPendingSequencesQueue() {
			closeErr := producer.close(Event{Reason: SocketClosed})
			if errors.Is(closeErr, AlreadyClosed) {
				closeErr = nil
			}
			return nil, errors.Join(net.ErrClosed, closeErr)
		}
	} else {
		// No public resource was returned and no publication worker started.
		_, _ = c.coordinator.ExtractProducerById(producer.id)
		producer.confirmationTimeoutTicker.Stop()
		producer.pendingSequencesQueue.Stop()
		producer.pendingSequencesQueue.Close()
		producer.setStatus(closed)
		producer.closeConfirmationStatus()
		return nil, res.Err
	}
	return producer, res.Err
}

func (c *Client) internalDeclarePublisherContext(ctx context.Context, streamName string, producer *Producer) responseError {
	if err := validateProtocolStrings(streamName); err != nil {
		return responseError{Err: err}
	}
	ctx, endOperation := ensureOperationContext(ctx)
	defer endOperation()
	publisherReferenceSize := 0
	if producer.options != nil {
		if producer.options.Name != "" {
			if err := validateProtocolString(producer.options.Name); err != nil {
				return responseError{Err: err}
			}
			publisherReferenceSize = len(producer.options.Name)
		}
	}

	if publisherReferenceSize > 0 {
		v, err := c.queryPublisherSequenceContext(ctx, producer.options.Name, streamName)
		if err != nil {
			// if the client can't get the sequence, the function will return an error
			// because is not able to set the sequence
			// in most of the case the error timeout is during the re-connection
			// in this case the producer can't be created and the client will return an error
			return responseError{Err: err}
		}
		producer.sequence = v
	}

	length := 2 + 2 + 4 + 1 + 2 + publisherReferenceSize + 2 + len(streamName)
	resp, allocationErr := c.coordinator.NewResponse(commandDeclarePublisher, streamName)
	if allocationErr != nil {
		return responseError{Err: allocationErr}
	}
	defer c.coordinator.retireResponse(resp)
	correlationId := resp.correlationid
	b, bufferErr := newProtocolBuffer(length)
	if bufferErr != nil {
		return responseError{Err: bufferErr}
	}
	if encodingErr := writeProtocolHeader(b, length, commandDeclarePublisher,
		correlationId); encodingErr != nil {
		return responseError{Err: encodingErr}
	}

	writeByte(b, producer.id)
	publisherName := ""
	if producer.options != nil {
		publisherName = producer.options.Name
	}
	if err := writeString(b, publisherName); err != nil {
		c.coordinator.retireResponse(resp)
		return responseError{Err: err}
	}
	if err := writeString(b, streamName); err != nil {
		c.coordinator.retireResponse(resp)
		return responseError{Err: err}
	}
	res := c.handleWriteContext(ctx, b.Bytes(), resp)

	return res
}

func (c *Client) metaDataContext(ctx context.Context, streams ...string) *StreamsMetadata {
	metadata, _ := c.queryMetadataContext(ctx, streams...)
	return metadata
}

func (c *Client) queryMetadataContext(ctx context.Context, streams ...string) (*StreamsMetadata, error) {
	if err := validateProtocolStrings(streams...); err != nil {
		return nil, err
	}
	ctx, endOperation := ensureOperationContext(ctx)
	defer endOperation()
	length := 2 + 2 + 4 + 4 // API code, version, correlation id, size of array
	for _, stream := range streams {
		length += 2
		length += len(stream)
	}
	resp, allocationErr := c.coordinator.NewResponse(commandMetadata)
	if allocationErr != nil {
		return nil, allocationErr
	}
	defer c.coordinator.retireResponse(resp)
	correlationId := resp.correlationid
	b, bufferErr := newProtocolBuffer(length)
	if bufferErr != nil {
		return nil, bufferErr
	}
	if encodingErr := writeProtocolHeader(b, length, commandMetadata,
		correlationId); encodingErr != nil {
		return nil, encodingErr
	}

	if encodingErr := writeInt(b, len(streams)); encodingErr != nil {
		return nil, encodingErr
	}
	for _, stream := range streams {
		if err := writeString(b, stream); err != nil {
			c.coordinator.retireResponse(resp)
			return nil, err
		}
	}

	err := c.handleWriteWithResponseContext(ctx, b.Bytes(), resp, false)
	if err.Err != nil {
		return nil, err.Err
	}

	data, dataErr := c.waitResponseData(ctx, resp)
	if dataErr != nil {
		c.coordinator.retireResponse(resp)
		return nil, dataErr
	}
	c.coordinator.retireResponse(resp)
	return data.(*StreamsMetadata), nil
}

func (c *Client) queryPublisherSequence(publisherReference string, stream string) (int64, error) {
	return c.queryPublisherSequenceContext(context.Background(), publisherReference, stream)
}

func (c *Client) queryPublisherSequenceContext(ctx context.Context, publisherReference string, stream string) (int64, error) {
	if err := validateProtocolStrings(publisherReference, stream); err != nil {
		return 0, err
	}
	ctx, endOperation := ensureOperationContext(ctx)
	defer endOperation()
	length := 2 + 2 + 4 + 2 + len(publisherReference) + 2 + len(stream)
	resp, allocationErr := c.coordinator.NewResponse(commandQueryPublisherSequence)
	if allocationErr != nil {
		return 0, allocationErr
	}
	defer c.coordinator.retireResponse(resp)
	correlationId := resp.correlationid
	b, bufferErr := newProtocolBuffer(length)
	if bufferErr != nil {
		return 0, bufferErr
	}
	if encodingErr := writeProtocolHeader(b, length, commandQueryPublisherSequence, correlationId); encodingErr != nil {
		return 0, encodingErr
	}

	if err := writeString(b, publisherReference); err != nil {
		c.coordinator.retireResponse(resp)
		return 0, err
	}
	if err := writeString(b, stream); err != nil {
		c.coordinator.retireResponse(resp)
		return 0, err
	}
	err := c.handleWriteWithResponseContext(ctx, b.Bytes(), resp, false)
	if err.Err != nil {
		return 0, err.Err
	}

	sequence, dataErr := c.waitResponseData(ctx, resp)
	if dataErr != nil {
		c.coordinator.retireResponse(resp)
		return 0, dataErr
	}
	c.coordinator.retireResponse(resp)
	return sequence.(int64), nil
}

func (c *Client) BrokerLeader(stream string) (*Broker, error) {
	return c.BrokerLeaderContext(context.Background(), stream)
}

func (c *Client) BrokerLeaderContext(ctx context.Context, stream string) (*Broker, error) {
	ctx, endOperation := ensureOperationContext(ctx)
	defer endOperation()
	return c.BrokerLeaderWithResolverContext(ctx, stream, nil)
}

func (c *Client) BrokerLeaderWithResolver(stream string, resolver *AddressResolver) (*Broker, error) {
	return c.BrokerLeaderWithResolverContext(context.Background(), stream, resolver)
}

func (c *Client) BrokerLeaderWithResolverContext(ctx context.Context, stream string, resolver *AddressResolver) (*Broker, error) {
	ctx, endOperation := ensureOperationContext(ctx)
	defer endOperation()
	streamsMetadata, metadataErr := c.queryMetadataContext(ctx, stream)
	if metadataErr != nil {
		return nil, metadataErr
	}
	if streamsMetadata == nil {
		return nil, fmt.Errorf("leader error for stream for stream: %s", stream)
	}

	streamMetadata := streamsMetadata.Get(stream)
	if streamMetadata == nil {
		return nil, StreamMetadataFailure
	}
	if streamMetadata.responseCode != responseCodeOk {
		return nil, lookErrorCode(streamMetadata.responseCode)
	}
	if streamMetadata.Leader == nil {
		return nil, LeaderNotReady
	}

	streamMetadata.Leader.advPort = streamMetadata.Leader.Port
	streamMetadata.Leader.advHost = streamMetadata.Leader.Host

	// If AddressResolver is configured, use it directly and skip DNS lookup
	if resolver != nil {
		streamMetadata.Leader.Host = resolver.Host
		streamMetadata.Leader.Port = strconv.Itoa(resolver.Port)
		return streamMetadata.Leader, nil
	}

	res := net.Resolver{}
	// see: https://github.com/rabbitmq/rabbitmq-stream-go-client/pull/317
	// DNS lookup belongs to this operation's caller context.
	_, err := res.LookupIPAddr(ctx, streamMetadata.Leader.Host)
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		var dnsError *net.DNSError
		if errors.As(err, &dnsError) {
			if strings.EqualFold(c.broker.Host, "localhost") {
				logs.LogWarn("Can't lookup the DNS for %s, error: %s. Trying localhost..", streamMetadata.Leader.Host, err)
				streamMetadata.Leader.Host = "localhost"
			} else {
				logs.LogWarn("Can't lookup the DNS for %s, error: %s", streamMetadata.Leader.Host, err)
			}
		}
	}

	return streamMetadata.Leader, nil
}

func (c *Client) StreamExists(stream string) bool {
	return c.StreamExistsContext(context.Background(), stream)
}

func (c *Client) StreamExistsContext(ctx context.Context, stream string) bool {
	ctx, endOperation := ensureOperationContext(ctx)
	defer endOperation()
	streamsMetadata := c.metaDataContext(ctx, stream)
	if streamsMetadata == nil {
		return false
	}

	streamMetadata := streamsMetadata.Get(stream)
	return streamMetadata.responseCode == responseCodeOk
}
func (c *Client) BrokerForConsumer(stream string) (*Broker, error) {
	return c.BrokerForConsumerContext(context.Background(), stream)
}

func (c *Client) BrokerForConsumerContext(ctx context.Context, stream string) (*Broker, error) {
	ctx, endOperation := ensureOperationContext(ctx)
	defer endOperation()
	streamsMetadata, metadataErr := c.queryMetadataContext(ctx, stream)
	if metadataErr != nil {
		return nil, metadataErr
	}
	if streamsMetadata == nil {
		return nil, fmt.Errorf("leader error for stream: %s", stream)
	}

	streamMetadata := streamsMetadata.Get(stream)
	if streamMetadata == nil {
		return nil, StreamMetadataFailure
	}
	if streamMetadata.responseCode != responseCodeOk {
		return nil, lookErrorCode(streamMetadata.responseCode)
	}

	if streamMetadata.Leader == nil {
		return nil, LeaderNotReady
	}

	brokers := make([]*Broker, 0, 1+len(streamMetadata.Replicas))

	// Count available replicas
	availableReplicas := 0
	for _, replica := range streamMetadata.Replicas {
		if replica != nil {
			availableReplicas++
		}
	}

	// Only add leader if no replicas are available
	if availableReplicas == 0 {
		streamMetadata.Leader.advPort = streamMetadata.Leader.Port
		streamMetadata.Leader.advHost = streamMetadata.Leader.Host
		brokers = append(brokers, streamMetadata.Leader)
	}

	// Add all available replicas
	for idx, replica := range streamMetadata.Replicas {
		if replica == nil {
			logs.LogWarn("Stream %s replica not ready: %d", stream, idx)
			continue
		}
		replica.advPort = replica.Port
		replica.advHost = replica.Host
		brokers = append(brokers, replica)
	}

	return brokers[rand.Intn(len(brokers))], nil // #nosec G404 -- Load-balancing selection among validated brokers, not a security token.
}

func (c *Client) DeclareStream(streamName string, options *StreamOptions) error {
	if err := validateProtocolStrings(streamName); err != nil {
		return err
	}
	if streamName == "" {
		return fmt.Errorf("stream Name can't be empty")
	}

	resp, allocationErr := c.coordinator.NewResponse(commandCreateStream, streamName)

	if allocationErr != nil {
		return allocationErr
	}

	defer c.coordinator.retireResponse(resp)
	length := 2 + 2 + 4 + 2 + len(streamName) + 4
	correlationId := resp.correlationid
	if options == nil {
		options = NewStreamOptions()
	}

	args, err := options.buildParameters()
	if err != nil {
		c.coordinator.retireResponse(resp)
		return err
	}
	for key, element := range args {
		if err := validateProtocolStrings(key, element); err != nil {
			c.coordinator.retireResponse(resp)
			return err
		}
		length = length + 2 + len(key) + 2 + len(element)
	}
	b, bufferErr := newProtocolBuffer(length)
	if bufferErr != nil {
		return bufferErr
	}
	if encodingErr := writeProtocolHeader(b, length, commandCreateStream,
		correlationId); encodingErr != nil {
		return encodingErr
	}
	if err := writeString(b, streamName); err != nil {
		c.coordinator.retireResponse(resp)
		return err
	}
	if encodingErr := writeInt(b, len(args)); encodingErr != nil {
		return encodingErr
	}

	for key, element := range args {
		if err := writeString(b, key); err != nil {
			c.coordinator.retireResponse(resp)
			return err
		}
		if err := writeString(b, element); err != nil {
			c.coordinator.retireResponse(resp)
			return err
		}
	}

	return c.handleWrite(b.Bytes(), resp).Err
}

func (c *Client) queryOffsetContext(ctx context.Context, consumerName string, streamName string) (int64, error) {
	if err := validateProtocolStrings(consumerName, streamName); err != nil {
		return 0, err
	}
	ctx, endOperation := ensureOperationContext(ctx)
	defer endOperation()
	length := 2 + 2 + 4 + 2 + len(consumerName) + 2 + len(streamName)

	resp, allocationErr := c.coordinator.NewResponse(CommandQueryOffset)

	if allocationErr != nil {
		return 0, allocationErr
	}

	defer c.coordinator.retireResponse(resp)
	correlationId := resp.correlationid
	b, bufferErr := newProtocolBuffer(length)
	if bufferErr != nil {
		return 0, bufferErr
	}
	if encodingErr := writeProtocolHeader(b, length, CommandQueryOffset,
		correlationId); encodingErr != nil {
		return 0, encodingErr
	}

	if err := writeString(b, consumerName); err != nil {
		c.coordinator.retireResponse(resp)
		return 0, err
	}
	if err := writeString(b, streamName); err != nil {
		c.coordinator.retireResponse(resp)
		return 0, err
	}
	err := c.handleWriteWithResponseContext(ctx, b.Bytes(), resp, false)
	if err.Err != nil {
		return 0, err.Err
	}

	offset, dataErr := c.waitResponseData(ctx, resp)
	if dataErr != nil {
		c.coordinator.retireResponse(resp)
		return 0, dataErr
	}
	c.coordinator.retireResponse(resp)
	return offset.(int64), nil
}

func (c *Client) StoreOffset(consumerName string, streamName string, offset int64) error {
	return c.StoreOffsetContext(context.Background(), consumerName, streamName, offset)
}

func (c *Client) StoreOffsetContext(ctx context.Context, consumerName string, streamName string, offset int64) error {
	if err := validateProtocolStrings(consumerName, streamName); err != nil {
		return err
	}
	ctx, endOperation := ensureOperationContext(ctx)
	defer endOperation()
	length := 2 + 2 + 2 + len(consumerName) + 2 +
		len(streamName) + 8
	b, bufferErr := newProtocolBuffer(length)
	if bufferErr != nil {
		return bufferErr
	}
	if encodingErr := writeProtocolHeader(b, length, commandStoreOffset); encodingErr != nil {
		return encodingErr
	}

	if err := writeString(b, consumerName); err != nil {
		return err
	}
	if err := writeString(b, streamName); err != nil {
		return err
	}
	writeLong(b, offset)
	return c.socket.writeAndFlushContext(ctx, b.Bytes())
}
func (c *Client) DeclareSubscriber(streamName string,
	messagesHandler MessagesHandler,
	options *ConsumerOptions) (*Consumer, error) {
	return c.DeclareSubscriberContext(context.Background(), streamName, messagesHandler, options)
}

func (c *Client) DeclareSubscriberContext(ctx context.Context, streamName string,
	messagesHandler MessagesHandler,
	options *ConsumerOptions) (*Consumer, error) {
	ctx, endOperation := ensureOperationContext(ctx)
	defer endOperation()
	return c.declareSubscriberContext(ctx, streamName, messagesHandler, options, nil)
}

func (c *Client) declareSubscriberContext(ctx context.Context, streamName string,
	messagesHandler MessagesHandler,
	options *ConsumerOptions, cleanUp func()) (*Consumer, error) {
	if err := validateProtocolStrings(streamName); err != nil {
		return nil, err
	}
	ctx, endOperation := ensureOperationContext(ctx)
	defer endOperation()
	if options == nil {
		options = NewConsumerOptions()
	}

	if err := validateProtocolString(options.ConsumerName); err != nil {
		return nil, err
	}
	if options.initialCredits <= 0 {
		options.initialCredits = 10
	}
	if containsOnlySpaces(options.ConsumerName) {
		return nil, fmt.Errorf("consumer name contains only spaces")
	}

	if options.IsSingleActiveConsumerEnabled() && !c.availableFeatures.IsBrokerSingleActiveConsumerEnabled() {
		return nil, SingleActiveConsumerNotSupported
	}

	if options.IsSingleActiveConsumerEnabled() && strings.TrimSpace(options.ConsumerName) == "" {
		return nil, fmt.Errorf("single active enabled but name is empty. You need to set a name")
	}

	if options.IsSingleActiveConsumerEnabled() && options.SingleActiveConsumer.ConsumerUpdate == nil {
		return nil, fmt.Errorf("single active enabled but consumer update function  is nil. Consumer update must be set")
	}

	if options.IsFilterEnabled() && !c.availableFeatures.BrokerFilterEnabled() {
		return nil, FilterNotSupported
	}

	if options.IsFilterEnabled() && options.Filter.PostFilter == nil {
		return nil, fmt.Errorf("filter enabled but post filter is nil. Post filter must be set")
	}

	if options.IsFilterEnabled() && (len(options.Filter.Values) == 0) {
		return nil, fmt.Errorf("filter enabled but no values. At least one value must be set")
	}

	if options.IsFilterEnabled() {
		for _, value := range options.Filter.Values {
			if value == "" {
				return nil, fmt.Errorf("filter enabled but one of the value is empty")
			}
		}
	}

	if options.Offset.typeOfs <= 0 || options.Offset.typeOfs > 6 {
		return nil, fmt.Errorf("specify a valid Offset")
	}

	if (options.autoCommitStrategy != nil) && (options.autoCommitStrategy.flushInterval < 1*time.Second) && options.autocommit {
		return nil, fmt.Errorf("flush internal must be bigger than one second")
	}

	if (options.autoCommitStrategy != nil) && options.autoCommitStrategy.messageCountBeforeStorage < 1 && options.autocommit {
		return nil, fmt.Errorf("message count before storage must be bigger than one")
	}

	if (options.autoCommitStrategy != nil) && options.ConsumerName == "" && options.autocommit {
		return nil, fmt.Errorf("consumer name must be set when autocommit is enabled")
	}

	if messagesHandler == nil {
		return nil, fmt.Errorf("messages Handler must be set")
	}

	if options.Offset.isLastConsumed() {
		lastOffset, err := c.queryOffsetContext(ctx, options.ConsumerName, streamName)
		switch {
		case err == nil, errors.Is(err, OffsetNotFoundError):
			if errors.Is(err, OffsetNotFoundError) {
				options.Offset.typeOfs = typeFirst
				options.Offset.offset = 0
				break
			} else {
				options.Offset.offset = lastOffset
				options.Offset.typeOfs = typeOffset
				break
			}
		default:
			return nil, err
		}
	}

	options.streamName = streamName
	consumer, consumerErr := c.coordinator.NewConsumer(messagesHandler, options, cleanUp)
	if consumerErr != nil {
		return nil, consumerErr
	}
	consumer.client = c
	length := 2 + 2 + 4 + 1 + 2 + len(streamName) + 2 + 2
	if options.Offset.isOffset() ||
		options.Offset.isTimestamp() {
		length += 8
	}

	// copy the option offset to the consumer offset
	// the option.offset won't change ( in case we need to retrieve the original configuration)
	// consumer.current offset will be moved when reading
	if !options.IsSingleActiveConsumerEnabled() && options.Offset.isOffset() {
		consumer.setCurrentOffset(options.Offset.offset)
	}

	/// define the consumerOptions
	consumerProperties := make(map[string]string)

	if options.ConsumerName != "" {
		consumerProperties["name"] = options.ConsumerName
	}

	if options.IsSingleActiveConsumerEnabled() {
		consumerProperties["single-active-consumer"] = "true"
		if options.SingleActiveConsumer.superStream != "" {
			consumerProperties["super-stream"] = options.SingleActiveConsumer.superStream
		}
	}

	if options.Filter != nil {
		for i, filterValue := range options.Filter.Values {
			k := fmt.Sprintf("%s%d", subscriptionPropertyFilterPrefix, i)
			consumerProperties[k] = filterValue
		}

		consumerProperties[subscriptionPropertyMatchUnfiltered] = strconv.FormatBool(options.Filter.MatchUnfiltered)
	}

	if len(consumerProperties) > 0 {
		length += 4 // size of the properties map

		for k, v := range consumerProperties {
			if err := validateProtocolStrings(k, v); err != nil {
				consumer.close(Event{Reason: SocketClosed})
				return nil, err
			}
			length += 2 + len(k)
			length += 2 + len(v)
		}
	}

	resp, allocationErr := c.coordinator.NewResponse(commandSubscribe, streamName)

	if allocationErr != nil {
		consumer.close(Event{Reason: SocketClosed})
		return nil, allocationErr
	}

	defer c.coordinator.retireResponse(resp)
	correlationId := resp.correlationid
	b, bufferErr := newProtocolBuffer(length)
	if bufferErr != nil {
		consumer.close(Event{Reason: SocketClosed})
		return nil, bufferErr
	}
	if encodingErr := writeProtocolHeader(b, length, commandSubscribe,
		correlationId); encodingErr != nil {
		consumer.close(Event{Reason: SocketClosed})
		return nil, encodingErr
	}
	writeByte(b, consumer.ID)

	if err := writeString(b, streamName); err != nil {
		c.coordinator.retireResponse(resp)
		consumer.close(Event{Reason: SocketClosed})
		return nil, err
	}
	writeShort(b, options.Offset.typeOfs)

	if options.Offset.isOffset() ||
		options.Offset.isTimestamp() {
		writeLong(b, options.Offset.offset)
	}
	writeShort(b, options.initialCredits)
	if len(consumerProperties) > 0 {
		if encodingErr := writeInt(b, len(consumerProperties)); encodingErr != nil {
			consumer.close(Event{Reason: SocketClosed})
			return nil, encodingErr
		}
		for k, v := range consumerProperties {
			if err := writeString(b, k); err != nil {
				c.coordinator.retireResponse(resp)
				consumer.close(Event{Reason: SocketClosed})
				return nil, err
			}
			if err := writeString(b, v); err != nil {
				c.coordinator.retireResponse(resp)
				consumer.close(Event{Reason: SocketClosed})
				return nil, err
			}
		}
	}

	err := c.handleWriteContext(ctx, b.Bytes(), resp)
	if err.Err != nil {
		return nil, err.Err
	}

	if !c.startConsumerDispatch(consumer, options, streamName) {
		consumer.close(Event{Reason: SocketClosed})
		return nil, net.ErrClosed
	}
	return consumer, nil
}

func (c *Client) startConsumerDispatch(consumer *Consumer, options *ConsumerOptions, streamName string) bool {
	canDispatch := func(offsetMessage *offsetMessage) bool {
		if !consumer.isActive() {
			logs.LogDebug("The consumer is not active anymore the message will be skipped, partition %s", streamName)
			return false
		}

		if options.IsFilterEnabled() && options.Filter.PostFilter != nil {
			return options.Filter.PostFilter(offsetMessage.message)
		}
		return true
	}

	return c.startTask(&consumer.tasks, func() {
		autoCommitTicker := time.NewTicker(time.Second)
		defer autoCommitTicker.Stop()
		for {
			// Prioritised shutdown check: if closeCh is already closed, exit
			// immediately without racing against buffered chunks in the select below.
			select {
			case <-c.socket.done:
				return
			case <-consumer.closeCh:
				return
			default:
			}
			select {
			case <-c.socket.done:
				return
			case <-consumer.closeCh:
				return
			case chunk := <-consumer.chunkForConsumer:

				halfChunkSize := len(chunk.offsetMessages) / 2
				for i, offMessage := range chunk.offsetMessages {
					select {
					case <-consumer.closeCh:
						return
					case <-c.socket.done:
						return
					default:
					}
					consumer.setCurrentOffset(offMessage.offset)
					if canDispatch(offMessage) {
						consumer.MessagesHandler(ConsumerContext{Consumer: consumer, chunkInfo: &chunk}, offMessage.message)
					}

					select {
					case <-consumer.closeCh:
						return
					case <-c.socket.done:
						return
					default:
					}
					// when half of the chunk is reached ask for a credit
					if halfChunkSize == i && consumer.options.CreditStrategy == AutomaticCreditStrategy {
						c.credit(consumer.ID, 1)
					}

					if consumer.options.autocommit {
						messageCountBeforeStorage := consumer.increaseMessageCountBeforeStorage()
						if messageCountBeforeStorage >= consumer.options.autoCommitStrategy.messageCountBeforeStorage ||
							time.Since(consumer.getLastAutoCommitStored()) >= consumer.options.autoCommitStrategy.flushInterval {
							consumer.cacheStoreOffset()
						}
					}
				}

			case <-autoCommitTicker.C:
				if consumer.options.autocommit && time.Since(consumer.getLastAutoCommitStored()) >= consumer.options.autoCommitStrategy.flushInterval {
					consumer.cacheStoreOffset()
				}

				// This is a very edge case where the consumer is not active anymore
				// but the consumer is still in the list of consumers
				// It can happen during the reconnection with load-balancing
				// found this problem with a chaos test where random killing the load-balancer and node where
				// the client should be connected
				if consumer.isZombie() {
					logs.LogWarn("Detected zombie consumer for stream %s, closing", streamName)
					consumer.close(Event{
						Command:    CommandUnsubscribe,
						StreamName: consumer.GetStreamName(),
						Name:       consumer.GetName(),
						Reason:     ZombieConsumer,
						Err:        nil,
					})
					return
				}
			}
		}
	})
}

func (c *Client) StreamStats(streamName string) (*StreamStats, error) {
	return c.StreamStatsContext(context.Background(), streamName)
}

func (c *Client) StreamStatsContext(ctx context.Context, streamName string) (*StreamStats, error) {
	if err := validateProtocolStrings(streamName); err != nil {
		return nil, err
	}
	ctx, endOperation := ensureOperationContext(ctx)
	defer endOperation()
	resp, allocationErr := c.coordinator.NewResponse(commandStreamStatus)
	if allocationErr != nil {
		return nil, allocationErr
	}
	defer c.coordinator.retireResponse(resp)
	correlationId := resp.correlationid

	length := 2 + 2 + 4 + 2 + len(streamName)

	b, bufferErr := newProtocolBuffer(length)
	if bufferErr != nil {
		return nil, bufferErr
	}
	if encodingErr := writeProtocolHeader(b, length, commandStreamStatus,
		correlationId); encodingErr != nil {
		return nil, encodingErr
	}
	if err := writeString(b, streamName); err != nil {
		c.coordinator.retireResponse(resp)
		return nil, err
	}
	err := c.handleWriteWithResponseContext(ctx, b.Bytes(), resp, false)
	if err.Err != nil {
		return nil, err.Err
	}

	offset, dataErr := c.waitResponseData(ctx, resp)
	if dataErr != nil {
		c.coordinator.retireResponse(resp)
		return nil, dataErr
	}
	c.coordinator.retireResponse(resp)
	m, ok := offset.(map[string]int64)
	if !ok {
		return nil, fmt.Errorf("invalid response, expected map[string]int64 but got %T", offset)
	}
	return newStreamStats(m, streamName), nil
}

func (c *Client) DeclareSuperStream(superStream string, options SuperStreamOptions) error {
	if err := validateProtocolStrings(superStream); err != nil {
		return err
	}
	if !c.availableFeatures.is313OrMore {
		return fmt.Errorf("declaring super stream via client API not supported, server version is less than 3.13.0")
	}

	if superStream == "" || containsOnlySpaces(superStream) {
		return fmt.Errorf("super Stream Name can't be empty")
	}

	if options == nil {
		return fmt.Errorf("options can't be nil")
	}

	if len(options.getPartitions(superStream)) == 0 {
		return fmt.Errorf("partitions can't be empty")
	}

	if len(options.getBindingKeys()) == 0 {
		return fmt.Errorf("binding keys can't be empty")
	}

	for _, key := range options.getBindingKeys() {
		if err := validateProtocolString(key); err != nil {
			return err
		}
		if key == "" || containsOnlySpaces(key) {
			return fmt.Errorf("binding key can't be empty")
		}
	}

	for _, partition := range options.getPartitions(superStream) {
		if err := validateProtocolString(partition); err != nil {
			return err
		}
		if partition == "" || containsOnlySpaces(partition) {
			return fmt.Errorf("partition can't be empty")
		}
	}

	for key, value := range options.getArgs() {
		if err := validateProtocolStrings(key, value); err != nil {
			return err
		}
	}
	length := 2 + 2 + 4 +
		2 + len(superStream) + 4 +
		sizeOfStringArray(options.getPartitions(superStream)) + 4 +
		sizeOfStringArray(options.getBindingKeys()) + 4 +
		sizeOfMapStringString(options.getArgs())

	resp, allocationErr := c.coordinator.NewResponse(commandCreateSuperStream, superStream)

	if allocationErr != nil {
		return allocationErr
	}

	defer c.coordinator.retireResponse(resp)
	correlationId := resp.correlationid
	b, bufferErr := newProtocolBuffer(length)
	if bufferErr != nil {
		return bufferErr
	}
	if encodingErr := writeProtocolHeader(b, length, commandCreateSuperStream, correlationId); encodingErr != nil {
		return encodingErr
	}
	if err := writeString(b, superStream); err != nil {
		c.coordinator.retireResponse(resp)
		return err
	}
	if err := writeStringArray(b, options.getPartitions(superStream)); err != nil {
		c.coordinator.retireResponse(resp)
		return err
	}
	if err := writeStringArray(b, options.getBindingKeys()); err != nil {
		c.coordinator.retireResponse(resp)
		return err
	}
	if err := writeMapStringString(b, options.getArgs()); err != nil {
		c.coordinator.retireResponse(resp)
		return err
	}
	return c.handleWrite(b.Bytes(), resp).Err
}

func (c *Client) DeleteSuperStream(superStream string) error {
	if err := validateProtocolStrings(superStream); err != nil {
		return err
	}
	if !c.availableFeatures.is313OrMore {
		return fmt.Errorf("deleting super stream not supported via client API, server version is less than 3.13.0")
	}

	length := 2 + 2 + 4 + 2 + len(superStream)
	resp, allocationErr := c.coordinator.NewResponse(commandDeleteSuperStream, superStream)
	if allocationErr != nil {
		return allocationErr
	}
	defer c.coordinator.retireResponse(resp)
	correlationId := resp.correlationid
	b, bufferErr := newProtocolBuffer(length)
	if bufferErr != nil {
		return bufferErr
	}
	if encodingErr := writeProtocolHeader(b, length, commandDeleteSuperStream,
		correlationId); encodingErr != nil {
		return encodingErr
	}
	if err := writeString(b, superStream); err != nil {
		c.coordinator.retireResponse(resp)
		return err
	}
	return c.handleWrite(b.Bytes(), resp).Err
}

func (c *Client) QueryPartitions(superStream string) ([]string, error) {
	return c.QueryPartitionsContext(context.Background(), superStream)
}

func (c *Client) QueryPartitionsContext(ctx context.Context, superStream string) ([]string, error) {
	if err := validateProtocolStrings(superStream); err != nil {
		return nil, err
	}
	ctx, endOperation := ensureOperationContext(ctx)
	defer endOperation()
	length := 2 + 2 + 4 + 2 + len(superStream)
	resp, allocationErr := c.coordinator.NewResponse(commandQueryPartition, superStream)
	if allocationErr != nil {
		return nil, allocationErr
	}
	defer c.coordinator.retireResponse(resp)
	correlationId := resp.correlationid
	b, bufferErr := newProtocolBuffer(length)
	if bufferErr != nil {
		return nil, bufferErr
	}
	if encodingErr := writeProtocolHeader(b, length, commandQueryPartition,
		correlationId); encodingErr != nil {
		return nil, encodingErr
	}
	if err := writeString(b, superStream); err != nil {
		c.coordinator.retireResponse(resp)
		return nil, err
	}
	err := c.handleWriteWithResponseContext(ctx, b.Bytes(), resp, false)
	if err.Err != nil {
		return nil, err.Err
	}

	data, dataErr := c.waitResponseData(ctx, resp)
	if dataErr != nil {
		c.coordinator.retireResponse(resp)
		return nil, dataErr
	}
	c.coordinator.retireResponse(resp)
	return data.([]string), nil
}

func (c *Client) queryRouteContext(ctx context.Context, superStream string, routingKey string) ([]string, error) {
	if err := validateProtocolStrings(superStream, routingKey); err != nil {
		return nil, err
	}
	ctx, endOperation := ensureOperationContext(ctx)
	defer endOperation()
	length := 2 + 2 + 4 + 2 + len(superStream) + 2 + len(routingKey)
	resp, allocationErr := c.coordinator.NewResponse(commandQueryRoute, superStream)
	if allocationErr != nil {
		return nil, allocationErr
	}
	defer c.coordinator.retireResponse(resp)
	correlationId := resp.correlationid
	b, bufferErr := newProtocolBuffer(length)
	if bufferErr != nil {
		return nil, bufferErr
	}
	if encodingErr := writeProtocolHeader(b, length, commandQueryRoute,
		correlationId); encodingErr != nil {
		return nil, encodingErr
	}
	if err := writeString(b, routingKey); err != nil {
		c.coordinator.retireResponse(resp)
		return nil, err
	}
	if err := writeString(b, superStream); err != nil {
		c.coordinator.retireResponse(resp)
		return nil, err
	}
	err := c.handleWriteWithResponseContext(ctx, b.Bytes(), resp, false)
	if err.Err != nil {
		return nil, err.Err
	}

	data, dataErr := c.waitResponseData(ctx, resp)
	if dataErr != nil {
		c.coordinator.retireResponse(resp)
		return nil, dataErr
	}
	c.coordinator.retireResponse(resp)
	return data.([]string), nil
}

func (c *Client) otelBaseAttributes() []attribute.KeyValue {
	attrs := []attribute.KeyValue{}
	attrs = append(attrs, semconv.ServerAddress(c.broker.Host))
	if p, err := strconv.Atoi(c.broker.Port); err == nil {
		attrs = append(attrs, semconv.ServerPort(p))
	}
	return attrs
}

func (c *Client) otelAttributesForClient() attribute.Set {
	return attribute.NewSet(c.otelBaseAttributes()...)
}

func (c *Client) otelAttributesWithError(err error) attribute.Set {
	attrs := c.otelBaseAttributes()
	errAtt := semconv.ErrorType(err)
	attrs = append(attrs, errAtt)
	return attribute.NewSet(attrs...)
}

func (c *Client) otelAttributesForConfirm() attribute.Set {
	attrs := c.otelBaseAttributes()
	attrs = append(attrs, semconv.MessagingOperationTypeSettle, semconv.MessagingOperationName("confirm"))
	return attribute.NewSet(attrs...)
}

func (c *Client) otelAttributesForConsumer(streamName string) attribute.Set {
	attrs := c.otelBaseAttributes()
	attrs = append(attrs, semconv.MessagingOperationTypeReceive, semconv.MessagingOperationName("deliver"), semconv.MessagingDestinationName(streamName))
	return attribute.NewSet(attrs...)
}
