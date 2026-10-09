package stream

import (
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
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
)

type locator struct {
	operationGate contextGate
	client        *Client
	mutex         sync.Mutex
}

func newLocator(client *Client) *locator {
	return &locator{
		client: client,
		mutex:  sync.Mutex{},
	}
}

type Environment struct {
	producers    *producersEnvironment
	consumers    *consumersEnvironment
	options      *EnvironmentOptions
	locator      *locator
	closed       atomic.Bool
	metrics      *streamMetrics
	lifetime     context.Context
	stopLifetime context.CancelFunc
}

func NewEnvironment(options *EnvironmentOptions) (*Environment, error) {
	return NewEnvironmentContext(context.Background(), options)
}

// NewEnvironmentContext bounds connection opening, including DNS, TCP, TLS,
// authentication and protocol negotiation. Success transfers lifetime ownership
// to Close/Abort; cancelling ctx afterward does not close the environment.
func NewEnvironmentContext(ctx context.Context, options *EnvironmentOptions) (*Environment, error) {
	ctx, endOperation := ensureOperationContext(ctx)
	defer endOperation()
	if options == nil {
		options = NewEnvironmentOptions()
	}

	copiedOptions := *options
	options = &copiedOptions
	if options.TCPParameters != nil {
		copiedTCP := *options.TCPParameters
		options.TCPParameters = &copiedTCP
	}
	if options.RPCTimeout <= 0 {
		options.RPCTimeout = defaultSocketCallTimeout
	}

	if options.TCPParameters == nil {
		options.TCPParameters = newTCPParameterDefault()
	}
	if err := options.TCPParameters.validateTune(); err != nil {
		return nil, err
	}

	options.TCPParameters.lifecycle = &taskOwner{}
	var mp metric.MeterProvider
	if options.meterProvider == nil {
		mp = otel.GetMeterProvider()
	} else {
		mp = options.meterProvider
	}
	metrics, err := newStreamMetrics(mp)
	if err != nil {
		return nil, fmt.Errorf("failed to initialise metrics: %w", err)
	}

	// we put a limit to the heartbeat.
	// it doesn't make sense to have a heartbeat less than 3 seconds
	if options.TCPParameters.RequestedHeartbeat < (3 * time.Second) {
		return nil, errors.New("RequestedHeartbeat must be greater than 3 seconds")
	}

	if options.MaxConsumersPerClient <= 0 || options.MaxProducersPerClient <= 0 ||
		options.MaxConsumersPerClient > 254 || options.MaxProducersPerClient > 254 {
		return nil, fmt.Errorf(" MaxConsumersPerClient and MaxProducersPerClient must be between 1 and 254")
	}

	if options.SaslConfiguration != nil {
		if options.SaslConfiguration.Mechanism != SaslConfigurationPlain && options.SaslConfiguration.Mechanism != SaslConfigurationExternal {
			return nil, fmt.Errorf("SaslConfiguration mechanism must be PLAIN or EXTERNAL")
		}
	}

	if len(options.ConnectionParameters) == 0 {
		options.ConnectionParameters = []*Broker{newBrokerDefault()}
	} else {
		// fill the missing field for the connection parameters
		for _, parameter := range options.ConnectionParameters {
			if parameter.Uri != "" {
				u, err := url.Parse(parameter.Uri)
				if err != nil {
					return nil, err
				}
				parameter.Scheme = u.Scheme
				if u.User != nil {
					parameter.User = u.User.Username()
					parameter.Password, _ = u.User.Password()
				}
				parameter.Host = u.Hostname()
				parameter.Port = u.Port()

				if vhost := strings.TrimPrefix(u.Path, "/"); len(vhost) > 0 {
					if vhost != "/" && strings.Contains(vhost, "/") {
						return nil, errors.New("multiple segments in URI path: " + u.Path)
					}
					parameter.Vhost = vhost
				}
			}
			parameter.mergeWithDefault()
		}
	}

	var connectionError error
	var client *Client
	for idx, parameter := range options.ConnectionParameters {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		client = newClient(connectionParameters{
			connectionName: "go-stream-locator", broker: parameter,
			tcpParameters: options.TCPParameters, saslConfiguration: options.SaslConfiguration,
			rpcTimeout: options.RPCTimeout, metrics: metrics,
		})

		connectionError = client.connectContext(ctx)
		if connectionError == nil {
			break
		} else {
			client.Close()
			cleanup, cancelCleanup := context.WithTimeout(context.Background(), defaultSocketCallTimeout)
			_ = client.WaitContext(cleanup)
			cancelCleanup()
			nextIfThereIs := ""
			if idx < len(options.ConnectionParameters)-1 {
				nextIfThereIs = "Trying the next broker..."
			}
			logs.LogError("New environment creation. Can't connect to the broker: %s port: %s. %s",
				parameter.Host, parameter.Port, nextIfThereIs)
		}
	}
	if connectionError != nil {
		return nil, connectionError
	}

	lifetime, stopLifetime := context.WithCancel(context.Background())
	return &Environment{
		options:      options,
		producers:    newProducersEnvironment(options.MaxProducersPerClient, metrics),
		consumers:    newConsumersEnvironment(options.MaxConsumersPerClient, metrics),
		locator:      newLocator(client),
		metrics:      metrics,
		lifetime:     lifetime,
		stopLifetime: stopLifetime,
	}, connectionError
}

func (env *Environment) maybeReconnectLocator() error {
	return env.maybeReconnectLocatorContext(context.Background())
}

func (env *Environment) maybeReconnectLocatorContext(ctx context.Context) error {
	if env.IsClosed() {
		return net.ErrClosed
	}
	ctx, endOperation := env.operationContext(ctx)
	defer endOperation()
	release, err := env.locator.operationGate.acquire(ctx)
	if err != nil {
		return err
	}
	defer release()
	if env.locator.current() != nil && env.locator.current().socket.isOpen() {
		return nil
	}

	broker := env.options.ConnectionParameters[0]
	c := newClient(connectionParameters{
		connectionName:    "go-stream-locator",
		broker:            broker,
		tcpParameters:     env.options.TCPParameters,
		saslConfiguration: env.options.SaslConfiguration,
		rpcTimeout:        env.options.RPCTimeout,
		metrics:           env.metrics,
	})

	if err := env.setLocatorClient(c); err != nil {
		return err
	}
	err = c.connectContext(ctx)
	tentatives := 1
	for err != nil {
		sleepTime := rand.Intn(5000) + (tentatives * 1000) // #nosec G404 -- Retry scheduling jitter, not a security token.

		brokerUri := fmt.Sprintf("%s://%s:***@%s:%s/%s", c.broker.Scheme, c.broker.User, c.broker.Host, c.broker.Port, c.broker.Vhost)
		logs.LogError("Can't connect the locator client, error:%s, retry in %d milliseconds, broker: %s", err, sleepTime, brokerUri)

		_ = c.socket.abort()
		c.closeHeartBeat()
		if err := sleepContext(ctx, time.Duration(sleepTime)*time.Millisecond); err != nil {
			return err
		}
		r := rand.New(rand.NewSource(time.Now().UnixNano())) // #nosec G404 -- Broker load selection, not a security token.
		n := r.Intn(len(env.options.ConnectionParameters))
		c1 := newClient(connectionParameters{
			connectionName:    "go-stream-locator",
			broker:            env.options.ConnectionParameters[n],
			tcpParameters:     env.options.TCPParameters,
			saslConfiguration: env.options.SaslConfiguration,
			rpcTimeout:        env.options.RPCTimeout,
			metrics:           env.metrics,
		})
		tentatives++
		if err := env.setLocatorClient(c1); err != nil {
			return err
		}
		err = c1.connectContext(ctx)
		c = c1
	}

	return env.locator.current().connectContext(ctx)
}

func (env *Environment) DeclareStream(streamName string, options *StreamOptions) error {
	err := env.maybeReconnectLocator()
	if err != nil {
		return err
	}
	if err := env.locator.current().DeclareStream(streamName, options); err != nil && err != StreamAlreadyExists {
		return err
	}
	return nil
}

func (env *Environment) DeleteStream(streamName string) error {
	err := env.maybeReconnectLocator()
	if err != nil {
		return err
	}
	return env.locator.current().DeleteStream(streamName)
}

func (env *Environment) NewProducer(streamName string, producerOptions *ProducerOptions) (*Producer, error) {
	return env.NewProducerContext(context.Background(), streamName, producerOptions)
}

// NewProducerContext bounds topology discovery, connection and publisher setup.
// A returned producer has explicit resource lifetime, independent of ctx.
func (env *Environment) NewProducerContext(ctx context.Context, streamName string, producerOptions *ProducerOptions) (*Producer, error) {
	ctx, endOperation := env.operationContext(ctx)
	defer endOperation()
	err := env.maybeReconnectLocatorContext(ctx)

	if err != nil {
		return nil, err
	}

	return env.producers.newProducerContext(ctx, env.locator.current(), streamName, producerOptions, env.options.AddressResolver, env.options.RPCTimeout)
}

func (env *Environment) StreamExists(streamName string) (bool, error) {
	return env.StreamExistsContext(context.Background(), streamName)
}

// StreamExistsContext bounds metadata lookup and preserves transport failures
// rather than reporting an unavailable lookup as a missing stream.
func (env *Environment) StreamExistsContext(ctx context.Context, streamName string) (bool, error) {
	ctx, endOperation := env.operationContext(ctx)
	defer endOperation()
	err := env.maybeReconnectLocatorContext(ctx)
	if err != nil {
		return false, err
	}
	metadata, err := env.locator.current().queryMetadataContext(ctx, streamName)
	if err != nil {
		return false, err
	}
	stream := metadata.Get(streamName)
	if stream == nil {
		return false, StreamMetadataFailure
	}
	return stream.responseCode == responseCodeOk, nil
}

func (env *Environment) QueryOffset(consumerName string, streamName string) (int64, error) {
	return env.QueryOffsetContext(context.Background(), consumerName, streamName)
}

// QueryOffsetContext bounds named-offset lookup without changing its identity.
func (env *Environment) QueryOffsetContext(ctx context.Context, consumerName string, streamName string) (int64, error) {
	ctx, endOperation := env.operationContext(ctx)
	defer endOperation()
	err := env.maybeReconnectLocatorContext(ctx)
	if err != nil {
		return 0, err
	}
	return env.locator.current().queryOffsetContext(ctx, consumerName, streamName)
}

// QuerySequence gets the last id stored for a producer
// you can also see producer.GetLastPublishingId() that is the easier way to get the last-id
func (env *Environment) QuerySequence(publisherReference string, streamName string) (int64, error) {
	return env.QuerySequenceContext(context.Background(), publisherReference, streamName)
}

// QuerySequenceContext bounds stream-scoped publisher-sequence lookup.
func (env *Environment) QuerySequenceContext(ctx context.Context, publisherReference string, streamName string) (int64, error) {
	ctx, endOperation := env.operationContext(ctx)
	defer endOperation()
	err := env.maybeReconnectLocatorContext(ctx)
	if err != nil {
		return 0, err
	}
	return env.locator.current().queryPublisherSequenceContext(ctx, publisherReference, streamName)
}

func (env *Environment) StreamStats(streamName string) (*StreamStats, error) {
	return env.StreamStatsContext(context.Background(), streamName)
}

// StreamStatsContext bounds the broker's retained-stream statistics lookup.
func (env *Environment) StreamStatsContext(ctx context.Context, streamName string) (*StreamStats, error) {
	ctx, endOperation := env.operationContext(ctx)
	defer endOperation()
	err := env.maybeReconnectLocatorContext(ctx)
	if err != nil {
		return nil, err
	}
	return env.locator.current().StreamStatsContext(ctx, streamName)
}

func (env *Environment) StreamMetaData(streamName string) (*StreamMetadata, error) {
	return env.StreamMetaDataContext(context.Background(), streamName)
}

// StreamMetaDataContext bounds metadata lookup and leader-readiness retries.
func (env *Environment) StreamMetaDataContext(ctx context.Context, streamName string) (*StreamMetadata, error) {
	ctx, endOperation := env.operationContext(ctx)
	defer endOperation()
	err := env.maybeReconnectLocatorContext(ctx)
	if err != nil {
		return nil, err
	}
	streamsMetadata, err := env.locator.current().queryMetadataContext(ctx, streamName)
	if err != nil {
		return nil, err
	}
	if streamsMetadata == nil {
		return nil, StreamMetadataFailure
	}
	streamMetadata := streamsMetadata.Get(streamName)
	if streamMetadata == nil {
		return nil, StreamMetadataFailure
	}
	if streamMetadata.responseCode != responseCodeOk {
		return nil, lookErrorCode(streamMetadata.responseCode)
	}

	tentatives := 0
	for streamMetadata.Leader == nil && tentatives < 3 {
		streamsMetadata, err = env.locator.current().queryMetadataContext(ctx, streamName)
		if err != nil {
			return nil, err
		}
		if streamsMetadata == nil {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			return nil, StreamMetadataFailure
		}
		streamMetadata = streamsMetadata.Get(streamName)
		if streamMetadata == nil {
			return nil, StreamMetadataFailure
		}
		tentatives++
		if err := sleepContext(ctx, 100*time.Millisecond); err != nil {
			return nil, err
		}
	}

	if streamMetadata.Leader == nil {
		return nil, LeaderNotReady
	}

	return streamMetadata, nil
}

func (env *Environment) NewConsumer(streamName string,
	messagesHandler MessagesHandler,
	options *ConsumerOptions) (*Consumer, error) {
	return env.NewConsumerContext(context.Background(), streamName, messagesHandler, options)
}

// NewConsumerContext bounds topology, connection, offset resolution and
// subscription setup. Dispatch lifetime is owned by the returned resource.
func (env *Environment) NewConsumerContext(ctx context.Context, streamName string,
	messagesHandler MessagesHandler,
	options *ConsumerOptions) (*Consumer, error) {
	ctx, endOperation := env.operationContext(ctx)
	defer endOperation()
	err := env.maybeReconnectLocatorContext(ctx)
	if err != nil {
		return nil, err
	}

	return env.consumers.NewSubscriberContext(ctx, env.locator.current(), streamName, messagesHandler, options, env.options.AddressResolver, env.options.RPCTimeout)
}

func (env *Environment) NewSuperStreamProducer(superStream string, superStreamProducerOptions *SuperStreamProducerOptions) (*SuperStreamProducer, error) {
	var p, err = newSuperStreamProducer(env, superStream, superStreamProducerOptions)
	if err != nil {
		return nil, err
	}
	return p, p.init()
}

// Close is a nonjoining terminal stop request; external owners use CloseContext.
func (env *Environment) Close() error { return env.Abort() }

func (env *Environment) IsClosed() bool {
	return env.closed.Load()
}

type EnvironmentOptions struct {
	ConnectionParameters  []*Broker
	TCPParameters         *TCPParameters
	SaslConfiguration     *SaslConfiguration
	MaxProducersPerClient int
	MaxConsumersPerClient int
	AddressResolver       *AddressResolver
	RPCTimeout            time.Duration
	meterProvider         metric.MeterProvider
}

func NewEnvironmentOptions() *EnvironmentOptions {
	return &EnvironmentOptions{
		MaxProducersPerClient: 1,
		MaxConsumersPerClient: 1,
		ConnectionParameters:  []*Broker{},
		TCPParameters:         newTCPParameterDefault(),
		SaslConfiguration:     newSaslConfigurationDefault(),
		RPCTimeout:            defaultSocketCallTimeout,
		meterProvider:         otel.GetMeterProvider(),
	}
}

func (envOptions *EnvironmentOptions) SetAddressResolver(addressResolver AddressResolver) *EnvironmentOptions {
	envOptions.AddressResolver = &AddressResolver{
		Host: addressResolver.Host,
		Port: addressResolver.Port,
	}
	return envOptions
}

func (envOptions *EnvironmentOptions) SetMaxProducersPerClient(maxProducersPerClient int) *EnvironmentOptions {
	envOptions.MaxProducersPerClient = maxProducersPerClient
	return envOptions
}

func (envOptions *EnvironmentOptions) SetMaxConsumersPerClient(maxConsumersPerClient int) *EnvironmentOptions {
	envOptions.MaxConsumersPerClient = maxConsumersPerClient
	return envOptions
}

func (envOptions *EnvironmentOptions) SetMeterProvider(meterProvider metric.MeterProvider) *EnvironmentOptions {
	envOptions.meterProvider = meterProvider
	return envOptions
}

func (envOptions *EnvironmentOptions) SetUri(uri string) *EnvironmentOptions {
	if len(envOptions.ConnectionParameters) == 0 {
		envOptions.ConnectionParameters = append(envOptions.ConnectionParameters, &Broker{Uri: uri})
	} else {
		envOptions.ConnectionParameters[0].Uri = uri
	}

	return envOptions
}

func (envOptions *EnvironmentOptions) SetUris(uris []string) *EnvironmentOptions {
	for _, s := range uris {
		envOptions.ConnectionParameters = append(envOptions.ConnectionParameters, &Broker{Uri: s})
	}
	return envOptions
}

func (envOptions *EnvironmentOptions) SetHost(host string) *EnvironmentOptions {
	if len(envOptions.ConnectionParameters) == 0 {
		envOptions.ConnectionParameters = append(envOptions.ConnectionParameters, &Broker{Host: host})
	} else {
		envOptions.ConnectionParameters[0].Host = host
	}
	return envOptions
}

func (envOptions *EnvironmentOptions) SetVHost(vhost string) *EnvironmentOptions {
	if len(envOptions.ConnectionParameters) == 0 {
		envOptions.ConnectionParameters = append(envOptions.ConnectionParameters, &Broker{Vhost: vhost})
	} else {
		envOptions.ConnectionParameters[0].Vhost = vhost
	}
	return envOptions
}

func (envOptions *EnvironmentOptions) SetSaslConfiguration(value string) *EnvironmentOptions {
	if envOptions.SaslConfiguration == nil {
		envOptions.SaslConfiguration = newSaslConfigurationDefault()
	}
	envOptions.SaslConfiguration.Mechanism = value
	return envOptions
}

func (envOptions *EnvironmentOptions) SetTLSConfig(config *tls.Config) *EnvironmentOptions {
	if envOptions.TCPParameters == nil {
		envOptions.TCPParameters = newTCPParameterDefault()
	}
	envOptions.TCPParameters.tlsConfig = config
	return envOptions
}

func (envOptions *EnvironmentOptions) IsTLS(val bool) *EnvironmentOptions {
	if val {
		if len(envOptions.ConnectionParameters) == 0 {
			envOptions.ConnectionParameters = append(envOptions.ConnectionParameters, &Broker{Scheme: "rabbitmq-stream+tls"})
		} else {
			for _, parameter := range envOptions.ConnectionParameters {
				parameter.Scheme = "rabbitmq-stream+tls"
			}
		}
	}
	return envOptions
}

func (envOptions *EnvironmentOptions) SetPort(port int) *EnvironmentOptions {
	if len(envOptions.ConnectionParameters) == 0 {
		brokerOptions := newBrokerDefault()
		brokerOptions.Port = strconv.Itoa(port)
		envOptions.ConnectionParameters = append(envOptions.ConnectionParameters, brokerOptions)
	} else {
		envOptions.ConnectionParameters[0].Port = strconv.Itoa(port)
	}
	return envOptions
}

func (envOptions *EnvironmentOptions) SetUser(user string) *EnvironmentOptions {
	if len(envOptions.ConnectionParameters) == 0 {
		envOptions.ConnectionParameters = append(envOptions.ConnectionParameters, &Broker{User: user})
	} else {
		envOptions.ConnectionParameters[0].User = user
	}
	return envOptions
}

func (envOptions *EnvironmentOptions) SetPassword(password string) *EnvironmentOptions {
	if len(envOptions.ConnectionParameters) == 0 {
		envOptions.ConnectionParameters = append(envOptions.ConnectionParameters, &Broker{Password: password})
	} else {
		envOptions.ConnectionParameters[0].Password = password
	}
	return envOptions
}

func (envOptions *EnvironmentOptions) SetRequestedHeartbeat(requestedHeartbeat time.Duration) *EnvironmentOptions {
	if envOptions.TCPParameters == nil {
		envOptions.TCPParameters = newTCPParameterDefault()
	}
	envOptions.TCPParameters.RequestedHeartbeat = requestedHeartbeat

	return envOptions
}

func (envOptions *EnvironmentOptions) SetRequestedMaxFrameSize(requestedMaxFrameSize int) *EnvironmentOptions {
	if envOptions.TCPParameters == nil {
		envOptions.TCPParameters = newTCPParameterDefault()
	}
	envOptions.TCPParameters.RequestedMaxFrameSize = requestedMaxFrameSize

	return envOptions
}

func (envOptions *EnvironmentOptions) SetWriteBuffer(writeBuffer int) *EnvironmentOptions {
	if envOptions.TCPParameters == nil {
		envOptions.TCPParameters = newTCPParameterDefault()
	}
	envOptions.TCPParameters.WriteBuffer = writeBuffer

	return envOptions
}

func (envOptions *EnvironmentOptions) SetReadBuffer(readBuffer int) *EnvironmentOptions {
	if envOptions.TCPParameters == nil {
		envOptions.TCPParameters = newTCPParameterDefault()
	}
	envOptions.TCPParameters.ReadBuffer = readBuffer

	return envOptions
}

func (envOptions *EnvironmentOptions) SetNoDelay(noDelay bool) *EnvironmentOptions {
	if envOptions.TCPParameters == nil {
		envOptions.TCPParameters = newTCPParameterDefault()
	}
	envOptions.TCPParameters.NoDelay = noDelay

	return envOptions
}

func (envOptions *EnvironmentOptions) SetRPCTimeout(timeout time.Duration) *EnvironmentOptions {
	envOptions.RPCTimeout = timeout
	return envOptions
}

type clientOptions interface {
	GetClientProvidedName(defaultClientProvidedName string) string
}

type environmentCoordinator struct {
	operationGate     contextGate
	mutex             *sync.Mutex
	clientsPerContext sync.Map
	maxItemsForClient int
	nextId            int
	metrics           *streamMetrics
}

func (cc *environmentCoordinator) isProducerListFull(clientsPerContextId int) bool {
	client, ok := cc.clientsPerContext.Load(clientsPerContextId)
	if !ok {
		logs.LogError("client not found")
		return false
	}
	return client.(*Client).coordinator.ProducersCount() >= cc.maxItemsForClient
}

func (cc *environmentCoordinator) isConsumerListFull(clientsPerContextId int) bool {
	client, ok := cc.clientsPerContext.Load(clientsPerContextId)
	if !ok {
		logs.LogError("client not found")
		return false
	}
	return client.(*Client).coordinator.ConsumersCount() >= cc.maxItemsForClient
}

func (cc *environmentCoordinator) maybeCleanClients() {
	// Note: Mutex is not needed here because:
	// 1. sync.Map operations (Range, Delete) are thread-safe and can be called concurrently
	// 2. Deleting the current entry during Range iteration is safe per Go's sync.Map documentation
	// 3. This function is called from cleanup callbacks which may run concurrently, but
	//    sync.Map handles concurrent access safely without requiring external synchronization
	// 4. We only delete entries for clients that are already closed (socket.isOpen() == false),
	//    so there's no risk of deleting active clients that are being used elsewhere

	cc.clientsPerContext.Range(func(key, value any) bool {
		client := value.(*Client)
		if !client.socket.isOpen() {
			cc.clientsPerContext.Delete(key)
		}
		return true
	})
}

func (c *Client) maybeCleanProducers(streamName string) {
	c.coordinator.Producers().Range(func(pidx, p any) bool {
		producer := p.(*Producer)
		if producer.GetStreamName() == streamName {
			err := c.coordinator.RemoveProducerById(pidx.(uint8), Event{
				Command:    CommandMetadataUpdate,
				StreamName: streamName,
				Name:       producer.GetName(),
				Reason:     MetaDataUpdate,
				Err:        nil,
			})
			if err != nil {
				return false
			}
		}

		return true
	})
}

func (c *Client) maybeCleanConsumers(streamName string) {
	c.coordinator.Consumers().Range(func(pidx, cs any) bool {
		consumer := cs.(*Consumer)
		if consumer.options.streamName == streamName {
			err := c.coordinator.RemoveConsumerById(pidx.(uint8), Event{
				Command:    CommandMetadataUpdate,
				StreamName: streamName,
				Name:       consumer.GetName(),
				Reason:     MetaDataUpdate,
				Err:        nil,
			})
			if err != nil {
				return false
			}
		}

		return true
	})
}

func (cc *environmentCoordinator) newClientEntityContext(ctx context.Context,
	isListFull func(int) bool,
	defaultClientName string,
	leader *Broker,
	tcpParameters *TCPParameters,
	saslConfiguration *SaslConfiguration,
	options clientOptions,
	rpcTimeout time.Duration,
) (*Client, error) {
	ctx, endOperation := ensureOperationContext(ctx)
	defer endOperation()
	release, admissionErr := cc.operationGate.acquire(ctx)
	if admissionErr != nil {
		return nil, admissionErr
	}
	defer release()
	var clientResult *Client

	cc.clientsPerContext.Range(func(key, value any) bool {
		if value.(*Client).socket.isOpen() && !isListFull(key.(int)) {
			clientResult = value.(*Client)
			return false
		}
		return true
	})

	clientProvidedName := defaultClientName
	if options != nil {
		clientProvidedName = options.GetClientProvidedName(defaultClientName)
	}

	if clientResult == nil {
		clientResult = cc.newClientForConnection(clientProvidedName, leader, tcpParameters, saslConfiguration, rpcTimeout)
	}

	err := clientResult.connectContext(ctx)
	if err != nil {
		_ = clientResult.socket.abort()
		return nil, err
	}

	return cc.validateBrokerConnectionContext(ctx, clientResult, leader,
		func() *Client {
			return cc.newClientForConnection(clientProvidedName, leader, tcpParameters, saslConfiguration, rpcTimeout)
		})
}

func (cc *environmentCoordinator) newProducerContext(ctx context.Context, leader *Broker, tcpParameters *TCPParameters, saslConfiguration *SaslConfiguration, streamName string, options *ProducerOptions, rpcTimeout time.Duration, cleanUp func()) (*Producer, error) {
	ctx, endOperation := ensureOperationContext(ctx)
	defer endOperation()
	client, err := cc.newClientEntityContext(ctx, cc.isProducerListFull, "go-stream-producer", leader, tcpParameters, saslConfiguration, options, rpcTimeout)
	if err != nil {
		return nil, err
	}
	return client.declarePublisherContext(ctx, streamName, options, cleanUp)
}

func (cc *environmentCoordinator) newConsumerContext(ctx context.Context, leader *Broker, tcpParameters *TCPParameters, saslConfiguration *SaslConfiguration,
	streamName string, messagesHandler MessagesHandler,
	options *ConsumerOptions, rpcTimeout time.Duration, cleanUp func()) (*Consumer, error) {
	ctx, endOperation := ensureOperationContext(ctx)
	defer endOperation()
	client, err := cc.newClientEntityContext(ctx, cc.isConsumerListFull, "go-stream-consumer", leader, tcpParameters, saslConfiguration, options, rpcTimeout)
	if err != nil {
		return nil, err
	}

	return client.declareSubscriberContext(ctx, streamName, messagesHandler, options, cleanUp)
}

func (cc *environmentCoordinator) validateBrokerConnectionContext(ctx context.Context, client *Client, broker *Broker, newClientFunc func() *Client) (*Client, error) {
	ctx, endOperation := ensureOperationContext(ctx)
	defer endOperation()
	for client.connectionProperties.host != broker.advHost ||
		client.connectionProperties.port != broker.advPort {
		logs.LogDebug("connectionProperties host %s doesn't match with the advertised_host %s, advertised_port %s .. retry",
			client.connectionProperties.host,
			broker.advHost, broker.advPort)
		// Safety check: Only close the client if there are no active consumers or producers.
		// This prevents premature disconnection during active operations, which could cause
		// message loss or connection errors. If there are active producers/consumers, we
		// create a new client without closing the old one, allowing graceful migration.
		if client.coordinator.ConsumersCount() == 0 && client.coordinator.ProducersCount() == 0 {
			_ = client.socket.abort()
			client.closeHeartBeat()
		}
		client = newClientFunc()
		err := client.connectContext(ctx)
		if err != nil {
			return nil, err
		}
		if err := sleepContext(ctx, time.Duration(500+rand.Intn(1000))*time.Millisecond); err != nil { // #nosec G404 -- Retry scheduling jitter, not a security token.
			_ = client.socket.abort()
			return nil, err
		}
	}
	return client, nil
}

func (cc *environmentCoordinator) newClientForConnection(connectionName string, broker *Broker, tcpParameters *TCPParameters, saslConfiguration *SaslConfiguration, rpcTimeout time.Duration) *Client {
	clientResult := newClient(connectionParameters{
		connectionName:    connectionName,
		broker:            broker,
		tcpParameters:     tcpParameters,
		saslConfiguration: saslConfiguration,
		rpcTimeout:        rpcTimeout,
		metrics:           cc.metrics,
	})
	cc.mutex.Lock()
	cc.nextId++
	cc.clientsPerContext.Store(cc.nextId, clientResult)
	cc.mutex.Unlock()
	return clientResult
}

func (cc *environmentCoordinator) Close() error {
	cc.clientsPerContext.Range(func(_, value any) bool {
		value.(*Client).coordinator.Close()

		return true
	})

	return nil
}

type producersEnvironment struct {
	operationGate        contextGate
	mutex                *sync.Mutex
	producersCoordinator map[string]*environmentCoordinator
	maxItemsForClient    int
	metrics              *streamMetrics
}

func newProducersEnvironment(maxItemsForClient int, metrics *streamMetrics) *producersEnvironment {
	producers := &producersEnvironment{
		mutex:                &sync.Mutex{},
		producersCoordinator: map[string]*environmentCoordinator{},
		maxItemsForClient:    maxItemsForClient,
		metrics:              metrics,
	}
	return producers
}

func (ps *producersEnvironment) newProducerContext(ctx context.Context, clientLocator *Client, streamName string,
	options *ProducerOptions, resolver *AddressResolver, rpcTimeOut time.Duration) (*Producer, error) {
	ctx, endOperation := ensureOperationContext(ctx)
	defer endOperation()
	release, admissionErr := ps.operationGate.acquire(ctx)
	if admissionErr != nil {
		return nil, admissionErr
	}
	defer release()

	leader, err := clientLocator.BrokerLeaderWithResolverContext(ctx, streamName, resolver)
	if err != nil {
		return nil, err
	}

	coordinatorKey := leader.hostPort()
	ps.mutex.Lock()
	if ps.producersCoordinator[coordinatorKey] == nil {
		ps.producersCoordinator[coordinatorKey] = &environmentCoordinator{
			clientsPerContext: sync.Map{},
			mutex:             &sync.Mutex{},
			maxItemsForClient: ps.maxItemsForClient,
			nextId:            0,
			metrics:           ps.metrics,
		}
	}
	coordinator := ps.producersCoordinator[coordinatorKey]
	ps.mutex.Unlock()

	leader.cloneFrom(clientLocator.broker, resolver)

	cleanUp := func() {
		for _, coordinator := range ps.getCoordinators() {
			coordinator.maybeCleanClients()
		}
	}

	producer, err := coordinator.newProducerContext(ctx, leader, clientLocator.tcpParameters,
		clientLocator.saslConfiguration, streamName, options, rpcTimeOut, cleanUp)
	if err != nil {
		return nil, err
	}

	return producer, err
}

func (ps *producersEnvironment) getCoordinators() map[string]*environmentCoordinator {
	ps.mutex.Lock()
	defer ps.mutex.Unlock()
	result := make(map[string]*environmentCoordinator, len(ps.producersCoordinator))
	for key, value := range ps.producersCoordinator {
		result[key] = value
	}
	return result
}

type consumersEnvironment struct {
	operationGate        contextGate
	mutex                *sync.Mutex
	consumersCoordinator map[string]*environmentCoordinator
	maxItemsForClient    int
	metrics              *streamMetrics
}

func newConsumersEnvironment(maxItemsForClient int, metrics *streamMetrics) *consumersEnvironment {
	consumersEnv := &consumersEnvironment{
		mutex:                &sync.Mutex{},
		consumersCoordinator: map[string]*environmentCoordinator{},
		maxItemsForClient:    maxItemsForClient,
		metrics:              metrics,
	}
	return consumersEnv
}

func (ps *consumersEnvironment) NewSubscriber(clientLocator *Client, streamName string,
	messagesHandler MessagesHandler,
	consumerOptions *ConsumerOptions, resolver *AddressResolver, rpcTimeout time.Duration) (*Consumer, error) {
	return ps.NewSubscriberContext(context.Background(), clientLocator, streamName, messagesHandler, consumerOptions, resolver, rpcTimeout)
}

func (ps *consumersEnvironment) NewSubscriberContext(ctx context.Context, clientLocator *Client, streamName string,
	messagesHandler MessagesHandler,
	consumerOptions *ConsumerOptions, resolver *AddressResolver, rpcTimeout time.Duration) (*Consumer, error) {
	ctx, endOperation := ensureOperationContext(ctx)
	defer endOperation()
	release, admissionErr := ps.operationGate.acquire(ctx)
	if admissionErr != nil {
		return nil, admissionErr
	}
	defer release()

	consumerBroker, err := clientLocator.BrokerForConsumerContext(ctx, streamName)
	if err != nil {
		return nil, err
	}

	coordinatorKey := consumerBroker.hostPort()
	ps.mutex.Lock()
	if ps.consumersCoordinator[coordinatorKey] == nil {
		ps.consumersCoordinator[coordinatorKey] = &environmentCoordinator{
			clientsPerContext: sync.Map{},
			mutex:             &sync.Mutex{},
			maxItemsForClient: ps.maxItemsForClient,
			nextId:            0,
			metrics:           ps.metrics,
		}
	}
	coordinator := ps.consumersCoordinator[coordinatorKey]
	ps.mutex.Unlock()

	consumerBroker.cloneFrom(clientLocator.broker, resolver)

	cleanUp := func() {
		for _, coordinator := range ps.getCoordinators() {
			coordinator.maybeCleanClients()
		}
	}

	consumer, err := coordinator.
		newConsumerContext(ctx, consumerBroker, clientLocator.tcpParameters,
			clientLocator.saslConfiguration,
			streamName, messagesHandler, consumerOptions, rpcTimeout, cleanUp)
	if err != nil {
		return nil, err
	}

	return consumer, err
}

func (ps *consumersEnvironment) getCoordinators() map[string]*environmentCoordinator {
	ps.mutex.Lock()
	defer ps.mutex.Unlock()
	result := make(map[string]*environmentCoordinator, len(ps.consumersCoordinator))
	for key, value := range ps.consumersCoordinator {
		result[key] = value
	}
	return result
}

// Super stream

func (env *Environment) DeclareSuperStream(superStreamName string, options SuperStreamOptions) error {
	err := env.maybeReconnectLocator()
	if err != nil {
		return err
	}
	if err := env.locator.current().DeclareSuperStream(superStreamName, options); err != nil && !errors.Is(err, StreamAlreadyExists) {
		return err
	}
	return nil
}

func (env *Environment) DeleteSuperStream(superStreamName string) error {
	err := env.maybeReconnectLocator()
	if err != nil {
		return err
	}
	return env.locator.current().DeleteSuperStream(superStreamName)
}

func (env *Environment) QueryPartitions(superStreamName string) ([]string, error) {
	return env.QueryPartitionsContext(context.Background(), superStreamName)
}

// QueryPartitionsContext bounds discovery without changing broker partition order.
func (env *Environment) QueryPartitionsContext(ctx context.Context, superStreamName string) ([]string, error) {
	ctx, endOperation := env.operationContext(ctx)
	defer endOperation()
	err := env.maybeReconnectLocatorContext(ctx)
	if err != nil {
		return nil, err
	}
	return env.locator.current().QueryPartitionsContext(ctx, superStreamName)
}

// StoreOffset stores the offset for a consumer for a stream
// You should use the StoreOffset method of the Consumer interface
// to store the offset for a consumer
// The StoreOffset should not be called for each message.
// the best practice is to store after a batch of messages
// StoreOffset does not return any application error, if the stream does not exist or the consumer does not exist
// the error is logged in the server
func (env *Environment) StoreOffset(consumerName string, streamName string, offset int64) error {
	return env.StoreOffsetContext(context.Background(), consumerName, streamName, offset)
}

// StoreOffsetContext bounds the native offset write. Stream offset storage has
// no response acknowledgment, so successful writing is not durable commit proof.
func (env *Environment) StoreOffsetContext(ctx context.Context, consumerName string, streamName string, offset int64) error {
	ctx, endOperation := env.operationContext(ctx)
	defer endOperation()
	err := env.maybeReconnectLocatorContext(ctx)
	if err != nil {
		return err
	}
	return env.locator.current().StoreOffsetContext(ctx, consumerName, streamName, offset)
}

func (env *Environment) QueryRoute(superStream string, routingKey string) ([]string, error) {
	return env.QueryRouteContext(context.Background(), superStream, routingKey)
}

// QueryRouteContext bounds stream routing lookup without changing routing policy.
func (env *Environment) QueryRouteContext(ctx context.Context, superStream string, routingKey string) ([]string, error) {
	ctx, endOperation := env.operationContext(ctx)
	defer endOperation()
	err := env.maybeReconnectLocatorContext(ctx)
	if err != nil {
		return nil, err
	}
	return env.locator.current().queryRouteContext(ctx, superStream, routingKey)
}

func (env *Environment) NewSuperStreamConsumer(superStream string, messagesHandler MessagesHandler, options *SuperStreamConsumerOptions) (*SuperStreamConsumer, error) {
	s, err := newSuperStreamConsumer(env, superStream, messagesHandler, options)
	if err != nil {
		return nil, err
	}
	err = s.init()
	return s, err
}
