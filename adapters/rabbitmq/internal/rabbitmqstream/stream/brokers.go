package stream

import (
	"crypto/tls"
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"
	"time"
)

// AddressResolver is a caller-selected endpoint override, not DNS validation or
// an address allowlist.
type AddressResolver struct {
	// Host replaces discovered connection host selection, not a network allowlist.
	Host string
	// Port replaces the discovered port; root endpoint validation is separate.
	Port int
}

// TCPParameters is mutable native connection/decoder policy configured before
// opening; it is not root TLS security validation.
type TCPParameters struct {
	lifecycle *taskOwner
	// DecoderLimits bounds protocol allocation/decoding independently of requested frame tuning.
	DecoderLimits DecoderLimits
	tlsConfig     *tls.Config
	// RequestedHeartbeat is the whole-second client tune request, not an operation deadline.
	RequestedHeartbeat time.Duration
	// RequestedMaxFrameSize is the negotiation request; decoder admission remains independently finite.
	RequestedMaxFrameSize int
	// WriteBuffer sizes the per-connection buffered writer, not total process memory.
	WriteBuffer int
	// ReadBuffer sizes the per-connection buffered reader, not total process memory.
	ReadBuffer int
	// NoDelay requests TCP_NODELAY; it is not an admission or delivery guarantee.
	NoDelay bool
}

func (p *TCPParameters) validateTune() error {
	if p.RequestedMaxFrameSize < 8 || int64(p.RequestedMaxFrameSize) > math.MaxUint32 {
		return fmt.Errorf("requested frame size must be between 8 and %d", uint64(math.MaxUint32))
	}
	seconds := p.RequestedHeartbeat / time.Second
	if seconds < 3 || int64(seconds) > math.MaxUint32 {
		return fmt.Errorf("requested heartbeat seconds must be between 3 and %d", uint64(math.MaxUint32))
	}
	return nil
}

// Broker is mutable native connection metadata that can contain credential/URI
// secrets. It must not be formatted as a public diagnostic or mutated during
// connection use.
type Broker struct {
	// Host is mutable seed/discovery connection data; validate trust before dialing.
	Host string
	// Port is the raw decimal connection port string.
	Port string
	// User is the SASL username and can appear in GetUri; it is not redacted.
	User string
	// Vhost selects the broker virtual host; it is not tenant authorization.
	Vhost string
	// Uri caches a credential-bearing URI; GetUri can populate it lazily. Do not log it.
	Uri string
	// Password is retained credential material without secure-erasure semantics.
	Password string
	// Scheme selects the raw stream/TLS URI route, not root security admission.
	Scheme string

	advHost string
	advPort string
}

func newBrokerDefault() *Broker {
	return &Broker{
		Scheme:   "rabbitmq-stream",
		Host:     "localhost",
		Port:     StreamTcpPort,
		User:     "guest",
		Password: "guest",
		Vhost:    "/",
	}
}

func newTCPParameterDefault() *TCPParameters {
	return &TCPParameters{
		RequestedHeartbeat:    defaultHeartbeat,
		RequestedMaxFrameSize: 1048576,
		WriteBuffer:           defaultWriteSocketBuffer,
		ReadBuffer:            defaultReadSocketBuffer,
		NoDelay:               true,
		tlsConfig:             nil,
	}
}

func (br *Broker) isTLS() bool {
	return strings.Index(br.Scheme, "+tls") > 0
}

func (br *Broker) mergeWithDefault() {
	broker := newBrokerDefault()
	if br.Host == "" {
		br.Host = broker.Host
	}
	if br.Vhost == "" {
		br.Vhost = broker.Vhost
	}

	if br.User == "" {
		br.User = broker.User
	}
	if br.User == "" {
		br.User = broker.User
	}
	if br.Password == "" {
		br.Password = broker.Password
	}
	if br.Port == "" || br.Port == "0" {
		br.Port = broker.Port
	}
	if br.Scheme == "" {
		br.Scheme = broker.Scheme
	}
}

func (br *Broker) cloneFrom(broker *Broker, resolver *AddressResolver) {
	br.User = broker.User
	br.Password = broker.Password
	br.Vhost = broker.Vhost
	br.Scheme = broker.Scheme
	if resolver != nil {
		br.Host = resolver.Host
		br.Port = strconv.Itoa(resolver.Port)
	}
}

// GetUri lazily stores and returns a URI containing User/Password. This mutates
// the receiver and is not a redacted diagnostic.
func (br *Broker) GetUri() string {
	if br.Uri == "" {
		br.Uri = fmt.Sprintf("%s://%s:%s@%s:%s/%s",
			br.Scheme,
			br.User, br.Password,
			br.Host, br.Port, br.Vhost)
	}
	return br.Uri
}

func newBroker(host string, port string) *Broker {
	return &Broker{
		Host: host,
		Port: port,
	}
}

// Brokers owns a concurrent lookup map, not immutability of returned Broker
// pointers; its zero value is not initialized.
type Brokers struct {
	items *sync.Map
}

func newBrokers() *Brokers {
	return &Brokers{items: &sync.Map{}}
}

// Add registers and returns a mutable broker pointer; concurrent map storage
// does not synchronize that pointer field state.
func (brs *Brokers) Add(brokerReference int16, host string, port uint32) *Broker {
	broker := newBroker(host, strconv.Itoa(int(port)))
	brs.items.Store(brokerReference, broker)
	return broker
}

// Get returns the borrowed pointer or nil when the reference is absent.
func (brs *Brokers) Get(brokerReference int16) *Broker {
	value, ok := brs.items.Load(brokerReference)
	if !ok {
		return nil
	}

	return value.(*Broker)
}

func (br *Broker) hostPort() string {
	return fmt.Sprintf("%s:%s", br.Host, br.Port)
}

// StreamMetadata contains borrowed native broker pointers and replica slices
// from one response, not an atomic cluster snapshot.
type StreamMetadata struct {
	stream       string
	responseCode uint16
	// Leader borrows a mutable discovered broker and may be nil on failed metadata.
	Leader *Broker
	// Replicas borrows the current discovered broker list; it is not a stable topology.
	Replicas []*Broker
}

// String formats endpoint/replica data and requires a non-nil Leader; it is not
// a redacted identity.
func (sm StreamMetadata) String() string {
	replicas := ""
	for _, replica := range sm.Replicas {
		replicas += fmt.Sprintf(" - %s:%s", replica.Host, replica.Port)
	}
	return fmt.Sprintf("leader %s:%s, followers %s ", sm.Leader.Host, sm.Leader.Port, replicas)
}

// New borrows the leader and replica slice for one response, without a deep
// copy.
func (StreamMetadata) New(stream string, responseCode uint16,
	leader *Broker, replicas []*Broker) *StreamMetadata {
	return &StreamMetadata{stream: stream, responseCode: responseCode,
		Leader: leader, Replicas: replicas}
}

// StreamsMetadata owns concurrent lookup entries but returns borrowed mutable
// metadata; initialize with New.
type StreamsMetadata struct {
	items *sync.Map
}

// New returns a fresh initialized lookup; the receiver contents are ignored.
func (StreamsMetadata) New() *StreamsMetadata {
	return &StreamsMetadata{&sync.Map{}}
}

// Add stores borrowed leader/replica state and returns the registered metadata
// pointer.
func (smd *StreamsMetadata) Add(stream string, responseCode uint16,
	leader *Broker, replicas []*Broker) *StreamMetadata {
	streamMetadata := StreamMetadata{}.New(stream, responseCode,
		leader, replicas)
	smd.items.Store(stream, streamMetadata)
	return streamMetadata
}

// Get returns borrowed response state or nil when absent.
func (smd *StreamsMetadata) Get(stream string) *StreamMetadata {
	value, ok := smd.items.Load(stream)
	if !ok {
		return nil
	}
	return value.(*StreamMetadata)
}
