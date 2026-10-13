package stream

// IClient is the legacy context-free native client seam, not root v2 policy.
// Implementations own broker I/O and resource lifetime; these methods do not add
// application-effect transactions, root admission or universal cancellation.
type IClient interface {
	// Close requests resource stop without an external task join.
	Close()
	// DeleteStream requests broker deletion; unknown I/O outcomes need reconciliation.
	DeleteStream(streamName string) error
	// DeclarePublisher returns an owned publisher over borrowed validated options.
	DeclarePublisher(streamName string, options *ProducerOptions) (*Producer, error)
	// BrokerLeader returns borrowed peer discovery data, not an allowlist.
	BrokerLeader(stream string) (*Broker, error)
	// BrokerLeaderWithResolver applies explicit connection-address replacement policy.
	BrokerLeaderWithResolver(stream string, resolver *AddressResolver) (*Broker, error)
	// StreamExists collapses native lookup failures to false as well as absence.
	StreamExists(stream string) bool
	// BrokerForConsumer selects a discovered broker without stable-placement guarantees.
	BrokerForConsumer(stream string) (*Broker, error)
	// DeclareStream requests native provisioning, not root topology management.
	DeclareStream(streamName string, options *StreamOptions) error
	// StoreOffset writes named progress without an atomic external-effect receipt.
	StoreOffset(consumerName string, streamName string, offset int64) error
	// DeclareSubscriber returns an owned subscription over borrowed cooperative callbacks.
	DeclareSubscriber(streamName string, messagesHandler MessagesHandler, options *ConsumerOptions) (*Consumer, error)
	// StreamStats returns one broker chunk-statistics response, not exact stream end.
	StreamStats(streamName string) (*StreamStats, error)
	// DeclareSuperStream requests native fan-out provisioning without an effect transaction.
	DeclareSuperStream(superStream string, options SuperStreamOptions) error
	// DeleteSuperStream requests deletion with possible unknown broker outcome.
	DeleteSuperStream(superStream string) error
	// QueryPartitions returns one ordered broker topology response.
	QueryPartitions(superStream string) ([]string, error)
}
