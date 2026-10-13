# Source documentation audit

[Documentation map](index.md) · [Repository README](../README.md)
[Next: Operations and capacity runbook](operations.md)

## Acceptance, ownership, consumers and smallest checks

Base ec03fcaf353a63a39a220f2c3f6913aa6fae3396, freshly fetched remote main in a clean detached external
worktree. Tier A comments/docs only; original unrelated checkout is preserved.
The root v2 owns bounded application policy over explicit transports. The
canonical RabbitMQ v2 adapter owns a private upstream-derived protocol binding;
its deprecated v2 facade retains its published successor dependency. Both
OpenTelemetry modules intentionally consume published root v1 types, not v2.
Catalog direct consumers are the corresponding adapters and compatibility
facades; application effect, broker/ACL/retention policy and SDK lifecycle remain
external owner boundaries.

Named risks: confirmation versus admission and ambiguous delivery, byte/slice
ownership, bounded async admission and shutdown completion, stored offsets and
external effects, retained-range/topology drift, reconnect opening/late cleanup,
AMQP/frame/decompression admission, credentials/TLS/network discovery, inline
observers/providers and telemetry cardinality, internal fork versus public
policy and legacy major identities.

Smallest checks: complete tracked source/assurance/variant inventory, source-led
public/private rationale, exact AST/raw tokens/literals/directives and affected
formatting; parsed HTML/native package renders; native configured API producer
only if one exists; selected existing Examples with TestMain/tag/service scope
inspected first; local docs/offline specification/links, unchanged authenticated
inputs and pinned full/changed/staged secret scans; ordinary complete final diff
truth/usefulness inspection and explicit-path Intent commit. No runtime/dependency
repair, desired-decision/history/corpus rewrite, broker/HA/race/fuzz/benchmark,
public-consumer or release campaign. All Go/native checks await coordinator grant
and use three task-owned disposable caches with immediate cleanup on all paths.

## Missing documentation versus investigation

Root and adapter headers are thin, while private protocol packages need canonical
ownership/authority headers. Missing public field/interface/declaration contracts
must be investigated rather than mechanically expanded. Existing upstream MIT
notices, pinned source identities, useful AMQP field semantics and deprecation
markers are retained. Internal exported fork APIs are not interchangeable with
root policy: their capability/security/lifetime limits need explicit treatment.
The six TODOs in decimal decoding, transactional state/body mapping, metric
initialization/attributes and metadata notifications were investigated and
replaced with actual limitations or maintenance rationale. No runtime change is
authorized; no generator, lint or deprecation marker is suppressed.

## Modules, packages and native provenance

- Module .: github.com/faustbrian/go-rabbitmq-streams/v2; Go 1.27.2.
- Module otel: github.com/faustbrian/go-rabbitmq-streams/otel; Go 1.27.0.
- Module rabbitmq: github.com/faustbrian/go-rabbitmq-streams/rabbitmq/v2; Go 1.27.2.
- Module adapters/otel: github.com/faustbrian/go-rabbitmq-streams/adapters/otel; Go 1.27.0.
- Module adapters/rabbitmq: github.com/faustbrian/go-rabbitmq-streams/adapters/rabbitmq/v2; Go 1.27.2.
- Package . / .: rabbitstream (public).
- Package otel / .: rabbitstreamotel (public).
- Package rabbitmq / .: rabbitmq (public).
- Package adapters/otel / .: rabbitstreamotel (public).
- Package adapters/rabbitmq / .: rabbitmq (public).
- Package adapters/rabbitmq / internal/rabbitmqstream/amqp: amqp (internal).
- Package adapters/rabbitmq / internal/rabbitmqstream/logs: logs (internal).
- Package adapters/rabbitmq / internal/rabbitmqstream/message: message (internal).
- Package adapters/rabbitmq / internal/rabbitmqstream/stream: stream (internal).

Five `.golib.yaml` baselines use binary apidiff mode (root, canonical RabbitMQ,
canonical telemetry and both legacy modules); no prose snapshot or generation
script was found. Make/native CI select authenticated Tools 2.3.0. No
package-local docs script or docs-specific operation is configured. Root TestMain
runs local mutation-safety guards before selected Examples; adapter broker tests
are integration-tagged. RabbitMQ setup Examples have no Output and compile only.
Internal build variants include debug and !pkgerrors; all are inventoried below.
The private binding is an owned upstream-derived fork, not generated or vendored
code; its source/license/changes require inspection before contract edits.

Reproduce tracked inputs with `git ls-files '*.go' '*go.mod' '*go.sum'` and read
`modules.json`/`packages.json` for module/package ownership, `.golib.yaml` and
Make/CI for native baseline mode and operation selection.

## Every tracked Go file (prewrite inventory)

| Path | Module | Package | Role and retained entrypoints/markers |
| --- | --- | --- | --- |
| adapters/otel/benchmark_test.go | adapters/otel | rabbitstreamotel_test | assurance; BenchmarkObserve, BenchmarkInjectTraceContext |
| adapters/otel/boundary_internal_test.go | adapters/otel | rabbitstreamotel | assurance |
| adapters/otel/configuration_internal_test.go | adapters/otel | rabbitstreamotel | assurance |
| adapters/otel/example_test.go | adapters/otel | rabbitstreamotel_test | assurance; Example |
| adapters/otel/fuzz_test.go | adapters/otel | rabbitstreamotel_test | assurance; FuzzPropagationOwnershipAndBounds, FuzzObservationIsolation |
| adapters/otel/observer_internal_test.go | adapters/otel | rabbitstreamotel | assurance |
| adapters/otel/otel.go | adapters/otel | rabbitstreamotel | source: public/private contracts |
| adapters/otel/otel_test.go | adapters/otel | rabbitstreamotel_test | assurance |
| adapters/otel/propagation_test.go | adapters/otel | rabbitstreamotel_test | assurance |
| adapters/rabbitmq/aaa_mutation_contract_test.go | adapters/rabbitmq | rabbitmq | assurance |
| adapters/rabbitmq/benchmark_test.go | adapters/rabbitmq | rabbitmq | assurance; BenchmarkWireMessagePolicy, BenchmarkSuperStreamHashRouting |
| adapters/rabbitmq/boundary_test.go | adapters/rabbitmq | rabbitmq | assurance |
| adapters/rabbitmq/canceled_environment_admission_test.go | adapters/rabbitmq | rabbitmq | assurance |
| adapters/rabbitmq/client.go | adapters/rabbitmq | rabbitmq | source: public/private contracts |
| adapters/rabbitmq/client_test.go | adapters/rabbitmq | rabbitmq | assurance |
| adapters/rabbitmq/consumer.go | adapters/rabbitmq | rabbitmq | source: public/private contracts |
| adapters/rabbitmq/consumer_notification_join_test.go | adapters/rabbitmq | rabbitmq | assurance |
| adapters/rabbitmq/consumer_test.go | adapters/rabbitmq | rabbitmq | assurance |
| adapters/rabbitmq/decoder_admission_test.go | adapters/rabbitmq | rabbitmq | assurance |
| adapters/rabbitmq/environment.go | adapters/rabbitmq | rabbitmq | source: public/private contracts |
| adapters/rabbitmq/example_test.go | adapters/rabbitmq | rabbitmq_test | assurance; ExampleOpenProducer, ExampleOpenConsumer |
| adapters/rabbitmq/fuzz_test.go | adapters/rabbitmq | rabbitmq | assurance; FuzzWireMessageRoundTrip, FuzzHashPartition, FuzzConfirmationClassification |
| adapters/rabbitmq/inspection.go | adapters/rabbitmq | rabbitmq | source: public/private contracts |
| adapters/rabbitmq/integration_a_upgrade_test.go | adapters/rabbitmq | rabbitmq | assurance; variant integration |
| adapters/rabbitmq/integration_benchmark_test.go | adapters/rabbitmq | rabbitmq | assurance; variant integration; BenchmarkEquivalentConfirmedPublish, BenchmarkBoundedConfirmedWindow, BenchmarkBacklogCatchUp |
| adapters/rabbitmq/integration_resources_test.go | adapters/rabbitmq | rabbitmq | assurance; variant integration; BenchmarkIdleProducerResources |
| adapters/rabbitmq/integration_test.go | adapters/rabbitmq | rabbitmq | assurance; variant integration |
| adapters/rabbitmq/internal/rabbitmqstream/amqp/annotation_keys_test.go | adapters/rabbitmq | amqp | assurance |
| adapters/rabbitmq/internal/rabbitmqstream/amqp/buffer.go | adapters/rabbitmq | amqp | source: public/private contracts |
| adapters/rabbitmq/internal/rabbitmqstream/amqp/decode.go | adapters/rabbitmq | amqp | source: public/private contracts; retained/investigate TODO |
| adapters/rabbitmq/internal/rabbitmqstream/amqp/decoder_limits_test.go | adapters/rabbitmq | amqp | assurance |
| adapters/rabbitmq/internal/rabbitmqstream/amqp/decoder_policy.go | adapters/rabbitmq | amqp | source: public/private contracts |
| adapters/rabbitmq/internal/rabbitmqstream/amqp/encode.go | adapters/rabbitmq | amqp | source: public/private contracts |
| adapters/rabbitmq/internal/rabbitmqstream/amqp/encoder_bounds_test.go | adapters/rabbitmq | amqp | assurance |
| adapters/rabbitmq/internal/rabbitmqstream/amqp/error_stdlib.go | adapters/rabbitmq | amqp | source: public/private contracts; variant !pkgerrors |
| adapters/rabbitmq/internal/rabbitmqstream/amqp/fuzz_test.go | adapters/rabbitmq | amqp | assurance; FuzzBoundedMessageDecoder |
| adapters/rabbitmq/internal/rabbitmqstream/amqp/map_keys_test.go | adapters/rabbitmq | amqp | assurance |
| adapters/rabbitmq/internal/rabbitmqstream/amqp/numeric_admission_test.go | adapters/rabbitmq | amqp | assurance |
| adapters/rabbitmq/internal/rabbitmqstream/amqp/types.go | adapters/rabbitmq | amqp | source: public/private contracts; retained/investigate TODO |
| adapters/rabbitmq/internal/rabbitmqstream/logs/default_test.go | adapters/rabbitmq | logs | assurance |
| adapters/rabbitmq/internal/rabbitmqstream/logs/log.go | adapters/rabbitmq | logs | source: public/private contracts |
| adapters/rabbitmq/internal/rabbitmqstream/message/interface.go | adapters/rabbitmq | message | source: public/private contracts |
| adapters/rabbitmq/internal/rabbitmqstream/stream/aggregation.go | adapters/rabbitmq | stream | source: public/private contracts |
| adapters/rabbitmq/internal/rabbitmqstream/stream/annotation_keys_test.go | adapters/rabbitmq | stream | assurance |
| adapters/rabbitmq/internal/rabbitmqstream/stream/available_features.go | adapters/rabbitmq | stream | source: public/private contracts |
| adapters/rabbitmq/internal/rabbitmqstream/stream/blocking_queue.go | adapters/rabbitmq | stream | source: public/private contracts |
| adapters/rabbitmq/internal/rabbitmqstream/stream/brokers.go | adapters/rabbitmq | stream | source: public/private contracts |
| adapters/rabbitmq/internal/rabbitmqstream/stream/buffer_reader.go | adapters/rabbitmq | stream | source: public/private contracts |
| adapters/rabbitmq/internal/rabbitmqstream/stream/buffer_writer.go | adapters/rabbitmq | stream | source: public/private contracts |
| adapters/rabbitmq/internal/rabbitmqstream/stream/chunk_decoder.go | adapters/rabbitmq | stream | source: public/private contracts |
| adapters/rabbitmq/internal/rabbitmqstream/stream/client.go | adapters/rabbitmq | stream | source: public/private contracts |
| adapters/rabbitmq/internal/rabbitmqstream/stream/constants.go | adapters/rabbitmq | stream | source: public/private contracts |
| adapters/rabbitmq/internal/rabbitmqstream/stream/consumer.go | adapters/rabbitmq | stream | source: public/private contracts |
| adapters/rabbitmq/internal/rabbitmqstream/stream/converters.go | adapters/rabbitmq | stream | source: public/private contracts |
| adapters/rabbitmq/internal/rabbitmqstream/stream/coordinator.go | adapters/rabbitmq | stream | source: public/private contracts |
| adapters/rabbitmq/internal/rabbitmqstream/stream/decoder_limits_test.go | adapters/rabbitmq | stream | assurance |
| adapters/rabbitmq/internal/rabbitmqstream/stream/decoder_policy.go | adapters/rabbitmq | stream | source: public/private contracts |
| adapters/rabbitmq/internal/rabbitmqstream/stream/environment.go | adapters/rabbitmq | stream | source: public/private contracts |
| adapters/rabbitmq/internal/rabbitmqstream/stream/environment_debug.go | adapters/rabbitmq | stream | source: public/private contracts; variant debug |
| adapters/rabbitmq/internal/rabbitmqstream/stream/exchange_commands.go | adapters/rabbitmq | stream | source: public/private contracts |
| adapters/rabbitmq/internal/rabbitmqstream/stream/fuzz_test.go | adapters/rabbitmq | stream | assurance; FuzzBoundedDeliveryDecoder |
| adapters/rabbitmq/internal/rabbitmqstream/stream/iclient.go | adapters/rabbitmq | stream | source: public/private contracts |
| adapters/rabbitmq/internal/rabbitmqstream/stream/lifecycle.go | adapters/rabbitmq | stream | source: public/private contracts |
| adapters/rabbitmq/internal/rabbitmqstream/stream/lifecycle_test.go | adapters/rabbitmq | stream | assurance |
| adapters/rabbitmq/internal/rabbitmqstream/stream/listeners.go | adapters/rabbitmq | stream | source: public/private contracts |
| adapters/rabbitmq/internal/rabbitmqstream/stream/metrics.go | adapters/rabbitmq | stream | source: public/private contracts; retained/investigate TODO |
| adapters/rabbitmq/internal/rabbitmqstream/stream/operation_context.go | adapters/rabbitmq | stream | source: public/private contracts |
| adapters/rabbitmq/internal/rabbitmqstream/stream/operation_context_test.go | adapters/rabbitmq | stream | assurance |
| adapters/rabbitmq/internal/rabbitmqstream/stream/pool.go | adapters/rabbitmq | stream | source: public/private contracts |
| adapters/rabbitmq/internal/rabbitmqstream/stream/producer.go | adapters/rabbitmq | stream | source: public/private contracts |
| adapters/rabbitmq/internal/rabbitmqstream/stream/producer_unconfirmed.go | adapters/rabbitmq | stream | source: public/private contracts |
| adapters/rabbitmq/internal/rabbitmqstream/stream/protocol_numeric_test.go | adapters/rabbitmq | stream | assurance |
| adapters/rabbitmq/internal/rabbitmqstream/stream/protocol_string_test.go | adapters/rabbitmq | stream | assurance |
| adapters/rabbitmq/internal/rabbitmqstream/stream/server_frame.go | adapters/rabbitmq | stream | source: public/private contracts; retained/investigate TODO |
| adapters/rabbitmq/internal/rabbitmqstream/stream/socket.go | adapters/rabbitmq | stream | source: public/private contracts |
| adapters/rabbitmq/internal/rabbitmqstream/stream/socket_context_test.go | adapters/rabbitmq | stream | assurance |
| adapters/rabbitmq/internal/rabbitmqstream/stream/stream_options.go | adapters/rabbitmq | stream | source: public/private contracts |
| adapters/rabbitmq/internal/rabbitmqstream/stream/stream_stats.go | adapters/rabbitmq | stream | source: public/private contracts |
| adapters/rabbitmq/internal/rabbitmqstream/stream/super_stream.go | adapters/rabbitmq | stream | source: public/private contracts |
| adapters/rabbitmq/internal/rabbitmqstream/stream/super_stream_consumer.go | adapters/rabbitmq | stream | source: public/private contracts |
| adapters/rabbitmq/internal/rabbitmqstream/stream/super_stream_producer.go | adapters/rabbitmq | stream | source: public/private contracts |
| adapters/rabbitmq/internal/rabbitmqstream/stream/unconfirmed_admission_test.go | adapters/rabbitmq | stream | assurance |
| adapters/rabbitmq/internal/rabbitmqstream/stream/utils.go | adapters/rabbitmq | stream | source: public/private contracts |
| adapters/rabbitmq/metrics_isolation_test.go | adapters/rabbitmq | rabbitmq | assurance |
| adapters/rabbitmq/native_publication_test.go | adapters/rabbitmq | rabbitmq | assurance |
| adapters/rabbitmq/notification_lifecycle_test.go | adapters/rabbitmq | rabbitmq | assurance |
| adapters/rabbitmq/observation.go | adapters/rabbitmq | rabbitmq | source: public/private contracts |
| adapters/rabbitmq/partial_open_cleanup_test.go | adapters/rabbitmq | rabbitmq | assurance |
| adapters/rabbitmq/producer_retirement_contract_test.go | adapters/rabbitmq | rabbitmq | assurance |
| adapters/rabbitmq/reconnect_lifecycle_test.go | adapters/rabbitmq | rabbitmq | assurance |
| adapters/rabbitmq/replay.go | adapters/rabbitmq | rabbitmq | source: public/private contracts |
| adapters/rabbitmq/retry_replay_contract_test.go | adapters/rabbitmq | rabbitmq | assurance |
| adapters/rabbitmq/wire_admission_test.go | adapters/rabbitmq | rabbitmq | assurance |
| adapters/rabbitmq/wire_semantic_admission_test.go | adapters/rabbitmq | rabbitmq | assurance |
| benchmark_test.go | . | rabbitstream | assurance; BenchmarkMessagePolicy, BenchmarkProducerPolicy, BenchmarkConsumerPolicy, BenchmarkReplayPolicy |
| boundary_test.go | . | rabbitstream | assurance |
| config.go | . | rabbitstream | source: public/private contracts |
| config_test.go | . | rabbitstream | assurance |
| consumer.go | . | rabbitstream | source: public/private contracts |
| consumer_boundary_test.go | . | rabbitstream | assurance |
| consumer_byte_admission_test.go | . | rabbitstream | assurance |
| consumer_test.go | . | rabbitstream | assurance |
| delivery_admission_test.go | . | rabbitstream | assurance |
| doc.go | . | rabbitstream | source: public/private contracts |
| error_test.go | . | rabbitstream | assurance |
| errors.go | . | rabbitstream | source: public/private contracts |
| example_test.go | . | rabbitstream_test | assurance; ExampleNewProducer, ExampleNewConsumer |
| fuzz_test.go | . | rabbitstream | assurance; FuzzMessageValidationAndOwnership, FuzzConnectionConfigNormalization, FuzzReplayRequestValidation |
| inspection.go | . | rabbitstream | source: public/private contracts |
| inspection_test.go | . | rabbitstream | assurance |
| message.go | . | rabbitstream | source: public/private contracts |
| message_boundaries_test.go | . | rabbitstream | assurance |
| message_test.go | . | rabbitstream | assurance |
| mutation_guard_test.go | . | rabbitstream | assurance |
| observation.go | . | rabbitstream | source: public/private contracts |
| observation_test.go | . | rabbitstream | assurance |
| otel/example_test.go | otel | rabbitstreamotel_test | assurance; Example |
| otel/facade_test.go | otel | rabbitstreamotel_test | assurance |
| otel/otel.go | otel | rabbitstreamotel | source: public/private contracts |
| producer.go | . | rabbitstream | source: public/private contracts |
| producer_boundary_test.go | . | rabbitstream | assurance |
| producer_test.go | . | rabbitstream | assurance |
| rabbitmq/cohort_contract_test.go | rabbitmq | rabbitmq_test | assurance |
| rabbitmq/example_test.go | rabbitmq | rabbitmq_test | assurance; ExampleOpenProducer, ExampleOpenConsumer |
| rabbitmq/facade.go | rabbitmq | rabbitmq | source: public/private contracts |
| rabbitmq/facade_test.go | rabbitmq | rabbitmq_test | assurance |
| replay.go | . | rabbitstream | source: public/private contracts |
| replay_admission_test.go | . | rabbitstream | assurance |
| replay_test.go | . | rabbitstream | assurance |
| shutdown_ownership_test.go | . | rabbitstream | assurance |
| test_wait_test.go | . | rabbitstream | assurance |

## Investigation and verification disposition

Prewrite inventory complete: 131 tracked Go files, 58 production/support files,
73 assurance files, five modules and nine catalog packages. Source investigation
and meaningful gap resolution are complete. Selected native documentation and
local Example checks preserve the assurance boundaries below; no new broker, HA,
independent interoperability or release qualification is claimed.

## Baseline gaps and investigated contracts

Baseline missing/malformed declaration leads were reproduced from the base
with tracked-file declaration/comment inspection. Existing useful contracts were
retained rather than expanded solely for a name check. Native AST inspection
covers all exported fields/interface methods as well as this compact declaration
inventory; public method names on private error/codec receivers were considered
for observable diagnostics and internal call contracts.

| Base path | Missing or detached declaration leads investigated |
| --- | --- |
| adapters/rabbitmq/internal/rabbitmqstream/amqp/decoder_policy.go | DefaultDecoderLimits, Validate |
| adapters/rabbitmq/internal/rabbitmqstream/amqp/types.go | ProtoID, String, Error, NewMessage, SetPublishingId, HasPublishingId, GetPublishingId, MarshalBinary, UnmarshalBinary, GetData, GetMessageProperties, GetMessageAnnotations, GetApplicationProperties, GetMessageHeader, GetAMQPValue |
| adapters/rabbitmq/internal/rabbitmqstream/logs/log.go | LogInfo, LogError, LogDebug, LogWarn |
| adapters/rabbitmq/internal/rabbitmqstream/stream/aggregation.go | Compression, String, None, Gzip, Snappy, Zstd, Lz4, Compress, UnCompress |
| adapters/rabbitmq/internal/rabbitmqstream/stream/available_features.go | Is311OrMore, Is313OrMore, BrokerFilterEnabled, IsBrokerSingleActiveConsumerEnabled, SetVersion, GetCommands, ParseCommandVersions, String, IsVersionGreaterOrEqual, Version, Compare |
| adapters/rabbitmq/internal/rabbitmqstream/stream/blocking_queue.go | ErrBlockingQueueStopped, BlockingQueue, GetChannel, Size, IsEmpty, Close, IsStopped |
| adapters/rabbitmq/internal/rabbitmqstream/stream/brokers.go | AddressResolver, TCPParameters, Broker, GetUri, Brokers, Add, Get, StreamMetadata, String, New, StreamsMetadata |
| adapters/rabbitmq/internal/rabbitmqstream/stream/buffer_writer.go | ErrProtocolStringTooLong, ErrProtocolNumberRange |
| adapters/rabbitmq/internal/rabbitmqstream/stream/client.go | SaslConfiguration, TuneState, ClientProperties, ConnectionProperties, HeartBeat, Client, DeleteStream, DeclarePublisher, DeclarePublisherContext, BrokerLeader, BrokerLeaderContext, BrokerLeaderWithResolver, BrokerLeaderWithResolverContext, StreamExists, StreamExistsContext, BrokerForConsumer, BrokerForConsumerContext, DeclareStream, StoreOffset, StoreOffsetContext, DeclareSubscriber, DeclareSubscriberContext, StreamStats, StreamStatsContext, DeclareSuperStream, DeleteSuperStream, QueryPartitions, QueryPartitionsContext |
| adapters/rabbitmq/internal/rabbitmqstream/stream/constants.go | AlreadyClosed, PreconditionFailed, AuthenticationFailure, StreamDoesNotExist, StreamMetadataFailure, StreamAlreadyExists, VirtualHostAccessFailure, SubscriptionIdDoesNotExist, PublisherDoesNotExist, OffsetNotFoundError, FrameTooLarge, CodeAccessRefused, ConnectionClosed, StreamNotAvailable, UnknownFrame, InternalError, AuthenticationFailureLoopbackError, ConfirmationTimoutError, FilterNotSupported, SingleActiveConsumerNotSupported, ErrProducerNotFound, ErrMessageRouteNotFound, ErrSuperStreamProducerOptionsNotDefined, ErrSuperStreamConsumerOptionsNotDefined, ErrEnvironmentNotDefined, LeaderNotReady |
| adapters/rabbitmq/internal/rabbitmqstream/stream/consumer.go | Consumer, GetStreamName, GetName, GetOffset, GetID, GetLastStoredOffset, GetCloseHandler, NotifyClose, Credit, ConsumerContext, GetEntriesCount, AutoCommitStrategy, SetCountBeforeStorage, SetFlushInterval, NewAutoCommitStrategy, PostFilter, ConsumerFilter, NewConsumerFilter, ConsumerUpdate, SingleActiveConsumer, NewSingleActiveConsumer, SetEnabled, CreditStrategy, SetConsumerName, SetCRCCheck, SetInitialCredits, SetAutoCommit, SetManualCommit, SetOffset, SetClientProvidedName, GetClientProvidedName, SetFilter, SetSingleActiveConsumer, IsSingleActiveConsumerEnabled, IsFilterEnabled, Close, OffsetSpecification, First, Last, Next, Offset, Timestamp, LastConsumed, String |
| adapters/rabbitmq/internal/rabbitmqstream/stream/converters.go | ByteCapacity, B, KB, MB, GB, TB, From |
| adapters/rabbitmq/internal/rabbitmqstream/stream/coordinator.go | Coordinator, Code, Response, NewCoordinator, NewProducer, RemoveConsumerById, Consumers, RemoveProducerById, RemoveResponseById, ProducersCount, NewResponseWithName, NewResponse, GetResponseByName, RemoveResponseByName, NewConsumer, GetConsumerById, ExtractConsumerById, GetResponseById, ConsumersCount, GetProducerById, ExtractProducerById, Producers, Close |
| adapters/rabbitmq/internal/rabbitmqstream/stream/decoder_policy.go | ErrDecoderLimit, DefaultDecoderLimits, Validate |
| adapters/rabbitmq/internal/rabbitmqstream/stream/environment.go | Environment, NewEnvironment, DeclareStream, DeleteStream, NewProducer, StreamExists, QueryOffset, StreamStats, StreamMetaData, NewConsumer, NewSuperStreamProducer, IsClosed, EnvironmentOptions, NewEnvironmentOptions, SetAddressResolver, SetMaxProducersPerClient, SetMaxConsumersPerClient, SetMeterProvider, SetUri, SetUris, SetHost, SetVHost, SetSaslConfiguration, SetTLSConfig, IsTLS, SetPort, SetUser, SetPassword, SetRequestedHeartbeat, SetRequestedMaxFrameSize, SetWriteBuffer, SetReadBuffer, SetNoDelay, SetRPCTimeout, Close, NewSubscriber, NewSubscriberContext, DeclareSuperStream, DeleteSuperStream, QueryPartitions, QueryRoute, NewSuperStreamConsumer |
| adapters/rabbitmq/internal/rabbitmqstream/stream/exchange_commands.go | GetMinVersion, GetMaxVersion, GetCommandKey, PublishFilter |
| adapters/rabbitmq/internal/rabbitmqstream/stream/lifecycle.go | WaitContext |
| adapters/rabbitmq/internal/rabbitmqstream/stream/listeners.go | Event, ChannelClose, ChannelPublishConfirm |
| adapters/rabbitmq/internal/rabbitmqstream/stream/producer.go | ConfirmationStatus, IsConfirmed, GetProducerID, GetPublishingId, GetError, LinkedMessages, GetMessage, GetErrorCode, Producer, FilterValue, ProducerFilter, NewProducerFilter, ProducerOptions, SetBatchPublishingDelay, GetClientProvidedName, NewProducerOptions, GetOptions, GetBroker, GetID, GetStreamName, GetName |
| adapters/rabbitmq/internal/rabbitmqstream/stream/producer_unconfirmed.go | ErrUnconfirmedCapacity, ErrPendingPublishingID |
| adapters/rabbitmq/internal/rabbitmqstream/stream/server_frame.go | ReaderProtocol |
| adapters/rabbitmq/internal/rabbitmqstream/stream/socket.go | Error, Unwrap, Write |
| adapters/rabbitmq/internal/rabbitmqstream/stream/stream_options.go | StreamOptions, SetMaxAge, SetMaxLengthBytes, SetMaxSegmentSizeBytes, NewStreamOptions |
| adapters/rabbitmq/internal/rabbitmqstream/stream/stream_stats.go | StreamStats, LastOffset |
| adapters/rabbitmq/internal/rabbitmqstream/stream/super_stream.go | SuperStreamOptions, PartitionsOptions, NewPartitionsOptions, SetMaxAge, SetMaxLengthBytes, SetMaxSegmentSizeBytes, SetBalancedLeaderLocator, SetClientLocalLocator, BindingsOptions, NewBindingsOptions |
| adapters/rabbitmq/internal/rabbitmqstream/stream/super_stream_consumer.go | SuperStreamConsumerOptions, NewSuperStreamConsumerOptions, SetClientProvidedName, SetOffset, SetSingleActiveConsumer, SetFilter, SetConsumerName, SetAutoCommit, SetManualCommit, SuperStreamConsumer, ConnectPartition, Close |
| adapters/rabbitmq/internal/rabbitmqstream/stream/super_stream_producer.go | RoutingStrategy, SEED, HashRoutingStrategy, NewHashRoutingStrategy, Route, SetRouteParameters, NewKeyRoutingStrategy, SuperStreamProducerOptions, SetClientProvidedName, SetFilter, SuperStreamProducer, GetPartitions, Close |
| otel/otel.go | Meter |

Public field/interface gaps additionally included native Broker/TCP/Environment
options, consumer filter/promotion/credit state, producer aggregation/confirmation
policy, fan-out notification/reconnect seams, AMQP message/header/property/error
containers, raw version fields and native command/settlement/error values.
Normative AMQP field semantics were retained after concrete name-led ownership,
projection or trust boundaries. Defined string enums do not validate arbitrary
casts; raw native diagnostics and credential-bearing Broker.GetUri differ from
root category/cause redaction.

| Owner/source slice | Actual contracts, rationale and source/test anchors |
| --- | --- |
| Root config/message | Endpoint bounds are not DNS/URI/network allowlisting. Endpoints/retained payload and metadata are copied; tls.Config.Clone is shallow and providers/nested TLS state remain borrowed. Static password copying is not secure erasure. config_test.go, message_test.go, config.go and message.go own admission/ownership. |
| Producer | Whole batch validation precedes I/O; sequential batches are not atomic. Async slots/retention are bounded; context-free supplier cleanup can outlive caller wait. Confirmation timeout starts after Send returns nil. Attempted write errors can be ambiguous; late confirmation does not add external-effect atomicity. producer_boundary_test.go, shutdown_ownership_test.go and adapter native_publication_test.go preserve selected expectations. |
| Consumer | Partition workers retain owned copies but handlers borrow them across retries/failure mapping. Handler deadlines do not preempt inline code; nil success after an ignored deadline is accepted. Offset-count/batch policy selects command cadence, not a broker durable receipt or strict crash-redelivery bound. Reroute publish and source storage are separate effects. consumer_boundary_test.go, consumer_byte_admission_test.go and retry_replay_contract_test.go trace admission/range/ownership. |
| Root replay/inspection | Replay source is borrowed, cursors owned, end inclusive, checkpoints caller-authoritative, and AllowSideEffects is passed to the handler rather than enforced as a sandbox. Root admission is structural, not a universal custom-source every-offset oracle. Retention/topology can move between observations; inspection is not an atomic broker-wide snapshot. replay_admission_test.go and inspection_test.go retain desired validation. |
| Canonical RabbitMQ acquisition/reconnect | Context-bound resource acquisition cleans partial/late results; returned lifetime is independent of opening context. Credentials/callbacks are borrowed, synchronous and must return. Session Next retires/reopens after any uncanceled non-Closed read error; opening classification and finite attempts are separate. canceled_environment_admission_test.go, partial_open_cleanup_test.go and reconnect_lifecycle_test.go own selected cleanup cases. |
| Publication/offset native boundary | Pending wire identity is registered before send; exactly one terminal owner wins confirmation/abort races. Admission/capacity/duplicate-ID refusal preserves prior ownership. Attempted socket write is uncertain even on error; one-way StoreOffset succeeds on local write, not broker persistence. producer_unconfirmed.go, socket.go, native_publication_test.go and unconfirmed_admission_test.go own these boundaries. |
| AMQP wire mapping | Outbound fields use one data section and copied selected metadata; inbound accepts zero/one Data section and selected string/binary fields, sorting metadata keys. Value/header/delivery annotations/footer are not independently rejected/projected. Structural scanner recognizes widths that semantic decimal decoding does not implement. wire_semantic_admission_test.go, numeric_admission_test.go and RABBITMQ-STREAM-DEC-001 retain desired expectations without claiming missing cases passed. |
| Fork numeric/decoder boundary | Frame length is admitted before allocation; count/length narrowing and map-key comparability are checked. Chunk records/entries, encoded message bytes, decompressed output and actual structural values have independent finite budgets. Legacy read helpers are safe only after schema preflight; codec workspace is additional to returned bytes and not process-wide memory. decoder_admission_test.go, encoder_bounds_test.go, protocol_numeric_test.go and numeric_admission_test.go remain unchanged. |
| Native task and mailbox ownership | Close/Abort requests stop; WaitContext outside callbacks joins registered reachable tasks. Cancellation callbacks finish before deadline ownership passes. Lookup retirement leaves reader-owned mailboxes intact; correlation wrap does not replace live ownership. Queue Stop fences senders/drains, Close owns channel destruction. lifecycle_test.go, operation_context_test.go, socket_context_test.go and notification_lifecycle_test.go define selected cases. |
| Native raw helpers versus canonical route | Raw options can borrow process-global metrics and credential-bearing URI state. Canonical opening supplies Noop meter and root verified TLS; log functions discard without formatting. Private fan-out helpers use delayed close goroutines and an unbounded key-route cache outside canonical manual integration ownership. Provisioning retention, version arithmetic and decimal capacity utilities are not hostile-input/root policy APIs. |
| Telemetry and legacy cohorts | Canonical OTel and deprecated OTel consume published root v1, with closed labels/number clamps, owned inject and borrowed nonmutating extract. Remote trace context is not authentication. SDK calls may block or have partial effects after panic containment; export/sampling/provider shutdown stay caller-owned. Legacy RabbitMQ delegates its pinned published v2 successor, not sibling source. observer_internal_test.go, propagation_test.go and facade_test.go retain their different cohorts. |

## Specification, provenance, variants and assurance

The canonical [private-source record](../adapters/rabbitmq/THIRD_PARTY_RABBITMQ_STREAM.md)
pins Go client v1.8.3/e56918bc2858425622a7bd8f569d46daecba289a and its verified
original archive/file identities. Original-file hashes are historical upstream
inputs, not checksums of this locally patched fork. MIT license and copyright
blocks remain intact; no Go file is generated and there is no go:generate recipe.

The [decision register](../adapters/rabbitmq/specification/decisions.json) and
[maintained decision prose](../adapters/rabbitmq/docs/specification-decisions.md)
contain six stable RABBITMQ-STREAM-DEC records. Their current selected expectations,
typed histories, provider evidence and frozen authoritative sources are untouched.
Authorities include RabbitMQ 4.3.5 protocol source/server, pinned client v1.8.3,
RabbitMQ 4.3 documentation and the pinned Osiris chunk interpretation; the native
client's semconv/v1.39.0 import differs intentionally from consumed OTel 1.47.0
APIs. Neither seed/hash equality nor version flags certify cross-client/broker
interoperability. Publish-v2 negotiation is not incoming Deliver-v2 support.

Variants: four integration-tagged adapter files own broker tests/resource and
performance assurance; none is selected for this prose pass. The retained debug
file has no compiled API/runtime declarations; stale commented-out introspection
was removed without changing its directive. The attributed !pkgerrors stdlib
error helper has no tracked pkgerrors counterpart; that unselected tag is not a
qualified alternative. No platform-specific Go path or assembly/cgo source was
found. All directives, deprecations, scanner dispositions and authenticated
baseline/source bytes remain protected.

The backlog benchmark timer stops on final handler-invocation notification
before the last handler return/offset store and shutdown; its prior durable-store
latency description was corrected without changing its measured workload.

Ten fuzz targets cover root message/config/replay, public wire/hash/confirmation,
OTel propagation/observation and private AMQP/chunk admission. Twelve benchmarks
cover root policy (4), local wire/hash (2), no-op OTel (2) and tagged broker
publish/window/backlog/idle-resource (4). Target intent/fixture/non-goal comments
are source-only; no fuzz campaign or performance measurement is executed. No
tracked saved fuzz-crash corpus was found. Executable test/helper bodies, seeds, fixtures and normative expected outcomes
are unchanged; only target-intent comments were added.

Root two Output Examples use owned local fakes; root TestMain additionally runs
local default-capacity, canceled worker and pre-Run Close guards. Each telemetry
module has one Output-bearing no-op-provider Example using its published root-v1
cohort. Canonical and legacy RabbitMQ each have two broker-dependent no-Output
Examples: compilation is useful but their bodies are not executed by the selector.
No extra manual broker/example probe or duplicate runtime campaign is intended.

## Concrete unresolved implementation/qualification questions

These are investigated source limitations, not missing prose and not authorization
to repair behavior or silently revise selected policy:

- Adapter maintainers own reconciliation of desired DEC-001 unsupported-body
  rejection with actual fromWireMessage Value/header/footer omission and zero-data
  acceptance. Existing selected tests do not establish every other section case.
- Replay maintainers own qualification of exact last-offset discovery for
  compressed multi-record sub-entry chunks: snapshotLastOffset uses the callback
  GetEntriesCount (wire entry count), while chunk decoding separately counts
  expanded records. No new broker defect/conformance result is asserted here.
- Native broker-statistics helpers treat only -1 as absent; missing map keys read
  zero. Current direct-broker protocol fixtures, not arbitrary custom statistics,
  delimit the existing support evidence.
- Root backoff doubling and raw ByteCapacity/Version helpers retain unchecked
  extreme-input arithmetic; From also slices short suffix input. Raw helpers are
  outside the canonical bounded root route. A behavior/security owner must select
  any future regression/repair rather than changing them during documentation.
- Arbitrary credential/SDK/handler callbacks cannot be preempted. Private raw
  SuperStream convenience goroutines/key caches are outside the canonical adapter
  registered-owner slice; no whole-private-SDK or whole-process guarantee follows.
- Integration/debug/pkgerrors variants, live broker/HA/retention interoperability,
  load/resource budgets and clean public consumers are not newly qualified by
  source documentation, local Examples or parsed/rendered comments.

## Final evidence disposition

Go 1.27.2 native source proof covers all 131 tracked files, executable ASTs,
tokens/literals, directives and parsed comment HTML, plus 1,015 name-led exported
declarations/fields/methods and nine canonical package headers. The only raw
token delta is a grammar-optional implicit semicolon immediately before the
closing brace of socket.go's formerly one-line WriteAdmittedError struct; all
other tokens/literals and ASTs remain exact. This does not waive token changes.

All nine native go doc -all renders were inspected. The root/private stream
renders are refreshed after final affected prose; seven unchanged renders retain
their original execution identity. All five binary apidiff archives remain byte-
frozen: no go-doc snapshot generation route exists or is invented. Six native
typed decision digests and offline specification validation passed unchanged.

Exactly five module-root go test -run ^Example -count=1 -timeout=3m operations
passed. Four Output-bearing examples executed (root two, each telemetry module
one); root TestMain's local guards also ran. Four canonical/legacy RabbitMQ
no-Output bodies compiled but did not execute. This unchanged behavior evidence
is reused after final comments; no runtime suite or tag qualification is claimed.

The current structural source-docs checker and authenticated declared Tools 2.3
local-docs/offline-spec checks passed. Missing ReaderProtocol field leads and
six deprecation paragraph boundaries were repaired; all original directive,
deprecation text, source/authentication pins and desired histories are retained.
Cheap owned/added/staged default secret checks, links, complete ordinary final-
diff truth/usefulness inspection and explicit-path Intent commit finish delivery.
Task-owned native resources are removed on success and failure; parent owns
remote main, exact-source hosted CI and any publication, not this source commit.

## Objective Enforcement

The checksum-pinned public Tools v2.4.0 verifier now supplies
`make source-docs-check` as a prerequisite of the existing local and CI Make
routes. Ordinary shared CI selects strict source-documentation checking and
requires its result. Native prerequisites, operation budgets, security and
release controls are retained. Hosted exact-head evidence is recorded by the
coordination ledger; configuration alone does not establish green delivery.
