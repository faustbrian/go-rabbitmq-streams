package stream

import (
	"fmt"
	"time"

	"github.com/faustbrian/go-rabbitmq-streams/adapters/rabbitmq/v2/internal/rabbitmqstream/logs"
	"github.com/pkg/errors"
)

// is needed to indicate the general status
// for example the producer status

const (
	open   = iota
	closed = iota
)

const initBufferPublishSize = 2 + 2 + 1 + 4

const (
	// ClientVersion identifies the attributed upstream client baseline, not a fork release or qualification.
	ClientVersion = "1.8.3"

	commandDeclarePublisher       = 1
	commandPublish                = 2
	commandPublishConfirm         = 3
	commandPublishError           = 4
	commandQueryPublisherSequence = 5
	// CommandDeletePublisher is the native publisher-deletion command identifier.
	CommandDeletePublisher = 6
	commandSubscribe       = 7
	commandDeliver         = 8
	commandCredit          = 9
	commandStoreOffset     = 10
	// CommandQueryOffset is the native named-offset query command identifier.
	CommandQueryOffset = 11
	// CommandUnsubscribe is the native unsubscribe command identifier.
	CommandUnsubscribe  = 12
	commandCreateStream = 13
	commandDeleteStream = 14
	commandMetadata     = 15
	// CommandMetadataUpdate is the broker topology-update command identifier.
	CommandMetadataUpdate   = 16
	commandPeerProperties   = 17
	commandSaslHandshake    = 18
	commandSaslAuthenticate = 19
	commandTune             = 20
	commandOpen             = 21
	// CommandClose is the native connection-close command identifier.
	CommandClose                = 22
	commandHeartbeat            = 23
	commandQueryRoute           = 24
	commandQueryPartition       = 25
	consumerUpdateQueryResponse = 26
	commandExchangeVersion      = 27
	commandStreamStatus         = 28
	commandCreateSuperStream    = 29
	commandDeleteSuperStream    = 30
	commandConsumerUpdate       = 0x801a

	/// used only for tests
	commandUnitTest = 99

	version1    = 1
	version2    = 2
	unicodeNull = "\u0000"

	responseCodeOk                            = uint16(1)
	responseCodeStreamDoesNotExist            = uint16(2)
	responseCodeSubscriptionIdAlreadyExists   = uint16(3)
	responseCodeSubscriptionIdDoesNotExist    = uint16(4)
	responseCodeStreamAlreadyExists           = uint16(5)
	responseCodeStreamNotAvailable            = uint16(6)
	responseCodeSaslMechanismNotSupported     = uint16(7)
	responseCodeAuthenticationFailure         = uint16(8)
	responseCodeSaslError                     = uint16(9)
	responseCodeSaslChallenge                 = uint16(10)
	responseCodeAuthenticationFailureLoopback = uint16(11)
	responseCodeVirtualHostAccessFailure      = uint16(12)
	responseCodeUnknownFrame                  = uint16(13)
	responseCodeFrameTooLarge                 = uint16(14)
	responseCodeInternalError                 = uint16(15)
	responseCodeAccessRefused                 = uint16(16)
	responseCodePreconditionFailed            = uint16(17)
	responseCodePublisherDoesNotExist         = uint16(18)
	responseCodeNoOffset                      = uint16(19)

	/// responses out of protocol
	timeoutError = uint16(62)
	entityClosed = uint16(63)

	///
	defaultSocketCallTimeout = 10 * time.Second

	defaultHeartbeat    = 60 * time.Second
	defaultMaxFrameSize = 1048574

	// LocalhostUriConnection is the credential-bearing guest development seed; never use it as a log-safe value.
	//
	LocalhostUriConnection = "rabbitmq-stream://guest:guest@localhost:5552/%2f"

	///
	defaultWriteSocketBuffer  = 8192
	defaultReadSocketBuffer   = 8192
	defaultQueuePublisherSize = 10_000
	minQueuePublisherSize     = 500
	maxQueuePublisherSize     = 1_000_000

	minBatchSize = 1
	maxBatchSize = 10_000

	minSubEntrySize = 1
	maxSubEntrySize = 65535

	minBatchPublishingDelay = 1
	maxBatchPublishingDelay = 500

	defaultBatchSize            = 100
	defaultBatchPublishingDelay = 100
	defaultConfirmationTimeOut  = 10 * time.Second
	//

	// ZombieConsumer labels native liveness-based consumer retirement.
	ZombieConsumer = "zombie-consumer"
	// SocketClosed labels native socket retirement.
	SocketClosed = "socket client closed"
	// MetaDataUpdate labels native metadata-driven retirement.
	MetaDataUpdate = "metadata Data update"
	// LeaderLocatorBalanced selects broker balanced leader placement.
	LeaderLocatorBalanced = "balanced"
	// LeaderLocatorClientLocal selects broker client-local leader placement.
	LeaderLocatorClientLocal = "client-local"
	// DeletePublisher labels explicit native publisher deletion.
	DeletePublisher = "deletePublisher"
	// UnSubscribe labels explicit native subscription close.
	UnSubscribe = "unSubscribe"

	// StreamTcpPort is the raw default plaintext Streams listener port; TLS/security policy is separate.
	StreamTcpPort = "5552"

	subscriptionPropertyFilterPrefix    = "filter."
	subscriptionPropertyMatchUnfiltered = "match-unfiltered"
)

// AlreadyClosed reports local entity closure.
var AlreadyClosed = errors.New("Already Closed")

// PreconditionFailed maps broker precondition refusal.
var PreconditionFailed = errors.New("Precondition Failed")

// AuthenticationFailure maps broker SASL authentication failure.
var AuthenticationFailure = errors.New("Authentication Failure")

// StreamDoesNotExist maps broker stream absence.
var StreamDoesNotExist = errors.New("Stream Does Not Exist")

// StreamMetadataFailure reports missing/invalid metadata for a requested stream.
var StreamMetadataFailure = errors.New("Stream Metadata Failure")

// StreamAlreadyExists maps broker duplicate stream declaration.
var StreamAlreadyExists = errors.New("Stream Already Exists")

// VirtualHostAccessFailure maps broker virtual-host access failure.
var VirtualHostAccessFailure = errors.New("Virtual Host Access Failure")

// SubscriptionIdDoesNotExist maps broker subscription identifier absence.
var SubscriptionIdDoesNotExist = errors.New("Subscription Id Does Not Exist")

// PublisherDoesNotExist maps broker publisher identifier absence.
var PublisherDoesNotExist = errors.New("Publisher Does Not Exist")

// OffsetNotFoundError maps absence of a stored named offset, not offset zero.
var OffsetNotFoundError = errors.New("Offset not found")

// FrameTooLarge reports frame-size refusal, including local publication admission.
var FrameTooLarge = errors.New("Frame Too Large, the buffer is too big")

// CodeAccessRefused maps broker resource access refusal.
var CodeAccessRefused = errors.New("Resources Access Refused")

// ConnectionClosed reports a stopped connection; by itself it does not prove non-delivery.
var ConnectionClosed = errors.New("Can't Send the message, connection closed")

// StreamNotAvailable maps temporary stream unavailability.
var StreamNotAvailable = errors.New("Stream Not Available")

// UnknownFrame maps broker unknown-frame refusal.
var UnknownFrame = errors.New("Unknown Frame")

// InternalError maps broker internal error; details remain outside this sentinel.
var InternalError = errors.New("Internal Error")

// AuthenticationFailureLoopbackError maps broker loopback-user authentication refusal.
var AuthenticationFailureLoopbackError = errors.New("Authentication Failure Loopback Error")

// ConfirmationTimoutError reports native confirmation expiry, not proof of non-delivery.
var ConfirmationTimoutError = errors.New("Confirmation Timeout Error")

// FilterNotSupported reports failed native Publish-v2 filtering capability admission.
var FilterNotSupported = errors.New("Filtering is not supported by the broker " +
	"(requires RabbitMQ 3.13+ and stream_filtering feature flag activated)")

// SingleActiveConsumerNotSupported reports failed native single-active-consumer version admission.
var SingleActiveConsumerNotSupported = errors.New("Single Active Consumer is not supported by the broker " +
	"(requires RabbitMQ 3.11+ and stream_single_active_consumer feature flag activated)")

// ErrProducerNotFound reports no active raw fan-out publisher for a routed partition.
var ErrProducerNotFound = errors.New("Producer not found in the SuperStream Producer")

// ErrMessageRouteNotFound reports that raw routing selected no usable partition.
var ErrMessageRouteNotFound = errors.New("Message Route not found for the message key")

// ErrSuperStreamProducerOptionsNotDefined rejects missing raw producer/routing options.
var ErrSuperStreamProducerOptionsNotDefined = errors.New("SuperStreamProducerOptions not defined. The SuperStreamProducerOptions is mandatory with the RoutingStrategy")

// ErrSuperStreamConsumerOptionsNotDefined rejects missing raw fan-out consumer options.
var ErrSuperStreamConsumerOptionsNotDefined = errors.New("SuperStreamConsumerOptions not defined.")

// ErrEnvironmentNotDefined rejects a missing raw fan-out environment.
var ErrEnvironmentNotDefined = errors.New("Environment not defined")

// LeaderNotReady reports metadata without a usable leader.
var LeaderNotReady = errors.New("Leader not Ready yet")

func lookErrorCode(errorCode uint16) error {
	switch errorCode {
	case responseCodeOk:
		return nil
	case responseCodeAuthenticationFailure:
		return AuthenticationFailure
	case responseCodeStreamDoesNotExist:
		return StreamDoesNotExist
	case responseCodeStreamAlreadyExists:
		return StreamAlreadyExists
	case responseCodeVirtualHostAccessFailure:
		return VirtualHostAccessFailure
	case responseCodeSubscriptionIdDoesNotExist:
		return SubscriptionIdDoesNotExist
	case responseCodePublisherDoesNotExist:
		return PublisherDoesNotExist
	case responseCodeNoOffset:
		return OffsetNotFoundError
	case responseCodePreconditionFailed:
		return PreconditionFailed
	case responseCodeFrameTooLarge:
		return FrameTooLarge
	case responseCodeAccessRefused:
		return CodeAccessRefused
	case responseCodeStreamNotAvailable:
		return StreamNotAvailable
	case responseCodeUnknownFrame:
		return UnknownFrame
	case responseCodeInternalError:
		return InternalError
	case responseCodeAuthenticationFailureLoopback:
		return AuthenticationFailureLoopbackError
	case timeoutError:
		return ConfirmationTimoutError
	case entityClosed:
		return AlreadyClosed

	default:
		{
			logs.LogWarn("Error not handled %d", errorCode)
			return errors.New("Generic Error")
		}
	}
}

func lookUpCommand(command uint16) string {
	var constLookup = map[uint16]string{
		commandPeerProperties:         `commandPeerProperties`,
		commandSaslHandshake:          `commandSaslHandshake`,
		commandSaslAuthenticate:       `commandSaslAuthenticate`,
		commandTune:                   `commandTune`,
		commandOpen:                   `commandOpen`,
		commandHeartbeat:              `commandHeartbeat`,
		CommandMetadataUpdate:         `CommandMetadataUpdate`,
		commandMetadata:               `CommandMetadata`,
		commandDeleteStream:           `CommandDeleteStream`,
		commandCreateStream:           `CommandCreateStream`,
		CommandUnsubscribe:            `CommandUnsubscribe`,
		CommandQueryOffset:            `CommandQueryOffset`,
		commandCredit:                 `CommandCredit`,
		commandDeliver:                `CommandDeliver`,
		commandSubscribe:              `CommandSubscribe`,
		CommandDeletePublisher:        `CommandDeletePublisher`,
		commandPublishError:           `CommandPublishError`,
		commandPublishConfirm:         `CommandPublishConfirm`,
		commandDeclarePublisher:       `CommandDeclarePublisher`,
		commandPublish:                `CommandPublish`,
		commandQueryPublisherSequence: `CommandQueryPublisherSequence`,
		commandQueryRoute:             `CommandQueryRoute`,
		commandQueryPartition:         `CommandQueryPartition`,
		commandExchangeVersion:        `CommandExchangeVersion`,
		commandStreamStatus:           `CommandStreamStatus`,
		commandCreateSuperStream:      `CommandCreateSuperStream`,
		commandDeleteSuperStream:      `CommandDeleteSuperStream`,
		commandConsumerUpdate:         `CommandConsumerUpdate`,

		commandUnitTest: `UnitTest`,
		CommandClose:    `CommandClose`,
	}
	if constLookup[command] == "" {
		return fmt.Sprintf("Command not handled %d", command)
	}

	return constLookup[command]
}

const (
	// SaslConfigurationPlain selects raw SASL PLAIN; confidentiality requires independently verified TLS.
	SaslConfigurationPlain = "PLAIN"
	// SaslConfigurationExternal selects raw SASL EXTERNAL; peer certificate authentication remains broker policy.
	SaslConfigurationExternal = "EXTERNAL"
)
