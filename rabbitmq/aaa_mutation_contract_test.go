package rabbitmq

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/faustbrian/golib/pkg/rabbitstream"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"
)

func TestMutationContractConsumerReturnsOnlyTerminalOutcomes(t *testing.T) {
	if !shouldReturnConsumerMessage(nil, nil) {
		t.Fatal("successful delivery was treated as reconnectable")
	}
	if !shouldReturnConsumerMessage(rabbitstream.ErrConnection, context.Canceled) {
		t.Fatal("canceled delivery was treated as reconnectable")
	}
	if !shouldReturnConsumerMessage(rabbitstream.ErrClosed, nil) {
		t.Fatal("closed delivery was treated as reconnectable")
	}
	if shouldReturnConsumerMessage(rabbitstream.ErrConnection, nil) {
		t.Fatal("connection failure was treated as terminal")
	}
}

func TestMutationContractReplayAcceptsOnlyValidBoundedOffsets(t *testing.T) {
	nonterminal := mutationReplayCursor(5)
	nonterminal.accept(4, &amqp.Message{Data: [][]byte{[]byte("event")}})
	message, err := mutationNext(t, nonterminal)
	if err != nil || message.Offset != 4 {
		t.Fatalf("nonterminal replay = %#v, %v", message, err)
	}
	assertChannelOpen(t, nonterminal.completed, "nonterminal replay completion")

	terminal := mutationReplayCursor(5)
	terminal.accept(5, &amqp.Message{Data: [][]byte{[]byte("last")}})
	message, err = mutationNext(t, terminal)
	if err != nil || message.Offset != 5 {
		t.Fatalf("terminal replay = %#v, %v", message, err)
	}
	assertChannelClosed(t, terminal.completed, "terminal replay completion")
	if _, err := mutationNext(t, terminal); !errors.Is(err, io.EOF) {
		t.Fatalf("completed replay error = %v", err)
	}

	negative := mutationReplayCursor(5)
	negative.accept(-1, &amqp.Message{})
	if _, err := mutationNext(t, negative); !errors.Is(err, rabbitstream.ErrReplayRange) {
		t.Fatalf("negative replay error = %v", err)
	}

	beyond := mutationReplayCursor(5)
	beyond.accept(6, &amqp.Message{})
	if _, err := mutationNext(t, beyond); !errors.Is(err, io.EOF) {
		t.Fatalf("beyond-range replay error = %v", err)
	}

	oversized := mutationReplayCursor(5)
	oversized.limits.MaxPayloadBytes = 1
	oversized.accept(4, &amqp.Message{Data: [][]byte{[]byte("too large")}})
	if _, err := mutationNext(t, oversized); !errors.Is(err, rabbitstream.ErrMessageTooLarge) {
		t.Fatalf("oversized replay error = %v", err)
	}
}

func TestMutationContractReplayTopologyRetainsEverySupportedPartition(t *testing.T) {
	empty := rabbitstream.ReplayRequest{SuperStream: "tracking"}
	if err := ensureReplayTopology(&fakeRabbitEnvironment{}, empty); !errors.Is(err, rabbitstream.ErrPartitionUnavailable) {
		t.Fatalf("empty topology error = %v", err)
	}

	partitions := make([]string, rabbitstream.MaxSuperStreamPartitions)
	for index := range partitions {
		partitions[index] = "tracking-" + string(rune('a'+index))
	}
	request := rabbitstream.ReplayRequest{SuperStream: "tracking", ExpectedPartitions: partitions}
	if err := ensureReplayTopology(&fakeRabbitEnvironment{partitions: partitions}, request); err != nil {
		t.Fatalf("maximum supported topology error = %v", err)
	}
}

func TestMutationContractReplayClosePreservesTheFirstOwnedFailure(t *testing.T) {
	consumerFailure := errors.New("consumer close")
	environmentFailure := errors.New("environment close")
	consumer := newFakeRabbitConsumer()
	consumer.closeErr = consumerFailure
	cursor := mutationReplayCursor(0)
	cursor.consumer = consumer
	cursor.environment = &fakeRabbitEnvironment{closeErr: environmentFailure}
	if err := cursor.Close(); !errors.Is(err, consumerFailure) {
		t.Fatalf("combined close error = %v", err)
	}

	alreadyClosed := newFakeRabbitConsumer()
	alreadyClosed.closeErr = stream.AlreadyClosed
	cursor = mutationReplayCursor(0)
	cursor.consumer = alreadyClosed
	cursor.environment = &fakeRabbitEnvironment{}
	if err := cursor.Close(); err != nil {
		t.Fatalf("already-closed consumer error = %v", err)
	}

	cursor = mutationReplayCursor(0)
	cursor.environment = &fakeRabbitEnvironment{closeErr: environmentFailure}
	if err := cursor.Close(); !errors.Is(err, environmentFailure) {
		t.Fatalf("environment-only close error = %v", err)
	}
}

func TestMutationContractConnectionBudgetsAndBackoffAreExact(t *testing.T) {
	if got := connectionAttemptNumber(0); got != 1 {
		t.Fatalf("first attempt number = %d", got)
	}
	if got := connectionAttemptNumber(1); got != 2 {
		t.Fatalf("second attempt number = %d", got)
	}
	if got := boundedAttemptTimeout(90*time.Millisecond, 3, 0, time.Second); got != 30*time.Millisecond {
		t.Fatalf("first attempt timeout = %v", got)
	}
	if got := boundedAttemptTimeout(90*time.Millisecond, 3, 1, time.Second); got != 45*time.Millisecond {
		t.Fatalf("second attempt timeout = %v", got)
	}
	if got := boundedAttemptTimeout(90*time.Millisecond, 3, 2, 20*time.Millisecond); got != 20*time.Millisecond {
		t.Fatalf("configured attempt timeout = %v", got)
	}
	if shouldWaitBeforeConnectionAttempt(0) || !shouldWaitBeforeConnectionAttempt(1) {
		t.Fatal("connection retry wait boundary is incorrect")
	}
	if got := nextReconnectBackoff(time.Second, 3*time.Second); got != 2*time.Second {
		t.Fatalf("doubled backoff = %v", got)
	}
	if got := nextReconnectBackoff(2*time.Second, 3*time.Second); got != 3*time.Second {
		t.Fatalf("capped backoff = %v", got)
	}
}

func TestMutationContractConnectionAcceptsExactCredentialBounds(t *testing.T) {
	connection := mutationConnection(
		rabbitstream.StaticCredentials(strings.Repeat("u", 255), []byte(strings.Repeat("p", 4096))),
		time.Second,
	)
	want := &fakeRabbitEnvironment{}
	got, err := openFreshEnvironmentWith(context.Background(), connection, func(*stream.EnvironmentOptions) (producerEnvironment, error) {
		return want, nil
	})
	if err != nil || got != want {
		t.Fatalf("exact-bound credentials open = %#v, %v", got, err)
	}
}

func TestMutationContractConnectionRejectsEachMissingCredential(t *testing.T) {
	for name, credentials := range map[string]rabbitstream.CredentialProvider{
		"username": rabbitstream.StaticCredentials("", []byte("credential")),
		"password": rabbitstream.StaticCredentials("user", nil),
	} {
		t.Run(name, func(t *testing.T) {
			openerCalls := 0
			connection := mutationConnection(credentials, time.Second)
			environment, err := openFreshEnvironmentWith(
				context.Background(), connection,
				func(*stream.EnvironmentOptions) (producerEnvironment, error) {
					openerCalls++
					return &fakeRabbitEnvironment{}, nil
				},
			)
			if environment != nil || !errors.Is(err, rabbitstream.ErrInvalidConfiguration) || openerCalls != 0 {
				t.Fatalf("missing %s open = %#v, %v after %d opener calls", name, environment, err, openerCalls)
			}
		})
	}
}

func TestMutationContractConnectionRejectsZeroAttemptTimeout(t *testing.T) {
	openerCalls := 0
	connection := mutationConnection(rabbitstream.StaticCredentials("user", []byte("credential")), 0)
	environment, err := openFreshEnvironmentWith(
		context.Background(), connection,
		func(*stream.EnvironmentOptions) (producerEnvironment, error) {
			openerCalls++
			return &fakeRabbitEnvironment{}, nil
		},
	)
	if environment != nil || !errors.Is(err, context.DeadlineExceeded) || openerCalls != 0 {
		t.Fatalf("zero-timeout open = %#v, %v after %d opener calls", environment, err, openerCalls)
	}
}

func TestMutationContractLateConnectionCleanupOwnsOnlyOpenedResources(t *testing.T) {
	closeLateEnvironment(nil)
	environment := &fakeRabbitEnvironment{}
	closeLateEnvironment(environment)
	if environment.closeCalls != 1 {
		t.Fatalf("late environment closes = %d", environment.closeCalls)
	}
}

func mutationReplayCursor(end uint64) *replayCursor {
	return &replayCursor{
		target:    "tracking.events",
		end:       end,
		limits:    rabbitstream.DefaultLimits(),
		messages:  make(chan rabbitstream.Message, 1),
		done:      make(chan struct{}),
		completed: make(chan struct{}),
		failed:    make(chan struct{}),
	}
}

func mutationConnection(credentials rabbitstream.CredentialProvider, rpcTimeout time.Duration) rabbitstream.ConnectionConfig {
	return rabbitstream.ConnectionConfig{
		Endpoints:             []rabbitstream.Endpoint{{Host: "localhost", Port: 5552}},
		Credentials:           credentials,
		Security:              rabbitstream.DevelopmentPlaintextSecurity(),
		ConnectTimeout:        time.Second,
		RPCTimeout:            rpcTimeout,
		MaxReconnectAttempts:  1,
		InitialReconnectDelay: time.Millisecond,
		MaxReconnectBackoff:   time.Millisecond,
	}
}

func mutationNext(t *testing.T, cursor *replayCursor) (rabbitstream.Message, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()
	return cursor.Next(ctx)
}

func assertChannelClosed(t *testing.T, channel <-chan struct{}, name string) {
	t.Helper()
	select {
	case <-channel:
	default:
		t.Fatalf("%s remained open", name)
	}
}

func assertChannelOpen(t *testing.T, channel <-chan struct{}, name string) {
	t.Helper()
	select {
	case <-channel:
		t.Fatalf("%s closed", name)
	default:
	}
}
