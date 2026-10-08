package rabbitstream

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestShutdownWaitTimeoutDoesNotPublishCleanupCompletion(t *testing.T) {
	for _, kind := range []string{"producer", "consumer"} {
		t.Run(kind, func(t *testing.T) {
			release := make(chan struct{})
			defer releaseShutdownFixture(release)
			var shutdown func(context.Context) error
			var done <-chan struct{}
			var calls func() int
			if kind == "producer" {
				transport := newFakeProducerTransport()
				transport.closeBlock, transport.closeErr = release, ErrAuthorization
				producer, err := NewProducer(ProducerConfig{Stream: "stream", Policy: ProducerPolicy{CloseTimeout: time.Millisecond}}, transport)
				if err != nil {
					t.Fatal(err)
				}
				shutdown, done, calls = producer.Shutdown, producer.closeDone, transport.closeCalls
			} else {
				transport := newFakeConsumerTransport()
				transport.closeBlock, transport.closeErr = release, ErrAuthorization
				consumer, err := NewConsumer(ConsumerConfig{Stream: "stream", ConsumerName: "consumer", Policy: ConsumerPolicy{CloseTimeout: time.Millisecond}}, transport)
				if err != nil {
					t.Fatal(err)
				}
				shutdown, done = consumer.Shutdown, consumer.closeDone
				calls = func() int { transport.mutex.Lock(); defer transport.mutex.Unlock(); return transport.closeCount }
			}
			if err := shutdown(boundedTestContext()); !errors.Is(err, ErrTimeout) {
				t.Fatalf("bounded wait: %v", err)
			}
			select {
			case <-done:
				t.Fatal("cleanup completion published before transport close returned")
			default:
			}
			close(release)
			awaitShutdownCompletion(t, done)
			if err := shutdown(boundedTestContext()); !errors.Is(err, ErrAuthorization) {
				t.Fatalf("actual cleanup result: %v", err)
			}
			if calls() != 1 {
				t.Fatalf("transport close calls: %d", calls())
			}
		})
	}
}

func TestConsumerDrainTimeoutRetainsEventualTransportCleanup(t *testing.T) {
	release, entered := make(chan struct{}), make(chan struct{})
	defer releaseShutdownFixture(release)
	transport := newFakeConsumerTransport(Message{Stream: "stream", Partition: "stream", Offset: 1, HasOffset: true})
	consumer, err := NewConsumer(ConsumerConfig{Stream: "stream", ConsumerName: "consumer", Policy: ConsumerPolicy{CloseTimeout: time.Millisecond}}, transport)
	if err != nil {
		t.Fatal(err)
	}
	run := make(chan error, 1)
	go func() {
		run <- consumer.Run(boundedTestContext(), func(context.Context, Message) error { close(entered); <-release; return nil })
	}()
	awaitShutdownCompletion(t, entered)
	defer func() { releaseShutdownFixture(release); receiveTest(t, run) }()
	if err := consumer.Shutdown(boundedTestContext()); !errors.Is(err, ErrTimeout) {
		t.Fatalf("drain wait: %v", err)
	}
	select {
	case <-consumer.closeDone:
		t.Fatal("drain timeout abandoned cleanup owner")
	default:
	}
	close(release)
	awaitShutdownCompletion(t, consumer.closeDone)
	if err := consumer.Shutdown(boundedTestContext()); err != nil {
		t.Fatalf("completed cleanup: %v", err)
	}
	transport.mutex.Lock()
	calls := transport.closeCount
	transport.mutex.Unlock()
	if calls != 1 {
		t.Fatalf("eventual transport close calls: %d", calls)
	}
}

type heldShutdownProducerTransport struct {
	entered, release chan struct{}
	closed           int
}

func (transport *heldShutdownProducerTransport) Send(_ context.Context, _ Message, confirm func(TransportConfirmation)) error {
	close(transport.entered)
	<-transport.release
	confirm(TransportConfirmation{Confirmed: true})
	return nil
}

func (transport *heldShutdownProducerTransport) Close() error { transport.closed++; return nil }

func TestProducerPolicyBoundsDrainWaitWithoutLosingCleanup(t *testing.T) {
	transport := &heldShutdownProducerTransport{entered: make(chan struct{}), release: make(chan struct{})}
	defer releaseShutdownFixture(transport.release)
	producer, err := NewProducer(ProducerConfig{Stream: "stream", Policy: ProducerPolicy{CloseTimeout: time.Millisecond}}, transport)
	if err != nil {
		t.Fatal(err)
	}
	published := make(chan error, 1)
	go func() { _, err := producer.Publish(boundedTestContext(), Message{Stream: "stream"}); published <- err }()
	awaitShutdownCompletion(t, transport.entered)
	defer func() {
		releaseShutdownFixture(transport.release)
		receiveTest(t, published)
		awaitShutdownCompletion(t, producer.closeDone)
	}()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := producer.Shutdown(ctx); !errors.Is(err, ErrTimeout) {
		t.Fatalf("policy drain bound: %v", err)
	}
	select {
	case <-producer.closeDone:
		t.Fatal("admitted send still running after reported cleanup")
	default:
	}
	close(transport.release)
	awaitShutdownCompletion(t, producer.closeDone)
	if err := producer.Shutdown(boundedTestContext()); err != nil {
		t.Fatalf("actual cleanup result: %v", err)
	}
	if transport.closed != 1 {
		t.Fatalf("transport close calls: %d", transport.closed)
	}
}

func releaseShutdownFixture(release chan struct{}) {
	select {
	case <-release:
	default:
		close(release)
	}
}

func awaitShutdownCompletion(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("controlled shutdown fixture did not complete")
	}
}
