//go:build integration

package rabbitmq

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/faustbrian/golib/pkg/rabbitstream"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"
)

const benchmarkOperationTimeout = 30 * time.Second

type benchmarkPublishCandidate struct {
	name  string
	open  func(testing.TB, rabbitstream.ConnectionConfig, *stream.Environment, string) benchmarkPublisher
	close func(testing.TB, benchmarkPublisher)
}

type benchmarkPublisher interface {
	Publish(context.Context, []byte) error
}

var benchmarkPublishCandidates = []benchmarkPublishCandidate{
	{name: "policy-wrapper", open: openPolicyBenchmarkPublisher, close: closeBenchmarkPublisher},
	{name: "raw-supported-client", open: openRawBenchmarkPublisher, close: closeBenchmarkPublisher},
}

// BenchmarkEquivalentConfirmedPublish compares one confirmed publish at a
// time. Both candidates use the same broker, stream, payload, queue bound, and
// confirmation timeout; connection setup and shutdown are outside the timer.
func BenchmarkEquivalentConfirmedPublish(b *testing.B) {
	for _, transport := range []string{"plaintext", "tls"} {
		transport := transport
		b.Run(transport, func(b *testing.B) {
			connection, environment := benchmarkBroker(b, transport == "tls")
			defer closeBenchmarkEnvironment(b, environment)
			for _, payloadBytes := range []int{128, 1 << 10, 64 << 10} {
				payloadBytes := payloadBytes
				b.Run(fmt.Sprintf("%dB", payloadBytes), func(b *testing.B) {
					for _, candidate := range benchmarkPublishCandidates {
						candidate := candidate
						b.Run(candidate.name, func(b *testing.B) {
							streamName := declareBenchmarkStream(b, environment, "publish")
							publisher := candidate.open(b, connection, environment, streamName)
							defer candidate.close(b, publisher)
							payload := make([]byte, payloadBytes)
							ctx, cancel := context.WithTimeout(context.Background(), benchmarkOperationTimeout)
							if err := publisher.Publish(ctx, payload); err != nil {
								cancel()
								b.Fatalf("warm %s publisher: %v", candidate.name, err)
							}
							cancel()

							b.SetBytes(int64(payloadBytes))
							b.ReportAllocs()
							b.ResetTimer()
							for b.Loop() {
								ctx, cancel = context.WithTimeout(context.Background(), benchmarkOperationTimeout)
								err := publisher.Publish(ctx, payload)
								cancel()
								if err != nil {
									b.Fatalf("publish with %s: %v", candidate.name, err)
								}
							}
							b.StopTimer()
							reportMessageRate(b, 1)
						})
					}
				})
			}
		})
	}
}

// BenchmarkBoundedConfirmedWindow measures bounded asynchronous capacity. One
// operation admits exactly window messages and waits for every confirmation.
func BenchmarkBoundedConfirmedWindow(b *testing.B) {
	connection, environment := benchmarkBroker(b, false)
	defer closeBenchmarkEnvironment(b, environment)
	for _, window := range []int{10, 100} {
		window := window
		b.Run(fmt.Sprintf("%d-messages/1024B", window), func(b *testing.B) {
			streamName := declareBenchmarkStream(b, environment, "window")
			producer, err := OpenProducer(context.Background(), connection, rabbitstream.ProducerConfig{
				Stream: streamName,
				Policy: rabbitstream.ProducerPolicy{MaxOutstanding: window, ConfirmationTimeout: 10 * time.Second},
			})
			if err != nil {
				b.Fatal(err)
			}
			defer closePolicyBenchmarkProducer(b, producer)
			message := rabbitstream.Message{Stream: streamName, Payload: make([]byte, 1<<10)}

			b.SetBytes(int64(window << 10))
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				ctx, cancel := context.WithTimeout(context.Background(), benchmarkOperationTimeout)
				outcomes := make([]<-chan rabbitstream.PublishOutcome, window)
				for index := range outcomes {
					outcome, publishErr := producer.PublishAsync(ctx, message)
					if publishErr != nil {
						cancel()
						b.Fatalf("admit message %d: %v", index, publishErr)
					}
					outcomes[index] = outcome
				}
				for index, outcome := range outcomes {
					result := <-outcome
					if result.Err != nil || result.Result.State != rabbitstream.DeliveryConfirmed {
						cancel()
						b.Fatalf("confirmation %d = %#v", index, result)
					}
				}
				cancel()
			}
			b.StopTimer()
			reportMessageRate(b, window)
			b.ReportMetric(float64(window), "messages/op")
		})
	}
}

// BenchmarkBacklogCatchUp seeds retained history outside the timer, then
// measures handler completion and durable offset storage from the beginning.
func BenchmarkBacklogCatchUp(b *testing.B) {
	connection, environment := benchmarkBroker(b, false)
	streamName := integrationName("benchmark-backlog")
	if err := environment.DeclareStream(streamName, stream.NewStreamOptions().SetMaxAge(time.Hour)); err != nil {
		closeBenchmarkEnvironment(b, environment)
		b.Fatalf("declare backlog benchmark stream: %v", err)
	}
	defer func() {
		if err := environment.DeleteStream(streamName); err != nil {
			b.Errorf("delete backlog benchmark stream: %v", err)
		}
		closeBenchmarkEnvironment(b, environment)
	}()

	producer, err := OpenProducer(context.Background(), connection, rabbitstream.ProducerConfig{Stream: streamName})
	if err != nil {
		b.Fatal(err)
	}
	payload := make([]byte, 1<<10)
	for range b.N {
		result, publishErr := producer.Publish(context.Background(), rabbitstream.Message{
			Stream: streamName, Payload: payload,
		})
		if publishErr != nil || result.State != rabbitstream.DeliveryConfirmed {
			b.Fatalf("seed backlog = %#v, %v", result, publishErr)
		}
	}
	closePolicyBenchmarkProducer(b, producer)

	consumer, err := OpenConsumer(context.Background(), connection, rabbitstream.ConsumerConfig{
		Stream: streamName, ConsumerName: integrationName("benchmark-backlog-consumer"),
		Start: rabbitstream.StartPosition{Kind: rabbitstream.OffsetStartBeginning},
	})
	if err != nil {
		b.Fatal(err)
	}
	runCtx, cancelRun := context.WithCancel(context.Background())
	runDone := make(chan error, 1)
	caughtUp := make(chan struct{})
	count := 0
	b.SetBytes(1 << 10)
	b.ReportAllocs()
	b.ResetTimer()
	go func() {
		runDone <- consumer.Run(runCtx, func(context.Context, rabbitstream.Message) error {
			count++
			if count == b.N {
				close(caughtUp)
			}
			return nil
		})
	}()
	select {
	case <-caughtUp:
	case err := <-runDone:
		b.Fatalf("backlog consumer stopped: %v", err)
	case <-time.After(benchmarkOperationTimeout):
		b.Fatal("backlog catch-up timed out")
	}
	b.StopTimer()
	reportMessageRate(b, 1)
	cancelRun()
	select {
	case <-runDone:
	case <-time.After(10 * time.Second):
		b.Fatal("backlog consumer did not stop")
	}
	ctx, cancelClose := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelClose()
	if err := consumer.Close(ctx); err != nil {
		b.Errorf("close backlog consumer: %v", err)
	}
}

type policyBenchmarkPublisher struct {
	producer *rabbitstream.Producer
	stream   string
}

func openPolicyBenchmarkPublisher(
	tb testing.TB,
	connection rabbitstream.ConnectionConfig,
	_ *stream.Environment,
	streamName string,
) benchmarkPublisher {
	tb.Helper()
	producer, err := OpenProducer(context.Background(), connection, rabbitstream.ProducerConfig{
		Stream: streamName,
		Policy: rabbitstream.ProducerPolicy{MaxOutstanding: 500, ConfirmationTimeout: 10 * time.Second},
	})
	if err != nil {
		tb.Fatalf("open policy publisher: %v", err)
	}
	return &policyBenchmarkPublisher{producer: producer, stream: streamName}
}

func (publisher *policyBenchmarkPublisher) Publish(ctx context.Context, payload []byte) error {
	result, err := publisher.producer.Publish(ctx, rabbitstream.Message{
		Stream: publisher.producerStream(), Payload: payload,
	})
	if err != nil {
		return err
	}
	if result.State != rabbitstream.DeliveryConfirmed {
		return fmt.Errorf("policy delivery state %v", result.State)
	}
	return nil
}

func (publisher *policyBenchmarkPublisher) producerStream() string {
	return publisher.stream
}

type rawBenchmarkPublisher struct {
	producer      *stream.Producer
	confirmations stream.ChannelPublishConfirm
}

func openRawBenchmarkPublisher(
	tb testing.TB,
	_ rabbitstream.ConnectionConfig,
	environment *stream.Environment,
	streamName string,
) benchmarkPublisher {
	tb.Helper()
	producer, err := environment.NewProducer(streamName, stream.NewProducerOptions().
		SetQueueSize(500).
		SetConfirmationTimeOut(10*time.Second))
	if err != nil {
		tb.Fatalf("open raw publisher: %v", err)
	}
	return &rawBenchmarkPublisher{producer: producer, confirmations: producer.NotifyPublishConfirmation()}
}

func (publisher *rawBenchmarkPublisher) Publish(ctx context.Context, payload []byte) error {
	if err := publisher.producer.Send(amqp.NewMessage(payload)); err != nil {
		return err
	}
	select {
	case statuses, ok := <-publisher.confirmations:
		if !ok || len(statuses) != 1 || !statuses[0].IsConfirmed() {
			if ok && len(statuses) == 1 && statuses[0].GetError() != nil {
				return statuses[0].GetError()
			}
			return fmt.Errorf("raw confirmation count or state is invalid")
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func closeBenchmarkPublisher(tb testing.TB, publisher benchmarkPublisher) {
	tb.Helper()
	switch candidate := publisher.(type) {
	case *policyBenchmarkPublisher:
		closePolicyBenchmarkProducer(tb, candidate.producer)
	case *rawBenchmarkPublisher:
		if err := candidate.producer.Close(); err != nil {
			tb.Errorf("close raw publisher: %v", err)
		}
	}
}

func closePolicyBenchmarkProducer(tb testing.TB, producer *rabbitstream.Producer) {
	tb.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := producer.Close(ctx); err != nil {
		tb.Errorf("close policy publisher: %v", err)
	}
}

func declareBenchmarkStream(tb testing.TB, environment *stream.Environment, role string) string {
	tb.Helper()
	name := integrationName("benchmark-" + role)
	if err := environment.DeclareStream(name, stream.NewStreamOptions().SetMaxAge(time.Hour)); err != nil {
		tb.Fatalf("declare benchmark stream: %v", err)
	}
	tb.Cleanup(func() {
		if err := environment.DeleteStream(name); err != nil {
			tb.Errorf("delete benchmark stream: %v", err)
		}
	})
	return name
}

func benchmarkBroker(tb testing.TB, useTLS bool) (rabbitstream.ConnectionConfig, *stream.Environment) {
	tb.Helper()
	if useTLS {
		return benchmarkTLSBroker(tb)
	}
	host := requiredBenchmarkEnv(tb, "RABBITSTREAM_TEST_HOST")
	port := requiredBenchmarkPort(tb, "RABBITSTREAM_TEST_PORT")
	username := requiredBenchmarkEnv(tb, "RABBITSTREAM_TEST_USER")
	password := requiredBenchmarkEnv(tb, "RABBITSTREAM_TEST_PASSWORD")
	connection := rabbitstream.ConnectionConfig{
		Endpoints:   []rabbitstream.Endpoint{{Host: host, Port: port}},
		Credentials: rabbitstream.StaticCredentials(username, []byte(password)),
		Security:    rabbitstream.DevelopmentPlaintextSecurity(), ConnectTimeout: 10 * time.Second,
		RPCTimeout: 10 * time.Second, MaxReconnectAttempts: 2,
	}
	environment, err := stream.NewEnvironment(stream.NewEnvironmentOptions().
		SetHost(host).SetPort(int(port)).SetUser(username).SetPassword(password).SetRPCTimeout(10 * time.Second))
	if err != nil {
		tb.Fatalf("open benchmark environment: %v", err)
	}
	return connection, environment
}

func benchmarkTLSBroker(tb testing.TB) (rabbitstream.ConnectionConfig, *stream.Environment) {
	tb.Helper()
	host := requiredBenchmarkEnv(tb, "RABBITSTREAM_TLS_HOST")
	port := requiredBenchmarkPort(tb, "RABBITSTREAM_TLS_PORT")
	username := requiredBenchmarkEnv(tb, "RABBITSTREAM_TLS_USER")
	password := requiredBenchmarkEnv(tb, "RABBITSTREAM_TLS_PASSWORD")
	runtimeDirectory := requiredBenchmarkEnv(tb, "RABBITSTREAM_TLS_RUNTIME")
	caPEM, err := os.ReadFile(filepath.Join(runtimeDirectory, "ca.pem"))
	if err != nil {
		tb.Fatalf("read benchmark CA: %v", err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(caPEM) {
		tb.Fatal("benchmark CA is invalid")
	}
	certificate, err := tls.LoadX509KeyPair(
		filepath.Join(runtimeDirectory, "client.pem"), filepath.Join(runtimeDirectory, "client-key.pem"),
	)
	if err != nil {
		tb.Fatalf("load benchmark client certificate: %v", err)
	}
	tlsConfig := &tls.Config{MinVersion: tls.VersionTLS13, RootCAs: roots, Certificates: []tls.Certificate{certificate}}
	connection := rabbitstream.ConnectionConfig{
		Endpoints:   []rabbitstream.Endpoint{{Host: host, Port: port}},
		Credentials: rabbitstream.StaticCredentials(username, []byte(password)),
		Security:    rabbitstream.SecurityConfig{TLS: tlsConfig}, ConnectTimeout: 10 * time.Second,
		RPCTimeout: 10 * time.Second, MaxReconnectAttempts: 2,
	}
	provisioningTLS := tlsConfig.Clone()
	provisioningTLS.ServerName = host
	environment, err := stream.NewEnvironment(stream.NewEnvironmentOptions().
		SetHost(host).SetPort(int(port)).SetUser(username).SetPassword(password).
		SetRPCTimeout(10 * time.Second).IsTLS(true).SetTLSConfig(provisioningTLS))
	if err != nil {
		tb.Fatalf("open TLS benchmark environment: %v", err)
	}
	return connection, environment
}

func closeBenchmarkEnvironment(tb testing.TB, environment *stream.Environment) {
	tb.Helper()
	if err := environment.Close(); err != nil {
		tb.Errorf("close benchmark environment: %v", err)
	}
}

func requiredBenchmarkEnv(tb testing.TB, name string) string {
	tb.Helper()
	value := os.Getenv(name)
	if value == "" {
		tb.Skipf("%s is required for this benchmark", name)
	}
	return value
}

func requiredBenchmarkPort(tb testing.TB, name string) uint16 {
	tb.Helper()
	value := requiredBenchmarkEnv(tb, name)
	port, err := strconv.ParseUint(value, 10, 16)
	if err != nil || port == 0 {
		tb.Fatalf("%s is invalid", name)
	}
	return uint16(port)
}

func reportMessageRate(b *testing.B, messagesPerOperation int) {
	b.Helper()
	if elapsed := b.Elapsed(); elapsed > 0 {
		b.ReportMetric(float64(b.N*messagesPerOperation)/elapsed.Seconds()*3600, "messages/hour")
	}
}
