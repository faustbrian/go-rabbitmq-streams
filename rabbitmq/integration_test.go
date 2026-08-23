//go:build integration

package rabbitmq

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/faustbrian/golib/pkg/rabbitstream"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"
)

func TestTLSMutualAuthenticationAndCustomCAPublish(t *testing.T) {
	connection, environment := integrationTLSBroker(t)
	streamName := integrationName("allowed-mtls")
	if err := environment.DeclareStream(streamName, stream.NewStreamOptions().SetMaxAge(time.Hour)); err != nil {
		t.Fatalf("declare disposable TLS stream: %v", err)
	}
	t.Cleanup(func() {
		if err := environment.DeleteStream(streamName); err != nil {
			t.Errorf("delete disposable TLS stream: %v", err)
		}
		if err := environment.Close(); err != nil {
			t.Errorf("close TLS provisioning environment: %v", err)
		}
	})

	producer, err := OpenProducer(t.Context(), connection, rabbitstream.ProducerConfig{Stream: streamName})
	if err != nil {
		t.Fatalf("OpenProducer() over mTLS error = %v", err)
	}
	t.Cleanup(func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := producer.Close(closeCtx); err != nil {
			t.Errorf("close TLS producer: %v", err)
		}
	})
	result, err := producer.Publish(t.Context(), rabbitstream.Message{
		Stream: streamName, Payload: []byte("verified mTLS"),
	})
	if err != nil || result.State != rabbitstream.DeliveryConfirmed {
		t.Fatalf("TLS Publish() = %#v, %v", result, err)
	}
}

func TestTLSRejectsMissingUntrustedAndWrongHostCertificates(t *testing.T) {
	connection, environment := integrationTLSBroker(t)
	streamName := integrationName("allowed-tls-rejection")
	if err := environment.DeclareStream(streamName, stream.NewStreamOptions().SetMaxAge(time.Hour)); err != nil {
		t.Fatalf("declare disposable TLS stream: %v", err)
	}
	t.Cleanup(func() {
		if err := environment.DeleteStream(streamName); err != nil {
			t.Errorf("delete disposable TLS stream: %v", err)
		}
		if err := environment.Close(); err != nil {
			t.Errorf("close TLS provisioning environment: %v", err)
		}
	})
	runtimeDirectory := requiredIntegrationEnv(t, "RABBITSTREAM_TLS_RUNTIME")
	untrustedCertificate, err := tls.LoadX509KeyPair(
		filepath.Join(runtimeDirectory, "untrusted-client.pem"),
		filepath.Join(runtimeDirectory, "untrusted-client-key.pem"),
	)
	if err != nil {
		t.Fatalf("load untrusted client certificate: %v", err)
	}

	tests := map[string]func(*tls.Config){
		"missing client certificate": func(config *tls.Config) { config.Certificates = nil },
		"untrusted client certificate": func(config *tls.Config) {
			config.Certificates = []tls.Certificate{untrustedCertificate}
		},
		"wrong server hostname": func(config *tls.Config) { config.ServerName = "not-localhost.invalid" },
		"untrusted server CA":   func(config *tls.Config) { config.RootCAs = x509.NewCertPool() },
	}
	for name, modify := range tests {
		t.Run(name, func(t *testing.T) {
			candidate := connection
			candidate.Security.TLS = connection.Security.TLS.Clone()
			candidate.MaxReconnectAttempts = 1
			candidate.ConnectTimeout = 3 * time.Second
			modify(candidate.Security.TLS)
			producer, err := OpenProducer(t.Context(), candidate, rabbitstream.ProducerConfig{Stream: streamName})
			if producer != nil {
				t.Fatal("OpenProducer() returned a producer for rejected TLS identity")
			}
			if !errors.Is(err, rabbitstream.ErrTimeout) {
				t.Fatalf("OpenProducer() error = %v, want bounded TLS handshake timeout", err)
			}
		})
	}
}

func TestTLSClassifiesAuthenticationAndAuthorizationFailures(t *testing.T) {
	connection, environment := integrationTLSBroker(t)
	deniedStream := integrationName("denied-authorization")
	allowedStream := integrationName("allowed-authorization")
	if err := environment.DeclareStream(deniedStream, stream.NewStreamOptions().SetMaxAge(time.Hour)); err != nil {
		t.Fatalf("declare denied stream: %v", err)
	}
	if err := environment.DeclareStream(allowedStream, stream.NewStreamOptions().SetMaxAge(time.Hour)); err != nil {
		t.Fatalf("declare allowed stream: %v", err)
	}
	t.Cleanup(func() {
		if err := environment.DeleteStream(deniedStream); err != nil {
			t.Errorf("delete denied stream: %v", err)
		}
		if err := environment.DeleteStream(allowedStream); err != nil {
			t.Errorf("delete allowed stream: %v", err)
		}
		if err := environment.Close(); err != nil {
			t.Errorf("close TLS provisioning environment: %v", err)
		}
	})

	badCredentials := connection
	authObserver := &integrationObserver{}
	badCredentials.Observer = authObserver
	badCredentials.Credentials = rabbitstream.StaticCredentials(
		requiredIntegrationEnv(t, "RABBITSTREAM_TLS_USER"), []byte("intentionally-invalid"),
	)
	badCredentials.MaxReconnectAttempts = 1
	if producer, err := OpenProducer(t.Context(), badCredentials, rabbitstream.ProducerConfig{Stream: deniedStream}); producer != nil || !errors.Is(err, rabbitstream.ErrAuthentication) {
		t.Fatalf("authentication OpenProducer() = %#v, %v", producer, err)
	}
	if !authObserver.HasAll(rabbitstream.ObservationAuthenticationError) {
		t.Fatalf("authentication observations = %#v", authObserver.Kinds())
	}

	restricted := connection
	restricted.Credentials = rabbitstream.StaticCredentials(
		requiredIntegrationEnv(t, "RABBITSTREAM_RESTRICTED_USER"),
		[]byte(requiredIntegrationEnv(t, "RABBITSTREAM_RESTRICTED_PASSWORD")),
	)
	restricted.MaxReconnectAttempts = 1
	if producer, err := OpenProducer(t.Context(), restricted, rabbitstream.ProducerConfig{Stream: deniedStream}); producer != nil || !errors.Is(err, rabbitstream.ErrAuthorization) {
		t.Fatalf("authorization OpenProducer() = %#v, %v", producer, err)
	}
	producer, err := OpenProducer(t.Context(), restricted, rabbitstream.ProducerConfig{Stream: allowedStream})
	if err != nil {
		t.Fatalf("restricted OpenProducer() for allowed stream error = %v", err)
	}
	closeCtx, cancelClose := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelClose()
	defer func() {
		if err := producer.Close(closeCtx); err != nil {
			t.Errorf("close restricted producer: %v", err)
		}
	}()
	result, err := producer.Publish(t.Context(), rabbitstream.Message{
		Stream: allowedStream, Payload: []byte("authorized"),
	})
	if err != nil || result.State != rabbitstream.DeliveryConfirmed {
		t.Fatalf("restricted Publish() = %#v, %v", result, err)
	}
}

func TestSingleStreamPublishConsumeOffsetAndReplay(t *testing.T) {
	connection, environment := integrationBroker(t)
	streamName := integrationName("single")
	if err := environment.DeclareStream(
		streamName,
		stream.NewStreamOptions().SetMaxAge(time.Hour),
	); err != nil {
		t.Fatalf("declare disposable stream: %v", err)
	}
	t.Cleanup(func() {
		if err := environment.DeleteStream(streamName); err != nil {
			t.Errorf("delete disposable stream: %v", err)
		}
		if err := environment.Close(); err != nil {
			t.Errorf("close provisioning environment: %v", err)
		}
	})

	producer, err := OpenProducer(t.Context(), connection, rabbitstream.ProducerConfig{
		Stream: streamName,
		Policy: rabbitstream.ProducerPolicy{
			Deduplication: rabbitstream.DeduplicationPublishingID,
			ProducerName:  integrationName("producer"),
		},
	})
	if err != nil {
		t.Fatalf("OpenProducer() error = %v", err)
	}
	t.Cleanup(func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := producer.Close(closeCtx); err != nil {
			t.Errorf("close producer: %v", err)
		}
	})

	timestamp := time.Now().UTC().Truncate(time.Millisecond)
	result, err := producer.Publish(t.Context(), rabbitstream.Message{
		Stream:          streamName,
		RoutingKey:      "tracking-123",
		PublishingID:    1,
		HasPublishingID: true,
		Timestamp:       timestamp,
		ContentType:     "application/octet-stream",
		MessageID:       "event-123",
		CorrelationID:   "tracking-123",
		Payload:         []byte("payload"),
		Headers: []rabbitstream.MetadataEntry{
			{Key: "traceparent", Value: []byte("00-0123456789abcdef0123456789abcdef-0123456789abcdef-01")},
		},
		Properties: []rabbitstream.MetadataEntry{
			{Key: "schema", Value: []byte("tracking.v1")},
		},
	})
	if err != nil || result.State != rabbitstream.DeliveryConfirmed {
		t.Fatalf("Publish() = %#v, %v", result, err)
	}

	consumerName := integrationName("consumer")
	consumer, err := OpenConsumer(t.Context(), connection, rabbitstream.ConsumerConfig{
		Stream: streamName, ConsumerName: consumerName,
		Start: rabbitstream.StartPosition{Kind: rabbitstream.OffsetStartBeginning},
	})
	if err != nil {
		t.Fatalf("OpenConsumer() error = %v", err)
	}
	runCtx, cancelRun := context.WithCancel(t.Context())
	received := make(chan rabbitstream.Message, 1)
	runDone := make(chan error, 1)
	go func() {
		runDone <- consumer.Run(runCtx, func(_ context.Context, message rabbitstream.Message) error {
			received <- message.Retain()
			return nil
		})
	}()
	delivery := <-received
	if delivery.Stream != streamName || delivery.Partition != streamName ||
		delivery.Offset != 0 || !delivery.HasOffset || delivery.RoutingKey != "tracking-123" ||
		delivery.ContentType != "application/octet-stream" || delivery.MessageID != "event-123" ||
		delivery.CorrelationID != "tracking-123" || !delivery.Timestamp.Equal(timestamp) ||
		string(delivery.Payload) != "payload" {
		t.Fatalf("delivery = %#v", delivery)
	}

	inspector, err := NewInspector(connection, rabbitstream.DefaultLimits())
	if err != nil {
		t.Fatalf("NewInspector() error = %v", err)
	}
	waitForStoredOffset(t, inspector, streamName, consumerName, delivery.Offset)
	inspectionObserver := &integrationObserver{}
	inspectionConnection := connection
	inspectionConnection.Observer = inspectionObserver
	observedInspector, err := NewInspector(inspectionConnection, rabbitstream.DefaultLimits())
	if err != nil {
		t.Fatalf("NewInspector() with observer error = %v", err)
	}
	inspection, err := observedInspector.Inspect(t.Context(), rabbitstream.InspectionRequest{
		Stream: streamName, ConsumerName: consumerName,
	})
	if err != nil {
		t.Fatalf("Inspect() exact offsets error = %v", err)
	}
	if len(inspection.Partitions) != 1 || inspection.Partitions[0].LastOffset == nil ||
		*inspection.Partitions[0].LastOffset != delivery.Offset || inspection.Partitions[0].Lag == nil ||
		*inspection.Partitions[0].Lag != 0 {
		t.Fatalf("exact inspection = %#v", inspection)
	}
	if !inspectionObserver.HasAll(
		rabbitstream.ObservationStreamEndOffset,
		rabbitstream.ObservationConsumerLag,
	) {
		t.Fatalf("inspection observations = %#v", inspectionObserver.Kinds())
	}
	if health := observedInspector.Health(t.Context()); health.State != rabbitstream.DependencyHealthy {
		t.Fatalf("healthy dependency = %#v", health)
	}
	unavailableConnection := connection
	unavailableConnection.Endpoints = []rabbitstream.Endpoint{{Host: connection.Endpoints[0].Host, Port: 1}}
	unavailableConnection.ConnectTimeout = 2 * time.Second
	unavailableInspector, err := NewInspector(unavailableConnection, rabbitstream.DefaultLimits())
	if err != nil {
		t.Fatalf("NewInspector() for unavailable endpoint error = %v", err)
	}
	healthCtx, cancelHealth := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancelHealth()
	if health := unavailableInspector.Health(healthCtx); health.State != rabbitstream.DependencyUnavailable || health.Category != rabbitstream.CategoryConnection {
		t.Fatalf("unavailable dependency = %#v", health)
	}
	cancelRun()
	if err := <-runDone; !errors.Is(err, rabbitstream.ErrCanceled) {
		t.Fatalf("consumer Run() error = %v", err)
	}
	closeCtx, cancelClose := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelClose()
	if err := consumer.Close(closeCtx); err != nil {
		t.Fatalf("consumer Close() error = %v", err)
	}

	replayer, err := NewReplayer(connection, rabbitstream.DefaultLimits())
	if err != nil {
		t.Fatalf("NewReplayer() error = %v", err)
	}
	end := delivery.Offset
	var replayed []rabbitstream.Message
	if err := replayer.Run(t.Context(), rabbitstream.ReplayRequest{
		Stream:    streamName,
		Start:     rabbitstream.StartPosition{Kind: rabbitstream.OffsetStartExplicit, Offset: delivery.Offset},
		EndOffset: &end,
	}, func(_ context.Context, replay rabbitstream.ReplayDelivery) error {
		replayed = append(replayed, replay.Message.Retain())
		return nil
	}); err != nil {
		t.Fatalf("replay Run() error = %v", err)
	}
	if len(replayed) != 1 || string(replayed[0].Payload) != "payload" {
		t.Fatalf("replayed = %#v", replayed)
	}
}

func TestReplayReportsRetentionGapBeyondEndAndDeletedStream(t *testing.T) {
	connection, environment := integrationBroker(t)
	retainedStream := integrationName("retention")
	if err := environment.DeclareStream(
		retainedStream,
		stream.NewStreamOptions().
			SetMaxLengthBytes(stream.ByteCapacity{}.MB(1)).
			SetMaxSegmentSizeBytes(stream.ByteCapacity{}.KB(500)),
	); err != nil {
		t.Fatalf("declare retention stream: %v", err)
	}
	deletedStream := integrationName("deleted")
	if err := environment.DeclareStream(deletedStream, stream.NewStreamOptions().SetMaxAge(time.Hour)); err != nil {
		t.Fatalf("declare deleted stream: %v", err)
	}
	t.Cleanup(func() {
		exists, err := environment.StreamExists(retainedStream)
		if err == nil && exists {
			if err := environment.DeleteStream(retainedStream); err != nil {
				t.Errorf("delete retention stream: %v", err)
			}
		}
		if err := environment.Close(); err != nil {
			t.Errorf("close retention provisioning environment: %v", err)
		}
	})

	producer, err := OpenProducer(t.Context(), connection, rabbitstream.ProducerConfig{Stream: retainedStream})
	if err != nil {
		t.Fatalf("OpenProducer() error = %v", err)
	}
	payload := make([]byte, 320*1024)
	for index := 0; index < 8; index++ {
		result, publishErr := producer.Publish(t.Context(), rabbitstream.Message{
			Stream: retainedStream, Payload: payload,
		})
		if publishErr != nil || result.State != rabbitstream.DeliveryConfirmed {
			t.Fatalf("Publish(%d) = %#v, %v", index, result, publishErr)
		}
	}
	closeCtx, cancelClose := context.WithTimeout(context.Background(), 10*time.Second)
	if err := producer.Close(closeCtx); err != nil {
		cancelClose()
		t.Fatalf("close retention producer: %v", err)
	}
	cancelClose()

	inspector, err := NewInspector(connection, rabbitstream.DefaultLimits())
	if err != nil {
		t.Fatalf("NewInspector() error = %v", err)
	}
	retentionCtx, cancelRetention := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancelRetention()
	retentionPoll := time.NewTicker(50 * time.Millisecond)
	defer retentionPoll.Stop()
	for {
		inspection, inspectErr := inspector.Inspect(retentionCtx, rabbitstream.InspectionRequest{Stream: retainedStream})
		if inspectErr != nil {
			t.Fatalf("inspect retention stream: %v", inspectErr)
		}
		if len(inspection.Partitions) == 1 && inspection.Partitions[0].FirstOffset != nil &&
			*inspection.Partitions[0].FirstOffset > 0 {
			break
		}
		select {
		case <-retentionPoll.C:
		case <-retentionCtx.Done():
			t.Fatalf("retention did not remove offset zero: %v", retentionCtx.Err())
		}
	}

	replayer, err := NewReplayer(connection, rabbitstream.DefaultLimits())
	if err != nil {
		t.Fatalf("NewReplayer() error = %v", err)
	}
	err = replayer.Run(t.Context(), rabbitstream.ReplayRequest{
		Stream: retainedStream, Start: rabbitstream.StartPosition{Kind: rabbitstream.OffsetStartExplicit},
	}, func(context.Context, rabbitstream.ReplayDelivery) error { return nil })
	if !errors.Is(err, rabbitstream.ErrRetentionGap) {
		t.Fatalf("retention-gap Run() error = %v", err)
	}

	rangeSnapshot, err := replayer.Inspect(t.Context(), rabbitstream.ReplayRequest{
		Stream: retainedStream, Start: rabbitstream.StartPosition{Kind: rabbitstream.OffsetStartBeginning},
	})
	if err != nil {
		t.Fatalf("Inspect() retained range error = %v", err)
	}
	beyondEnd := rangeSnapshot.LastOffset + 1
	err = replayer.Run(t.Context(), rabbitstream.ReplayRequest{
		Stream: retainedStream, Start: rabbitstream.StartPosition{Kind: rabbitstream.OffsetStartBeginning},
		EndOffset: &beyondEnd,
	}, func(context.Context, rabbitstream.ReplayDelivery) error { return nil })
	if !errors.Is(err, rabbitstream.ErrReplayRange) {
		t.Fatalf("beyond-end Run() error = %v", err)
	}

	if err := environment.DeleteStream(deletedStream); err != nil {
		t.Fatalf("delete stream before replay: %v", err)
	}
	_, err = replayer.Inspect(t.Context(), rabbitstream.ReplayRequest{
		Stream: deletedStream, Start: rabbitstream.StartPosition{Kind: rabbitstream.OffsetStartBeginning},
	})
	if !errors.Is(err, rabbitstream.ErrStreamUnavailable) {
		t.Fatalf("deleted-stream Inspect() error = %v", err)
	}
}

func TestProducerReconnectsAfterBrokerRestart(t *testing.T) {
	container := requiredIntegrationEnv(t, "RABBITSTREAM_TEST_RESTART_CONTAINER")
	if !strings.HasPrefix(container, "codex-rabbitstream-") {
		t.Fatal("restart container is not task-owned")
	}
	connection, environment := integrationBroker(t)
	observer := &integrationObserver{}
	connection.Observer = observer
	streamName := integrationName("restart")
	if err := environment.DeclareStream(streamName, stream.NewStreamOptions().SetMaxAge(time.Hour)); err != nil {
		t.Fatalf("declare disposable stream: %v", err)
	}
	t.Cleanup(func() {
		ensureIntegrationContainerRunning(t, container)
		if err := environment.DeleteStream(streamName); err != nil {
			t.Errorf("delete disposable stream: %v", err)
		}
		if err := environment.Close(); err != nil {
			t.Errorf("close provisioning environment: %v", err)
		}
	})

	producer, err := OpenProducer(t.Context(), connection, rabbitstream.ProducerConfig{Stream: streamName})
	if err != nil {
		t.Fatalf("OpenProducer() error = %v", err)
	}
	t.Cleanup(func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := producer.Close(closeCtx); err != nil {
			t.Errorf("close producer: %v", err)
		}
	})

	if result, err := producer.Publish(t.Context(), rabbitstream.Message{
		Stream: streamName, Payload: []byte("before restart"),
	}); err != nil || result.State != rabbitstream.DeliveryConfirmed {
		t.Fatalf("publish before restart = %#v, %v", result, err)
	}
	recoveryStarted := time.Now()
	restartIntegrationContainer(t, container)
	waitForIntegrationBroker(t, connection, streamName)
	lossCtx, cancelLoss := context.WithTimeout(t.Context(), 10*time.Second)
	if !observer.WaitFor(lossCtx, rabbitstream.ObservationConnectionLost) {
		cancelLoss()
		t.Fatalf("connection loss was not observed: %#v", observer.Kinds())
	}
	cancelLoss()

	publishCtx, cancelPublish := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancelPublish()
	recovered := false
	var lastResult rabbitstream.DeliveryResult
	var lastErr error
	for attempt := 1; publishCtx.Err() == nil; attempt++ {
		lastResult, lastErr = producer.Publish(publishCtx, rabbitstream.Message{
			Stream: streamName, Payload: []byte("after restart probe " + strconv.Itoa(attempt)),
		})
		if lastErr == nil && lastResult.State == rabbitstream.DeliveryConfirmed {
			recovered = true
			break
		}
		if !errors.Is(lastErr, rabbitstream.ErrPublishAmbiguous) &&
			!errors.Is(lastErr, rabbitstream.ErrConnection) &&
			!errors.Is(lastErr, rabbitstream.ErrTimeout) {
			t.Fatalf("publish recovery probe %d = %#v, %v", attempt, lastResult, lastErr)
		}
		retryTimer := time.NewTimer(100 * time.Millisecond)
		select {
		case <-retryTimer.C:
		case <-publishCtx.Done():
			stopAndDrainTimer(retryTimer)
		}
	}
	if !recovered {
		t.Fatalf("producer did not confirm a bounded recovery probe: %#v, %v", lastResult, lastErr)
	}
	t.Logf("producer restart recovery confirmed in %s", time.Since(recoveryStarted))
	if !observer.HasAll(
		rabbitstream.ObservationConnectionLost,
		rabbitstream.ObservationReconnectAttempt,
		rabbitstream.ObservationConnectionReady,
	) {
		t.Fatalf("restart observations = %#v", observer.Kinds())
	}
}

func TestProducerRecoversAfterNetworkInterruptionAndBoundsReconnectStorm(t *testing.T) {
	proxyAPI := requiredIntegrationEnv(t, "RABBITSTREAM_TEST_PROXY_API")
	proxyName := requiredIntegrationEnv(t, "RABBITSTREAM_TEST_PROXY_NAME")
	connection, environment := integrationBroker(t)
	connection.ConnectTimeout = 3 * time.Second
	connection.MaxReconnectAttempts = 2
	connection.Heartbeat = 3 * time.Second
	observer := &integrationObserver{}
	connection.Observer = observer
	streamName := integrationName("network")
	if err := environment.DeclareStream(streamName, stream.NewStreamOptions().SetMaxAge(time.Hour)); err != nil {
		t.Fatalf("declare disposable stream: %v", err)
	}
	proxyDisabled := false
	t.Cleanup(func() {
		if proxyDisabled {
			setIntegrationProxyEnabled(t, proxyAPI, proxyName, true)
			proxyDisabled = false
			waitForIntegrationBroker(t, connection)
		}
		if err := environment.DeleteStream(streamName); err != nil {
			t.Errorf("delete disposable stream: %v", err)
		}
		if err := environment.Close(); err != nil {
			t.Errorf("close provisioning environment: %v", err)
		}
	})

	producer, err := OpenProducer(t.Context(), connection, rabbitstream.ProducerConfig{Stream: streamName})
	if err != nil {
		t.Fatalf("OpenProducer() error = %v", err)
	}
	t.Cleanup(func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := producer.Close(closeCtx); err != nil {
			t.Errorf("close producer: %v", err)
		}
	})

	setIntegrationProxyEnabled(t, proxyAPI, proxyName, false)
	proxyDisabled = true
	triggerCtx, cancelTrigger := context.WithTimeout(t.Context(), 4*time.Second)
	_, triggerErr := producer.Publish(triggerCtx, rabbitstream.Message{
		Stream: streamName, Payload: []byte("detect outage"),
	})
	cancelTrigger()
	if triggerErr == nil {
		t.Fatal("publish unexpectedly confirmed while broker network was disconnected")
	}
	lossCtx, cancelLoss := context.WithTimeout(t.Context(), 10*time.Second)
	if !observer.WaitFor(lossCtx, rabbitstream.ObservationConnectionLost) {
		cancelLoss()
		t.Fatalf("connection loss was not observed: %#v", observer.Kinds())
	}
	cancelLoss()

	const publishers = 8
	results := make(chan error, publishers)
	for range publishers {
		go func() {
			publishCtx, cancel := context.WithTimeout(t.Context(), 4*time.Second)
			defer cancel()
			_, publishErr := producer.Publish(publishCtx, rabbitstream.Message{
				Stream: streamName, Payload: []byte("during outage"),
			})
			results <- publishErr
		}()
	}
	for range publishers {
		if publishErr := <-results; publishErr == nil {
			t.Fatal("publish unexpectedly confirmed while broker network was disconnected")
		}
	}
	reconnects := observer.Count(rabbitstream.ObservationReconnectAttempt)
	if reconnects == 0 || reconnects > publishers {
		t.Fatalf("reconnect attempts during storm = %d, want 1..%d", reconnects, publishers)
	}

	setIntegrationProxyEnabled(t, proxyAPI, proxyName, true)
	proxyDisabled = false
	waitForIntegrationBroker(t, connection, streamName)
	publishCtx, cancelPublish := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancelPublish()
	if result, err := producer.Publish(publishCtx, rabbitstream.Message{
		Stream: streamName, Payload: []byte("after network recovery"),
	}); err != nil || result.State != rabbitstream.DeliveryConfirmed {
		t.Fatalf("publish after network recovery = %#v, %v", result, err)
	}
}

func TestConsumerReconnectsFromStoredOffsetAfterBrokerRestart(t *testing.T) {
	container := requiredIntegrationEnv(t, "RABBITSTREAM_TEST_RESTART_CONTAINER")
	if !strings.HasPrefix(container, "codex-rabbitstream-") {
		t.Fatal("restart container is not task-owned")
	}
	connection, environment := integrationBroker(t)
	connection.MaxReconnectAttempts = rabbitstream.MaxReconnectAttempts
	producerObserver := &integrationObserver{}
	producerConnection := connection
	producerConnection.Observer = producerObserver
	streamName := integrationName("consumer-restart")
	if err := environment.DeclareStream(streamName, stream.NewStreamOptions().SetMaxAge(time.Hour)); err != nil {
		t.Fatalf("declare disposable stream: %v", err)
	}
	t.Cleanup(func() {
		ensureIntegrationContainerRunning(t, container)
		if err := environment.DeleteStream(streamName); err != nil {
			t.Errorf("delete disposable stream: %v", err)
		}
		if err := environment.Close(); err != nil {
			t.Errorf("close provisioning environment: %v", err)
		}
	})

	producer, err := OpenProducer(t.Context(), producerConnection, rabbitstream.ProducerConfig{
		Stream: streamName,
		Policy: rabbitstream.ProducerPolicy{
			Deduplication: rabbitstream.DeduplicationPublishingID,
			ProducerName:  integrationName("consumer-restart-producer"),
		},
	})
	if err != nil {
		t.Fatalf("OpenProducer() error = %v", err)
	}
	consumerName := integrationName("restart-consumer")
	consumer, err := OpenConsumer(t.Context(), connection, rabbitstream.ConsumerConfig{
		Stream: streamName, ConsumerName: consumerName,
		Start: rabbitstream.StartPosition{Kind: rabbitstream.OffsetStartBeginning},
	})
	if err != nil {
		t.Fatalf("OpenConsumer() error = %v", err)
	}
	runCtx, cancelRun := context.WithCancel(t.Context())
	deliveries := make(chan rabbitstream.Message, 2)
	runDone := make(chan error, 1)
	go func() {
		runDone <- consumer.Run(runCtx, func(_ context.Context, message rabbitstream.Message) error {
			deliveries <- message.Retain()
			return nil
		})
	}()
	t.Cleanup(func() {
		cancelRun()
		closeCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := consumer.Close(closeCtx); err != nil {
			t.Errorf("close consumer: %v", err)
		}
		if err := producer.Close(closeCtx); err != nil {
			t.Errorf("close producer: %v", err)
		}
	})

	if result, err := producer.Publish(t.Context(), rabbitstream.Message{
		Stream: streamName, Payload: []byte("before restart"), PublishingID: 1, HasPublishingID: true,
	}); err != nil || result.State != rabbitstream.DeliveryConfirmed {
		t.Fatalf("publish before restart = %#v, %v", result, err)
	}
	first := <-deliveries
	inspector, err := NewInspector(connection, rabbitstream.DefaultLimits())
	if err != nil {
		t.Fatalf("NewInspector() error = %v", err)
	}
	waitForStoredOffset(t, inspector, streamName, consumerName, first.Offset)

	restartIntegrationContainer(t, container)
	waitForIntegrationBroker(t, connection, streamName)
	lossCtx, cancelLoss := context.WithTimeout(t.Context(), 10*time.Second)
	if !producerObserver.WaitFor(lossCtx, rabbitstream.ObservationConnectionLost) {
		cancelLoss()
		t.Fatalf("producer connection loss was not observed: %#v", producerObserver.Kinds())
	}
	cancelLoss()
	publishCtx, cancelPublish := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancelPublish()
	recovered := false
	var lastResult rabbitstream.DeliveryResult
	var lastErr error
	for attempt := 1; publishCtx.Err() == nil; attempt++ {
		lastResult, lastErr = producer.Publish(publishCtx, rabbitstream.Message{
			Stream: streamName, Payload: []byte("after restart"), PublishingID: 2, HasPublishingID: true,
		})
		if lastErr == nil && lastResult.State == rabbitstream.DeliveryConfirmed {
			recovered = true
			break
		}
		if !errors.Is(lastErr, rabbitstream.ErrPublishAmbiguous) &&
			!errors.Is(lastErr, rabbitstream.ErrConnection) &&
			!errors.Is(lastErr, rabbitstream.ErrTimeout) {
			t.Fatalf("publish recovery probe %d = %#v, %v", attempt, lastResult, lastErr)
		}
		retryTimer := time.NewTimer(100 * time.Millisecond)
		select {
		case <-retryTimer.C:
		case <-publishCtx.Done():
			stopAndDrainTimer(retryTimer)
		}
	}
	if !recovered {
		t.Fatalf("producer did not confirm the deduplicated recovery publish: %#v, %v", lastResult, lastErr)
	}
	redeliveries := 0
	for {
		select {
		case delivery := <-deliveries:
			if string(delivery.Payload) == "after restart" {
				if delivery.Offset <= first.Offset {
					t.Fatalf("new delivery offset = %d, first offset = %d", delivery.Offset, first.Offset)
				}
				if redeliveries > 1 {
					t.Fatalf("restart produced %d redeliveries", redeliveries)
				}
				return
			}
			if delivery.Offset != first.Offset || string(delivery.Payload) != "before restart" {
				t.Fatalf("unexpected restart redelivery = %#v", delivery)
			}
			redeliveries++
		case err := <-runDone:
			t.Fatalf("consumer stopped during restart: %v", err)
		case <-publishCtx.Done():
			t.Fatalf("consumer did not recover: %v", publishCtx.Err())
		}
	}
}

func TestConsumerCatchesUpBacklogAcrossApplicationRestart(t *testing.T) {
	connection, environment := integrationBroker(t)
	streamName := integrationName("backlog")
	if err := environment.DeclareStream(streamName, stream.NewStreamOptions().SetMaxAge(time.Hour)); err != nil {
		t.Fatalf("declare backlog stream: %v", err)
	}
	t.Cleanup(func() {
		if err := environment.DeleteStream(streamName); err != nil {
			t.Errorf("delete backlog stream: %v", err)
		}
		if err := environment.Close(); err != nil {
			t.Errorf("close backlog provisioning environment: %v", err)
		}
	})

	producer, err := OpenProducer(t.Context(), connection, rabbitstream.ProducerConfig{Stream: streamName})
	if err != nil {
		t.Fatalf("OpenProducer() error = %v", err)
	}
	for offset := 0; offset < 1000; offset++ {
		result, publishErr := producer.Publish(t.Context(), rabbitstream.Message{
			Stream: streamName, Payload: []byte(strconv.Itoa(offset)),
		})
		if publishErr != nil || result.State != rabbitstream.DeliveryConfirmed {
			t.Fatalf("Publish(%d) = %#v, %v", offset, result, publishErr)
		}
	}
	closeCtx, cancelProducerClose := context.WithTimeout(context.Background(), 10*time.Second)
	if err := producer.Close(closeCtx); err != nil {
		cancelProducerClose()
		t.Fatalf("close backlog producer: %v", err)
	}
	cancelProducerClose()

	consumerName := integrationName("rolling-consumer")
	first, err := OpenConsumer(t.Context(), connection, rabbitstream.ConsumerConfig{
		Stream: streamName, ConsumerName: consumerName,
		Start: rabbitstream.StartPosition{Kind: rabbitstream.OffsetStartBeginning},
	})
	if err != nil {
		t.Fatalf("OpenConsumer(first) error = %v", err)
	}
	firstCtx, cancelFirst := context.WithCancel(t.Context())
	firstBlocked := make(chan struct{})
	firstDone := make(chan error, 1)
	go func() {
		firstDone <- first.Run(firstCtx, func(ctx context.Context, message rabbitstream.Message) error {
			if message.Offset == 499 {
				close(firstBlocked)
				<-ctx.Done()
				return ctx.Err()
			}
			return nil
		})
	}()
	handoffTimer := time.NewTimer(30 * time.Second)
	defer handoffTimer.Stop()
	select {
	case <-firstBlocked:
	case <-handoffTimer.C:
		t.Fatal("first application instance did not reach backlog handoff")
	}
	inspector, err := NewInspector(connection, rabbitstream.DefaultLimits())
	if err != nil {
		t.Fatalf("NewInspector() error = %v", err)
	}
	waitForStoredOffset(t, inspector, streamName, consumerName, 498)
	cancelFirst()
	if err := <-firstDone; !errors.Is(err, rabbitstream.ErrCanceled) {
		t.Fatalf("first Run() error = %v", err)
	}
	firstCloseCtx, cancelFirstClose := context.WithTimeout(context.Background(), 10*time.Second)
	if err := first.Close(firstCloseCtx); err != nil {
		cancelFirstClose()
		t.Fatalf("close first consumer: %v", err)
	}
	cancelFirstClose()

	second, err := OpenConsumer(t.Context(), connection, rabbitstream.ConsumerConfig{
		Stream: streamName, ConsumerName: consumerName,
		Start: rabbitstream.StartPosition{Kind: rabbitstream.OffsetStartStored},
	})
	if err != nil {
		t.Fatalf("OpenConsumer(second) error = %v", err)
	}
	secondCtx, cancelSecond := context.WithCancel(t.Context())
	secondDone := make(chan error, 1)
	secondOffsets := make(chan uint64, 502)
	catchupStarted := time.Now()
	go func() {
		secondDone <- second.Run(secondCtx, func(_ context.Context, message rabbitstream.Message) error {
			secondOffsets <- message.Offset
			return nil
		})
	}()
	var observed []uint64
	catchupTimer := time.NewTimer(30 * time.Second)
	defer catchupTimer.Stop()
	for {
		select {
		case offset := <-secondOffsets:
			observed = append(observed, offset)
			if offset == 999 {
				goto caughtUp
			}
		case <-catchupTimer.C:
			t.Fatal("second application instance did not catch up backlog")
		}
	}

caughtUp:
	waitForStoredOffset(t, inspector, streamName, consumerName, 999)
	t.Logf("consumer backlog catch-up completed in %s", time.Since(catchupStarted))
	cancelSecond()
	if err := <-secondDone; !errors.Is(err, rabbitstream.ErrCanceled) {
		t.Fatalf("second Run() error = %v", err)
	}
	secondCloseCtx, cancelSecondClose := context.WithTimeout(context.Background(), 10*time.Second)
	if err := second.Close(secondCloseCtx); err != nil {
		cancelSecondClose()
		t.Fatalf("close second consumer: %v", err)
	}
	cancelSecondClose()
	if len(observed) < 501 || observed[0] < 498 || observed[0] > 499 {
		t.Fatalf("restart offsets begin = %#v, count = %d", observed[:min(len(observed), 3)], len(observed))
	}
	for index := 1; index < len(observed); index++ {
		if observed[index] != observed[index-1]+1 {
			t.Fatalf("restart offsets are not contiguous at %d: %d then %d", index, observed[index-1], observed[index])
		}
	}
}

func TestInitialConnectionRotatesEndpoints(t *testing.T) {
	connection, environment := integrationBroker(t)
	streamName := integrationName("endpoint-rotation")
	if err := environment.DeclareStream(streamName, stream.NewStreamOptions().SetMaxAge(time.Hour)); err != nil {
		t.Fatalf("declare disposable stream: %v", err)
	}
	t.Cleanup(func() {
		if err := environment.DeleteStream(streamName); err != nil {
			t.Errorf("delete disposable stream: %v", err)
		}
		if err := environment.Close(); err != nil {
			t.Errorf("close provisioning environment: %v", err)
		}
	})
	connection.Endpoints = append(
		[]rabbitstream.Endpoint{{Host: connection.Endpoints[0].Host, Port: 1}},
		connection.Endpoints...,
	)

	producer, err := OpenProducer(t.Context(), connection, rabbitstream.ProducerConfig{Stream: streamName})
	if err != nil {
		t.Fatalf("OpenProducer() error = %v", err)
	}
	closeCtx, cancelClose := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelClose()
	if err := producer.Close(closeCtx); err != nil {
		t.Fatalf("close producer: %v", err)
	}
}

func TestInitialConnectionReresolvesCredentialsAfterAuthenticationFailure(t *testing.T) {
	connection, environment := integrationBroker(t)
	streamName := integrationName("credential-rotation")
	if err := environment.DeclareStream(streamName, stream.NewStreamOptions().SetMaxAge(time.Hour)); err != nil {
		t.Fatalf("declare disposable stream: %v", err)
	}
	t.Cleanup(func() {
		if err := environment.DeleteStream(streamName); err != nil {
			t.Errorf("delete disposable stream: %v", err)
		}
		if err := environment.Close(); err != nil {
			t.Errorf("close provisioning environment: %v", err)
		}
	})
	valid, err := connection.Credentials.Credentials(t.Context())
	if err != nil {
		t.Fatalf("resolve valid credentials: %v", err)
	}
	provider := &integrationCredentialSequence{credentials: []rabbitstream.Credentials{
		{Username: valid.Username, Password: []byte("intentionally-invalid")},
		valid,
	}}
	connection.Credentials = provider

	producer, err := OpenProducer(t.Context(), connection, rabbitstream.ProducerConfig{Stream: streamName})
	if err != nil {
		t.Fatalf("OpenProducer() error = %v", err)
	}
	if provider.Calls() != 2 {
		t.Fatalf("credential resolutions = %d, want 2", provider.Calls())
	}
	closeCtx, cancelClose := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelClose()
	if err := producer.Close(closeCtx); err != nil {
		t.Fatalf("close producer: %v", err)
	}
}

func TestClusterSuperStreamPreservesPerPartitionOrdering(t *testing.T) {
	connection, environment := integrationClusterBroker(t)
	superStream := integrationName("super")
	if err := environment.DeclareSuperStream(
		superStream,
		stream.NewPartitionsOptions(3).SetMaxAge(time.Hour).SetBalancedLeaderLocator(),
	); err != nil {
		t.Fatalf("declare disposable super stream: %v", err)
	}
	t.Cleanup(func() {
		if err := environment.DeleteSuperStream(superStream); err != nil {
			t.Errorf("delete disposable super stream: %v", err)
		}
		if err := environment.Close(); err != nil {
			t.Errorf("close provisioning environment: %v", err)
		}
	})
	partitions, err := environment.QueryPartitions(superStream)
	if err != nil || len(partitions) != 3 {
		t.Fatalf("super stream partitions = %#v, %v", partitions, err)
	}
	keys := routingKeysForPartitions(t, partitions)

	producer, err := OpenProducer(t.Context(), connection, rabbitstream.ProducerConfig{
		SuperStream: superStream, ExpectedPartitions: len(partitions),
	})
	if err != nil {
		t.Fatalf("OpenProducer() error = %v", err)
	}
	consumer, err := OpenConsumer(t.Context(), connection, rabbitstream.ConsumerConfig{
		SuperStream: superStream, ConsumerName: integrationName("super-consumer"),
		Start:  rabbitstream.StartPosition{Kind: rabbitstream.OffsetStartBeginning},
		Policy: rabbitstream.ConsumerPolicy{MaxConcurrency: len(partitions)},
	})
	if err != nil {
		t.Fatalf("OpenConsumer() error = %v", err)
	}
	runCtx, cancelRun := context.WithCancel(t.Context())
	deliveries := make(chan rabbitstream.Message, 4)
	runDone := make(chan error, 1)
	go func() {
		runDone <- consumer.Run(runCtx, func(_ context.Context, message rabbitstream.Message) error {
			deliveries <- message.Retain()
			return nil
		})
	}()
	t.Cleanup(func() {
		cancelRun()
		closeCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := consumer.Close(closeCtx); err != nil {
			t.Errorf("close consumer: %v", err)
		}
		if err := producer.Close(closeCtx); err != nil {
			t.Errorf("close producer: %v", err)
		}
	})

	publish := func(key, payload string) {
		t.Helper()
		result, publishErr := producer.Publish(t.Context(), rabbitstream.Message{
			SuperStream: superStream, RoutingKey: key, Payload: []byte(payload),
		})
		if publishErr != nil || result.State != rabbitstream.DeliveryConfirmed {
			t.Fatalf("publish %q = %#v, %v", payload, result, publishErr)
		}
	}
	publish(keys[partitions[0]], "partition-0-first")
	publish(keys[partitions[1]], "partition-1")
	publish(keys[partitions[0]], "partition-0-second")
	publish(keys[partitions[2]], "partition-2")

	seen := make(map[string][]rabbitstream.Message, len(partitions))
	for range 4 {
		select {
		case delivery := <-deliveries:
			seen[delivery.Partition] = append(seen[delivery.Partition], delivery)
		case err := <-runDone:
			t.Fatalf("consumer stopped: %v", err)
		case <-time.After(10 * time.Second):
			t.Fatal("timed out waiting for Super Stream deliveries")
		}
	}
	ordered := seen[partitions[0]]
	if len(ordered) != 2 || ordered[0].Offset >= ordered[1].Offset ||
		string(ordered[0].Payload) != "partition-0-first" ||
		string(ordered[1].Payload) != "partition-0-second" {
		t.Fatalf("ordered partition deliveries = %#v", ordered)
	}
	for _, partition := range partitions[1:] {
		if len(seen[partition]) != 1 {
			t.Fatalf("partition %q deliveries = %#v", partition, seen[partition])
		}
	}
}

func TestReplayRejectsChangedSuperStreamTopology(t *testing.T) {
	connection, environment := integrationClusterBroker(t)
	superStream := integrationName("replay-topology")
	if err := environment.DeclareSuperStream(
		superStream,
		stream.NewPartitionsOptions(3).SetMaxAge(time.Hour),
	); err != nil {
		t.Fatalf("declare disposable Super Stream: %v", err)
	}
	partitions, err := environment.QueryPartitions(superStream)
	if err != nil || len(partitions) != 3 {
		t.Fatalf("query Super Stream partitions = %#v, %v", partitions, err)
	}
	t.Cleanup(func() {
		if err := environment.DeleteSuperStream(superStream); err != nil {
			t.Errorf("delete disposable Super Stream: %v", err)
		}
		if err := environment.Close(); err != nil {
			t.Errorf("close cluster provisioning environment: %v", err)
		}
	})

	replayer, err := NewReplayer(connection, rabbitstream.DefaultLimits())
	if err != nil {
		t.Fatalf("NewReplayer() error = %v", err)
	}
	expected := append([]string(nil), partitions...)
	expected[2] = integrationName("removed-partition")
	_, err = replayer.Inspect(t.Context(), rabbitstream.ReplayRequest{
		SuperStream: superStream, Partition: partitions[0], ExpectedPartitions: expected,
		Start: rabbitstream.StartPosition{Kind: rabbitstream.OffsetStartBeginning},
	})
	if !errors.Is(err, rabbitstream.ErrPartitionUnavailable) {
		t.Fatalf("Inspect() error = %v, want changed-topology category", err)
	}
}

func TestClusterProducerRecoversAfterLeaderFailure(t *testing.T) {
	connection, environment := integrationClusterBroker(t)
	streamName := integrationName("leader-failure")
	if err := environment.DeclareStream(
		streamName,
		stream.NewStreamOptions().SetMaxAge(time.Hour),
	); err != nil {
		t.Fatalf("declare replicated stream: %v", err)
	}
	t.Cleanup(func() {
		if err := environment.DeleteStream(streamName); err != nil {
			t.Errorf("delete disposable stream: %v", err)
		}
		if err := environment.Close(); err != nil {
			t.Errorf("close provisioning environment: %v", err)
		}
	})
	metadata := waitForStreamReplicas(t, connection, streamName, 2)
	containers := integrationClusterContainers(t)
	leaderContainer := containers[metadata.Leader.Port]
	if leaderContainer == "" || !strings.HasPrefix(leaderContainer, "codex-rabbitstream-") {
		t.Fatalf("leader port %q is not mapped to a task-owned container", metadata.Leader.Port)
	}

	producer, err := OpenProducer(t.Context(), connection, rabbitstream.ProducerConfig{Stream: streamName})
	if err != nil {
		t.Fatalf("OpenProducer() error = %v", err)
	}
	t.Cleanup(func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := producer.Close(closeCtx); err != nil {
			t.Errorf("close producer: %v", err)
		}
	})
	if result, err := producer.Publish(t.Context(), rabbitstream.Message{
		Stream: streamName, Payload: []byte("before leader failure"),
	}); err != nil || result.State != rabbitstream.DeliveryConfirmed {
		t.Fatalf("publish before leader failure = %#v, %v", result, err)
	}

	stopIntegrationContainer(t, leaderContainer)
	t.Cleanup(func() { ensureIntegrationContainerRunning(t, leaderContainer) })
	newLeader := waitForStreamLeaderChange(t, connection, streamName, metadata.Leader.Port)
	if newLeader == metadata.Leader.Port {
		t.Fatalf("stream leader did not change from port %s", metadata.Leader.Port)
	}
	publishCtx, cancelPublish := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancelPublish()
	if result, err := producer.Publish(publishCtx, rabbitstream.Message{
		Stream: streamName, Payload: []byte("after leader failure"),
	}); err != nil || result.State != rabbitstream.DeliveryConfirmed {
		t.Fatalf("publish after leader failure = %#v, %v", result, err)
	}
	ensureIntegrationContainerRunning(t, leaderContainer)
	waitForIntegrationEndpoint(t, connection, metadata.Leader.Port)
}

func TestClusterPublishContinuesDuringReplicaFailure(t *testing.T) {
	connection, environment := integrationClusterBroker(t)
	streamName := integrationName("replica-failure")
	if err := environment.DeclareStream(streamName, stream.NewStreamOptions().SetMaxAge(time.Hour)); err != nil {
		t.Fatalf("declare replicated stream: %v", err)
	}
	t.Cleanup(func() {
		if err := environment.DeleteStream(streamName); err != nil {
			t.Errorf("delete disposable stream: %v", err)
		}
		if err := environment.Close(); err != nil {
			t.Errorf("close provisioning environment: %v", err)
		}
	})
	metadata := waitForStreamReplicas(t, connection, streamName, 2)
	containers := integrationClusterContainers(t)
	replicaPort := metadata.Replicas[0].Port
	replicaContainer := containers[replicaPort]
	if replicaContainer == "" || !strings.HasPrefix(replicaContainer, "codex-rabbitstream-") {
		t.Fatalf("replica port %q is not mapped to a task-owned container", replicaPort)
	}
	producer, err := OpenProducer(t.Context(), connection, rabbitstream.ProducerConfig{Stream: streamName})
	if err != nil {
		t.Fatalf("OpenProducer() error = %v", err)
	}
	t.Cleanup(func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := producer.Close(closeCtx); err != nil {
			t.Errorf("close producer: %v", err)
		}
	})

	stopIntegrationContainer(t, replicaContainer)
	t.Cleanup(func() { ensureIntegrationContainerRunning(t, replicaContainer) })
	publishCtx, cancelPublish := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancelPublish()
	if result, err := producer.Publish(publishCtx, rabbitstream.Message{
		Stream: streamName, Payload: []byte("replica unavailable"),
	}); err != nil || result.State != rabbitstream.DeliveryConfirmed {
		t.Fatalf("publish during replica failure = %#v, %v", result, err)
	}
	ensureIntegrationContainerRunning(t, replicaContainer)
	waitForIntegrationEndpoint(t, connection, replicaPort)
}

func integrationBroker(t *testing.T) (rabbitstream.ConnectionConfig, *stream.Environment) {
	t.Helper()
	host := requiredIntegrationEnv(t, "RABBITSTREAM_TEST_HOST")
	portText := requiredIntegrationEnv(t, "RABBITSTREAM_TEST_PORT")
	username := requiredIntegrationEnv(t, "RABBITSTREAM_TEST_USER")
	password := requiredIntegrationEnv(t, "RABBITSTREAM_TEST_PASSWORD")
	port, err := strconv.ParseUint(portText, 10, 16)
	if err != nil || port == 0 {
		t.Fatal("RABBITSTREAM_TEST_PORT is invalid")
	}
	connection := rabbitstream.ConnectionConfig{
		Endpoints:            []rabbitstream.Endpoint{{Host: host, Port: uint16(port)}},
		Credentials:          rabbitstream.StaticCredentials(username, []byte(password)),
		Security:             rabbitstream.DevelopmentPlaintextSecurity(),
		ConnectTimeout:       10 * time.Second,
		RPCTimeout:           10 * time.Second,
		MaxReconnectAttempts: 2,
	}
	environment, err := stream.NewEnvironment(
		stream.NewEnvironmentOptions().
			SetHost(host).
			SetPort(int(port)).
			SetUser(username).
			SetPassword(password).
			SetRPCTimeout(10 * time.Second),
	)
	if err != nil {
		t.Fatalf("open provisioning environment: %v", err)
	}
	return connection, environment
}

func integrationTLSBroker(t *testing.T) (rabbitstream.ConnectionConfig, *stream.Environment) {
	t.Helper()
	host := requiredIntegrationEnv(t, "RABBITSTREAM_TLS_HOST")
	portText := requiredIntegrationEnv(t, "RABBITSTREAM_TLS_PORT")
	username := requiredIntegrationEnv(t, "RABBITSTREAM_TLS_USER")
	password := requiredIntegrationEnv(t, "RABBITSTREAM_TLS_PASSWORD")
	port, err := strconv.ParseUint(portText, 10, 16)
	if err != nil || port == 0 {
		t.Fatal("RABBITSTREAM_TEST_PORT is invalid")
	}
	runtimeDirectory := requiredIntegrationEnv(t, "RABBITSTREAM_TLS_RUNTIME")
	caPEM, err := os.ReadFile(filepath.Join(runtimeDirectory, "ca.pem"))
	if err != nil {
		t.Fatalf("read integration CA: %v", err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(caPEM) {
		t.Fatal("integration CA is invalid")
	}
	certificate, err := tls.LoadX509KeyPair(
		filepath.Join(runtimeDirectory, "client.pem"),
		filepath.Join(runtimeDirectory, "client-key.pem"),
	)
	if err != nil {
		t.Fatalf("load integration client certificate: %v", err)
	}
	tlsConfig := &tls.Config{
		MinVersion:   tls.VersionTLS13,
		RootCAs:      roots,
		Certificates: []tls.Certificate{certificate},
	}
	connection := rabbitstream.ConnectionConfig{
		Endpoints:            []rabbitstream.Endpoint{{Host: host, Port: uint16(port)}},
		Credentials:          rabbitstream.StaticCredentials(username, []byte(password)),
		Security:             rabbitstream.SecurityConfig{TLS: tlsConfig},
		ConnectTimeout:       10 * time.Second,
		RPCTimeout:           10 * time.Second,
		MaxReconnectAttempts: 2,
	}
	provisioningTLS := tlsConfig.Clone()
	provisioningTLS.ServerName = host
	environment, err := stream.NewEnvironment(
		stream.NewEnvironmentOptions().
			SetHost(host).
			SetPort(int(port)).
			SetUser(username).
			SetPassword(password).
			SetRPCTimeout(10 * time.Second).
			IsTLS(true).
			SetTLSConfig(provisioningTLS),
	)
	if err != nil {
		t.Fatalf("open TLS provisioning environment: %v", err)
	}
	return connection, environment
}

func integrationClusterBroker(t *testing.T) (rabbitstream.ConnectionConfig, *stream.Environment) {
	t.Helper()
	host := requiredIntegrationEnv(t, "RABBITSTREAM_TEST_HOST")
	portTexts := strings.Split(requiredIntegrationEnv(t, "RABBITSTREAM_CLUSTER_PORTS"), ",")
	username := requiredIntegrationEnv(t, "RABBITSTREAM_TEST_USER")
	password := requiredIntegrationEnv(t, "RABBITSTREAM_TEST_PASSWORD")
	endpoints := make([]rabbitstream.Endpoint, 0, len(portTexts))
	for _, portText := range portTexts {
		port, err := strconv.ParseUint(strings.TrimSpace(portText), 10, 16)
		if err != nil || port == 0 {
			t.Fatal("RABBITSTREAM_CLUSTER_PORTS is invalid")
		}
		endpoints = append(endpoints, rabbitstream.Endpoint{Host: host, Port: uint16(port)})
	}
	if len(endpoints) != 3 {
		t.Fatal("RABBITSTREAM_CLUSTER_PORTS must contain exactly three ports")
	}
	connection := rabbitstream.ConnectionConfig{
		Endpoints: endpoints, Credentials: rabbitstream.StaticCredentials(username, []byte(password)),
		Security: rabbitstream.DevelopmentPlaintextSecurity(), ConnectTimeout: 15 * time.Second,
		RPCTimeout: 10 * time.Second, MaxReconnectAttempts: 3,
	}
	environment, err := stream.NewEnvironment(
		stream.NewEnvironmentOptions().
			SetHost(host).
			SetPort(int(endpoints[0].Port)).
			SetUser(username).
			SetPassword(password).
			SetRPCTimeout(10 * time.Second),
	)
	if err != nil {
		t.Fatalf("open cluster provisioning environment: %v", err)
	}
	return connection, environment
}

func routingKeysForPartitions(t *testing.T, partitions []string) map[string]string {
	t.Helper()
	keys := make(map[string]string, len(partitions))
	for index := 0; len(keys) < len(partitions) && index < 100_000; index++ {
		key := "routing-" + strconv.Itoa(index)
		partition, err := hashPartition(key, partitions)
		if err != nil {
			t.Fatalf("hash routing key: %v", err)
		}
		if _, exists := keys[partition]; !exists {
			keys[partition] = key
		}
	}
	if len(keys) != len(partitions) {
		t.Fatalf("could not find routing keys for partitions %#v", partitions)
	}
	return keys
}

func requiredIntegrationEnv(t testing.TB, name string) string {
	t.Helper()
	value := os.Getenv(name)
	if value == "" {
		t.Fatalf("%s is required", name)
	}
	return value
}

func integrationName(role string) string {
	return "codex-rabbitstream-" + role + "-" + strconv.FormatInt(time.Now().UnixNano(), 36)
}

type integrationObserver struct {
	mutex sync.Mutex
	kinds []rabbitstream.ObservationKind
}

func (observer *integrationObserver) Observe(observation rabbitstream.Observation) {
	observer.mutex.Lock()
	defer observer.mutex.Unlock()
	observer.kinds = append(observer.kinds, observation.Kind)
}

func (observer *integrationObserver) Kinds() []rabbitstream.ObservationKind {
	observer.mutex.Lock()
	defer observer.mutex.Unlock()
	return append([]rabbitstream.ObservationKind(nil), observer.kinds...)
}

func (observer *integrationObserver) HasAll(expected ...rabbitstream.ObservationKind) bool {
	kinds := observer.Kinds()
	for _, want := range expected {
		found := false
		for _, kind := range kinds {
			if kind == want {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

func (observer *integrationObserver) Count(expected rabbitstream.ObservationKind) int {
	kinds := observer.Kinds()
	count := 0
	for _, kind := range kinds {
		if kind == expected {
			count++
		}
	}
	return count
}

func (observer *integrationObserver) WaitFor(ctx context.Context, expected rabbitstream.ObservationKind) bool {
	poll := time.NewTicker(10 * time.Millisecond)
	defer poll.Stop()
	for {
		if observer.Count(expected) > 0 {
			return true
		}
		select {
		case <-poll.C:
		case <-ctx.Done():
			return false
		}
	}
}

func restartIntegrationContainer(t *testing.T, container string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	if err := exec.CommandContext(ctx, "docker", "restart", "--time", "1", container).Run(); err != nil {
		t.Fatalf("restart task-owned broker: %v", err)
	}
}

func setIntegrationProxyEnabled(t *testing.T, api string, name string, enabled bool) {
	t.Helper()
	parsed, err := url.Parse(api)
	if err != nil || parsed.Scheme != "http" || parsed.Path != "" ||
		(parsed.Hostname() != "127.0.0.1" && parsed.Hostname() != "localhost") ||
		parsed.Port() != "18474" || name != "rabbitstream" {
		t.Fatal("network-interruption proxy must be the exact task-owned loopback fixture")
	}
	body := `{"enabled":false}`
	if enabled {
		body = `{"enabled":true}`
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	request, err := http.NewRequestWithContext(
		ctx, http.MethodPost, api+"/proxies/"+url.PathEscape(name), strings.NewReader(body),
	)
	if err != nil {
		t.Fatalf("create proxy control request: %v", err)
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatalf("control task-owned network proxy: %v", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("proxy control status = %d", response.StatusCode)
	}
}

func stopIntegrationContainer(t *testing.T, container string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	if err := exec.CommandContext(ctx, "docker", "stop", "--time", "1", container).Run(); err != nil {
		t.Fatalf("stop task-owned broker: %v", err)
	}
}

func ensureIntegrationContainerRunning(t *testing.T, container string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := exec.CommandContext(ctx, "docker", "start", container).Run(); err != nil {
		t.Errorf("restore task-owned broker: %v", err)
	}
}

func waitForIntegrationBroker(
	t *testing.T,
	connection rabbitstream.ConnectionConfig,
	streams ...string,
) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	credentials, err := connection.Credentials.Credentials(ctx)
	if err != nil {
		t.Fatalf("resolve integration credentials: %v", err)
	}
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	endpoint := connection.Endpoints[0]
	consecutiveReady := 0
	for {
		environment, err := stream.NewEnvironment(
			stream.NewEnvironmentOptions().
				SetHost(endpoint.Host).
				SetPort(int(endpoint.Port)).
				SetUser(credentials.Username).
				SetPassword(string(credentials.Password)).
				SetRPCTimeout(time.Second),
		)
		if err == nil {
			for _, streamName := range streams {
				metadata, metadataErr := environment.StreamMetaData(streamName)
				if metadataErr != nil {
					err = metadataErr
					break
				}
				if metadata.Leader == nil {
					err = errors.New("stream leader is unavailable")
					break
				}
			}
			_ = environment.Close()
			if err == nil {
				consecutiveReady++
				if consecutiveReady == 3 {
					return
				}
			} else {
				consecutiveReady = 0
			}
		} else {
			consecutiveReady = 0
		}
		select {
		case <-ctx.Done():
			t.Fatalf("broker protocol did not recover: %v (last error %v)", ctx.Err(), err)
		case <-ticker.C:
		}
	}
}

func waitForIntegrationEndpoint(
	t *testing.T,
	connection rabbitstream.ConnectionConfig,
	port string,
) {
	t.Helper()
	parsed, err := strconv.ParseUint(port, 10, 16)
	if err != nil || parsed == 0 {
		t.Fatalf("integration endpoint port %q is invalid", port)
	}
	selected := connection
	selected.Endpoints = []rabbitstream.Endpoint{{
		Host: connection.Endpoints[0].Host, Port: uint16(parsed),
	}}
	waitForIntegrationBroker(t, selected)
}

func waitForStreamLeaderChange(
	t *testing.T,
	connection rabbitstream.ConnectionConfig,
	streamName string,
	previousPort string,
) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	credentials, err := connection.Credentials.Credentials(ctx)
	if err != nil {
		t.Fatalf("resolve integration credentials: %v", err)
	}
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		for _, endpoint := range connection.Endpoints {
			environment, openErr := stream.NewEnvironment(
				stream.NewEnvironmentOptions().
					SetHost(endpoint.Host).
					SetPort(int(endpoint.Port)).
					SetUser(credentials.Username).
					SetPassword(string(credentials.Password)).
					SetRPCTimeout(time.Second),
			)
			if openErr != nil {
				continue
			}
			metadata, metadataErr := environment.StreamMetaData(streamName)
			_ = environment.Close()
			if metadataErr == nil && metadata.Leader != nil && metadata.Leader.Port != previousPort {
				return metadata.Leader.Port
			}
		}
		select {
		case <-ctx.Done():
			t.Fatalf("stream leader did not change: %v", ctx.Err())
		case <-ticker.C:
		}
	}
}

func waitForStreamReplicas(
	t *testing.T,
	connection rabbitstream.ConnectionConfig,
	streamName string,
	want int,
) *stream.StreamMetadata {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	credentials, err := connection.Credentials.Credentials(ctx)
	if err != nil {
		t.Fatalf("resolve integration credentials: %v", err)
	}
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		for _, endpoint := range connection.Endpoints {
			environment, openErr := stream.NewEnvironment(
				stream.NewEnvironmentOptions().
					SetHost(endpoint.Host).
					SetPort(int(endpoint.Port)).
					SetUser(credentials.Username).
					SetPassword(string(credentials.Password)).
					SetRPCTimeout(time.Second),
			)
			if openErr != nil {
				continue
			}
			metadata, metadataErr := environment.StreamMetaData(streamName)
			_ = environment.Close()
			if metadataErr == nil && metadata.Leader != nil && len(metadata.Replicas) == want {
				return metadata
			}
		}
		select {
		case <-ctx.Done():
			t.Fatalf("stream did not reach %d replicas: %v", want, ctx.Err())
		case <-ticker.C:
		}
	}
}

func integrationClusterContainers(t *testing.T) map[string]string {
	t.Helper()
	entries := strings.Split(requiredIntegrationEnv(t, "RABBITSTREAM_CLUSTER_CONTAINERS"), ",")
	containers := make(map[string]string, len(entries))
	for _, entry := range entries {
		port, container, ok := strings.Cut(entry, "=")
		if !ok || strings.TrimSpace(port) == "" || strings.TrimSpace(container) == "" {
			t.Fatal("RABBITSTREAM_CLUSTER_CONTAINERS is invalid")
		}
		containers[strings.TrimSpace(port)] = strings.TrimSpace(container)
	}
	return containers
}

func waitForStoredOffset(
	t *testing.T,
	inspector *Inspector,
	streamName string,
	consumerName string,
	want uint64,
) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		inspection, err := inspector.Inspect(ctx, rabbitstream.InspectionRequest{
			Stream: streamName, ConsumerName: consumerName,
		})
		if err == nil && len(inspection.Partitions) == 1 &&
			inspection.Partitions[0].StoredOffset != nil &&
			*inspection.Partitions[0].StoredOffset == want {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("stored offset was not observable: %v", err)
		case <-ticker.C:
		}
	}
}

type integrationCredentialSequence struct {
	mutex       sync.Mutex
	credentials []rabbitstream.Credentials
	calls       int
}

func (provider *integrationCredentialSequence) Credentials(context.Context) (rabbitstream.Credentials, error) {
	provider.mutex.Lock()
	defer provider.mutex.Unlock()
	index := provider.calls
	if index >= len(provider.credentials) {
		index = len(provider.credentials) - 1
	}
	provider.calls++
	credentials := provider.credentials[index]
	credentials.Password = append([]byte(nil), credentials.Password...)
	return credentials, nil
}

func (provider *integrationCredentialSequence) Calls() int {
	provider.mutex.Lock()
	defer provider.mutex.Unlock()
	return provider.calls
}
