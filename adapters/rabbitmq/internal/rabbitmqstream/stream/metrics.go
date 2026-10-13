package stream

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	semconv "go.opentelemetry.io/otel/semconv/v1.39.0"
)

type streamMetrics struct {
	// Instrument fields are assigned before this object is shared and then read
	// concurrently. Provider implementations own instrument concurrency; do not
	// introduce runtime replacement without a synchronization owner.
	connections                     metric.Int64UpDownCounter
	publishedMessages               metric.Int64Counter
	confirmedMessages               metric.Int64Counter
	erroredMessages                 metric.Int64Counter
	chunks                          metric.Int64Counter
	chunkSize                       metric.Int64Histogram
	consumedMessages                metric.Int64Counter
	outstandingPublishConfirmations metric.Int64UpDownCounter
}

// The recommendation in OTEL package go-doc is to use the package name as the meter name.
// https://pkg.go.dev/go.opentelemetry.io/otel/metric@v1.39.0#MeterProvider

// RabbitMQStreamClientMeterName is the name of the meter for the RabbitMQ Stream Go Client.
const RabbitMQStreamClientMeterName = "github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"

func newStreamMetrics(mp metric.MeterProvider) (*streamMetrics, error) {
	m := mp.Meter(RabbitMQStreamClientMeterName,
		metric.WithInstrumentationAttributes(semconv.MessagingSystemRabbitMQ),
	)
	streamMetrics := new(streamMetrics)
	var err error
	streamMetrics.connections, err = m.Int64UpDownCounter(
		"rabbitmq.stream.connections",
		metric.WithDescription("Number of active connections"),
		metric.WithUnit("{connection}"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create connections metric: %w", err)
	}
	streamMetrics.publishedMessages, err = m.Int64Counter(
		"rabbitmq.stream.published",
		metric.WithDescription("Number of messages published"),
		metric.WithUnit("{message}"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create published messages metric: %w", err)
	}
	streamMetrics.confirmedMessages, err = m.Int64Counter(
		"rabbitmq.stream.confirmed",
		metric.WithDescription("Number of messages confirmed"),
		metric.WithUnit("{message}"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create confirmed messages metric: %w", err)
	}
	streamMetrics.erroredMessages, err = m.Int64Counter(
		"rabbitmq.stream.errored",
		metric.WithDescription("Number of messages errored"),
		metric.WithUnit("{message}"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create errored messages metric: %w", err)
	}
	streamMetrics.chunks, err = m.Int64Counter(
		"rabbitmq.stream.chunks",
		metric.WithDescription("Number of chunks received"),
		metric.WithUnit("{chunk}"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create chunks metric: %w", err)
	}
	streamMetrics.chunkSize, err = m.Int64Histogram(
		"rabbitmq.stream.chunk_size",
		metric.WithDescription("Number of entries in chunk received"),
		metric.WithUnit("{entries}"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create chunk size metric: %w", err)
	}
	streamMetrics.consumedMessages, err = m.Int64Counter(
		"rabbitmq.stream.consumed",
		metric.WithDescription("Number of messages consumed"),
		metric.WithUnit("{message}"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create consumed messages metric: %w", err)
	}
	streamMetrics.outstandingPublishConfirmations, err = m.Int64UpDownCounter(
		"rabbitmq.stream.outstanding_publish_confirmations",
		metric.WithDescription("Number of outstanding publish confirmations"),
		metric.WithUnit("{confirmation}"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create outstanding publish confirmations metric: %w", err)
	}
	return streamMetrics, nil
}

func (s *streamMetrics) connectionOpened(ctx context.Context, attributes attribute.Set) {
	// The caller supplies the already selected attribute set; native helpers can
	// include peer/resource data. The public adapter explicitly supplies a no-op
	// meter instead of exposing these labels through its root Observer.
	s.connections.Add(ctx, 1, metric.WithAttributeSet(attributes))
}

func (s *streamMetrics) connectionClosed(ctx context.Context, attributes attribute.Set) {
	s.connections.Add(ctx, -1, metric.WithAttributeSet(attributes))
}

func (s *streamMetrics) published(ctx context.Context, count int64, attributes attribute.Set) {
	s.publishedMessages.Add(ctx, count, metric.WithAttributeSet(attributes))
	s.outstandingPublishConfirmations.Add(ctx, count, metric.WithAttributeSet(attributes))
}

func (s *streamMetrics) confirmed(ctx context.Context, count int64, attributes attribute.Set) {
	s.confirmedMessages.Add(ctx, count, metric.WithAttributeSet(attributes))
	s.outstandingPublishConfirmations.Add(ctx, -count, metric.WithAttributeSet(attributes))
}

func (s *streamMetrics) errored(ctx context.Context, count int64, attributes attribute.Set) {
	s.erroredMessages.Add(ctx, count, metric.WithAttributeSet(attributes))
	s.outstandingPublishConfirmations.Add(ctx, -count, metric.WithAttributeSet(attributes))
}

func (s *streamMetrics) chunkReceived(ctx context.Context, entries int64, attributes attribute.Set) {
	s.chunks.Add(ctx, 1, metric.WithAttributeSet(attributes))
	s.chunkSize.Record(ctx, entries, metric.WithAttributeSet(attributes))
}

func (s *streamMetrics) consumed(ctx context.Context, count int64, attributes attribute.Set) {
	s.consumedMessages.Add(ctx, count, metric.WithAttributeSet(attributes))
}
