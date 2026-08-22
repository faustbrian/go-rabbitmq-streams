//go:build integration

package rabbitmq

import (
	"context"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/faustbrian/golib/pkg/rabbitstream"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"
)

func TestRepeatedProducerLifecycleReleasesConnectionsGoroutinesAndTimers(t *testing.T) {
	connection, environment := integrationBroker(t)
	streamName := integrationName("resource-lifecycle")
	if err := environment.DeclareStream(streamName, stream.NewStreamOptions().SetMaxAge(time.Hour)); err != nil {
		t.Fatalf("declare resource-lifecycle stream: %v", err)
	}
	t.Cleanup(func() {
		if err := environment.DeleteStream(streamName); err != nil {
			t.Errorf("delete resource-lifecycle stream: %v", err)
		}
		if err := environment.Close(); err != nil {
			t.Errorf("close resource-lifecycle environment: %v", err)
		}
	})
	container := requiredIntegrationEnv(t, "RABBITSTREAM_TEST_RESTART_CONTAINER")
	baselineConnections := integrationConnectionCount(t, container)
	baselineGoroutines := runtime.NumGoroutine()
	baselineDescriptors := integrationDescriptorCount(t)

	for index := 0; index < 20; index++ {
		producer, err := OpenProducer(t.Context(), connection, rabbitstream.ProducerConfig{Stream: streamName})
		if err != nil {
			t.Fatalf("open producer %d: %v", index, err)
		}
		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
		_, publishErr := producer.Publish(ctx, rabbitstream.Message{Stream: streamName, Payload: []byte("probe")})
		closeErr := producer.Close(ctx)
		cancel()
		if publishErr != nil || closeErr != nil {
			t.Fatalf("producer %d publish/close = %v/%v", index, publishErr, closeErr)
		}
	}

	deadline := time.Now().Add(15 * time.Second)
	for {
		runtime.GC()
		connections := integrationConnectionCount(t, container)
		goroutines := runtime.NumGoroutine()
		descriptors := integrationDescriptorCount(t)
		if connections <= baselineConnections && goroutines <= baselineGoroutines+2 && descriptors <= baselineDescriptors+2 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf(
				"resource counts did not return to bounds: connections %d/%d, goroutines %d/%d, descriptors %d/%d",
				connections, baselineConnections, goroutines, baselineGoroutines, descriptors, baselineDescriptors,
			)
		}
		time.Sleep(25 * time.Millisecond)
	}
}

func BenchmarkIdleProducerResources(b *testing.B) {
	connection, environment := benchmarkBroker(b, false)
	defer closeBenchmarkEnvironment(b, environment)
	streamName := declareBenchmarkStream(b, environment, "idle-resources")
	producer, err := OpenProducer(context.Background(), connection, rabbitstream.ProducerConfig{Stream: streamName})
	if err != nil {
		b.Fatal(err)
	}
	defer closePolicyBenchmarkProducer(b, producer)
	ctx, cancel := context.WithTimeout(context.Background(), benchmarkOperationTimeout)
	if _, err := producer.Publish(ctx, rabbitstream.Message{Stream: streamName, Payload: []byte("warm")}); err != nil {
		cancel()
		b.Fatal(err)
	}
	cancel()
	runtime.GC()
	before := idleResourceSnapshot(b)

	b.ResetTimer()
	for b.Loop() {
		time.Sleep(10 * time.Millisecond)
	}
	b.StopTimer()
	runtime.GC()
	after := idleResourceSnapshot(b)
	b.ReportMetric(after.cpuSeconds-before.cpuSeconds, "idle-cpu-seconds")
	b.ReportMetric(float64(after.heapBytes), "idle-heap-bytes")
	b.ReportMetric(float64(after.goroutines), "idle-goroutines")
	b.ReportMetric(float64(after.descriptors), "idle-file-descriptors")
	b.ReportMetric(float64(after.connections), "idle-broker-connections")
}

type idleResources struct {
	cpuSeconds  float64
	heapBytes   uint64
	goroutines  int
	descriptors int
	connections int
}

func idleResourceSnapshot(tb testing.TB) idleResources {
	tb.Helper()
	var memory runtime.MemStats
	runtime.ReadMemStats(&memory)
	return idleResources{
		cpuSeconds:  integrationProcessCPUSeconds(tb),
		heapBytes:   memory.HeapAlloc,
		goroutines:  runtime.NumGoroutine(),
		descriptors: integrationDescriptorCount(tb),
		connections: integrationConnectionCount(tb, requiredIntegrationEnv(tb, "RABBITSTREAM_TEST_RESTART_CONTAINER")),
	}
}

func integrationConnectionCount(tb testing.TB, container string) int {
	tb.Helper()
	if !strings.HasPrefix(container, "codex-rabbitstream-") {
		tb.Fatal("connection-count container is not task-owned")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	output, err := exec.CommandContext(
		ctx, "docker", "exec", container,
		"rabbitmq-streams", "-q", "list_stream_connections", "conn_name",
	).Output()
	if err != nil {
		tb.Fatalf("count broker connections: %v", err)
	}
	count := 0
	for _, line := range strings.Split(strings.TrimSpace(string(output)), "\n") {
		if strings.TrimSpace(line) != "" && !strings.HasPrefix(line, "name") {
			count++
		}
	}
	return count
}

func integrationProcessCPUSeconds(tb testing.TB) float64 {
	tb.Helper()
	var usage syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &usage); err != nil {
		tb.Fatalf("read process CPU usage: %v", err)
	}
	return float64(usage.Utime.Sec+usage.Stime.Sec) +
		float64(usage.Utime.Usec+usage.Stime.Usec)/1_000_000
}

func integrationDescriptorCount(tb testing.TB) int {
	tb.Helper()
	directory, err := os.Open("/dev/fd")
	if err != nil {
		tb.Fatalf("open process descriptor directory: %v", err)
	}
	names, readErr := directory.Readdirnames(-1)
	closeErr := directory.Close()
	if readErr != nil || closeErr != nil {
		tb.Fatalf("count process descriptors: %v/%v", readErr, closeErr)
	}
	return len(names)
}
