//go:build integration

package rabbitmq

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/faustbrian/golib/pkg/rabbitstream"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"
)

func TestClusterSupportsRollingPatchUpgrade(t *testing.T) {
	project := requiredIntegrationEnv(t, "RABBITSTREAM_CLUSTER_PROJECT")
	if !strings.HasPrefix(project, "codex-rabbitstream-") || !strings.HasSuffix(project, "-cluster") {
		t.Fatal("rolling-upgrade project is not task-owned")
	}
	containers := integrationClusterContainers(t)
	services := []struct {
		name      string
		port      string
		container string
	}{
		{name: "rabbit1", port: "15561", container: containers["15561"]},
		{name: "rabbit2", port: "15562", container: containers["15562"]},
		{name: "rabbit3", port: "15563", container: containers["15563"]},
	}
	fromVersion := requiredIntegrationEnv(t, "RABBITSTREAM_UPGRADE_FROM_VERSION")
	toVersion := requiredIntegrationEnv(t, "RABBITSTREAM_UPGRADE_TO_VERSION")
	allUpgraded := true
	for _, service := range services {
		if service.container != project+"-"+service.name+"-1" {
			t.Fatalf("rolling-upgrade container %q is outside project %q", service.container, project)
		}
		version := integrationRabbitMQVersion(t, service.container)
		if version != toVersion {
			allUpgraded = false
		}
		if version != fromVersion && version != toVersion {
			t.Fatalf("unexpected RabbitMQ version %q in %s", version, service.container)
		}
	}
	if allUpgraded {
		t.Skip("task-owned cluster was already upgraded by an earlier gate")
	}
	for _, service := range services {
		if version := integrationRabbitMQVersion(t, service.container); version != fromVersion {
			t.Fatalf("mixed initial cluster version %q in %s", version, service.container)
		}
	}

	connection, environment := integrationClusterBroker(t)
	streamName := integrationName("rolling-upgrade")
	if err := environment.DeclareStream(streamName, stream.NewStreamOptions().SetMaxAge(time.Hour)); err != nil {
		t.Fatalf("declare rolling-upgrade stream: %v", err)
	}
	t.Cleanup(func() {
		if err := environment.DeleteStream(streamName); err != nil {
			t.Errorf("delete rolling-upgrade stream: %v", err)
		}
		if err := environment.Close(); err != nil {
			t.Errorf("close rolling-upgrade provisioning environment: %v", err)
		}
	})
	producer, err := OpenProducer(t.Context(), connection, rabbitstream.ProducerConfig{Stream: streamName})
	if err != nil {
		t.Fatalf("open rolling-upgrade producer: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := producer.Close(ctx); err != nil {
			t.Errorf("close rolling-upgrade producer: %v", err)
		}
	})

	for index, service := range services {
		assertIntegrationPublish(t, producer, streamName, "before "+service.name)
		runIntegrationCLI(t, service.container, "rabbitmq-upgrade", "await_online_quorum_plus_one")
		runIntegrationCLI(t, service.container, "rabbitmq-upgrade", "drain")
		recreateIntegrationService(t, project, service.name)
		runIntegrationCLI(t, service.container, "rabbitmqctl", "await_online_nodes", "3")
		waitForIntegrationEndpoint(t, connection, service.port)
		waitForIntegrationStreamMembers(t, service.container, streamName, 3)
		if version := integrationRabbitMQVersion(t, service.container); version != toVersion {
			t.Fatalf("upgraded %s version = %q, want %q", service.container, version, toVersion)
		}
		assertIntegrationPublish(t, producer, streamName, "after "+service.name)
		t.Logf("rolling upgrade step %d/%d confirmed on %s", index+1, len(services), service.container)
	}
	waitForStreamReplicas(t, connection, streamName, 2)
}

func runIntegrationCLI(t *testing.T, container string, command ...string) {
	t.Helper()
	arguments := append([]string{"exec", container}, command...)
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	if output, err := exec.CommandContext(ctx, "docker", arguments...).CombinedOutput(); err != nil {
		t.Fatalf("run %s in %s: %v: %s", command[0], container, err, strings.TrimSpace(string(output)))
	}
}

func recreateIntegrationService(t *testing.T, project string, service string) {
	t.Helper()
	integration, err := filepath.Abs("integration")
	if err != nil || filepath.Base(integration) != "integration" {
		t.Fatal("resolve integration fixture directory")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancel()
	command := exec.CommandContext(
		ctx, "docker", "compose", "--project-directory", integration,
		"-f", filepath.Join(integration, "compose.yaml"), "-p", project,
		"up", "-d", "--no-deps", "--force-recreate", "--wait", service,
	)
	command.Env = append(os.Environ(),
		"RABBITSTREAM_USER="+requiredIntegrationEnv(t, "RABBITSTREAM_TEST_USER"),
		"RABBITSTREAM_PASSWORD="+requiredIntegrationEnv(t, "RABBITSTREAM_TEST_PASSWORD"),
		"RABBITSTREAM_ERLANG_COOKIE="+requiredIntegrationEnv(t, "RABBITSTREAM_ERLANG_COOKIE"),
		"RABBITSTREAM_IMAGE="+requiredIntegrationEnv(t, "RABBITSTREAM_UPGRADE_IMAGE"),
	)
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("recreate task-owned %s: %v: %s", service, err, strings.TrimSpace(string(output)))
	}
}

func integrationRabbitMQVersion(t *testing.T, container string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	output, err := exec.CommandContext(ctx, "docker", "exec", container, "rabbitmqctl", "version").Output()
	if err != nil {
		t.Fatalf("read RabbitMQ version from %s: %v", container, err)
	}
	return strings.TrimSpace(string(output))
}

func waitForIntegrationStreamMembers(t *testing.T, container string, streamName string, want int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		command := exec.CommandContext(
			ctx,
			"docker", "exec", container,
			"rabbitmqctl", "-q", "list_queues", "name", "online",
		)
		output, err := command.Output()
		if err == nil {
			for _, line := range strings.Split(strings.TrimSpace(string(output)), "\n") {
				if strings.HasPrefix(line, streamName+"\t") && strings.Count(line, "rabbit@rabbit") == want {
					return
				}
			}
		}
		select {
		case <-ctx.Done():
			t.Fatalf("stream did not reach %d online members: %v", want, ctx.Err())
		case <-ticker.C:
		}
	}
}

func assertIntegrationPublish(t *testing.T, producer *rabbitstream.Producer, streamName string, payload string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	result, err := producer.Publish(ctx, rabbitstream.Message{Stream: streamName, Payload: []byte(payload)})
	if err != nil || result.State != rabbitstream.DeliveryConfirmed {
		t.Fatalf("rolling-upgrade publish = %#v, %v", result, err)
	}
}
