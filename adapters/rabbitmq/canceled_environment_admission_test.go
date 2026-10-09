package rabbitmq

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/faustbrian/go-rabbitmq-streams/adapters/rabbitmq/v2/internal/rabbitmqstream/stream"
	rabbitstream "github.com/faustbrian/go-rabbitmq-streams/v2"
)

func TestCanceledInspectionDoesNotResolveCredentials(t *testing.T) {
	for _, expired := range []bool{false, true} {
		for _, operation := range []string{"inspect", "stored-offset", "health", "shared-open"} {
			t.Run(operation+"/expired="+fmtAdmissionBool(expired), func(t *testing.T) {
				ctx, cancel := context.WithCancel(t.Context())
				wantCause := error(context.Canceled)
				wantCategory := rabbitstream.CategoryCanceled
				if expired {
					cancel()
					ctx, cancel = context.WithDeadline(t.Context(), time.Now().Add(-time.Second))
					wantCause = context.DeadlineExceeded
					wantCategory = rabbitstream.CategoryTimeout
				}
				cancel()
				provider := &countingCredentialProvider{}
				observer := &recordingObserver{}
				connection := rabbitstream.ConnectionConfig{
					Endpoints:   []rabbitstream.Endpoint{{Host: "rabbitmq.invalid", Port: 5551}},
					Credentials: provider, Observer: observer,
					Security: rabbitstream.DevelopmentPlaintextSecurity(),
				}
				inspector, err := NewInspector(connection, rabbitstream.Limits{})
				if err != nil {
					t.Fatal(err)
				}
				switch operation {
				case "inspect":
					_, err = inspector.Inspect(ctx, rabbitstream.InspectionRequest{Stream: "events"})
				case "stored-offset":
					_, err = inspector.StoredOffset(ctx, "events", "worker")
				case "health":
					health := inspector.Health(ctx)
					if health.State != rabbitstream.DependencyUnavailable || health.Category != wantCategory {
						t.Fatalf("canceled health = %v", health)
					}
				case "shared-open":
					var environment producerEnvironment
					environment, err = openFreshEnvironmentWith(ctx, inspector.connection,
						func(context.Context, *stream.EnvironmentOptions) (producerEnvironment, error) {
							t.Error("canceled admission invoked transport opener")
							return nil, errors.New("unexpected transport opening")
						})
					if environment != nil {
						t.Fatal("canceled admission returned an environment")
					}
				}
				if operation != "health" && !errors.Is(err, wantCause) {
					t.Fatalf("cancellation classification = %v, want %v", err, wantCause)
				}
				if provider.calls != 0 || observer.calls != 0 {
					t.Fatalf("canceled admission invoked credentials=%d observations=%d", provider.calls, observer.calls)
				}
			})
		}
	}
}

func fmtAdmissionBool(value bool) string {
	if value {
		return "true"
	}
	return "false"
}
