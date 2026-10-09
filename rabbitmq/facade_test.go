//nolint:staticcheck // This test intentionally verifies the deprecated compatibility facade.
package rabbitmq_test

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"

	successor "github.com/faustbrian/go-rabbitmq-streams/adapters/rabbitmq/v2"
	legacy "github.com/faustbrian/go-rabbitmq-streams/rabbitmq/v2"
	rabbitstream "github.com/faustbrian/go-rabbitmq-streams/v2"
)

func TestFacadePreservesTypeAndErrorContracts(t *testing.T) {
	t.Parallel()

	for name, value := range map[string]any{
		"Inspector": (*legacy.Inspector)(nil),
		"Replayer":  (*legacy.Replayer)(nil),
	} {
		got := reflect.TypeOf(value).Elem()
		if got.PkgPath() != "github.com/faustbrian/go-rabbitmq-streams/rabbitmq/v2" || got.Name() != name {
			t.Fatalf("%s reflection identity = %s.%s", name, got.PkgPath(), got.Name())
		}
	}

	var nilContext context.Context
	producerConfig := rabbitstream.ProducerConfig{Stream: "events"}
	legacyProducer, legacyErr := legacy.OpenProducer(nilContext, rabbitstream.ConnectionConfig{}, producerConfig)
	canonicalProducer, canonicalErr := successor.OpenProducer(nilContext, rabbitstream.ConnectionConfig{}, producerConfig)
	if legacyProducer != canonicalProducer || !sameOperationError(legacyErr, canonicalErr) {
		t.Fatalf("OpenProducer() = %#v, %v; successor = %#v, %v", legacyProducer, legacyErr, canonicalProducer, canonicalErr)
	}

	consumerConfig := rabbitstream.ConsumerConfig{Stream: "events", ConsumerName: "worker"}
	legacyConsumer, legacyErr := legacy.OpenConsumer(nilContext, rabbitstream.ConnectionConfig{}, consumerConfig)
	canonicalConsumer, canonicalErr := successor.OpenConsumer(nilContext, rabbitstream.ConnectionConfig{}, consumerConfig)
	if legacyConsumer != canonicalConsumer || !sameOperationError(legacyErr, canonicalErr) {
		t.Fatalf("OpenConsumer() = %#v, %v; successor = %#v, %v", legacyConsumer, legacyErr, canonicalConsumer, canonicalErr)
	}
}

func TestFacadeConstructorsPreserveValidationBehavior(t *testing.T) {
	t.Parallel()

	legacyInspector, legacyErr := legacy.NewInspector(rabbitstream.ConnectionConfig{}, rabbitstream.Limits{})
	canonicalInspector, canonicalErr := successor.NewInspector(rabbitstream.ConnectionConfig{}, rabbitstream.Limits{})
	if legacyInspector != nil || canonicalInspector != nil || !sameOperationError(legacyErr, canonicalErr) {
		t.Fatalf("NewInspector() = %#v, %v; successor = %#v, %v", legacyInspector, legacyErr, canonicalInspector, canonicalErr)
	}

	legacyReplayer, legacyErr := legacy.NewReplayer(rabbitstream.ConnectionConfig{}, rabbitstream.Limits{})
	canonicalReplayer, canonicalErr := successor.NewReplayer(rabbitstream.ConnectionConfig{}, rabbitstream.Limits{})
	if legacyReplayer != nil || canonicalReplayer != nil || !sameOperationError(legacyErr, canonicalErr) {
		t.Fatalf("NewReplayer() = %#v, %v; successor = %#v, %v", legacyReplayer, legacyErr, canonicalReplayer, canonicalErr)
	}
}

func sameOperationError(left error, right error) bool {
	if left == nil || right == nil || left.Error() != right.Error() {
		return left == nil && right == nil
	}
	var leftOperation *rabbitstream.OperationError
	var rightOperation *rabbitstream.OperationError
	return errors.As(left, &leftOperation) && errors.As(right, &rightOperation) &&
		leftOperation.Operation == rightOperation.Operation &&
		leftOperation.Category == rightOperation.Category
}

func TestFacadeConstructorsAndReadOnlyDelegates(t *testing.T) {
	t.Parallel()
	var nilContext context.Context
	for _, limits := range []rabbitstream.Limits{{}, rabbitstream.DefaultLimits()} {
		t.Run(fmt.Sprintf("limits-%d", limits.MaxStreamNameBytes), func(t *testing.T) {
			credentials := &unresolvedCredentials{t: t}
			connection := rabbitstream.ConnectionConfig{Endpoints: []rabbitstream.Endpoint{{Host: "rabbitmq.invalid", Port: 5551}}, Credentials: credentials, Security: rabbitstream.DevelopmentPlaintextSecurity()}
			inspector, err := legacy.NewInspector(connection, limits)
			if err != nil || inspector == nil {
				t.Fatalf("NewInspector=%v, %v", inspector, err)
			}
			replayer, err := legacy.NewReplayer(connection, limits)
			if err != nil || replayer == nil {
				t.Fatalf("NewReplayer=%v, %v", replayer, err)
			}
			result, err := inspector.Inspect(nilContext, rabbitstream.InspectionRequest{Stream: "events"})
			if !reflect.DeepEqual(result, rabbitstream.InspectionResult{}) {
				t.Fatalf("invalid inspection result=%#v", result)
			}
			assertFacadeError(t, err, rabbitstream.OperationInspect, rabbitstream.CategoryInvalidConfiguration)
			offset, err := inspector.StoredOffset(nilContext, "events", "worker")
			if offset != nil {
				t.Fatalf("invalid stored offset=%v", offset)
			}
			assertFacadeError(t, err, rabbitstream.OperationInspect, rabbitstream.CategoryInvalidConfiguration)
			health := inspector.Health(nilContext)
			if health.State != rabbitstream.DependencyUnavailable || health.Category != rabbitstream.CategoryInvalidConfiguration || health.ObservedAt.IsZero() {
				t.Fatalf("health=%#v", health)
			}
			retained, err := replayer.Inspect(nilContext, rabbitstream.ReplayRequest{Stream: "events"})
			if retained != (rabbitstream.RetainedRange{}) {
				t.Fatalf("invalid retained range=%#v", retained)
			}
			assertFacadeError(t, err, rabbitstream.OperationReplay, rabbitstream.CategoryValidation)
			assertFacadeError(t, replayer.Run(context.Background(), rabbitstream.ReplayRequest{Stream: "events"}, nil), rabbitstream.OperationReplay, rabbitstream.CategoryValidation)
			invalid := rabbitstream.DefaultLimits()
			invalid.MaxPayloadBytes = -1
			badInspector, err := legacy.NewInspector(connection, invalid)
			if badInspector != nil || !errors.Is(err, rabbitstream.ErrValidation) {
				t.Fatalf("invalid inspector=%v, %v", badInspector, err)
			}
			badReplayer, err := legacy.NewReplayer(connection, invalid)
			if badReplayer != nil || !errors.Is(err, rabbitstream.ErrValidation) {
				t.Fatalf("invalid replayer=%v, %v", badReplayer, err)
			}
		})
	}
}

type unresolvedCredentials struct{ t *testing.T }

func (provider *unresolvedCredentials) Credentials(context.Context) (rabbitstream.Credentials, error) {
	provider.t.Error("validation unexpectedly resolved credentials")
	return rabbitstream.Credentials{}, errors.New("unexpected credential resolution")
}
func assertFacadeError(t *testing.T, err error, operation rabbitstream.Operation, category rabbitstream.ErrorCategory) {
	t.Helper()
	var actual *rabbitstream.OperationError
	if !errors.As(err, &actual) || actual.Operation != operation || actual.Category != category {
		t.Fatalf("error=%v, want %s/%s", err, operation, category)
	}
}
