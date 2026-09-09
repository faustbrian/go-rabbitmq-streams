//nolint:staticcheck // This test intentionally verifies the deprecated compatibility facade.
package rabbitmq_test

import (
	"context"
	"errors"
	"reflect"
	"testing"

	rabbitstream "github.com/faustbrian/go-rabbitmq-streams"
	successor "github.com/faustbrian/go-rabbitmq-streams/adapters/rabbitmq"
	legacy "github.com/faustbrian/go-rabbitmq-streams/rabbitmq"
)

func TestFacadePreservesTypeAndErrorContracts(t *testing.T) {
	t.Parallel()

	for name, value := range map[string]any{
		"Inspector": (*legacy.Inspector)(nil),
		"Replayer":  (*legacy.Replayer)(nil),
	} {
		got := reflect.TypeOf(value).Elem()
		if got.PkgPath() != "github.com/faustbrian/go-rabbitmq-streams/rabbitmq" || got.Name() != name {
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
