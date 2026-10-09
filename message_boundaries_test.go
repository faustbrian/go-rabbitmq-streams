package rabbitstream

import (
	"errors"
	"testing"
)

func TestDeliveryNameBoundaryAndEnvelopeFailure(t *testing.T) {
	limits := DefaultLimits()
	limits.MaxStreamNameBytes = 2
	message := Message{Stream: "ab", Partition: "ab", HasOffset: true}
	if err := message.ValidateDelivery(limits); err != nil {
		t.Fatal("exact delivery name boundary was rejected:", err)
	}
	message.Stream, message.Partition = "abc", "abc"
	err := message.ValidateDelivery(limits)
	var operation *OperationError
	if !errors.Is(err, ErrValidation) || !errors.As(err, &operation) || operation.Operation != OperationConsume {
		t.Fatal("oversized delivery envelope did not retain consume validation classification")
	}
	if errors.Unwrap(err) != nil {
		t.Fatal("invalid delivery envelope leaked a nested publish-validation cause")
	}
}

func TestBatchMetadataKeyCanExhaustByteBudget(t *testing.T) {
	limits := DefaultLimits()
	limits.MaxBatchBytes = 2
	message := Message{Stream: "events", Headers: []MetadataEntry{{Key: "ab"}}}
	if err := ValidateBatch([]Message{message}, limits); err != nil {
		t.Fatal("exact metadata-key byte budget was rejected:", err)
	}
	limits.MaxBatchBytes = 1
	if err := ValidateBatch([]Message{message}, limits); !errors.Is(err, ErrValidation) {
		t.Fatal("metadata key exceeded the batch budget without rejection")
	}
}
