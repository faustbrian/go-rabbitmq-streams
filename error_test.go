package rabbitstream

import (
	"errors"
	"fmt"
	"testing"
)

func TestOperationErrorSupportsStableClassificationWithoutRenderingCause(t *testing.T) {
	t.Parallel()

	cause := fmt.Errorf("dial broker with password secret: %w", errors.New("connection reset"))
	err := &OperationError{Operation: OperationConnect, Category: CategoryConnection, Cause: cause}

	if !errors.Is(err, ErrConnection) {
		t.Fatal("operation error does not match ErrConnection")
	}
	var operationError *OperationError
	if !errors.As(err, &operationError) || operationError.Unwrap() != cause {
		t.Fatalf("errors.As/Unwrap lost cause: %#v", operationError)
	}
	if got := err.Error(); got != "rabbitstream connect failed: connection" {
		t.Fatalf("Error() = %q", got)
	}
}

func TestEveryRequiredCategoryHasStableSentinel(t *testing.T) {
	t.Parallel()

	categories := map[ErrorCategory]error{
		CategoryInvalidConfiguration: ErrInvalidConfiguration,
		CategoryValidation:           ErrValidation,
		CategoryClosed:               ErrClosed,
		CategoryCanceled:             ErrCanceled,
		CategoryTimeout:              ErrTimeout,
		CategoryAuthentication:       ErrAuthentication,
		CategoryAuthorization:        ErrAuthorization,
		CategoryConnection:           ErrConnection,
		CategoryStreamUnavailable:    ErrStreamUnavailable,
		CategoryPartitionUnavailable: ErrPartitionUnavailable,
		CategoryBrokerRejected:       ErrBrokerRejected,
		CategoryMessageTooLarge:      ErrMessageTooLarge,
		CategoryPublishAmbiguous:     ErrPublishAmbiguous,
		CategoryConfirmation:         ErrConfirmation,
		CategoryRetentionGap:         ErrRetentionGap,
		CategoryReplayRange:          ErrReplayRange,
		CategoryOffset:               ErrOffset,
		CategoryHandler:              ErrHandler,
		CategoryFatal:                ErrFatal,
	}

	for category, sentinel := range categories {
		err := &OperationError{Operation: OperationPublish, Category: category}
		if !errors.Is(err, sentinel) {
			t.Errorf("category %q does not match %v", category, sentinel)
		}
	}
}
