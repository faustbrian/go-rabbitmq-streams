package rabbitstream

import (
	"context"
	"errors"
	"fmt"
)

// ErrorCategory is a stable, low-cardinality failure classification. It is
// safe for branching and diagnostics; it never contains broker or caller data.
type ErrorCategory string

const (
	// CategoryInvalidConfiguration classifies rejected static policy.
	CategoryInvalidConfiguration ErrorCategory = "invalid_configuration"
	// CategoryValidation classifies rejected caller message or request data.
	CategoryValidation ErrorCategory = "validation"
	// CategoryClosed classifies operations attempted after lifecycle closure.
	CategoryClosed ErrorCategory = "closed"
	// CategoryCanceled classifies caller-requested cancellation.
	CategoryCanceled ErrorCategory = "canceled"
	// CategoryTimeout classifies an exhausted bounded operation deadline.
	CategoryTimeout ErrorCategory = "timeout"
	// CategoryAuthentication classifies rejected or unavailable credentials.
	CategoryAuthentication ErrorCategory = "authentication"
	// CategoryAuthorization classifies insufficient broker permissions.
	CategoryAuthorization ErrorCategory = "authorization"
	// CategoryConnection classifies broker connectivity or session failure.
	CategoryConnection ErrorCategory = "connection"
	// CategoryStreamUnavailable classifies a missing or unavailable stream.
	CategoryStreamUnavailable ErrorCategory = "stream_unavailable"
	// CategoryPartitionUnavailable classifies a missing backing stream.
	CategoryPartitionUnavailable ErrorCategory = "partition_unavailable"
	// CategoryBrokerRejected classifies a definitive publish rejection.
	CategoryBrokerRejected ErrorCategory = "broker_rejected"
	// CategoryMessageTooLarge classifies a message exceeding package or broker limits.
	CategoryMessageTooLarge ErrorCategory = "message_too_large"
	// CategoryPublishAmbiguous classifies transmission without delivery certainty.
	CategoryPublishAmbiguous ErrorCategory = "publish_ambiguous"
	// CategoryConfirmation classifies an invalid or failed confirmation.
	CategoryConfirmation ErrorCategory = "confirmation"
	// CategoryRetentionGap classifies requested history no longer retained.
	CategoryRetentionGap ErrorCategory = "retention_gap"
	// CategoryReplayRange classifies an invalid or incomplete replay range.
	CategoryReplayRange ErrorCategory = "replay_range"
	// CategoryOffset classifies broker offset tracking failure.
	CategoryOffset ErrorCategory = "offset"
	// CategoryHandler classifies application handler failure.
	CategoryHandler ErrorCategory = "handler"
	// CategoryFatal classifies a permanent client failure requiring intervention.
	CategoryFatal ErrorCategory = "fatal"
)

type categoryError struct {
	category ErrorCategory
}

// Error renders the stable category without broker or caller data.
func (err categoryError) Error() string { return string(err.category) }

var (
	// ErrInvalidConfiguration matches CategoryInvalidConfiguration.
	ErrInvalidConfiguration = categoryError{CategoryInvalidConfiguration}
	// ErrValidation matches CategoryValidation.
	ErrValidation = categoryError{CategoryValidation}
	// ErrClosed matches CategoryClosed.
	ErrClosed = categoryError{CategoryClosed}
	// ErrCanceled matches CategoryCanceled.
	ErrCanceled = categoryError{CategoryCanceled}
	// ErrTimeout matches CategoryTimeout.
	ErrTimeout = categoryError{CategoryTimeout}
	// ErrAuthentication matches CategoryAuthentication.
	ErrAuthentication = categoryError{CategoryAuthentication}
	// ErrAuthorization matches CategoryAuthorization.
	ErrAuthorization = categoryError{CategoryAuthorization}
	// ErrConnection matches CategoryConnection.
	ErrConnection = categoryError{CategoryConnection}
	// ErrStreamUnavailable matches CategoryStreamUnavailable.
	ErrStreamUnavailable = categoryError{CategoryStreamUnavailable}
	// ErrPartitionUnavailable matches CategoryPartitionUnavailable.
	ErrPartitionUnavailable = categoryError{CategoryPartitionUnavailable}
	// ErrBrokerRejected matches CategoryBrokerRejected.
	ErrBrokerRejected = categoryError{CategoryBrokerRejected}
	// ErrMessageTooLarge matches CategoryMessageTooLarge.
	ErrMessageTooLarge = categoryError{CategoryMessageTooLarge}
	// ErrPublishAmbiguous matches CategoryPublishAmbiguous.
	ErrPublishAmbiguous = categoryError{CategoryPublishAmbiguous}
	// ErrConfirmation matches CategoryConfirmation.
	ErrConfirmation = categoryError{CategoryConfirmation}
	// ErrRetentionGap matches CategoryRetentionGap.
	ErrRetentionGap = categoryError{CategoryRetentionGap}
	// ErrReplayRange matches CategoryReplayRange.
	ErrReplayRange = categoryError{CategoryReplayRange}
	// ErrOffset matches CategoryOffset.
	ErrOffset = categoryError{CategoryOffset}
	// ErrHandler matches CategoryHandler.
	ErrHandler = categoryError{CategoryHandler}
	// ErrFatal matches CategoryFatal.
	ErrFatal = categoryError{CategoryFatal}
)

// Operation identifies a stable package operation without including resource
// names or other high-cardinality data.
type Operation string

const (
	// OperationConnect establishes or restores a broker session.
	OperationConnect Operation = "connect"
	// OperationPublish sends and confirms a message.
	OperationPublish Operation = "publish"
	// OperationConsume receives, handles, or stores consumer progress.
	OperationConsume Operation = "consume"
	// OperationReplay reads an isolated retained range.
	OperationReplay Operation = "replay"
	// OperationInspect reads broker topology or offsets.
	OperationInspect Operation = "inspect"
	// OperationClose drains and releases owned resources.
	OperationClose Operation = "close"
)

// OperationError preserves a programmatically inspectable cause while its
// rendered form exposes only a stable operation and category. Callers must not
// log the unwrapped cause unless they have independently established it is safe.
type OperationError struct {
	// Operation is the stable operation that failed.
	Operation Operation
	// Category is the stable low-cardinality failure class.
	Category ErrorCategory
	// Cause preserves programmatic detail and may require redaction before logging.
	Cause error
}

// Error renders a bounded operation and category without the underlying cause.
func (err *OperationError) Error() string {
	if err == nil {
		return "<nil>"
	}
	return fmt.Sprintf("rabbitstream %s failed: %s", err.Operation, err.Category)
}

// Unwrap preserves the original cause for errors.Is and errors.As.
func (err *OperationError) Unwrap() error {
	if err == nil {
		return nil
	}
	return err.Cause
}

// Is matches the sentinel corresponding to Category.
func (err *OperationError) Is(target error) bool {
	if err == nil {
		return false
	}
	var categoryTarget categoryError
	return errors.As(target, &categoryTarget) && categoryTarget.category == err.Category
}

func invalidConfiguration(cause error) error {
	return &OperationError{
		Operation: OperationConnect,
		Category:  CategoryInvalidConfiguration,
		Cause:     cause,
	}
}

func validationError(cause error) error {
	return &OperationError{
		Operation: OperationPublish,
		Category:  CategoryValidation,
		Cause:     cause,
	}
}

func categoryForError(err error, fallback ErrorCategory) ErrorCategory {
	switch {
	case errors.Is(err, ErrInvalidConfiguration):
		return CategoryInvalidConfiguration
	case errors.Is(err, ErrValidation):
		return CategoryValidation
	case errors.Is(err, ErrClosed):
		return CategoryClosed
	case errors.Is(err, ErrCanceled), errors.Is(err, context.Canceled):
		return CategoryCanceled
	case errors.Is(err, ErrTimeout), errors.Is(err, context.DeadlineExceeded):
		return CategoryTimeout
	case errors.Is(err, ErrAuthentication):
		return CategoryAuthentication
	case errors.Is(err, ErrAuthorization):
		return CategoryAuthorization
	case errors.Is(err, ErrStreamUnavailable):
		return CategoryStreamUnavailable
	case errors.Is(err, ErrPartitionUnavailable):
		return CategoryPartitionUnavailable
	case errors.Is(err, ErrBrokerRejected):
		return CategoryBrokerRejected
	case errors.Is(err, ErrMessageTooLarge):
		return CategoryMessageTooLarge
	case errors.Is(err, ErrPublishAmbiguous):
		return CategoryPublishAmbiguous
	case errors.Is(err, ErrConfirmation):
		return CategoryConfirmation
	case errors.Is(err, ErrRetentionGap):
		return CategoryRetentionGap
	case errors.Is(err, ErrReplayRange):
		return CategoryReplayRange
	case errors.Is(err, ErrOffset):
		return CategoryOffset
	case errors.Is(err, ErrHandler):
		return CategoryHandler
	case errors.Is(err, ErrFatal):
		return CategoryFatal
	case errors.Is(err, ErrConnection):
		return CategoryConnection
	default:
		return fallback
	}
}
