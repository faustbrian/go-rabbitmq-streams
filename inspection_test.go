package rabbitstream

import (
	"errors"
	"testing"
)

func TestInspectionRequestRequiresOneBoundedReadOnlyTarget(t *testing.T) {
	t.Parallel()

	limits := DefaultLimits()
	valid := []InspectionRequest{
		{Stream: "tracking.events"},
		{SuperStream: "tracking", ConsumerName: "tracking-indexer"},
	}
	for _, request := range valid {
		if err := request.Validate(limits); err != nil {
			t.Fatalf("Validate(%#v) error = %v", request, err)
		}
	}
	invalid := []InspectionRequest{
		{},
		{Stream: "tracking.events", SuperStream: "tracking"},
		{Stream: " tracking.events"},
		{Stream: "tracking.events", ConsumerName: repeatByte('c', limits.MaxStreamNameBytes+1)},
	}
	for _, request := range invalid {
		if err := request.Validate(limits); !errors.Is(err, ErrValidation) {
			t.Fatalf("Validate(%#v) error = %v", request, err)
		}
	}
}
