package stream

import (
	"fmt"
	"time"
)

// public TimeSpan MaxAge
//{
// set => Args["max-age"] = $"{value.TotalSeconds}s";
//}
//
// public ulong MaxLengthBytes
//{
// set => Args["max-length-bytes"] = $"{value}";
//}
//
// public LeaderLocator LeaderLocator
//{
// set => Args["queue-leader-locator"] = $"{value.ToString()}";
//}
//
// public int MaxSegmentSizeBytes
//{
// set => Args["stream-max-segment-size-bytes"] = $"{value}";
//}

const maxAge = "max-age"
const maxLengthBytes = "max-length-bytes"
const queueLeaderLocator = "queue-leader-locator"
const streamMaxSegmentSizeBytes = "stream-max-segment-size-bytes"

// SuperStreamOptions is the closed native provisioning seam implemented by
// partition-count/binding builders. These helpers create topology, not a root
// replay topology snapshot.
type SuperStreamOptions interface {
	getPartitions(prefix string) []string
	getBindingKeys() []string
	getArgs() map[string]string
}

// PartitionsOptions is a mutable native provisioning builder that allocates
// names from Partitions. Its raw count must be trusted/admitted before use; the
// public adapter does not provision through it.
type PartitionsOptions struct {
	// Partitions selects trusted fan-out count; raw provisioning allocation is not root policy admission.
	Partitions int
	// MaxAge requests broker retention age; rounding/policy do not promise immediate deletion.
	MaxAge time.Duration
	// MaxLengthBytes borrows broker log-retention capacity, not process memory.
	MaxLengthBytes *ByteCapacity
	// MaxSegmentSizeBytes borrows broker segment policy, not a deletion-time bound.
	MaxSegmentSizeBytes *ByteCapacity
	// LeaderLocator selects broker placement policy, not a stable endpoint.
	LeaderLocator string
	args          map[string]string
}

// NewPartitionsOptions retains the raw trusted partition count for later name
// allocation; it does not apply root topology bounds.
func NewPartitionsOptions(partitions int) *PartitionsOptions {
	return &PartitionsOptions{
		Partitions: partitions,
		args:       make(map[string]string),
	}
}

// SetMaxAge stores retention duration for broker provisioning, not proof of
// actual deletion or timestamp precision.
func (t *PartitionsOptions) SetMaxAge(maxAge time.Duration) *PartitionsOptions {
	t.MaxAge = maxAge
	return t
}

// SetMaxLengthBytes borrows a capacity builder for broker retention
// configuration, not a process-memory limit.
func (t *PartitionsOptions) SetMaxLengthBytes(maxLengthBytes *ByteCapacity) *PartitionsOptions {
	t.MaxLengthBytes = maxLengthBytes
	return t
}

// SetMaxSegmentSizeBytes borrows a capacity builder for broker segment
// configuration, not an exact deletion boundary.
func (t *PartitionsOptions) SetMaxSegmentSizeBytes(maxSegmentSizeBytes *ByteCapacity) *PartitionsOptions {
	t.MaxSegmentSizeBytes = maxSegmentSizeBytes
	return t
}

// SetBalancedLeaderLocator selects the native balanced leader-placement
// argument; actual placement belongs to the broker.
func (t *PartitionsOptions) SetBalancedLeaderLocator() *PartitionsOptions {
	t.LeaderLocator = LeaderLocatorBalanced
	return t
}

// SetClientLocalLocator selects the native client-local leader-placement
// argument, not a local replica guarantee.
func (t *PartitionsOptions) SetClientLocalLocator() *PartitionsOptions {
	t.LeaderLocator = LeaderLocatorClientLocal
	return t
}

func (t *PartitionsOptions) getPartitions(prefix string) []string {
	partitions := make([]string, t.Partitions)
	for i := range t.Partitions {
		partitions[i] = fmt.Sprintf("%s-%d", prefix, i)
	}
	return partitions
}

func (t *PartitionsOptions) getBindingKeys() []string {
	var bindingKeys []string
	for i := 0; i < t.Partitions; i++ {
		bindingKeys = append(bindingKeys, fmt.Sprintf("%d", i))
	}
	return bindingKeys
}

func (t *PartitionsOptions) getArgs() map[string]string {
	if t.MaxAge > 0 {
		t.args[maxAge] = fmt.Sprintf("%ds", int(t.MaxAge.Seconds()))
	}
	if t.MaxLengthBytes != nil {
		t.args[maxLengthBytes] = fmt.Sprintf("%d", t.MaxLengthBytes.bytes)
	}
	if t.MaxSegmentSizeBytes != nil {
		t.args[streamMaxSegmentSizeBytes] = fmt.Sprintf("%d", t.MaxSegmentSizeBytes.bytes)
	}
	if t.LeaderLocator != "" {
		t.args[queueLeaderLocator] = t.LeaderLocator
	}
	return t.args
}

// BindingsOptions borrows the ordered binding slice and mutable native
// provisioning arguments. It does not copy or validate an application topology
// snapshot.
type BindingsOptions struct {
	// Bindings borrows routing keys; callers must keep them stable through provisioning.
	Bindings []string
	// MaxAge requests broker retention age; rounding/policy do not promise immediate deletion.
	MaxAge time.Duration
	// MaxLengthBytes borrows broker log-retention capacity, not process memory.
	MaxLengthBytes *ByteCapacity
	// MaxSegmentSizeBytes borrows broker segment policy, not a deletion-time bound.
	MaxSegmentSizeBytes *ByteCapacity
	// LeaderLocator selects broker placement policy, not a stable endpoint.
	LeaderLocator string
	args          map[string]string
}

// NewBindingsOptions borrows the ordered binding slice without copying or
// admission.
func NewBindingsOptions(bindings []string) *BindingsOptions {
	return &BindingsOptions{
		Bindings: bindings,
		args:     make(map[string]string),
	}
}

// SetMaxAge stores retention duration for broker provisioning, not proof of
// actual deletion or timestamp precision.
func (t *BindingsOptions) SetMaxAge(maxAge time.Duration) *BindingsOptions {
	t.MaxAge = maxAge
	return t
}

// SetMaxLengthBytes borrows a capacity builder for broker retention
// configuration, not a process-memory limit.
func (t *BindingsOptions) SetMaxLengthBytes(maxLengthBytes *ByteCapacity) *BindingsOptions {
	t.MaxLengthBytes = maxLengthBytes
	return t
}

// SetMaxSegmentSizeBytes borrows a capacity builder for broker segment
// configuration, not an exact deletion boundary.
func (t *BindingsOptions) SetMaxSegmentSizeBytes(maxSegmentSizeBytes *ByteCapacity) *BindingsOptions {
	t.MaxSegmentSizeBytes = maxSegmentSizeBytes
	return t
}

// SetBalancedLeaderLocator selects the native balanced leader-placement
// argument; actual placement belongs to the broker.
func (t *BindingsOptions) SetBalancedLeaderLocator() *BindingsOptions {
	t.LeaderLocator = LeaderLocatorBalanced
	return t
}

// SetClientLocalLocator selects the native client-local leader-placement
// argument, not a local replica guarantee.
func (t *BindingsOptions) SetClientLocalLocator() *BindingsOptions {
	t.LeaderLocator = LeaderLocatorClientLocal
	return t
}

func (t *BindingsOptions) getPartitions(prefix string) []string {
	partitions := make([]string, len(t.Bindings))
	for i, bindingKey := range t.Bindings {
		partitions[i] = fmt.Sprintf("%s-%s", prefix, bindingKey)
	}
	return partitions
}

func (t *BindingsOptions) getBindingKeys() []string {
	return t.Bindings
}

func (t *BindingsOptions) getArgs() map[string]string {
	if t.MaxAge > 0 {
		t.args[maxAge] = fmt.Sprintf("%ds", int(t.MaxAge.Seconds()))
	}
	if t.MaxLengthBytes != nil {
		t.args[maxLengthBytes] = fmt.Sprintf("%d", t.MaxLengthBytes.bytes)
	}
	if t.MaxSegmentSizeBytes != nil {
		t.args[streamMaxSegmentSizeBytes] = fmt.Sprintf("%d", t.MaxSegmentSizeBytes.bytes)
	}
	if t.LeaderLocator != "" {
		t.args[queueLeaderLocator] = t.LeaderLocator
	}
	return t.args
}
