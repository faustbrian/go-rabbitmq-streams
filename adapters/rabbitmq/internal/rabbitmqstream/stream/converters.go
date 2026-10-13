package stream

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/pkg/errors"
)

const (
	// UnitMb is a case-folded decimal-capacity suffix, not a binary unit.
	UnitMb = "mb"
	// UnitKb is a case-folded decimal-capacity suffix, not a binary unit.
	UnitKb = "kb"
	// UnitGb is a case-folded decimal-capacity suffix, not a binary unit.
	UnitGb = "gb"
	// UnitTb is a case-folded decimal-capacity suffix, not a binary unit.
	UnitTb              = "tb"
	kilobytesMultiplier = 1000
	megabytesMultiplier = 1000 * 1000
	gigabytesMultiplier = 1000 * 1000 * 1000
	terabytesMultiplier = 1000 * 1000 * 1000 * 1000
)

// ByteCapacity is the legacy decimal-unit builder used by private provisioning
// options. Multiplication is not checked for overflow and From is not a
// hostile-input parser; trusted profile input is required.
type ByteCapacity struct {
	bytes int64
	error error
}

// B returns a fresh raw byte-capacity value without sign/range validation.
func (byteCapacity ByteCapacity) B(value int64) *ByteCapacity {
	return &ByteCapacity{bytes: value, error: nil}
}

// KB returns a fresh decimal-capacity value using 1,000 bytes per unit.
// It does not validate sign or check multiplication overflow.
func (byteCapacity ByteCapacity) KB(value int64) *ByteCapacity {
	return &ByteCapacity{bytes: value * kilobytesMultiplier, error: nil}
}

// MB returns a fresh decimal-capacity value using 1,000,000 bytes per unit.
// It does not validate sign or check multiplication overflow.
func (byteCapacity ByteCapacity) MB(value int64) *ByteCapacity {
	return &ByteCapacity{bytes: value * megabytesMultiplier, error: nil}
}

// GB returns a fresh decimal-capacity value using 1,000,000,000 bytes per unit.
// It does not validate sign or check multiplication overflow.
func (byteCapacity ByteCapacity) GB(value int64) *ByteCapacity {
	return &ByteCapacity{bytes: value * gigabytesMultiplier, error: nil}
}

// TB returns a fresh decimal-capacity value using 1,000,000,000,000 bytes per unit.
// It does not validate sign or check multiplication overflow.
func (byteCapacity ByteCapacity) TB(value int64) *ByteCapacity {
	return &ByteCapacity{bytes: value * terabytesMultiplier, error: nil}
}

// From parses trusted empty/zero or integer-plus-two-character unit input.
// Other one-byte strings panic during suffix slicing; multiplication is unchecked
// and error text includes the input. Do not use as a hostile-input boundary.
func (byteCapacity ByteCapacity) From(value string) *ByteCapacity {
	if value == "" || value == "0" {
		return &ByteCapacity{bytes: 0, error: nil}
	}

	match := regexp.MustCompile("^((kb|mb|gb|tb))")
	foundUnitSize := strings.ToLower(value[len(value)-2:])

	if match.MatchString(foundUnitSize) {
		size, err := strconv.Atoi(value[:len(value)-2])
		if err != nil {
			return &ByteCapacity{bytes: 0, error: errors.New(fmt.Sprintf("Capacity, Invalid number format: %s", value))}
		}

		switch foundUnitSize {
		case UnitKb:
			return byteCapacity.KB(int64(size))

		case UnitMb:
			return byteCapacity.MB(int64(size))

		case UnitGb:
			return byteCapacity.GB(int64(size))

		case UnitTb:
			return byteCapacity.TB(int64(size))
		}
	}

	return &ByteCapacity{bytes: 0,
		error: errors.New(fmt.Sprintf("Capacity, Invalid unit size format: %s", value))}
}
