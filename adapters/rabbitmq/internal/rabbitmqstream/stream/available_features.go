package stream

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"sync"
)

var lock = &sync.Mutex{}

type availableFeatures struct {
	is313OrMore                       bool
	is311OrMore                       bool
	brokerFilterEnabled               bool
	brokerVersion                     string
	brokerSingleActiveConsumerEnabled bool
}

func newAvailableFeatures() *availableFeatures {
	lock.Lock()
	defer lock.Unlock()
	return &availableFeatures{}
}

// Is311OrMore reads the parsed version threshold under the native feature mutex.
func (a *availableFeatures) Is311OrMore() bool {
	lock.Lock()
	defer lock.Unlock()
	return a.is311OrMore
}

// Is313OrMore reads the parsed version threshold under the native feature mutex.
func (a *availableFeatures) Is313OrMore() bool {
	lock.Lock()
	defer lock.Unlock()
	return a.is313OrMore
}

// BrokerFilterEnabled reads negotiated Publish-v2 support, not Deliver-v2 support.
func (a *availableFeatures) BrokerFilterEnabled() bool {
	lock.Lock()
	defer lock.Unlock()
	return a.brokerFilterEnabled
}

// IsBrokerSingleActiveConsumerEnabled reads a version-derived flag, not live promotion or fencing.
func (a *availableFeatures) IsBrokerSingleActiveConsumerEnabled() bool {
	lock.Lock()
	defer lock.Unlock()
	return a.brokerSingleActiveConsumerEnabled
}

// SetVersion extracts a three-component version and sets native threshold flags.
// An invalid diagnostic includes its input; version admission is not conformance.
func (a *availableFeatures) SetVersion(version string) error {
	lock.Lock()
	defer lock.Unlock()
	if extractVersion(version) == "" {
		return fmt.Errorf("invalid version format: %s", version)
	}
	a.brokerVersion = version
	a.is311OrMore = IsVersionGreaterOrEqual(extractVersion(version), "3.11.0")
	a.is313OrMore = IsVersionGreaterOrEqual(extractVersion(version), "3.13.0")
	a.brokerSingleActiveConsumerEnabled = a.is311OrMore
	return nil
}

// GetCommands returns a fresh list advertising only native Publish filtering versions.
func (a *availableFeatures) GetCommands() []commandVersion {
	lock.Lock()
	defer lock.Unlock()
	return []commandVersion{
		&PublishFilter{},
	}
}

// ParseCommandVersions updates Publish filtering capability from peer version bounds.
func (a *availableFeatures) ParseCommandVersions(commandVersions []commandVersion) {
	lock.Lock()
	defer lock.Unlock()
	for _, commandVersion := range commandVersions {
		if commandVersion.GetCommandKey() == commandPublish {
			a.brokerFilterEnabled = commandVersion.GetMinVersion() <= PublishFilter{}.GetMinVersion() &&
				commandVersion.GetMaxVersion() >= PublishFilter{}.GetMaxVersion()
		}
	}
}

// String renders borrowed version/flags without redaction or locking.
// Callers must avoid concurrent feature mutation during this diagnostic.
func (a *availableFeatures) String() string {
	return fmt.Sprintf("brokerVersion: %s, is311OrMore: %t, is313OrMore: %t, brokerFilterEnabled: %t", a.brokerVersion, a.is311OrMore, a.is313OrMore, a.brokerFilterEnabled)
}

func extractVersion(fullVersion string) string {
	pattern := `(\d+\.\d+\.\d+)`
	regex := regexp.MustCompile(pattern)
	match := regex.FindStringSubmatch(fullVersion)

	if len(match) > 1 {
		return match[1]
	}
	return ""
}

// IsVersionGreaterOrEqual compares the native three-integer form and returns
// false on parse errors; it is not a SemVer or broker-qualification oracle.
func IsVersionGreaterOrEqual(version, target string) bool {
	v1, err := parseVersion(version)
	if err != nil {
		return false
	}

	v2, err := parseVersion(target)
	if err != nil {
		return false
	}
	return v1.Compare(v2) >= 0
}

func parseVersion(version string) (Version, error) {
	parts := strings.Split(version, ".")
	if len(parts) != 3 {
		return Version{}, fmt.Errorf("invalid version format: %s", version)
	}

	major, err := strconv.Atoi(parts[0])
	if err != nil {
		return Version{}, fmt.Errorf("invalid major version: %s", parts[0])
	}

	minor, err := strconv.Atoi(parts[1])
	if err != nil {
		return Version{}, fmt.Errorf("invalid minor version: %s", parts[1])
	}

	patch, err := strconv.Atoi(parts[2])
	if err != nil {
		return Version{}, fmt.Errorf("invalid patch version: %s", parts[2])
	}

	return Version{Major: major, Minor: minor, Patch: patch}, nil
}

// Version is a three-integer native capability-comparison value, not general
// SemVer or independent broker qualification.
type Version struct {
	// Major is the parsed native major integer, not a qualification result.
	Major int
	// Minor is the parsed native minor integer.
	Minor int
	// Patch is the parsed native patch integer.
	Patch int
}

// Compare compares major/minor/patch by integer subtraction. Inputs must have
// differences representable as int; arbitrary extreme values are not admitted
// here.
func (v Version) Compare(other Version) int {
	if v.Major != other.Major {
		return v.Major - other.Major
	}
	if v.Minor != other.Minor {
		return v.Minor - other.Minor
	}
	return v.Patch - other.Patch
}
