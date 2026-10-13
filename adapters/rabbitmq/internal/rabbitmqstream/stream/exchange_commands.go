package stream

type commandVersion interface {
	GetMinVersion() uint16
	GetMaxVersion() uint16
	GetCommandKey() uint16
}

type commandVersionResponse struct {
	minVersion uint16
	maxVersion uint16
	commandKey uint16
}

// GetMinVersion returns the received minimum command version.
func (c commandVersionResponse) GetMinVersion() uint16 {
	return c.minVersion
}

// GetMaxVersion returns the received maximum command version.
func (c commandVersionResponse) GetMaxVersion() uint16 {
	return c.maxVersion
}

// GetCommandKey returns the received command identity.
func (c commandVersionResponse) GetCommandKey() uint16 {
	return c.commandKey
}

func newCommandVersionResponse(minVersion, maxVersion, commandKey uint16) commandVersionResponse {
	return commandVersionResponse{
		minVersion: minVersion,
		maxVersion: maxVersion,
		commandKey: commandKey,
	}
}

// PublishFilter advertises the private client Publish v1..v2 command range; it
// does not negotiate incoming Deliver v2.
type PublishFilter struct {
}

// GetMinVersion advertises Publish v1 as the native minimum.
func (p PublishFilter) GetMinVersion() uint16 {
	return version1
}

// GetMaxVersion advertises Publish v2 filtering support, not Deliver v2
// decoding.
func (p PublishFilter) GetMaxVersion() uint16 {
	return version2
}

// GetCommandKey selects the Publish command for capability negotiation.
func (p PublishFilter) GetCommandKey() uint16 {
	return commandPublish
}
