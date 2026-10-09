package logs

// The private protocol implementation never emits supplier-controlled values
// through the process-global logger. Application observability belongs to the
// root package's bounded Observer; there is no mutable SDK logging registry.
func LogInfo(string, ...any)  {}
func LogError(string, ...any) {}
func LogDebug(string, ...any) {}
func LogWarn(string, ...any)  {}
