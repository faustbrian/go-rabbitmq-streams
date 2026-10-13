// Package logs supplies deliberately inert logger compatibility hooks for the
// private RabbitMQ Go client v1.8.3 closure. All levels discard their arguments;
// no process-global logger/registry is installed. This removes raw native
// diagnostics from the adapter's logging path, not from arbitrary caller errors,
// observers or SDKs. The root Observer is the application observability seam.
package logs

// LogInfo discards native informational arguments without formatting or output.
func LogInfo(string, ...any) {}

// LogError discards native error arguments without formatting or output.
func LogError(string, ...any) {}

// LogDebug discards native debug arguments without formatting or output.
func LogDebug(string, ...any) {}

// LogWarn discards native warning arguments without formatting or output.
func LogWarn(string, ...any) {}
