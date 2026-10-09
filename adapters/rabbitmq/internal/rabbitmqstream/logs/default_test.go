package logs

import (
	"bytes"
	"log"
	"testing"
)

// Protocol diagnostics must not bypass the application's bounded Observer.
func TestDefaultDiagnosticsDoNotWriteApplicationData(t *testing.T) {
	var output bytes.Buffer
	previous := log.Writer()
	log.SetOutput(&output)
	t.Cleanup(func() { log.SetOutput(previous) })

	for _, emit := range []func(string, ...any){LogInfo, LogError, LogDebug, LogWarn} {
		emit("protocol detail: %s", "application-data-marker")
	}
	if output.Len() != 0 {
		t.Fatal("private protocol diagnostics wrote to the process logger")
	}
}
