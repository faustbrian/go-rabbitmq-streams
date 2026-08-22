package rabbitstream

import (
	"context"
	"testing"
	"time"
)

func boundedTestContext() context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	time.AfterFunc(time.Second, cancel)
	return ctx
}

func receiveTest[T any](t *testing.T, channel <-chan T) T {
	t.Helper()
	timer := time.NewTimer(time.Second)
	defer timer.Stop()
	select {
	case value := <-channel:
		return value
	case <-timer.C:
		t.Fatal("timed out waiting for test synchronization")
		var zero T
		return zero
	}
}
