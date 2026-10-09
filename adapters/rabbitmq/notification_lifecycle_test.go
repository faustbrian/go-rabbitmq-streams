package rabbitmq

import (
	"testing"
	"time"

	"github.com/faustbrian/go-rabbitmq-streams/adapters/rabbitmq/v2/internal/rabbitmqstream/stream"
)

func TestProducerSessionCloseStopsNotificationWaiters(t *testing.T) {
	session := newRabbitProducerSessionForTest(nil)
	confirmations := make(chan []*stream.ConfirmationStatus)
	terminal := make(chan stream.Event)
	confirmationDone := make(chan struct{})
	terminalDone := make(chan struct{})
	go func() {
		defer close(confirmationDone)
		session.handleConfirmations(confirmations, "events")
	}()
	go func() {
		defer close(terminalDone)
		session.watchProducer(terminal)
	}()
	t.Cleanup(func() {
		close(confirmations)
		close(terminal)
		<-confirmationDone
		<-terminalDone
	})
	if err := session.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	for _, done := range []<-chan struct{}{confirmationDone, terminalDone} {
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("Close left a notification waiter dependent on supplier channel closure")
		}
	}
}
