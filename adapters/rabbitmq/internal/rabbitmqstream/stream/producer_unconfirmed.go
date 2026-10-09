package stream

import (
	"context"
	"errors"
	"net"
	"sync"
	"time"
)

// unConfirmed is a structure that holds unconfirmed messages
// And unconfirmed message is a message that has been sent to the broker but not yet confirmed,
// and it is added to the unConfirmed structure as soon is possible when
//
//	the Send() or BatchSend() method is called
//
// The confirmation status is updated when the confirmation is received from the broker (see server_frame.go)
// or due of timeout. The Timeout is configurable, and it is calculated client side.
type unConfirmed struct {
	messages        map[int64]*ConfirmationStatus
	mutexMessageMap sync.RWMutex
	maxSize         int
	changed         chan struct{}
	stopped         bool
}

func newUnConfirmed(maxSize int) *unConfirmed {
	r := &unConfirmed{
		messages:        make(map[int64]*ConfirmationStatus, maxSize),
		mutexMessageMap: sync.RWMutex{},
		maxSize:         maxSize,
		changed:         make(chan struct{}),
	}
	return r
}

var ErrUnconfirmedCapacity = errors.New("publication batch exceeds native confirmation capacity")
var ErrPendingPublishingID = errors.New("publishing ID already owns a pending confirmation")

// Nonzero size guarantees distinct live admission identities.
type confirmationAdmission struct{ _ byte }

// Admission and every capacity release share the map mutex. Waiters select
// native outcomes directly; no condition wait or cancellation proxy is owned.
func (u *unConfirmed) addFromSequencesContext(ctx context.Context, messages []*messageSequence, producerID uint8, socketDone <-chan struct{}) error {
	if len(messages) > u.maxSize {
		return ErrUnconfirmedCapacity
	}
	ids := make(map[int64]struct{}, len(messages))
	for _, sequence := range messages {
		if _, duplicate := ids[sequence.publishingId]; duplicate {
			return ErrPendingPublishingID
		}
		ids[sequence.publishingId] = struct{}{}
	}
	for {
		u.mutexMessageMap.Lock()
		if err := ctx.Err(); err != nil {
			u.mutexMessageMap.Unlock()
			return err
		}
		if u.stopped {
			u.mutexMessageMap.Unlock()
			return net.ErrClosed
		}
		select {
		case <-socketDone:
			u.mutexMessageMap.Unlock()
			return net.ErrClosed
		default:
		}
		for id := range ids {
			if _, pending := u.messages[id]; pending {
				u.mutexMessageMap.Unlock()
				return ErrPendingPublishingID
			}
		}
		if len(u.messages) <= u.maxSize-len(messages) {
			for _, sequence := range messages {
				sequence.admission = &confirmationAdmission{}
				u.messages[sequence.publishingId] = &ConfirmationStatus{inserted: time.Now(), message: sequence.sourceMsg, producerID: producerID, publishingId: sequence.publishingId, admission: sequence.admission}
			}
			u.mutexMessageMap.Unlock()
			return nil
		}
		changed := u.changed
		u.mutexMessageMap.Unlock()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-socketDone:
			return net.ErrClosed
		case <-changed:
		}
	}
}

func (u *unConfirmed) stop() {
	u.mutexMessageMap.Lock()
	defer u.mutexMessageMap.Unlock()
	if !u.stopped {
		u.stopped = true
		close(u.changed)
	}
}

// Release only this admission's entries, not a later use of an explicit ID.
// Only pre-write failures call this: admitted/partial writes remain ambiguous.
func (u *unConfirmed) removeUnsent(sequences []*messageSequence) []*ConfirmationStatus {
	u.mutexMessageMap.Lock()
	defer u.mutexMessageMap.Unlock()
	var removed []*ConfirmationStatus
	for _, sequence := range sequences {
		if pending := u.messages[sequence.publishingId]; pending != nil && pending.admission == sequence.admission {
			removed = append(removed, pending)
			delete(u.messages, sequence.publishingId)
		}
	}
	u.maybeUnLock()
	return removed
}

func (u *unConfirmed) link(from int64, to int64) {
	u.mutexMessageMap.Lock()
	defer u.mutexMessageMap.Unlock()
	r := u.messages[from]
	if r != nil {
		r.linkedTo = append(r.linkedTo, u.messages[to])
	}
}

func (u *unConfirmed) extractWithConfirms(ids []int64) []*ConfirmationStatus {
	u.mutexMessageMap.Lock()
	defer u.mutexMessageMap.Unlock()

	res := make([]*ConfirmationStatus, 0, len(ids))
	for _, v := range ids {
		m := u.extract(v, 0, true)
		if m != nil {
			res = append(res, m)
			if m.linkedTo != nil {
				res = append(res, m.linkedTo...)
			}
		}
	}
	u.maybeUnLock()
	return res
}

func (u *unConfirmed) extractWithError(id int64, errorCode uint16) *ConfirmationStatus {
	u.mutexMessageMap.Lock()
	defer u.mutexMessageMap.Unlock()
	cs := u.extract(id, errorCode, false)
	u.maybeUnLock()
	return cs
}

// extractWithErrors removes the ids and returns their failed confirmations
// (including linked sub-entry messages); already-removed ids are skipped (no nil).
func (u *unConfirmed) extractWithErrors(ids []int64, errorCode uint16) []*ConfirmationStatus {
	u.mutexMessageMap.Lock()
	defer u.mutexMessageMap.Unlock()
	res := make([]*ConfirmationStatus, 0, len(ids))
	for _, id := range ids {
		if m := u.extract(id, errorCode, false); m != nil {
			res = append(res, m)
			res = append(res, m.linkedTo...)
		}
	}
	u.maybeUnLock()
	return res
}

func (u *unConfirmed) extract(id int64, errorCode uint16, confirmed bool) *ConfirmationStatus {
	rootMessage := u.messages[id]
	if rootMessage != nil {
		u.updateStatus(rootMessage, errorCode, confirmed)

		for _, linkedMessage := range rootMessage.linkedTo {
			u.updateStatus(linkedMessage, errorCode, confirmed)
			delete(u.messages, linkedMessage.publishingId)
		}
		delete(u.messages, id)
	}
	return rootMessage
}

func (u *unConfirmed) updateStatus(rootMessage *ConfirmationStatus, errorCode uint16, confirmed bool) {
	rootMessage.confirmed = confirmed
	if confirmed {
		return
	}
	rootMessage.errorCode = errorCode
	rootMessage.err = lookErrorCode(errorCode)
}

func (u *unConfirmed) extractWithTimeOut(timeout time.Duration) []*ConfirmationStatus {
	u.mutexMessageMap.Lock()
	defer u.mutexMessageMap.Unlock()
	var res []*ConfirmationStatus
	for _, v := range u.messages {
		if time.Since(v.inserted) > timeout {
			v := u.extract(v.publishingId, timeoutError, false)
			res = append(res, v)
		}
	}
	u.maybeUnLock()
	return res
}

func (u *unConfirmed) size() int {
	u.mutexMessageMap.Lock()
	defer u.mutexMessageMap.Unlock()
	return len(u.messages)
}

func (u *unConfirmed) maybeUnLock() {
	if !u.stopped {
		close(u.changed)
		u.changed = make(chan struct{})
	}
}
