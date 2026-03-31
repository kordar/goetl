package ack

import "github.com/kordar/goetl/checkpoint"

type WindowAckTracker struct {
	strict *StrictAckTracker
	size   int
}

func NewWindowAckTracker(size int) *WindowAckTracker {
	if size <= 0 {
		size = 1
	}
	return &WindowAckTracker{strict: NewStrictAckTracker(), size: size}
}

func (t *WindowAckTracker) Add(id string, cursor *checkpoint.Cursor) {
	t.strict.Add(id, cursor)
}

func (t *WindowAckTracker) Ack(id string) {
	t.strict.Ack(id)
}

func (t *WindowAckTracker) Commit() (*checkpoint.Cursor, bool) {
	return t.strict.Commit()
}

func (t *WindowAckTracker) Close() error {
	return t.strict.Close()
}
