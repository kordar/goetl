package ack

import (
	"sync"
	"time"

	"github.com/kordar/goetl/checkpoint"
)

type TimeoutAckTracker struct {
	mu      sync.Mutex
	strict  *StrictAckTracker
	times   []time.Time
	timeout time.Duration
}

func NewTimeoutAckTracker(timeout time.Duration) *TimeoutAckTracker {
	return &TimeoutAckTracker{strict: NewStrictAckTracker(), timeout: timeout}
}

func (t *TimeoutAckTracker) Add(id string, cursor *checkpoint.Cursor) {
	t.mu.Lock()
	t.strict.Add(id, cursor)
	t.times = append(t.times, time.Now())
	t.mu.Unlock()
}

func (t *TimeoutAckTracker) Ack(id string) {
	t.strict.Ack(id)
}

func (t *TimeoutAckTracker) Commit() (*checkpoint.Cursor, bool) {
	t.mu.Lock()
	if t.timeout > 0 {
		now := time.Now()
		for i := range t.times {
			if t.strict.entries == nil || i >= len(t.strict.entries) {
				break
			}
			if !t.strict.entries[i].acked && now.Sub(t.times[i]) >= t.timeout {
				t.strict.entries[i].acked = true
			}
		}
	}
	t.mu.Unlock()
	return t.strict.Commit()
}

func (t *TimeoutAckTracker) Close() error {
	return t.strict.Close()
}
