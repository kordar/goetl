package ack

import (
	"sync"

	"github.com/kordar/goetl/checkpoint"
)

type strictEntry struct {
	id     string
	cursor *checkpoint.Cursor
	acked  bool
}

type StrictAckTracker struct {
	mu      sync.Mutex
	entries []strictEntry
	index   map[string]int
	closed  bool
}

func NewStrictAckTracker() *StrictAckTracker {
	return &StrictAckTracker{index: map[string]int{}}
}

func (t *StrictAckTracker) Add(id string, cursor *checkpoint.Cursor) {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return
	}
	t.index[id] = len(t.entries)
	t.entries = append(t.entries, strictEntry{id: id, cursor: cursor})
	t.mu.Unlock()
}

func (t *StrictAckTracker) Ack(id string) {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return
	}
	if i, ok := t.index[id]; ok && i < len(t.entries) {
		t.entries[i].acked = true
	}
	t.mu.Unlock()
}

func (t *StrictAckTracker) Commit() (*checkpoint.Cursor, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return nil, false
	}
	n := 0
	var cur *checkpoint.Cursor
	for n < len(t.entries) {
		if !t.entries[n].acked {
			break
		}
		cur = t.entries[n].cursor
		delete(t.index, t.entries[n].id)
		n++
	}
	if n == 0 {
		return nil, false
	}
	t.entries = t.entries[n:]
	for i := range t.entries {
		t.index[t.entries[i].id] = i
	}
	return cur, true
}

func (t *StrictAckTracker) Close() error {
	t.mu.Lock()
	t.closed = true
	t.entries = nil
	t.index = nil
	t.mu.Unlock()
	return nil
}
