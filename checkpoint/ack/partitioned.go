package ack

import (
	"hash/fnv"

	"github.com/kordar/goetl/checkpoint"
)

type PartitionedAckTracker struct {
	trackers []AckTracker
}

func NewPartitionedAckTracker(trackers []AckTracker) *PartitionedAckTracker {
	return &PartitionedAckTracker{trackers: append([]AckTracker(nil), trackers...)}
}

func (p *PartitionedAckTracker) route(id string) AckTracker {
	if len(p.trackers) == 0 {
		return nil
	}
	h := fnv.New64a()
	_, _ = h.Write([]byte(id))
	idx := int(h.Sum64() % uint64(len(p.trackers)))
	return p.trackers[idx]
}

func (p *PartitionedAckTracker) Add(id string, cursor *checkpoint.Cursor) {
	t := p.route(id)
	if t != nil {
		t.Add(id, cursor)
	}
}

func (p *PartitionedAckTracker) Ack(id string) {
	t := p.route(id)
	if t != nil {
		t.Ack(id)
	}
}

func (p *PartitionedAckTracker) Commit() (*checkpoint.Cursor, bool) {
	okAll := true
	var cur *checkpoint.Cursor
	first := true
	for _, t := range p.trackers {
		c, ok := t.Commit()
		if !ok {
			okAll = false
		}
		if !ok {
			continue
		}
		if first {
			cur = c
			first = false
		}
	}
	if !okAll {
		return nil, false
	}
	return cur, true
}

func (p *PartitionedAckTracker) Close() error {
	for _, t := range p.trackers {
		_ = t.Close()
	}
	return nil
}
