package engine

import (
	"sync"

	"github.com/kordar/goetl"
)

type sequencedMessage struct {
	goetl.Message
	partition string
	seq       uint64
}

type sequencer struct {
	mu   sync.Mutex
	next map[string]uint64
}

func newSequencer() *sequencer {
	return &sequencer{next: map[string]uint64{}}
}

func (s *sequencer) Next(partition string) uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	v := s.next[partition]
	s.next[partition] = v + 1
	return v
}

func sendErr(dst chan<- error, err error) {
	select {
	case dst <- err:
	default:
	}
}
