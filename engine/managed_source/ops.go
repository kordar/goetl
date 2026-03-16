package managed_source

import (
	"errors"
	"time"

	"github.com/kordar/goetl"
)

func (s *ManagedSource) Load(name string, src goetl.Source) error {
	return s.loadWithDrain(name, src, 0)
}

func (s *ManagedSource) LoadWithDrain(name string, src goetl.Source, drain time.Duration) error {
	return s.loadWithDrain(name, src, drain)
}

func (s *ManagedSource) loadWithDrain(name string, src goetl.Source, drain time.Duration) error {
	if src == nil {
		return errors.New("source is nil")
	}
	if name == "" {
		name = src.Name()
		if name == "" {
			return errors.New("source name is empty")
		}
	}

	var oldDone chan struct{}
	var gen uint64
	s.mu.Lock()
	ch := s.children[name]
	if ch == nil {
		ch = &ManagedChild{}
		s.children[name] = ch
	}
	ch.gen++
	gen = ch.gen
	ch.src = src
	if ch.running && ch.cancel != nil {
		ch.cancel()
		oldDone = ch.done
	}
	ch.running = false
	ch.cancel = nil
	ch.done = nil
	started := s.started
	s.mu.Unlock()

	if oldDone != nil {
		waitDone(oldDone, drain)
	}

	if !started {
		return nil
	}

	s.mu.Lock()
	ch = s.children[name]
	if ch != nil && ch.gen == gen && ch.src == src && !ch.running {
		s.startChildLocked(ch)
	}
	s.mu.Unlock()
	return nil
}

func (s *ManagedSource) Unload(name string) error {
	return s.unloadWithDrain(name, 0)
}

func (s *ManagedSource) UnloadWithDrain(name string, drain time.Duration) error {
	return s.unloadWithDrain(name, drain)
}

func (s *ManagedSource) unloadWithDrain(name string, drain time.Duration) error {
	if name == "" {
		return errors.New("source name is empty")
	}
	var done chan struct{}
	s.mu.Lock()
	ch := s.children[name]
	if ch == nil {
		s.mu.Unlock()
		return nil
	}
	delete(s.children, name)
	if ch.running && ch.cancel != nil {
		ch.cancel()
		done = ch.done
	}
	s.mu.Unlock()
	if done != nil {
		waitDone(done, drain)
	}
	return nil
}
