package engine

import (
	"context"
	"errors"
	"sync"

	"github.com/kordar/goetl"
)

type managedSource struct {
	mu      sync.Mutex
	started bool
	ctx     context.Context
	out     chan<- goetl.Message

	children map[string]*managedChild
}

type managedChild struct {
	gen     uint64
	src     goetl.Source
	running bool
	cancel  context.CancelFunc
	done    chan struct{}
}

func newManagedSource() *managedSource {
	return &managedSource{
		children: map[string]*managedChild{},
	}
}

func (s *managedSource) Name() string { return "managed_source" }

func (s *managedSource) Start(ctx context.Context, out chan<- goetl.Message) error {
	s.mu.Lock()
	if s.started {
		s.mu.Unlock()
		return errors.New("managed source already started")
	}
	s.started = true
	s.ctx = ctx
	s.out = out
	for _, ch := range s.children {
		if ch.src != nil && !ch.running {
			s.startChildLocked(ch)
		}
	}
	s.mu.Unlock()

	<-ctx.Done()

	s.mu.Lock()
	for _, ch := range s.children {
		if ch.running && ch.cancel != nil {
			ch.cancel()
		}
	}
	var doneChans []chan struct{}
	for _, ch := range s.children {
		if ch.running && ch.done != nil {
			doneChans = append(doneChans, ch.done)
		}
	}
	s.mu.Unlock()

	for _, d := range doneChans {
		<-d
	}
	return ctx.Err()
}

func (s *managedSource) Load(name string, src goetl.Source) error {
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
		ch = &managedChild{}
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
		<-oldDone
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

func (s *managedSource) startChildLocked(ch *managedChild) {
	childCtx, cancel := context.WithCancel(s.ctx)
	done := make(chan struct{})
	src := ch.src
	ch.cancel = cancel
	ch.done = done
	ch.running = true
	go func() {
		_ = src.Start(childCtx, s.out)
		close(done)
	}()
}
