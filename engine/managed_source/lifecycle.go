package managed_source

import (
	"context"
	"errors"

	"github.com/kordar/goetl"
)

func (s *ManagedSource) Start(ctx context.Context, out chan<- goetl.Message) error {
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	s.mu.Lock()
	if s.started {
		s.mu.Unlock()
		return errors.New("managed source already started")
	}
	s.started = true
	s.ctx = runCtx
	s.out = out
	for _, ch := range s.children {
		if ch.src != nil && !ch.running {
			s.startChildLocked(ch)
		}
	}
	exts := append([]ManagedSourceExtension(nil), s.exts...)
	s.mu.Unlock()

	extErrCh := make(chan error, 1)
	for _, ext := range exts {
		go func(ext ManagedSourceExtension) {
			err := ext.Start(runCtx, s)
			if err != nil && !errors.Is(err, context.Canceled) {
				select {
				case extErrCh <- err:
				default:
				}
				cancel()
			}
		}(ext)
	}

	var extErr error
	select {
	case <-runCtx.Done():
	case extErr = <-extErrCh:
	}

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
	if extErr != nil {
		return extErr
	}
	return runCtx.Err()
}

func (s *ManagedSource) startChildLocked(ch *ManagedChild) {
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
