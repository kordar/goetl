package engine

import (
	"context"

	"github.com/kordar/goetl"
	"github.com/kordar/goetl/checkpoint"
)

func (e *Engine) GetStore() checkpoint.Store {
	return e.Checkpoint
}

func (e *Engine) RunCommitter(ctx context.Context, in <-chan ack) error {
	if e.Checkpoint == nil {
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case _, ok := <-in:
				if !ok {
					return nil
				}
			}
		}
	}

	type state struct {
		next    uint64
		pending map[uint64]*goetl.Checkpoint
	}

	states := map[string]*state{}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case a, ok := <-in:
			if !ok {
				return nil
			}
			if a.checkpoint == nil {
				continue
			}
			s, ok := states[a.partition]
			if !ok {
				s = &state{pending: map[uint64]*goetl.Checkpoint{}}
				states[a.partition] = s
			}
			s.pending[a.seq] = a.checkpoint
			for {
				cp, ok := s.pending[s.next]
				if !ok {
					break
				}
				if err := e.Checkpoint.Save(ctx, cp.Key, cp.Value); err != nil {
					return err
				}
				delete(s.pending, s.next)
				s.next++
			}
		}
	}
}
