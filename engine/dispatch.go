package engine

import (
	"context"

	"github.com/kordar/goetl"
)

func (e *Engine) WithDispatcher(d goetl.Dispatcher) *Engine {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.dispatcher = d
	return e
}

func (e *Engine) dispatch(ctx context.Context, msg goetl.Message) {
	select {
	case e.outChan <- msg:
	case <-ctx.Done():
		return
	}

	e.mu.RLock()
	d := e.dispatcher
	e.mu.RUnlock()
	if d == nil {
		return
	}
	d.Deliver(ctx, msg)
}
