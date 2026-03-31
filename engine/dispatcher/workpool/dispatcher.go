package workpool

import (
	"context"
	"sync"

	"github.com/kordar/goetl"
)

type WorkpoolDispatcher struct {
	TH         *TaskHandle
	TaskIDFunc func(goetl.Message) string

	mu      sync.RWMutex
	errChan chan<- error
}

func (d *WorkpoolDispatcher) Start(ctx context.Context, errChan chan<- error) {
	if d.TH == nil {
		return
	}
	d.mu.Lock()
	d.errChan = errChan
	d.mu.Unlock()
	d.TH.StartWorkerPool(ctx, errChan)
}

func (d *WorkpoolDispatcher) Deliver(ctx context.Context, msg goetl.Message) {
	if d.TH == nil || d.TaskIDFunc == nil {
		return
	}
	taskID := d.TaskIDFunc(msg)
	ch := errChanFromCtx(ctx)
	if ch == nil {
		d.mu.RLock()
		ch = d.errChan
		d.mu.RUnlock()
	}
	d.TH.SendToTaskQueue(ctx, ch, taskID, msg)
}

func (d *WorkpoolDispatcher) Wait() {
	if d.TH == nil {
		return
	}
	d.TH.Wait()
}

// Helpers: allow Deliver to use an errChan embedded in context when needed
type errChanKey struct{}

func ContextWithErrChan(ctx context.Context, ch chan<- error) context.Context {
	return context.WithValue(ctx, errChanKey{}, ch)
}

func errChanFromCtx(ctx context.Context) chan<- error {
	v := ctx.Value(errChanKey{})
	if ch, ok := v.(chan<- error); ok {
		return ch
	}
	return nil
}
