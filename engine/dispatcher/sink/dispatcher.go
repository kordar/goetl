package sink

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/kordar/goetl"
	"github.com/kordar/goetl/checkpoint"
)

var ErrDispatchQueueFull = errors.New("dispatch queue full")

type BatchSinkDispatcher struct {
	sinks          []goetl.Sink
	runtimeFactory goetl.RuntimeFactory
	runtimeConfig  goetl.RuntimeConfig
	runtimeOnce    sync.Once
	runtime        *goetl.Runtime

	batchSize     int
	flushInterval time.Duration
	retries       int
	backoff       func(attempt int) time.Duration
	queueBuffer   int

	in     chan goetl.Message
	wg     sync.WaitGroup
	errMu  sync.Mutex
	errCh  chan<- error
	start  sync.Once
	closed sync.Once
}

func NewBatchSinkDispatcher(sinks ...goetl.Sink) *BatchSinkDispatcher {
	d := &BatchSinkDispatcher{
		sinks:         append([]goetl.Sink(nil), sinks...),
		batchSize:     100,
		flushInterval: 1 * time.Second,
		retries:       3,
		backoff: func(attempt int) time.Duration {
			return time.Second * time.Duration(attempt+1)
		},
		queueBuffer: 1024,
	}
	return d
}

func (d *BatchSinkDispatcher) WithSinks(sinks ...goetl.Sink) *BatchSinkDispatcher {
	d.sinks = append([]goetl.Sink(nil), sinks...)
	return d
}

func (d *BatchSinkDispatcher) WithRuntimeFactory(factory goetl.RuntimeFactory) *BatchSinkDispatcher {
	d.runtimeFactory = factory
	return d
}

func (d *BatchSinkDispatcher) WithRuntimeConfig(cfg goetl.RuntimeConfig) *BatchSinkDispatcher {
	d.runtimeConfig = cfg
	return d
}

func (d *BatchSinkDispatcher) WithBatchSize(n int) *BatchSinkDispatcher {
	if n > 0 {
		d.batchSize = n
	}
	return d
}

func (d *BatchSinkDispatcher) WithFlushInterval(v time.Duration) *BatchSinkDispatcher {
	if v > 0 {
		d.flushInterval = v
	}
	return d
}

func (d *BatchSinkDispatcher) WithRetries(n int) *BatchSinkDispatcher {
	if n >= 0 {
		d.retries = n
	}
	return d
}

func (d *BatchSinkDispatcher) WithBackoff(fn func(attempt int) time.Duration) *BatchSinkDispatcher {
	if fn != nil {
		d.backoff = fn
	}
	return d
}

func (d *BatchSinkDispatcher) WithQueueBuffer(n int) *BatchSinkDispatcher {
	if n > 0 {
		d.queueBuffer = n
	}
	return d
}

func (d *BatchSinkDispatcher) Start(ctx context.Context, errChan chan<- error) {
	d.start.Do(func() {
		d.errMu.Lock()
		d.errCh = errChan
		d.errMu.Unlock()

		d.runtimeOnce.Do(func() {
			rt, err := (&d.runtimeFactory).Create(d.runtimeConfig)
			if err != nil {
				d.report(err)
				return
			}
			d.runtime = rt
		})

		d.in = make(chan goetl.Message, d.queueBuffer)
		d.wg.Add(1)
		go d.loop(ctx)
	})
}

func (d *BatchSinkDispatcher) Deliver(ctx context.Context, msg goetl.Message) {
	if d.in == nil {
		d.report(ErrDispatchQueueFull)
		return
	}
	select {
	case <-ctx.Done():
		return
	case d.in <- msg:
	default:
		d.report(ErrDispatchQueueFull)
	}
}

func (d *BatchSinkDispatcher) Wait() {
	d.closed.Do(func() {
		if d.in != nil {
			close(d.in)
		}
	})
	d.wg.Wait()
}

func (d *BatchSinkDispatcher) loop(ctx context.Context) {
	defer d.wg.Done()

	batch := make([]goetl.Message, 0, d.batchSize)
	t := time.NewTicker(d.flushInterval)
	defer t.Stop()

	flush := func() {
		if len(batch) == 0 {
			return
		}
		d.flush(ctx, batch)
		batch = batch[:0]
	}

	for {
		select {
		case <-ctx.Done():
			flush()
			return
		case msg, ok := <-d.in:
			if !ok {
				flush()
				return
			}
			batch = append(batch, msg)
			if len(batch) >= d.batchSize {
				flush()
			}
		case <-t.C:
			flush()
		}
	}
}

func (d *BatchSinkDispatcher) flush(ctx context.Context, batch []goetl.Message) {
	allOK := true
	for _, sink := range d.sinks {
		if sink == nil {
			continue
		}
		if err := d.retry(ctx, func() error { return sink.WriteBatch(ctx, batch) }); err != nil {
			d.report(err)
			allOK = false
			continue
		}
	}
	if allOK {
		d.commit(ctx, batch)
	}
}

func (d *BatchSinkDispatcher) commit(ctx context.Context, batch []goetl.Message) {
	rt := d.runtime
	if rt == nil {
		return
	}

	if rt.Ack != nil {
		for _, msg := range batch {
			if msg.Ack == "" {
				continue
			}
			cur := msg.Cursor
			if msg.Checkpoint != "" {
				if cur.Meta == nil {
					cur.Meta = map[string]any{}
				}
				if _, ok := cur.Meta["checkpoint"]; !ok {
					cur.Meta["checkpoint"] = msg.Checkpoint
				}
			}
			rt.Ack.Add(msg.Ack, &cur)
			rt.Ack.Ack(msg.Ack)
		}

		if rt.CP != nil {
			for {
				c, ok := rt.Ack.Commit()
				if !ok {
					break
				}
				cur, k, ok := decodeCommittedCursor(c)
				if !ok || k == "" {
					continue
				}
				if err := rt.CP.Save(ctx, k, cur); err != nil {
					d.report(err)
					return
				}
			}
		}
		return
	}

	if rt.CP != nil {
		for _, msg := range batch {
			if msg.Checkpoint == "" {
				continue
			}
			if err := rt.CP.Save(ctx, msg.Checkpoint, msg.Cursor); err != nil {
				d.report(err)
				return
			}
		}
	}
}

func decodeCommittedCursor(v any) (checkpoint.Cursor, string, bool) {
	switch x := v.(type) {
	case checkpoint.Cursor:
		return x, extractCheckpointKey(x), true
	case *checkpoint.Cursor:
		if x == nil {
			return checkpoint.Cursor{}, "", false
		}
		return *x, extractCheckpointKey(*x), true
	default:
		return checkpoint.Cursor{}, "", false
	}
}

func extractCheckpointKey(cur checkpoint.Cursor) string {
	if cur.Meta == nil {
		return ""
	}
	v, ok := cur.Meta["checkpoint"]
	if !ok || v == nil {
		return ""
	}
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}

func (d *BatchSinkDispatcher) retry(ctx context.Context, fn func() error) error {
	var last error
	for attempt := 0; attempt <= d.retries; attempt++ {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err := fn(); err == nil {
			return nil
		} else {
			last = err
		}
		if attempt == d.retries {
			break
		}
		time.Sleep(d.backoff(attempt))
	}
	return last
}

func (d *BatchSinkDispatcher) report(err error) {
	if err == nil {
		return
	}
	d.errMu.Lock()
	ch := d.errCh
	d.errMu.Unlock()
	if ch == nil {
		return
	}
	select {
	case ch <- err:
	default:
	}
}
