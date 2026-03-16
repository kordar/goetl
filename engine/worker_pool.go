package engine

import (
	"context"
	"sync"

	"github.com/kordar/goetl"
	"github.com/kordar/goetl/engine/worker"
	"github.com/kordar/goetl/metrics"
)

type workerPoolDeps struct {
	pipeline *goetl.Pipeline
	sink     goetl.Sink
	metrics  metrics.Collector
	onError  func(ctx context.Context, msg goetl.Message, err error)
	source   string
	ackCh    chan<- ack
}

type workerPool struct {
	ctx context.Context

	pool *worker.Pool

	inBridgeWg sync.WaitGroup
	inBridgeCh chan worker.Message
}

func newWorkerPool(ctx context.Context, deps workerPoolDeps) *workerPool {
	ackRelayCh := make(chan worker.Ack, 4096)
	p := &workerPool{
		ctx:        ctx,
		inBridgeCh: nil,
	}

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case a := <-ackRelayCh:
				ra := ack{partition: a.Partition, seq: a.Seq, checkpoint: a.Checkpoint}
				select {
				case <-ctx.Done():
					return
				case deps.ackCh <- ra:
				}
			}
		}
	}()

	p.pool = worker.NewPool(ctx, worker.Deps{
		Pipeline: deps.pipeline,
		Sink:     deps.sink,
		Metrics:  deps.metrics,
		OnError:  deps.onError,
		Source:   deps.source,
		AckCh:    ackRelayCh,
	})
	return p
}

func (p *workerPool) SetDesired(n int) {
	p.pool.SetDesired(n)
}

func (p *workerPool) Start(in <-chan sequencedMessage) {
	buf := cap(in)
	if buf <= 0 {
		buf = 64
	}
	p.inBridgeCh = make(chan worker.Message, buf)
	p.inBridgeWg.Add(1)
	go func() {
		defer p.inBridgeWg.Done()
		defer close(p.inBridgeCh)
		for {
			select {
			case <-p.ctx.Done():
				return
			case m, ok := <-in:
				if !ok {
					return
				}
				item := worker.Message{
					Message:   m.Message,
					Partition: m.partition,
					Seq:       m.seq,
				}
				select {
				case <-p.ctx.Done():
					return
				case p.inBridgeCh <- item:
				}
			}
		}
	}()
	p.pool.Start(p.inBridgeCh)
}

func (p *workerPool) Wait() {
	p.inBridgeWg.Wait()
	p.pool.Wait()
}
