package worker

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/kordar/goetl"
	"github.com/kordar/goetl/metrics"
)

type Ack struct {
	Partition  string
	Seq        uint64
	Checkpoint *goetl.Checkpoint
}

type Message struct {
	goetl.Message
	Partition string
	Seq       uint64
}

type Deps struct {
	Pipeline *goetl.Pipeline
	Sink     goetl.Sink
	Metrics  metrics.Collector
	OnError  func(ctx context.Context, msg goetl.Message, err error)
	Source   string
	AckCh    chan<- Ack
}

type workerHandle struct {
	cancel context.CancelFunc
	idle   atomic.Bool
}

type Pool struct {
	ctx     context.Context
	deps    Deps
	desired atomic.Int32

	in <-chan Message

	mu      sync.Mutex
	running map[int]*workerHandle
	wg      sync.WaitGroup
	nextID  int

	acker *AckForwarder
}

func NewPool(ctx context.Context, deps Deps) *Pool {
	return &Pool{
		ctx:     ctx,
		deps:    deps,
		running: map[int]*workerHandle{},
	}
}

func (p *Pool) SetDesired(n int) {
	p.desired.Store(int32(n))
	p.reconcile()
}

func (p *Pool) Start(in <-chan Message) {
	p.in = in
	p.acker = NewAckForwarder(p.ctx, p.deps.AckCh)
	p.reconcile()
}

func (p *Pool) Wait() {
	p.wg.Wait()
	if p.acker != nil {
		p.acker.Close()
	}
}

func (p *Pool) reconcile() {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.in == nil {
		return
	}

	desired := int(p.desired.Load())
	if desired < 0 {
		desired = 0
	}
	for len(p.running) < desired {
		id := p.nextID
		p.nextID++
		wctx, cancel := context.WithCancel(p.ctx)
		handle := &workerHandle{cancel: cancel}
		p.running[id] = handle
		p.wg.Add(1)
		go func(workerID int, wctx context.Context, h *workerHandle) {
			defer p.wg.Done()
			runWorker(wctx, workerID, h, p.in, p.deps, p.acker)
		}(id, wctx, handle)
	}

	for len(p.running) > desired {
		id := -1
		for k, h := range p.running {
			if h.idle.Load() {
				id = k
				break
			}
		}
		if id == -1 {
			for k := range p.running {
				id = k
				break
			}
		}
		handle := p.running[id]
		delete(p.running, id)
		handle.cancel()
	}
}

func labelsForWorker(source string, workerID int) metrics.Labels {
	return metrics.Labels{"source": source, "worker": fmt.Sprintf("%d", workerID)}
}
