package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/kordar/goetl"
	"github.com/kordar/goetl/checkpoint"
	"github.com/kordar/goetl/metrics"
)

type Options struct {
	QueueBuffer    int
	MinWorkers     int
	MaxWorkers     int
	InitialWorkers int
	DynamicSources bool
}

type Engine struct {
	Source      goetl.Source
	Pipeline    *goetl.Pipeline
	Sink        goetl.Sink
	Checkpoints checkpoint.Store
	Metrics     metrics.Collector
	Logger      *slog.Logger
	Options     Options

	OnError func(ctx context.Context, msg goetl.Message, err error)

	desiredWorkers atomic.Int32

	managed *managedSource
}

func (e *Engine) SetWorkers(n int) {
	e.desiredWorkers.Store(int32(n))
}

func (e *Engine) Run(ctx context.Context) error {
	if e.Sink == nil {
		return errors.New("engine requires Sink")
	}
	if !e.Options.DynamicSources && e.Source == nil {
		return errors.New("engine requires Source")
	}
	if e.Pipeline == nil {
		e.Pipeline = goetl.NewPipeline()
	}
	if e.Metrics == nil {
		e.Metrics = metrics.NopCollector{}
	}
	if e.Logger == nil {
		e.Logger = slog.Default()
	}

	opts := e.Options
	if opts.QueueBuffer <= 0 {
		opts.QueueBuffer = 1024
	}
	if opts.MaxWorkers <= 0 {
		opts.MaxWorkers = 16
	}
	if opts.MinWorkers < 0 {
		opts.MinWorkers = 0
	}
	if opts.MinWorkers > opts.MaxWorkers {
		opts.MinWorkers = opts.MaxWorkers
	}
	if opts.InitialWorkers <= 0 {
		opts.InitialWorkers = opts.MaxWorkers
	}
	if opts.InitialWorkers < opts.MinWorkers {
		opts.InitialWorkers = opts.MinWorkers
	}
	if opts.InitialWorkers > opts.MaxWorkers {
		opts.InitialWorkers = opts.MaxWorkers
	}
	e.desiredWorkers.Store(int32(opts.InitialWorkers))

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	rawCh := make(chan goetl.Message, opts.QueueBuffer)
	workCh := make(chan sequencedMessage, opts.QueueBuffer)
	ackCh := make(chan ack, opts.QueueBuffer)
	errCh := make(chan error, 1)

	rt := goetl.Runtime{
		Logger:      e.Logger,
		Metrics:     e.Metrics,
		Checkpoints: e.Checkpoints,
	}

	sourceName := "source"
	if e.Source != nil {
		sourceName = e.Source.Name()
	}
	if e.Options.DynamicSources {
		if e.managed == nil {
			e.managed = newManagedSource()
		}
		ms := e.managed
		if e.Source != nil && e.Source != ms {
			_ = ms.Load(e.Source.Name(), e.Source)
		}
		e.Source = ms
		sourceName = ms.Name()
	}

	sourceErrCh := make(chan error, 1)
	go func() {
		defer close(rawCh)
		err := e.Source.Start(runCtx, rawCh)
		sourceErrCh <- err
		if err != nil && !errors.Is(err, context.Canceled) {
			sendErr(errCh, err)
			cancel()
		}
	}()

	commitDone := make(chan struct{})
	go func() {
		defer close(commitDone)
		if err := runCommitter(runCtx, rt, ackCh); err != nil && !errors.Is(err, context.Canceled) {
			sendErr(errCh, err)
			cancel()
		}
	}()

	pool := newWorkerPool(runCtx, workerPoolDeps{
		pipeline: e.Pipeline,
		sink:     e.Sink,
		metrics:  e.Metrics,
		onError:  e.OnError,
		source:   sourceName,
		ackCh:    ackCh,
	})
	pool.SetDesired(int(e.desiredWorkers.Load()))
	pool.Start(workCh)

	seq := newSequencer()
	dispatchDone := make(chan struct{})
	go func() {
		defer close(dispatchDone)
		for msg := range rawCh {
			select {
			case <-runCtx.Done():
				return
			default:
			}
			p := msg.Partition
			if p == "" && msg.Checkpoint != nil {
				p = msg.Checkpoint.Key
			}
			if p == "" {
				p = "default"
			}
			work := sequencedMessage{
				Message:   msg,
				partition: p,
				seq:       seq.Next(p),
			}
			select {
			case <-runCtx.Done():
				return
			case workCh <- work:
			}
		}
		close(workCh)
	}()

	scaleDone := make(chan struct{})
	go func() {
		defer close(scaleDone)
		t := time.NewTicker(250 * time.Millisecond)
		defer t.Stop()
		for {
			select {
			case <-runCtx.Done():
				return
			case <-t.C:
				desired := int(e.desiredWorkers.Load())
				if desired < opts.MinWorkers {
					desired = opts.MinWorkers
				}
				if desired > opts.MaxWorkers {
					desired = opts.MaxWorkers
				}
				pool.SetDesired(desired)
			}
		}
	}()

	<-dispatchDone
	pool.Wait()
	close(ackCh)
	<-commitDone
	cancel()
	<-scaleDone

	_ = e.Sink.Close(ctx)

	select {
	case err := <-errCh:
		return err
	default:
	}

	if err := <-sourceErrCh; err != nil && !errors.Is(err, context.Canceled) {
		return err
	}
	return nil
}

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

type ack struct {
	partition  string
	seq        uint64
	checkpoint *goetl.Checkpoint
}

func runCommitter(ctx context.Context, rt goetl.Runtime, in <-chan ack) error {
	if rt.Checkpoints == nil {
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
				if err := rt.Checkpoints.Save(ctx, cp.Key, cp.Value); err != nil {
					return err
				}
				delete(s.pending, s.next)
				s.next++
			}
		}
	}
}

func sendErr(dst chan<- error, err error) {
	select {
	case dst <- err:
	default:
	}
}

type workerPoolDeps struct {
	pipeline *goetl.Pipeline
	sink     goetl.Sink
	metrics  metrics.Collector
	onError  func(ctx context.Context, msg goetl.Message, err error)
	source   string
	ackCh    chan<- ack
}

type workerPool struct {
	ctx     context.Context
	deps    workerPoolDeps
	desired atomic.Int32

	in <-chan sequencedMessage

	mu      sync.Mutex
	running map[int]context.CancelFunc
	wg      sync.WaitGroup
	nextID  int
}

func newWorkerPool(ctx context.Context, deps workerPoolDeps) *workerPool {
	return &workerPool{
		ctx:     ctx,
		deps:    deps,
		running: map[int]context.CancelFunc{},
	}
}

func (p *workerPool) SetDesired(n int) {
	p.desired.Store(int32(n))
	p.reconcile()
}

func (p *workerPool) Start(in <-chan sequencedMessage) {
	p.in = in
	p.reconcile()
}

func (p *workerPool) reconcile() {
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
		p.running[id] = cancel
		p.wg.Add(1)
		go func(workerID int, wctx context.Context) {
			defer p.wg.Done()
			p.runWorker(wctx, workerID)
		}(id, wctx)
	}
	for len(p.running) > desired {
		var id int
		for k := range p.running {
			id = k
			break
		}
		cancel := p.running[id]
		delete(p.running, id)
		cancel()
	}
}

func (p *workerPool) runWorker(ctx context.Context, workerID int) {
	in := p.in

	labels := metrics.Labels{"source": p.deps.source, "worker": fmt.Sprintf("%d", workerID)}
	processed := p.deps.metrics.Counter("etl_records_total", labels)
	failed := p.deps.metrics.Counter("etl_records_error_total", labels)
	latency := p.deps.metrics.Histogram("etl_record_latency_ms", labels)

	sink := p.deps.sink
	bs, batched := sink.(goetl.BatchSink)
	var batchOpts goetl.BatchOptions
	if batched {
		batchOpts = bs.BatchOptions()
		if batchOpts.MaxBatchSize <= 0 {
			batchOpts.MaxBatchSize = 256
		}
		if batchOpts.FlushInterval <= 0 {
			batchOpts.FlushInterval = time.Second
		}
	}

	var batch []*goetl.Record
	var batchAcks []ack

	flush := func() error {
		if !batched || len(batch) == 0 {
			return nil
		}
		start := time.Now()
		err := bs.WriteBatch(ctx, batch)
		if err == nil {
			err = bs.Flush(ctx)
		}
		latency.Observe(float64(time.Since(start).Milliseconds()))
		if err != nil {
			return err
		}
		for _, a := range batchAcks {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case p.deps.ackCh <- a:
			}
		}
		batch = batch[:0]
		batchAcks = batchAcks[:0]
		return nil
	}

	timer := time.NewTimer(time.Hour)
	timer.Stop()
	if batched {
		timer.Reset(batchOpts.FlushInterval)
	}
	defer timer.Stop()
	defer func() { _ = flush() }()

	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			if err := flush(); err != nil {
				failed.Add(1)
				sendErrToHandler(p.deps.onError, ctx, goetl.Message{}, err)
				return
			}
			timer.Reset(batchOpts.FlushInterval)
		case m, ok := <-in:
			if !ok {
				_ = flush()
				return
			}

			start := time.Now()
			out, err := p.deps.pipeline.Process(ctx, m.Record)
			if err != nil {
				failed.Add(1)
				sendErrToHandler(p.deps.onError, ctx, m.Message, err)
				continue
			}

			if out == nil {
				processed.Add(1)
				if m.Checkpoint != nil {
					select {
					case <-ctx.Done():
						return
					case p.deps.ackCh <- ack{partition: m.partition, seq: m.seq, checkpoint: m.Checkpoint}:
					}
				}
				continue
			}

			if batched {
				batch = append(batch, out)
				if m.Checkpoint != nil {
					batchAcks = append(batchAcks, ack{partition: m.partition, seq: m.seq, checkpoint: m.Checkpoint})
				}
				processed.Add(1)
				if len(batch) >= batchOpts.MaxBatchSize {
					if err := flush(); err != nil {
						failed.Add(1)
						sendErrToHandler(p.deps.onError, ctx, m.Message, err)
						return
					}
					timer.Reset(batchOpts.FlushInterval)
				}
				latency.Observe(float64(time.Since(start).Milliseconds()))
				continue
			}

			if err := sink.Write(ctx, out); err != nil {
				failed.Add(1)
				sendErrToHandler(p.deps.onError, ctx, m.Message, err)
				continue
			}
			processed.Add(1)
			if m.Checkpoint != nil {
				select {
				case <-ctx.Done():
					return
				case p.deps.ackCh <- ack{partition: m.partition, seq: m.seq, checkpoint: m.Checkpoint}:
				}
			}
			latency.Observe(float64(time.Since(start).Milliseconds()))
		}
	}
}

func (p *workerPool) Wait() {
	p.wg.Wait()
}

func sendErrToHandler(handler func(ctx context.Context, msg goetl.Message, err error), ctx context.Context, msg goetl.Message, err error) {
	if handler == nil {
		return
	}
	handler(ctx, msg, err)
}

func HashPartitionKey(s string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(s))
	return h.Sum64()
}

func FormatMessageJSON(msg goetl.Message) string {
	b, _ := json.Marshal(msg)
	return string(b)
}
