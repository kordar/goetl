package engine

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/kordar/goetl"
	"github.com/kordar/goetl/engine/managed_source"
	"github.com/kordar/goetl/metrics"
)

func (e *Engine) SetWorkers(n int) {
	e.desiredWorkers.Store(int32(n))
}

func (e *Engine) Run(ctx context.Context) error {
	if e.Sink == nil {
		return errors.New("engine requires Sink")
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

	opts := normalizeOptions(e.Options)
	e.desiredWorkers.Store(int32(opts.InitialWorkers))

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	rawCh := make(chan goetl.Message, opts.QueueBuffer)
	workCh := make(chan sequencedMessage, opts.QueueBuffer)
	ackCh := make(chan ack, opts.QueueBuffer)
	errCh := make(chan error, 1)

	if e.managed == nil {
		e.managed = managed_source.NewManagedSource()
	}
	ms := e.managed
	sourceName := ms.Name()

	sourceErrCh := make(chan error, 1)
	go func() {
		defer close(rawCh)
		err := ms.Start(runCtx, rawCh)
		sourceErrCh <- err
		if err != nil && !errors.Is(err, context.Canceled) {
			sendErr(errCh, err)
			cancel()
		}
	}()

	commitDone := make(chan struct{})
	go func() {
		defer close(commitDone)
		if err := e.RunCommitter(runCtx, ackCh); err != nil && !errors.Is(err, context.Canceled) {
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

func normalizeOptions(opts Options) Options {
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
	return opts
}
