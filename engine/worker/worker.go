package worker

import (
	"context"
	"time"

	"github.com/kordar/goetl"
	"github.com/kordar/goetl/metrics"
)

func runWorker(
	ctx context.Context,
	workerID int,
	handle *workerHandle,
	in <-chan Message,
	deps Deps,
	acker *AckForwarder,
) {
	labels := labelsForWorker(deps.Source, workerID)
	processed := deps.Metrics.Counter("etl_records_total", labels)
	failed := deps.Metrics.Counter("etl_records_error_total", labels)
	latency := deps.Metrics.Histogram("etl_record_latency_ms", labels)

	sink := deps.Sink
	bs, batched := sink.(goetl.BatchSink)

	var batcher *Batcher
	var timer *time.Timer
	if batched {
		batcher = NewBatcher(bs)
		timer = time.NewTimer(time.Hour)
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timer.Reset(batcher.FlushInterval())
		defer timer.Stop()
	}

	sendAck := func(ctx context.Context, a Ack) error {
		if a.Checkpoint == nil {
			return nil
		}
		return acker.Send(ctx, a)
	}

	flushOnce := func() error {
		if !batched {
			return nil
		}
		start := time.Now()
		acks, err := batcher.Flush(ctx)
		latency.Observe(float64(time.Since(start).Milliseconds()))
		if err != nil {
			return err
		}
		for _, a := range acks {
			if err := sendAck(ctx, a); err != nil {
				return err
			}
		}
		return nil
	}

	flushWithRetry := func(msg goetl.Message) bool {
		err := Retry(ctx, func() error {
			err := flushOnce()
			if err != nil {
				failed.Add(1)
				sendErrToHandler(deps.OnError, ctx, msg, err)
			}
			return err
		})
		return err == nil
	}

	defer func() {
		_ = flushWithRetry(goetl.Message{})
	}()

	for {
		handle.idle.Store(true)

		if batched {
			select {
			case <-ctx.Done():
				return
			case <-timer.C:
				handle.idle.Store(false)
				if !flushWithRetry(goetl.Message{}) {
					return
				}
				ResetTimer(timer, batcher.FlushInterval())
			case m, ok := <-in:
				handle.idle.Store(false)
				if !ok {
					return
				}
				if !handleMessage(ctx, m, deps, processed, failed, latency, batched, batcher, sendAck, flushWithRetry, timer) {
					return
				}
			}
			continue
		}

		select {
		case <-ctx.Done():
			return
		case m, ok := <-in:
			handle.idle.Store(false)
			if !ok {
				return
			}
			if !handleMessage(ctx, m, deps, processed, failed, latency, false, nil, sendAck, flushWithRetry, nil) {
				return
			}
		}
	}
}

func handleMessage(
	ctx context.Context,
	m Message,
	deps Deps,
	processed metrics.Counter,
	failed metrics.Counter,
	latency metrics.Histogram,
	batched bool,
	batcher *Batcher,
	sendAck func(context.Context, Ack) error,
	flushWithRetry func(goetl.Message) bool,
	timer *time.Timer,
) bool {
	start := time.Now()
	out, err := deps.Pipeline.Process(ctx, m.Record)
	if err != nil {
		failed.Add(1)
		sendErrToHandler(deps.OnError, ctx, m.Message, err)
		return true
	}

	if out == nil {
		processed.Add(1)
		if err := sendAck(ctx, Ack{Partition: m.Partition, Seq: m.Seq, Checkpoint: m.Checkpoint}); err != nil {
			return false
		}
		return true
	}

	if batched {
		shouldFlush := batcher.Add(out, Ack{Partition: m.Partition, Seq: m.Seq, Checkpoint: m.Checkpoint})
		processed.Add(1)
		if shouldFlush {
			if !flushWithRetry(m.Message) {
				return false
			}
			ResetTimer(timer, batcher.FlushInterval())
		}
		latency.Observe(float64(time.Since(start).Milliseconds()))
		return true
	}

	if err := deps.Sink.Write(ctx, out); err != nil {
		failed.Add(1)
		sendErrToHandler(deps.OnError, ctx, m.Message, err)
		return true
	}
	processed.Add(1)
	if err := sendAck(ctx, Ack{Partition: m.Partition, Seq: m.Seq, Checkpoint: m.Checkpoint}); err != nil {
		return false
	}
	latency.Observe(float64(time.Since(start).Milliseconds()))
	return true
}

func sendErrToHandler(handler func(ctx context.Context, msg goetl.Message, err error), ctx context.Context, msg goetl.Message, err error) {
	if handler == nil {
		return
	}
	handler(ctx, msg, err)
}
