package worker

import (
	"context"
	"time"

	"github.com/kordar/goetl"
)

type Batcher struct {
	sink  goetl.BatchSink
	batch []*goetl.Record
	acks  []Ack
	opts  goetl.BatchOptions
}

func NewBatcher(sink goetl.BatchSink) *Batcher {
	opts := sink.BatchOptions()
	if opts.MaxBatchSize <= 0 {
		opts.MaxBatchSize = 256
	}
	if opts.FlushInterval <= 0 {
		opts.FlushInterval = time.Second
	}
	return &Batcher{
		sink: sink,
		opts: opts,
	}
}

func (b *Batcher) Add(record *goetl.Record, a Ack) bool {
	b.batch = append(b.batch, record)
	if a.Checkpoint != nil {
		b.acks = append(b.acks, a)
	}
	return len(b.batch) >= b.opts.MaxBatchSize
}

func (b *Batcher) Flush(ctx context.Context) ([]Ack, error) {
	if len(b.batch) == 0 {
		return nil, nil
	}
	err := b.sink.WriteBatch(ctx, b.batch)
	if err == nil {
		err = b.sink.Flush(ctx)
	}
	if err != nil {
		return nil, err
	}
	acks := b.acks
	b.batch = b.batch[:0]
	b.acks = b.acks[:0]
	return acks, nil
}

func (b *Batcher) FlushInterval() time.Duration {
	return b.opts.FlushInterval
}
