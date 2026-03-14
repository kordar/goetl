package goetl

import (
	"context"
	"time"
)

type Source interface {
	Name() string
	Start(ctx context.Context, out chan<- Message) error
}

type Transformer interface {
	Name() string
	Transform(ctx context.Context, r *Record) (*Record, error)
}

type Sink interface {
	Name() string
	Write(ctx context.Context, r *Record) error
	Close(ctx context.Context) error
}

type BatchSink interface {
	Sink
	WriteBatch(ctx context.Context, batch []*Record) error
	Flush(ctx context.Context) error
	BatchOptions() BatchOptions
}

type BatchOptions struct {
	MaxBatchSize  int
	FlushInterval time.Duration
}
