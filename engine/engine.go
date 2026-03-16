package engine

import (
	"context"
	"log/slog"
	"sync/atomic"

	"github.com/kordar/goetl"
	"github.com/kordar/goetl/checkpoint"
	"github.com/kordar/goetl/engine/managed_source"
	"github.com/kordar/goetl/metrics"
)

type Options struct {
	QueueBuffer    int
	MinWorkers     int
	MaxWorkers     int
	InitialWorkers int
}

type Engine struct {
	Pipeline   *goetl.Pipeline
	Sink       goetl.Sink
	Checkpoint checkpoint.Store
	Metrics    metrics.Collector
	Logger     *slog.Logger
	Options    Options

	OnError func(ctx context.Context, msg goetl.Message, err error)

	desiredWorkers atomic.Int32

	managed *managed_source.ManagedSource
}

func NewEngine(sink goetl.Sink, opts ...EngineOption) *Engine {
	e := &Engine{
		Sink: sink,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(e)
		}
	}
	return e
}
