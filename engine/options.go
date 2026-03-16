package engine

import (
	"context"
	"log/slog"

	"github.com/kordar/goetl"
	"github.com/kordar/goetl/checkpoint"
	"github.com/kordar/goetl/engine/managed_source"
	"github.com/kordar/goetl/metrics"
)

type EngineOption func(*Engine)

func WithManagedSourceExtension(ext managed_source.ManagedSourceExtension) EngineOption {
	return func(e *Engine) {
		if e.managed == nil {
			e.managed = managed_source.NewManagedSource()
		}
		e.managed.Use(ext)
	}
}

func WithSource(source goetl.Source) EngineOption {
	return func(e *Engine) {
		_ = e.LoadSource("", source)
	}
}

func WithLoadSource(name string, source goetl.Source) EngineOption {
	return func(e *Engine) {
		_ = e.LoadSource(name, source)
	}
}

func WithPipeline(pipeline *goetl.Pipeline) EngineOption {
	return func(e *Engine) {
		e.Pipeline = pipeline
	}
}

func WithMetrics(collector metrics.Collector) EngineOption {
	return func(e *Engine) {
		e.Metrics = collector
	}
}

func WithLogger(logger *slog.Logger) EngineOption {
	return func(e *Engine) {
		e.Logger = logger
	}
}

func WithQueueBuffer(n int) EngineOption {
	return func(e *Engine) {
		e.Options.QueueBuffer = n
	}
}

func WithWorkers(min, max, initial int) EngineOption {
	return func(e *Engine) {
		e.Options.MinWorkers = min
		e.Options.MaxWorkers = max
		e.Options.InitialWorkers = initial
	}
}

func WithCheckpoints(store checkpoint.Store) EngineOption {
	return func(e *Engine) {
		e.Checkpoint = store
	}
}

func WithOnError(handler func(ctx context.Context, msg goetl.Message, err error)) EngineOption {
	return func(e *Engine) {
		e.OnError = handler
	}
}
