package engine

import (
	"context"
	"log/slog"

	"github.com/kordar/goetl"
	"github.com/kordar/goetl/config"
	"github.com/kordar/goetl/metrics"
	"github.com/kordar/goetl/plugin"
)

func Build(ctx context.Context, job config.Job, rt goetl.Runtime) (*Engine, error) {
	if rt.Logger == nil {
		rt.Logger = slog.Default()
	}
	if rt.Metrics == nil {
		rt.Metrics = metrics.NopCollector{}
	}

	if job.Checkpoint.Type != "" {
		store, err := plugin.Checkpoints.Build(ctx, job.Checkpoint, rt)
		if err != nil {
			return nil, err
		}
		rt.Checkpoints = store
	}

	source, err := plugin.Sources.Build(ctx, job.Source, rt)
	if err != nil {
		return nil, err
	}
	sink, err := plugin.Sinks.Build(ctx, job.Sink, rt)
	if err != nil {
		return nil, err
	}

	transforms := make([]goetl.Transformer, 0, len(job.Transforms))
	for _, tcfg := range job.Transforms {
		t, err := plugin.Transforms.Build(ctx, tcfg, rt)
		if err != nil {
			return nil, err
		}
		transforms = append(transforms, t)
	}

	engineOpts := []EngineOption{
		WithPipeline(goetl.NewPipeline(transforms...)),
		WithMetrics(rt.Metrics),
		WithLogger(rt.Logger),
	}

	if job.Queue.Buffer > 0 {
		engineOpts = append(engineOpts, WithQueueBuffer(job.Queue.Buffer))
	}
	if job.Workers.Min != 0 || job.Workers.Max != 0 {
		engineOpts = append(engineOpts, WithWorkers(job.Workers.Min, job.Workers.Max, job.Workers.Max))
	}

	eng := NewEngine(sink, engineOpts...)
	if err := eng.LoadSource("", source); err != nil {
		return nil, err
	}
	return eng, nil
}
