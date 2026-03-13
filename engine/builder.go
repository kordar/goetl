package engine

import (
	"context"
	"log/slog"

	"github.com/kordar/go-etl"
	"github.com/kordar/go-etl/config"
	"github.com/kordar/go-etl/metrics"
	"github.com/kordar/go-etl/plugin"
)

func Build(ctx context.Context, job config.Job, rt etl.Runtime) (*Engine, error) {
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

	transforms := make([]etl.Transformer, 0, len(job.Transforms))
	for _, tcfg := range job.Transforms {
		t, err := plugin.Transforms.Build(ctx, tcfg, rt)
		if err != nil {
			return nil, err
		}
		transforms = append(transforms, t)
	}

	eng := &Engine{
		Source:      source,
		Pipeline:    etl.NewPipeline(transforms...),
		Sink:        sink,
		Checkpoints: rt.Checkpoints,
		Metrics:     rt.Metrics,
		Logger:      rt.Logger,
	}

	if job.Queue.Buffer > 0 {
		eng.Options.QueueBuffer = job.Queue.Buffer
	}
	if job.Workers.Min != 0 || job.Workers.Max != 0 {
		eng.Options.MinWorkers = job.Workers.Min
		eng.Options.MaxWorkers = job.Workers.Max
		eng.Options.InitialWorkers = job.Workers.Max
	}

	return eng, nil
}
