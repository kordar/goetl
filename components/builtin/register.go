package builtin

import (
	"context"
	"time"

	"github.com/kordar/go-etl"
	"github.com/kordar/go-etl/checkpoint"
	"github.com/kordar/go-etl/components/memory"
	"github.com/kordar/go-etl/components/sink"
	"github.com/kordar/go-etl/components/source/dynamic"
	"github.com/kordar/go-etl/components/source/multi"
	"github.com/kordar/go-etl/components/transform"
	"github.com/kordar/go-etl/config"
	"github.com/kordar/go-etl/plugin"
)

func Register() {
	plugin.Checkpoints.Register("memory", func(ctx context.Context, cfg config.Component, rt etl.Runtime) (checkpoint.Store, error) {
		_ = ctx
		_ = cfg
		_ = rt
		return memory.NewCheckpointStore(), nil
	})

	plugin.Sources.Register("memory_sequence", func(ctx context.Context, cfg config.Component, rt etl.Runtime) (etl.Source, error) {
		_ = ctx
		var s struct {
			CheckpointKey string `json:"checkpoint_key"`
			Partition     string `json:"partition"`
			Total         int    `json:"total"`
			DelayMs       int64  `json:"delay_ms"`
		}
		if err := config.DecodeSettings(cfg.Settings, &s); err != nil {
			return nil, err
		}
		return &memory.SequenceSource{
			Store:         rt.Checkpoints,
			CheckpointKey: s.CheckpointKey,
			Partition:     s.Partition,
			Total:         s.Total,
			Delay:         time.Duration(s.DelayMs) * time.Millisecond,
		}, nil
	})

	plugin.Sources.Register("multi", func(ctx context.Context, cfg config.Component, rt etl.Runtime) (etl.Source, error) {
		var s struct {
			Sources []config.Component `json:"sources"`
		}
		if err := config.DecodeSettings(cfg.Settings, &s); err != nil {
			return nil, err
		}

		children := make([]etl.Source, 0, len(s.Sources))
		for _, childCfg := range s.Sources {
			child, err := plugin.Sources.Build(ctx, childCfg, rt)
			if err != nil {
				return nil, err
			}
			children = append(children, child)
		}

		ms := multi.New(children...)
		ms.NameValue = cfg.Name
		return ms, nil
	})

	plugin.Sources.Register("dynamic_multi", func(ctx context.Context, cfg config.Component, rt etl.Runtime) (etl.Source, error) {
		var s struct {
			Provider struct {
				Type     string         `json:"type"`
				Settings map[string]any `json:"settings"`
			} `json:"provider"`
			ReloadIntervalMs int64  `json:"reload_interval_ms"`
			Strategy         string `json:"strategy"`
			DrainTimeoutMs   int64  `json:"drain_timeout_ms"`
			DedupLatest      bool   `json:"dedup_latest"`
		}
		if err := config.DecodeSettings(cfg.Settings, &s); err != nil {
			return nil, err
		}
		prov, err := dynamic.BuildProvider(s.Provider.Type, s.Provider.Settings)
		if err != nil {
			return nil, err
		}
		src := &dynamic.Source{
			NameValue:      cfg.Name,
			Provider:       prov,
			ReloadInterval: time.Duration(s.ReloadIntervalMs) * time.Millisecond,
			Strategy:       s.Strategy,
			DrainTimeout:   time.Duration(s.DrainTimeoutMs) * time.Millisecond,
			DedupLatest:    s.DedupLatest,
			BuildChild: func(cctx context.Context, c config.Component) (etl.Source, error) {
				return plugin.Sources.Build(cctx, c, rt)
			},
		}
		return src, nil
	})

	plugin.Transforms.Register("trim_strings", func(ctx context.Context, cfg config.Component, rt etl.Runtime) (etl.Transformer, error) {
		_ = ctx
		_ = cfg
		_ = rt
		return &transform.TrimStrings{}, nil
	})

	plugin.Sinks.Register("stdout", func(ctx context.Context, cfg config.Component, rt etl.Runtime) (etl.Sink, error) {
		_ = ctx
		_ = cfg
		_ = rt
		return sink.NewStdout(nil), nil
	})
}
