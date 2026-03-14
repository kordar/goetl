package engine

import (
	"context"
	"testing"
	"time"

	"github.com/kordar/goetl"
	"github.com/kordar/goetl/components/builtin"
	"github.com/kordar/goetl/components/memory"
	"github.com/kordar/goetl/config"
	"github.com/kordar/goetl/plugin"
)

func TestMultiSource_BuildAndRun(t *testing.T) {
	t.Parallel()

	builtin.Register()

	store := memory.NewCheckpointStore()
	rt := goetl.Runtime{Checkpoints: store}

	cfg := config.Component{
		Type: "multi",
		Settings: map[string]any{
			"sources": []any{
				map[string]any{
					"type": "memory_sequence",
					"settings": map[string]any{
						"checkpoint_key": "s1",
						"total":          20,
					},
				},
				map[string]any{
					"type": "memory_sequence",
					"settings": map[string]any{
						"checkpoint_key": "s2",
						"total":          15,
					},
				},
			},
		},
	}

	src, err := plugin.Sources.Build(context.Background(), cfg, rt)
	if err != nil {
		t.Fatalf("build multi source: %v", err)
	}

	if err := store.Save(context.Background(), "s1", "9"); err != nil {
		t.Fatalf("save checkpoint: %v", err)
	}

	sink := &collectSink{}
	eng := &Engine{
		Source:      src,
		Pipeline:    goetl.NewPipeline(),
		Sink:        sink,
		Checkpoints: store,
		Options: Options{
			QueueBuffer:    16,
			MinWorkers:     1,
			MaxWorkers:     8,
			InitialWorkers: 8,
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := eng.Run(ctx); err != nil {
		t.Fatalf("run: %v", err)
	}

	if sink.Count() != 25 {
		t.Fatalf("count=%d want=%d", sink.Count(), 25)
	}

	v1, err := store.Load(ctx, "s1")
	if err != nil {
		t.Fatalf("load s1: %v", err)
	}
	if v1 != "19" {
		t.Fatalf("s1 checkpoint=%s want=%s", v1, "19")
	}

	v2, err := store.Load(ctx, "s2")
	if err != nil {
		t.Fatalf("load s2: %v", err)
	}
	if v2 != "14" {
		t.Fatalf("s2 checkpoint=%s want=%s", v2, "14")
	}
}
