package engine

import (
	"context"
	"testing"
	"time"

	"github.com/kordar/goetl/components/memory"
)

func TestEngine_LoadSource_Replace(t *testing.T) {
	t.Parallel()

	store := memory.NewCheckpointStore()
	sink := &collectSink{}

	eng := &Engine{
		Sink:        sink,
		Checkpoints: store,
		Options: Options{
			DynamicSources: true,
			QueueBuffer:    16,
			MinWorkers:     1,
			MaxWorkers:     8,
			InitialWorkers: 4,
		},
	}

	if err := eng.LoadSource("s1", &memory.SequenceSource{
		Store:         store,
		CheckpointKey: "s1",
		Total:         10,
	}); err != nil {
		t.Fatalf("load source: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	done := make(chan error, 1)
	go func() { done <- eng.Run(ctx) }()

	waitUntil(t, 2*time.Second, func() bool { return sink.Count() == 10 })

	if err := eng.LoadSource("s1", &memory.SequenceSource{
		Store:         store,
		CheckpointKey: "s1",
		Total:         20,
	}); err != nil {
		t.Fatalf("reload source: %v", err)
	}

	waitUntil(t, 2*time.Second, func() bool { return sink.Count() == 20 })

	cancel()
	if err := <-done; err != nil {
		t.Fatalf("run: %v", err)
	}
}

func waitUntil(t *testing.T, d time.Duration, ok func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if ok() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timeout")
}
