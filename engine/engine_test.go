package engine

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/kordar/go-etl"
	"github.com/kordar/go-etl/components/memory"
)

type collectSink struct {
	mu   sync.Mutex
	recs []*etl.Record
}

func (s *collectSink) Name() string { return "collect" }

func (s *collectSink) Write(ctx context.Context, r *etl.Record) error {
	_ = ctx
	s.mu.Lock()
	s.recs = append(s.recs, r)
	s.mu.Unlock()
	return nil
}

func (s *collectSink) Close(ctx context.Context) error {
	_ = ctx
	return nil
}

func (s *collectSink) Count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.recs)
}

func TestEngine_SequenceCheckpoint(t *testing.T) {
	t.Parallel()

	store := memory.NewCheckpointStore()
	src := &memory.SequenceSource{
		Store:         store,
		CheckpointKey: "seq",
		Total:         100,
	}
	sink := &collectSink{}

	eng := &Engine{
		Source:      src,
		Pipeline:    etl.NewPipeline(),
		Sink:        sink,
		Checkpoints: store,
		Options: Options{
			QueueBuffer:    8,
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
	if sink.Count() != 100 {
		t.Fatalf("count=%d want=%d", sink.Count(), 100)
	}
	v, err := store.Load(ctx, "seq")
	if err != nil {
		t.Fatalf("load checkpoint: %v", err)
	}
	if v != "99" {
		t.Fatalf("checkpoint=%s want=%s", v, "99")
	}
}

func TestEngine_ResumeFromCheckpoint(t *testing.T) {
	t.Parallel()

	store := memory.NewCheckpointStore()
	if err := store.Save(context.Background(), "seq", "49"); err != nil {
		t.Fatalf("save checkpoint: %v", err)
	}
	src := &memory.SequenceSource{
		Store:         store,
		CheckpointKey: "seq",
		Total:         100,
	}
	sink := &collectSink{}

	eng := &Engine{
		Source:      src,
		Pipeline:    etl.NewPipeline(),
		Sink:        sink,
		Checkpoints: store,
		Options: Options{
			QueueBuffer:    8,
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
	if sink.Count() != 50 {
		t.Fatalf("count=%d want=%d", sink.Count(), 50)
	}
	v, err := store.Load(ctx, "seq")
	if err != nil {
		t.Fatalf("load checkpoint: %v", err)
	}
	if v != "99" {
		t.Fatalf("checkpoint=%s want=%s", v, "99")
	}
}
