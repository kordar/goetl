package sink

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/kordar/goetl"
	"github.com/kordar/goetl/checkpoint"
	"github.com/kordar/goetl/checkpoint/ack"
)

type testSink struct {
	mu   sync.Mutex
	fail bool
	n    int
}

func (s *testSink) Name() string { return "test" }

func (s *testSink) WriteBatch(ctx context.Context, messages []goetl.Message) error {
	_ = ctx
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.fail {
		return errors.New("sink fail")
	}
	s.n += len(messages)
	return nil
}

type testCheckpointStore struct {
	mu    sync.Mutex
	saved []struct {
		key string
		cur checkpoint.Cursor
	}
}

func (s *testCheckpointStore) Load(ctx context.Context, key string) (checkpoint.Cursor, error) {
	_ = ctx
	_ = key
	return checkpoint.Cursor{}, checkpoint.ErrNotFound
}

func (s *testCheckpointStore) Save(ctx context.Context, key string, cursor checkpoint.Cursor) error {
	_ = ctx
	s.mu.Lock()
	s.saved = append(s.saved, struct {
		key string
		cur checkpoint.Cursor
	}{key: key, cur: cursor})
	s.mu.Unlock()
	return nil
}

func (s *testCheckpointStore) LastKey() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.saved) == 0 {
		return ""
	}
	return s.saved[len(s.saved)-1].key
}

func TestBatchSinkDispatcher_CommitAfterSuccess(t *testing.T) {
	t.Parallel()
	sink := &testSink{}
	cp := &testCheckpointStore{}
	tracker := ack.NewStrictAckTracker()

	cpf := checkpoint.NewCheckpointFactory()
	cpf.Register("test", func(cfg map[string]any) (checkpoint.CheckpointStore, error) {
		_ = cfg
		return cp, nil
	})
	af := ack.NewFactory()
	af.Register("test", func(cfg map[string]any) (ack.AckTracker, error) {
		_ = cfg
		return tracker, nil
	})

	d := NewBatchSinkDispatcher(sink).
		WithBatchSize(2).
		WithFlushInterval(time.Hour).
		WithRuntimeFactory(goetl.RuntimeFactory{CPFactory: cpf, AckFactory: af}).
		WithRuntimeConfig(goetl.RuntimeConfig{
			Checkpoint: goetl.ComponentConfig{Type: "test", Config: map[string]any{}},
			Ack:        goetl.ComponentConfig{Type: "test", Config: map[string]any{}},
		})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errCh := make(chan error, 10)
	d.Start(ctx, errCh)

	d.Deliver(ctx, *goetl.NewMessage(goetl.NewRecord("a")).WithAck("a").WithCheckpoint("k1").WithCursorValues(1))
	d.Deliver(ctx, *goetl.NewMessage(goetl.NewRecord("b")).WithAck("b").WithCheckpoint("k1").WithCursorValues(2))
	d.Wait()

	if cp.LastKey() != "k1" {
		t.Fatalf("last checkpoint key=%s want=k1", cp.LastKey())
	}
}

func TestBatchSinkDispatcher_NoCommitOnSinkError(t *testing.T) {
	t.Parallel()
	sink := &testSink{fail: true}
	cp := &testCheckpointStore{}
	tracker := ack.NewStrictAckTracker()

	cpf := checkpoint.NewCheckpointFactory()
	cpf.Register("test", func(cfg map[string]any) (checkpoint.CheckpointStore, error) {
		_ = cfg
		return cp, nil
	})
	af := ack.NewFactory()
	af.Register("test", func(cfg map[string]any) (ack.AckTracker, error) {
		_ = cfg
		return tracker, nil
	})

	d := NewBatchSinkDispatcher(sink).
		WithBatchSize(2).
		WithFlushInterval(time.Hour).
		WithRuntimeFactory(goetl.RuntimeFactory{CPFactory: cpf, AckFactory: af}).
		WithRuntimeConfig(goetl.RuntimeConfig{
			Checkpoint: goetl.ComponentConfig{Type: "test", Config: map[string]any{}},
			Ack:        goetl.ComponentConfig{Type: "test", Config: map[string]any{}},
		})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errCh := make(chan error, 10)
	d.Start(ctx, errCh)

	d.Deliver(ctx, *goetl.NewMessage(goetl.NewRecord("a")).WithAck("a").WithCheckpoint("k1").WithCursorValues(1))
	d.Deliver(ctx, *goetl.NewMessage(goetl.NewRecord("b")).WithAck("b").WithCheckpoint("k1").WithCursorValues(2))
	d.Wait()

	if cp.LastKey() != "" {
		t.Fatalf("last checkpoint key=%s want empty", cp.LastKey())
	}
}
