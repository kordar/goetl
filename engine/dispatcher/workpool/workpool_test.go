package workpool

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/kordar/goetl"
	"github.com/kordar/goetl/checkpoint"
	ack "github.com/kordar/goetl/checkpoint/ack"
)

type testCPStore struct {
	mu    sync.Mutex
	saved []struct {
		key string
		cur checkpoint.Cursor
	}
}

func (s *testCPStore) Load(ctx context.Context, key string) (checkpoint.Cursor, error) {
	_ = ctx
	_ = key
	return checkpoint.Cursor{}, checkpoint.ErrNotFound
}

func (s *testCPStore) Save(ctx context.Context, key string, cursor checkpoint.Cursor) error {
	_ = ctx
	s.mu.Lock()
	s.saved = append(s.saved, struct {
		key string
		cur checkpoint.Cursor
	}{key: key, cur: cursor})
	s.mu.Unlock()
	return nil
}

func (s *testCPStore) LastKey() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.saved) == 0 {
		return ""
	}
	return s.saved[len(s.saved)-1].key
}

func TestWorkpool_CommitAfterSuccess(t *testing.T) {
	t.Parallel()
	h := NewTaskHandle(2, 16)
	h.AddTask("ok", func(ctx context.Context, msg goetl.Message) error {
		_ = ctx
		_ = msg
		return nil
	})

	cp := &testCPStore{}
	cpf := checkpoint.NewCheckpointFactory()
	cpf.Register("cp", func(cfg map[string]any) (checkpoint.CheckpointStore, error) {
		_ = cfg
		return cp, nil
	})
	af := ack.NewFactory()
	af.Register("ack", func(cfg map[string]any) (ack.AckTracker, error) {
		_ = cfg
		return ack.NewStrictAckTracker(), nil
	})

	h.WithRuntimeFactory(goetl.RuntimeFactory{CPFactory: cpf, AckFactory: af}).
		WithRuntimeConfig(goetl.RuntimeConfig{
			Checkpoint: goetl.ComponentConfig{Type: "cp", Config: map[string]any{}},
			Ack:        goetl.ComponentConfig{Type: "ack", Config: map[string]any{}},
		})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errCh := make(chan error, 10)
	h.StartWorkerPool(ctx, errCh)

	msg := *goetl.NewMessage(goetl.NewRecord("v")).WithAck("a").WithCheckpoint("k1").WithCursorValues(1)
	h.SendToTaskQueue(ctx, errCh, "ok", msg)

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cp.LastKey() == "k1" {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	h.Wait()

	if cp.LastKey() != "k1" {
		t.Fatalf("last checkpoint key=%s want k1", cp.LastKey())
	}
}

func TestWorkpool_NoCommitOnHandlerError(t *testing.T) {
	t.Parallel()
	h := NewTaskHandle(1, 16)
	h.AddTask("fail", func(ctx context.Context, msg goetl.Message) error {
		_ = ctx
		_ = msg
		return errors.New("handler fail")
	})

	cp := &testCPStore{}
	cpf := checkpoint.NewCheckpointFactory()
	cpf.Register("cp", func(cfg map[string]any) (checkpoint.CheckpointStore, error) {
		_ = cfg
		return cp, nil
	})
	af := ack.NewFactory()
	af.Register("ack", func(cfg map[string]any) (ack.AckTracker, error) {
		_ = cfg
		return ack.NewStrictAckTracker(), nil
	})

	h.WithRuntimeFactory(goetl.RuntimeFactory{CPFactory: cpf, AckFactory: af}).
		WithRuntimeConfig(goetl.RuntimeConfig{
			Checkpoint: goetl.ComponentConfig{Type: "cp", Config: map[string]any{}},
			Ack:        goetl.ComponentConfig{Type: "ack", Config: map[string]any{}},
		})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errCh := make(chan error, 10)
	h.StartWorkerPool(ctx, errCh)

	msg := *goetl.NewMessage(goetl.NewRecord("v")).WithAck("a").WithCheckpoint("k1").WithCursorValues(1)
	h.SendToTaskQueue(ctx, errCh, "fail", msg)

	time.Sleep(100 * time.Millisecond)
	cancel()
	h.Wait()

	if cp.LastKey() != "" {
		t.Fatalf("last checkpoint key=%s want empty", cp.LastKey())
	}
}

