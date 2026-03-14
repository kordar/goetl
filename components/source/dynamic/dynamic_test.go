package dynamic

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kordar/goetl"
	"github.com/kordar/goetl/config"
)

type stubProvider struct {
	list []config.Component
}

func (p *stubProvider) List(ctx context.Context) ([]config.Component, error) {
	_ = ctx
	return append([]config.Component(nil), p.list...), nil
}

type stubSrc struct {
	started *atomic.Int32
}

func (s *stubSrc) Name() string { return "stub" }
func (s *stubSrc) Start(ctx context.Context, out chan<- goetl.Message) error {
	s.started.Add(1)
	<-ctx.Done()
	_ = out
	return ctx.Err()
}

func TestDynamic_DedupLatestWins(t *testing.T) {
	t.Parallel()

	p := &stubProvider{
		list: []config.Component{
			{Type: "a", Name: "s", Settings: map[string]any{"v": 1}},
			{Type: "a", Name: "s", Settings: map[string]any{"v": 2}},
		},
	}
	var builtCount atomic.Int32
	var startedCount atomic.Int32
	var seenV atomic.Int32

	src := &Source{
		Provider:       p,
		ReloadInterval: time.Hour, // avoid second reload
		DedupLatest:    true,
		BuildChild: func(ctx context.Context, c config.Component) (goetl.Source, error) {
			_ = ctx
			builtCount.Add(1)
			if v, ok := c.Settings["v"].(int); ok {
				seenV.Store(int32(v))
			}
			return &stubSrc{started: &startedCount}, nil
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	out := make(chan goetl.Message, 1)
	go func() { _ = src.Start(ctx, out) }()

	// wait a bit for start
	time.Sleep(50 * time.Millisecond)
	cancel()
	time.Sleep(50 * time.Millisecond)

	if builtCount.Load() != 1 {
		t.Fatalf("built=%d want=1", builtCount.Load())
	}
	if seenV.Load() != 2 {
		t.Fatalf("latest value not used, got=%d want=2", seenV.Load())
	}
}
