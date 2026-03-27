package goetl

import (
	"context"
	"testing"
)

func TestChain_Process(t *testing.T) {
	c := NewChain(
		NewMapTransform("m", func(r *Record) (*Record, error) {
			return NewRecord(map[string]any{"n": 1}).WithMeta(r.Meta), nil
		}),
		NewFlatMapTransform("fm", func(r *Record) ([]*Record, error) {
			return []*Record{r, r}, nil
		}),
		NewFilterTransform("f", func(r *Record) bool {
			m, ok := r.Data.(map[string]any)
			return ok && m["n"] == 1
		}),
	)
	out, err := c.Process(context.Background(), NewRecord(map[string]any{"x": 0}))
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if len(out) != 2 {
		t.Fatalf("expected 2, got %d", len(out))
	}
}
