package sink

import (
	"context"
	"encoding/json"
	"io"
	"os"
	"sync"

	"github.com/kordar/goetl"
)

type Stdout struct {
	mu sync.Mutex
	w  io.Writer
}

func NewStdout(w io.Writer) *Stdout {
	if w == nil {
		w = os.Stdout
	}
	return &Stdout{w: w}
}

func (s *Stdout) Name() string { return "stdout" }

func (s *Stdout) Write(ctx context.Context, r *goetl.Record) error {
	_ = ctx
	s.mu.Lock()
	defer s.mu.Unlock()
	enc := json.NewEncoder(s.w)
	return enc.Encode(r)
}

func (s *Stdout) Close(ctx context.Context) error {
	_ = ctx
	return nil
}
