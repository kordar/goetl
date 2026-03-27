package source

import (
	"context"
	"time"

	"github.com/kordar/goetl"
)

type DemoSource struct {
	Name    string
	Limiter goetl.RateLimiter
}

func (s *DemoSource) Start(ctx context.Context, out chan<- goetl.Message) error {
	go func() {
		defer close(out)

		for {
			select {
			case <-ctx.Done():
				return
			default:
				if s.Limiter != nil {
					if err := s.Limiter.Acquire(ctx); err != nil {
						return
					}
				}
				now := time.Now()
				rec := goetl.NewRecord(map[string]any{
					"ts":   now.UTC().Format(time.RFC3339Nano),
					"name": s.Name,
				}).WithSource(s.Name).WithTimestamp(now.UnixNano())
				out <- goetl.Message{Record: rec}
				time.Sleep(500 * time.Millisecond)
			}
		}
	}()
	return nil
}
