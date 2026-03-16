package worker

import (
	"context"
	"time"
)

func Retry(ctx context.Context, fn func() error) error {
	backoff := 100 * time.Millisecond
	for {
		err := fn()
		if err == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
		if backoff < 2*time.Second {
			backoff *= 2
		}
	}
}
