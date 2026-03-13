package multi

import (
	"context"
	"sync"

	"github.com/kordar/go-etl"
)

type Source struct {
	NameValue string
	Sources   []etl.Source
}

func New(sources ...etl.Source) *Source {
	return &Source{Sources: append([]etl.Source(nil), sources...)}
}

func (s *Source) Name() string {
	if s.NameValue != "" {
		return s.NameValue
	}
	return "multi"
}

func (s *Source) Start(ctx context.Context, out chan<- etl.Message) error {
	if len(s.Sources) == 0 {
		return nil
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	errCh := make(chan error, len(s.Sources))
	var wg sync.WaitGroup
	wg.Add(len(s.Sources))
	for _, child := range s.Sources {
		ch := child
		go func() {
			defer wg.Done()
			if err := ch.Start(ctx, out); err != nil && ctx.Err() == nil {
				select {
				case errCh <- err:
				default:
				}
				cancel()
			}
		}()
	}
	wg.Wait()
	close(errCh)

	for err := range errCh {
		if err != nil {
			return err
		}
	}
	return ctx.Err()
}
