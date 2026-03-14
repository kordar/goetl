package memory

import (
	"context"
	"strconv"
	"time"

	"github.com/kordar/goetl"
	"github.com/kordar/goetl/checkpoint"
)

type SequenceSource struct {
	Store         checkpoint.Store
	CheckpointKey string
	Partition     string
	Total         int
	Delay         time.Duration
}

func (s *SequenceSource) Name() string { return "memory_sequence" }

func (s *SequenceSource) Start(ctx context.Context, out chan<- goetl.Message) error {
	total := s.Total
	if total < 0 {
		total = 0
	}

	start := 0
	if s.Store != nil && s.CheckpointKey != "" {
		v, err := s.Store.Load(ctx, s.CheckpointKey)
		if err == nil {
			if n, perr := strconv.Atoi(v); perr == nil {
				start = n + 1
			}
		}
	}

	p := s.Partition
	if p == "" {
		p = s.CheckpointKey
	}
	if p == "" {
		p = "default"
	}

	for i := start; i < total; i++ {
		if s.Delay > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(s.Delay):
			}
		}

		msg := goetl.Message{
			Partition: p,
			Record: &goetl.Record{
				ID:        strconv.Itoa(i),
				Timestamp: time.Now(),
				Source:    s.Name(),
				Data: map[string]any{
					"n": i,
				},
			},
		}
		if s.CheckpointKey != "" {
			msg.Checkpoint = &goetl.Checkpoint{Key: s.CheckpointKey, Value: strconv.Itoa(i)}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case out <- msg:
		}
	}
	return nil
}
