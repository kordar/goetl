package memory

import (
	"context"
	"sync"

	"github.com/kordar/goetl/checkpoint"
)

type CheckpointStore struct {
	mu sync.Mutex
	m  map[string]string
}

func NewCheckpointStore() *CheckpointStore {
	return &CheckpointStore{m: map[string]string{}}
}

func (s *CheckpointStore) Save(ctx context.Context, key string, value string) error {
	_ = ctx
	s.mu.Lock()
	s.m[key] = value
	s.mu.Unlock()
	return nil
}

func (s *CheckpointStore) Load(ctx context.Context, key string) (string, error) {
	_ = ctx
	s.mu.Lock()
	v, ok := s.m[key]
	s.mu.Unlock()
	if !ok {
		return "", checkpoint.ErrNotFound
	}
	return v, nil
}
