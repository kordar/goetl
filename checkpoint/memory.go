package checkpoint

import (
	"context"
	"sync"
)

type MemoryCheckpoint struct {
	mu sync.Mutex
	m  map[string]Cursor
}

func NewMemoryCheckpoint() *MemoryCheckpoint {
	return &MemoryCheckpoint{
		m: make(map[string]Cursor),
	}
}

func (c *MemoryCheckpoint) Load(ctx context.Context, key string) (Cursor, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	v, ok := c.m[key]
	if !ok {
		return Cursor{}, ErrNotFound
	}
	return v, nil
}

func (c *MemoryCheckpoint) Save(ctx context.Context, key string, cursor Cursor) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.m[key] = cursor
	return nil
}
