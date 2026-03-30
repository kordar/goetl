package checkpoint

import (
	"context"
	"sync"

	"github.com/kordar/goetl"
)

type MemoryCheckpoint struct {
	mu sync.Mutex
	m  map[string]goetl.Cursor
}

func NewMemoryCheckpoint() *MemoryCheckpoint {
	return &MemoryCheckpoint{
		m: make(map[string]goetl.Cursor),
	}
}

func (c *MemoryCheckpoint) Load(ctx context.Context, key string) (goetl.Cursor, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.m[key], nil
}

func (c *MemoryCheckpoint) Save(ctx context.Context, key string, cursor goetl.Cursor) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// old, ok := c.m[key]
	// if !ok || Compare(cursor, old) > 0 {
	// 	c.m[key] = cursor
	// }

	return nil
}
