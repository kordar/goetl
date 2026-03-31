package checkpoint

import (
	"fmt"
	"sync"
)

type CheckpointFactory interface {
	Register(name string, builder CheckpointBuilder)
	Get(name string) (CheckpointBuilder, bool)
	Create(name string, cfg map[string]any) (CheckpointStore, error)
}

type CheckpointBuilder func(cfg map[string]any) (CheckpointStore, error)

type checkpointFactory struct {
	mu       sync.RWMutex
	builders map[string]CheckpointBuilder
}

func NewCheckpointFactory() *checkpointFactory {
	return &checkpointFactory{
		builders: make(map[string]CheckpointBuilder),
	}
}

func (f *checkpointFactory) Register(name string, builder CheckpointBuilder) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.builders[name] = builder
}

func (f *checkpointFactory) Get(name string) (CheckpointBuilder, bool) {
	f.mu.RLock()
	b, ok := f.builders[name]
	f.mu.RUnlock()
	return b, ok
}

func (f *checkpointFactory) Create(name string, cfg map[string]any) (CheckpointStore, error) {
	builder, ok := f.Get(name)
	if !ok {
		return nil, fmt.Errorf("checkpoint type not found: %s", name)
	}

	return builder(cfg)
}

/**
使用方式（非常清晰）
1️⃣ 注册实现
factory := NewCheckpointFactory()

factory.Register("memory", func(cfg map[string]any) (CheckpointStore, error) {
    return NewMemoryCheckpoint(), nil
})

factory.Register("redis", func(cfg map[string]any) (CheckpointStore, error) {
    addr := cfg["addr"].(string)
    return NewRedisCheckpoint(addr), nil
})
2️⃣ 创建实例
cp, err := factory.Create("redis", map[string]any{
    "addr": "localhost:6379",
})

*/
