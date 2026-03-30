package checkpoint

import (
	"fmt"
	"sync"
)

type CheckpointFactory interface {
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

func (f *checkpointFactory) Create(name string, cfg map[string]any) (CheckpointStore, error) {
	f.mu.RLock()
	builder, ok := f.builders[name]
	f.mu.RUnlock()

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

type AckTrackerFactory interface {
	Create(name string, cfg map[string]any) (AckTracker, error)
}

type AckTrackerBuilder func(cfg map[string]any) (AckTracker, error)

type ackTrackerFactory struct {
	mu       sync.RWMutex
	builders map[string]AckTrackerBuilder
}

func NewAckTrackerFactory() *ackTrackerFactory {
	return &ackTrackerFactory{
		builders: make(map[string]AckTrackerBuilder),
	}
}

func (f *ackTrackerFactory) Register(name string, builder AckTrackerBuilder) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.builders[name] = builder
}

func (f *ackTrackerFactory) Create(name string, cfg map[string]any) (AckTracker, error) {
	f.mu.RLock()
	builder, ok := f.builders[name]
	f.mu.RUnlock()

	if !ok {
		return nil, fmt.Errorf("ack tracker not found: %s", name)
	}

	return builder(cfg)
}

/**
内置几种实现（建议你直接支持）
✅ 1. Strict（严格顺序，默认）
factory.Register("strict", func(cfg map[string]any) (AckTracker, error) {
    return NewStrictAckTracker(), nil
})

特点：

✔ 不允许跳跃提交
✔ 最安全（不丢数据）
❌ 可能卡住
✅ 2. Timeout（带超时）
factory.Register("timeout", func(cfg map[string]any) (AckTracker, error) {
    timeout := time.Duration(cfg["timeout_ms"].(int)) * time.Millisecond
    return NewTimeoutAckTracker(timeout), nil
})

特点：

✔ 防止卡死
✔ 超时自动处理（跳过 / 重试）
✅ 3. Window（乱序窗口）
factory.Register("window", func(cfg map[string]any) (AckTracker, error) {
    size := cfg["window_size"].(int)
    return NewWindowAckTracker(size), nil
})

特点：

✔ 高吞吐
❌ 可能丢数据（可控）
✅ 4. Partitioned（强烈推荐）
factory.Register("partitioned", func(cfg map[string]any) (AckTracker, error) {
    partitions := cfg["partitions"].(int)

    trackers := make([]AckTracker, partitions)
    for i := 0; i < partitions; i++ {
        trackers[i] = NewStrictAckTracker()
    }

    return NewPartitionedAckTracker(trackers), nil
})

👉 每个 partition 独立 AckTracker
*/
