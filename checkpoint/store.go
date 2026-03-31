package checkpoint

import (
	"context"
	"errors"
)

var ErrNotFound = errors.New("checkpoint not found")

type CheckpointStore interface {
	// 启动恢复：获取上一次处理到的边界
	Load(ctx context.Context, key string) (Cursor, error)

	// 提交边界：表示该位置之前的数据已处理完成
	Save(ctx context.Context, key string, cursor Cursor) error
}

type Cursor struct {
	Values []any          `json:"values"` // 边界值（如时间戳 / ID / offset）
	Meta   map[string]any `json:"meta"`   // 可扩展信息（分区、版本等）
}
