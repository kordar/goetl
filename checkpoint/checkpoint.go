package checkpoint

import (
	"context"
	"errors"
)

var ErrNotFound = errors.New("checkpoint not found")

type Store interface {
	Save(ctx context.Context, key string, value string) error
	Load(ctx context.Context, key string) (string, error)
}
