package plugin

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/kordar/go-etl"
	"github.com/kordar/go-etl/checkpoint"
	"github.com/kordar/go-etl/config"
	"github.com/kordar/go-etl/metrics"
)

var ErrNotRegistered = errors.New("plugin type not registered")

type Factory[T any] func(ctx context.Context, cfg config.Component, rt etl.Runtime) (T, error)

type Registry[T any] struct {
	kind      string
	mu        sync.RWMutex
	factories map[string]Factory[T]
}

func NewRegistry[T any](kind string) *Registry[T] {
	return &Registry[T]{kind: kind, factories: map[string]Factory[T]{}}
}

func (r *Registry[T]) Register(typeName string, f Factory[T]) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.factories[typeName] = f
}

func (r *Registry[T]) Build(ctx context.Context, cfg config.Component, rt etl.Runtime) (T, error) {
	r.mu.RLock()
	f, ok := r.factories[cfg.Type]
	r.mu.RUnlock()
	var zero T
	if !ok {
		return zero, fmt.Errorf("%w: kind=%s type=%s", ErrNotRegistered, r.kind, cfg.Type)
	}
	return f(ctx, cfg, rt)
}

var Sources = NewRegistry[etl.Source]("source")
var Transforms = NewRegistry[etl.Transformer]("transform")
var Sinks = NewRegistry[etl.Sink]("sink")
var Checkpoints = NewRegistry[checkpoint.Store]("checkpoint")
var MetricCollectors = NewRegistry[metrics.Collector]("metrics")
