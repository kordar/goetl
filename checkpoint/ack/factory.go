package ack

import (
	"fmt"
	"sync"
)

type AckTrackerBuilder func(cfg map[string]any) (AckTracker, error)

type AckTrackerFactory interface {
	Register(name string, builder AckTrackerBuilder)
	Get(name string) (AckTrackerBuilder, bool)
	Create(name string, cfg map[string]any) (AckTracker, error)
}

type factory struct {
	mu       sync.RWMutex
	builders map[string]AckTrackerBuilder
}

func NewFactory() *factory {
	return &factory{builders: map[string]AckTrackerBuilder{}}
}

func (f *factory) Register(name string, builder AckTrackerBuilder) {
	if name == "" || builder == nil {
		return
	}
	f.mu.Lock()
	f.builders[name] = builder
	f.mu.Unlock()
}

func (f *factory) Get(name string) (AckTrackerBuilder, bool) {
	f.mu.RLock()
	b, ok := f.builders[name]
	f.mu.RUnlock()
	return b, ok
}

func (f *factory) Create(name string, cfg map[string]any) (AckTracker, error) {
	b, ok := f.Get(name)
	if !ok {
		return nil, fmt.Errorf("ack tracker not found: %s", name)
	}
	return b(cfg)
}
