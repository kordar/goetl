package engine

import (
	"github.com/kordar/goetl"
	"github.com/kordar/goetl/engine/managed_source"
)

func (e *Engine) LoadSource(name string, src goetl.Source) error {
	if e.managed == nil {
		e.managed = managed_source.NewManagedSource()
	}
	return e.managed.Load(name, src)
}

func (e *Engine) UnloadSource(name string) error {
	if e.managed == nil {
		return nil
	}
	return e.managed.Unload(name)
}
