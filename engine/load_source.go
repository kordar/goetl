package engine

import (
	"errors"

	"github.com/kordar/goetl"
)

func (e *Engine) LoadSource(name string, src goetl.Source) error {
	if !e.Options.DynamicSources {
		return errors.New("dynamic sources disabled")
	}
	if e.managed == nil {
		e.managed = newManagedSource()
	}
	return e.managed.Load(name, src)
}
