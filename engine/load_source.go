package engine

import (
	"errors"

	"github.com/kordar/go-etl"
)

func (e *Engine) LoadSource(name string, src etl.Source) error {
	if !e.Options.DynamicSources {
		return errors.New("dynamic sources disabled")
	}
	if e.managed == nil {
		e.managed = newManagedSource()
	}
	return e.managed.Load(name, src)
}
