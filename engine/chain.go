package engine

import "github.com/kordar/goetl"

func (e *Engine) WithChain(c *goetl.Chain) *Engine {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.chain = c
	return e
}
