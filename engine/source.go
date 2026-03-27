package engine

import (
	"context"

	"github.com/kordar/goetl"
	"github.com/spf13/cast"
)

// WithSource 注册一个新的 Source（仅在 Start 前有效，Start 后建议使用 LoadSource）
func (e *Engine) WithSource(src goetl.Source, opts ...any) *Engine {
	e.mu.Lock()
	defer e.mu.Unlock()
	parallelism := 1
	if len(opts) > 0 {
		parallelism = cast.ToInt(opts[0])
		if parallelism <= 0 {
			parallelism = 1
		}
	}
	e.sourceConfigs = append(e.sourceConfigs, SourceConfig{
		Source:      src,
		Parallelism: parallelism,
	})
	return e
}

// LoadSource 在运行时动态加载并启动 Source
func (e *Engine) LoadSource(src goetl.Source, opts ...any) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.ctx == nil {
		parallelism := 1
		if len(opts) > 0 {
			parallelism = cast.ToInt(opts[0])
			if parallelism <= 0 {
				parallelism = 1
			}
		}
		e.sourceConfigs = append(e.sourceConfigs, SourceConfig{
			Source:      src,
			Parallelism: parallelism,
		})
		return nil
	}

	name := src.Name()
	if _, exists := e.runningSources[name]; exists {
		return nil
	}
	parallelism := 1
	if len(opts) > 0 {
		parallelism = cast.ToInt(opts[0])
		if parallelism <= 0 {
			parallelism = 1
		}
	}
	srcCtx, srcCancel := context.WithCancel(e.ctx)
	e.runningSources[name] = srcCancel
	for i := 0; i < parallelism; i++ {
		e.wg.Add(1)
		go e.runSource(srcCtx, src)
	}
	return nil
}

// UnloadSource 在运行时动态卸载 Source
func (e *Engine) UnloadSource(name string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if cancel, exists := e.runningSources[name]; exists {
		cancel()
		delete(e.runningSources, name)
	}
}

func (e *Engine) runSource(ctx context.Context, s goetl.Source) {
	defer e.wg.Done()
	ch := make(chan goetl.Message, 256)
	if err := s.Start(ctx, ch); err != nil {
		select {
		case e.errChan <- err:
		default:
		}
		return
	}
	for msg := range ch {
		if e.chain == nil {
			e.dispatch(ctx, msg)
			continue
		}
		outs, err := e.chain.ProcessMessage(ctx, msg)
		if err != nil {
			select {
			case e.errChan <- err:
			default:
			}
			continue
		}
		if len(outs) == 0 {
			continue
		}
		for _, outMsg := range outs {
			e.dispatch(ctx, outMsg)
		}
	}
}
