package engine

import (
	"context"
	"sync"

	"github.com/kordar/goetl"
)

type SourceConfig struct {
	Source      goetl.Source
	Parallelism int
}

type Engine struct {
	mu             sync.RWMutex
	sourceConfigs  []SourceConfig
	runningSources map[string]context.CancelFunc

	outChan chan goetl.Message
	errChan chan error

	wg     sync.WaitGroup
	ctx    context.Context
	cancel context.CancelFunc

	chain *goetl.Chain

	dispatcher goetl.Dispatcher
}

func NewEngine() *Engine {
	return &Engine{
		runningSources: make(map[string]context.CancelFunc),
	}
}

func (e *Engine) Start(ctx context.Context) (<-chan goetl.Message, <-chan error) {
	e.mu.Lock()
	e.ctx, e.cancel = context.WithCancel(ctx)
	e.outChan = make(chan goetl.Message, 1024)
	e.errChan = make(chan error, 1)

	if e.dispatcher != nil {
		e.dispatcher.Start(e.ctx, e.errChan)
	}

	// 启动已经配置好的 Source
	for _, cfg := range e.sourceConfigs {
		name := cfg.Source.Name()
		if _, exists := e.runningSources[name]; !exists {
			srcCtx, srcCancel := context.WithCancel(e.ctx)
			e.runningSources[name] = srcCancel

			for i := 0; i < cfg.Parallelism; i++ {
				e.wg.Add(1)
				go e.runSource(srcCtx, cfg.Source)
			}
		}
	}
	e.mu.Unlock()

	// 统一关闭 outChan
	go func() {
		<-e.ctx.Done()
		// 给还在运行的 source 发送取消信号
		e.mu.Lock()
		for _, cancel := range e.runningSources {
			cancel()
		}
		e.mu.Unlock()

		e.wg.Wait()
		if e.dispatcher != nil {
			e.dispatcher.Wait()
		}
		close(e.outChan)
		close(e.errChan)
	}()

	return e.outChan, e.errChan
}

func (e *Engine) Stop() {
	if e.cancel != nil {
		e.cancel()
	}
}
