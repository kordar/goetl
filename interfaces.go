package goetl

import "context"

type Source interface {
	Name() string
	Start(ctx context.Context, out chan<- Message) error
}

type RateLimiter interface {
	Acquire(ctx context.Context) error
}

type Dispatcher interface {
	Start(ctx context.Context, errChan chan<- error)
	Deliver(ctx context.Context, msg Message)
	Wait()
}

type Sink interface {
	Name() string
	WriteBatch(ctx context.Context, messages []Message) error
}
