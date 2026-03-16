package worker

import (
	"context"
	"sync"
)

type AckForwarder struct {
	ctx context.Context
	out chan<- Ack
	in  chan Ack
	wg  sync.WaitGroup
}

func NewAckForwarder(ctx context.Context, out chan<- Ack) *AckForwarder {
	a := &AckForwarder{
		ctx: ctx,
		out: out,
		in:  make(chan Ack, 4096),
	}
	a.wg.Add(1)
	go a.loop()
	return a
}

func (a *AckForwarder) loop() {
	defer a.wg.Done()
	for {
		select {
		case <-a.ctx.Done():
			return
		case item, ok := <-a.in:
			if !ok {
				return
			}
			select {
			case <-a.ctx.Done():
				return
			case a.out <- item:
			}
		}
	}
}

func (a *AckForwarder) Send(ctx context.Context, item Ack) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case a.in <- item:
		return nil
	}
}

func (a *AckForwarder) Close() {
	close(a.in)
	a.wg.Wait()
}
