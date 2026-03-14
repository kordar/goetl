package goetl

import (
	"context"
)

type Pipeline struct {
	transforms []Transformer
}

func NewPipeline(transforms ...Transformer) *Pipeline {
	return &Pipeline{transforms: append([]Transformer(nil), transforms...)}
}

func (p *Pipeline) Process(ctx context.Context, r *Record) (*Record, error) {
	var err error
	for _, t := range p.transforms {
		r, err = t.Transform(ctx, r)
		if err != nil {
			return nil, err
		}
		if r == nil {
			return nil, nil
		}
	}
	return r, nil
}
