package goetl

import "context"

type Transform interface {
	Name() string
	Process(ctx context.Context, in *Record) ([]*Record, error)
}

type MapFunc func(*Record) (*Record, error)

type MapTransform struct {
	name string
	fn   MapFunc
}

func NewMapTransform(name string, fn MapFunc) *MapTransform {
	if name == "" {
		name = "map"
	}
	return &MapTransform{name: name, fn: fn}
}

func (m *MapTransform) Name() string { return m.name }

func (m *MapTransform) Process(ctx context.Context, in *Record) ([]*Record, error) {
	_ = ctx
	out, err := m.fn(in)
	if err != nil {
		return nil, err
	}
	if out == nil {
		return nil, nil
	}
	return []*Record{out}, nil
}

type FilterFunc func(*Record) bool

type FilterTransform struct {
	name string
	fn   FilterFunc
}

func NewFilterTransform(name string, fn FilterFunc) *FilterTransform {
	if name == "" {
		name = "filter"
	}
	return &FilterTransform{name: name, fn: fn}
}

func (f *FilterTransform) Name() string { return f.name }

func (f *FilterTransform) Process(ctx context.Context, in *Record) ([]*Record, error) {
	_ = ctx
	if f.fn(in) {
		return []*Record{in}, nil
	}
	return nil, nil
}

type FlatMapFunc func(*Record) ([]*Record, error)

type FlatMapTransform struct {
	name string
	fn   FlatMapFunc
}

func NewFlatMapTransform(name string, fn FlatMapFunc) *FlatMapTransform {
	if name == "" {
		name = "flat_map"
	}
	return &FlatMapTransform{name: name, fn: fn}
}

func (f *FlatMapTransform) Name() string { return f.name }

func (f *FlatMapTransform) Process(ctx context.Context, in *Record) ([]*Record, error) {
	return f.fn(in)
}
