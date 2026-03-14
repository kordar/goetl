package transform

import (
	"context"
	"strings"

	"github.com/kordar/goetl"
)

type TrimStrings struct{}

func (t *TrimStrings) Name() string { return "trim_strings" }

func (t *TrimStrings) Transform(ctx context.Context, r *goetl.Record) (*goetl.Record, error) {
	_ = ctx
	if r == nil || r.Data == nil {
		return r, nil
	}
	for k, v := range r.Data {
		s, ok := v.(string)
		if !ok {
			continue
		}
		r.Data[k] = strings.TrimSpace(s)
	}
	return r, nil
}
