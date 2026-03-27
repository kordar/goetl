package goetl

import "context"

type Chain struct {
	transforms []Transform
}

func NewChain(ts ...Transform) *Chain {
	c := &Chain{}
	for _, t := range ts {
		if t != nil {
			c.transforms = append(c.transforms, t)
		}
	}
	return c
}

func (c *Chain) Add(t Transform) *Chain {
	if t == nil {
		return c
	}
	c.transforms = append(c.transforms, t)
	return c
}

func (c *Chain) Process(ctx context.Context, rec *Record) ([]*Record, error) {
	if rec == nil {
		return nil, nil
	}
	records := []*Record{rec}
	for _, t := range c.transforms {
		var next []*Record
		for _, r := range records {
			outs, err := t.Process(ctx, r)
			if err != nil {
				return nil, err
			}
			if len(outs) > 0 {
				next = append(next, outs...)
			}
		}
		records = next
		if len(records) == 0 {
			return nil, nil
		}
	}
	return records, nil
}

// ProcessMessage 将算子链应用于消息，保留原消息的元数据（Partition、Checkpoint、Attrs）
func (c *Chain) ProcessMessage(ctx context.Context, msg Message) ([]Message, error) {
	if c == nil || msg.Record == nil {
		return []Message{msg}, nil
	}
	recs, err := c.Process(ctx, msg.Record)
	if err != nil {
		return nil, err
	}
	if len(recs) == 0 {
		return nil, nil
	}
	out := make([]Message, 0, len(recs))
	for _, r := range recs {
		out = append(out, Message{
			Record:    r,
			Partition: msg.Partition,
			Attrs:     msg.Attrs,
		})
	}
	return out, nil
}
