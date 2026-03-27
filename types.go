package goetl

import "github.com/spf13/cast"

type Record struct {
	Data any
	Meta Meta
}

type Meta struct {
	Source    string
	Offset    any // checkpoint用
	Timestamp int64
	TraceID   string
}

type Message struct {
	Record    *Record
	Partition string
	Attrs     map[string]any
}

func NewRecord(data any) *Record {
	return &Record{Data: data}
}

func (r *Record) WithMeta(meta Meta) *Record {
	r.Meta = meta
	return r
}

func (r *Record) WithSource(source string) *Record {
	r.Meta.Source = source
	return r
}

func (r *Record) WithOffset(offset any) *Record {
	r.Meta.Offset = offset
	return r
}

func (r *Record) WithTimestamp(ts int64) *Record {
	r.Meta.Timestamp = ts
	return r
}

func (r *Record) WithTraceID(traceID string) *Record {
	r.Meta.TraceID = traceID
	return r
}

func (m *Message) With(key string, value any) *Message {
	if m.Attrs == nil {
		m.Attrs = map[string]any{}
	}
	m.Attrs[key] = value
	return m
}

func (m *Message) WithAttrs(kv map[string]any) *Message {
	if kv == nil {
		return m
	}
	if m.Attrs == nil {
		m.Attrs = map[string]any{}
	}
	for k, v := range kv {
		m.Attrs[k] = v
	}
	return m
}

func (m Message) Get(key string) any {
	if m.Attrs == nil {
		return nil
	}
	return m.Attrs[key]
}

func (m Message) String(key string) string {
	return cast.ToString(m.Get(key))
}

func (m Message) Int(key string) int {
	return cast.ToInt(m.Get(key))
}

func (m Message) Int64(key string) int64 {
	return cast.ToInt64(m.Get(key))
}

func (m Message) Float64(key string) float64 {
	return cast.ToFloat64(m.Get(key))
}

func (m Message) Bool(key string) bool {
	return cast.ToBool(m.Get(key))
}
