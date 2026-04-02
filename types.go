package goetl

import (
	"github.com/kordar/goetl/checkpoint"
	"github.com/spf13/cast"
)

type Record struct {
	Data any  `json:"data"`
	Meta Meta `json:"meta"`
}

type Meta struct {
	Source    string `json:"source"`
	Timestamp int64  `json:"timestamp"`
	TraceID   string `json:"trace_id"`
}

type Message struct {
	Record     *Record           `json:"record"`
	Partition  string            `json:"partition"`
	Ack        string            `json:"ack"`
	Checkpoint string            `json:"checkpoint"`
	Cursor     checkpoint.Cursor `json:"cursor"`
	Attrs      map[string]any    `json:"attrs"`
}

func NewMessage(record *Record) *Message {
	return &Message{Record: record}
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

func (r *Record) WithTimestamp(ts int64) *Record {
	r.Meta.Timestamp = ts
	return r
}

func (r *Record) WithTraceID(traceID string) *Record {
	r.Meta.TraceID = traceID
	return r
}

func (m *Message) WithRecord(record *Record) *Message {
	m.Record = record
	return m
}

func (m *Message) WithPartition(partition string) *Message {
	m.Partition = partition
	return m
}

func (m *Message) WithAck(ack string) *Message {
	m.Ack = ack
	return m
}

func (m *Message) WithCheckpoint(key string) *Message {
	m.Checkpoint = key
	return m
}

func (m *Message) WithCursor(cursor checkpoint.Cursor) *Message {
	m.Cursor = cursor
	return m
}

func (m *Message) WithCursorValues(values ...any) *Message {
	m.Cursor.Values = append([]any(nil), values...)
	return m
}

func (m *Message) WithCursorMeta(meta map[string]any) *Message {
	if meta == nil {
		m.Cursor.Meta = nil
		return m
	}
	m.Cursor.Meta = make(map[string]any, len(meta))
	for k, v := range meta {
		m.Cursor.Meta[k] = v
	}
	return m
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
