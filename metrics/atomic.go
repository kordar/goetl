package metrics

import (
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
)

type AtomicCounter struct {
	value atomic.Uint64
}

func (c *AtomicCounter) Add(delta float64) {
	c.value.Add(uint64(delta))
}

func (c *AtomicCounter) Value() float64 {
	return float64(c.value.Load())
}

type AtomicGauge struct {
	value atomic.Int64
}

func (g *AtomicGauge) Set(value float64) {
	g.value.Store(int64(value))
}

func (g *AtomicGauge) Add(delta float64) {
	g.value.Add(int64(delta))
}

func (g *AtomicGauge) Value() float64 {
	return float64(g.value.Load())
}

type AtomicHistogram struct {
	count atomic.Uint64
	sum   atomic.Uint64
}

func (h *AtomicHistogram) Observe(value float64) {
	h.count.Add(1)
	h.sum.Add(uint64(value))
}

func (h *AtomicHistogram) Snapshot() HistogramSnapshot {
	return HistogramSnapshot{
		Count: float64(h.count.Load()),
		Sum:   float64(h.sum.Load()),
	}
}

func metricKey(name string, labels Labels) string {
	if len(labels) == 0 {
		return name
	}
	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	b.WriteString(name)
	for _, k := range keys {
		b.WriteByte('|')
		b.WriteString(k)
		b.WriteByte('=')
		b.WriteString(strconv.Quote(labels[k]))
	}
	return b.String()
}
