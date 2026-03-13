package metrics

import "sync"

type Labels map[string]string

type Counter interface {
	Add(delta float64)
}

type Gauge interface {
	Set(value float64)
	Add(delta float64)
}

type Histogram interface {
	Observe(value float64)
}

type Collector interface {
	Counter(name string, labels Labels) Counter
	Gauge(name string, labels Labels) Gauge
	Histogram(name string, labels Labels) Histogram
}

type nopCounter struct{}

func (nopCounter) Add(float64) {}

type nopGauge struct{}

func (nopGauge) Set(float64) {}
func (nopGauge) Add(float64) {}

type nopHistogram struct{}

func (nopHistogram) Observe(float64) {}

type NopCollector struct{}

func (NopCollector) Counter(string, Labels) Counter     { return nopCounter{} }
func (NopCollector) Gauge(string, Labels) Gauge         { return nopGauge{} }
func (NopCollector) Histogram(string, Labels) Histogram { return nopHistogram{} }

type AtomicCollector struct {
	mu         sync.Mutex
	counters   map[string]*AtomicCounter
	gauges     map[string]*AtomicGauge
	histograms map[string]*AtomicHistogram
}

func NewAtomicCollector() *AtomicCollector {
	return &AtomicCollector{
		counters:   map[string]*AtomicCounter{},
		gauges:     map[string]*AtomicGauge{},
		histograms: map[string]*AtomicHistogram{},
	}
}

func (c *AtomicCollector) Counter(name string, labels Labels) Counter {
	key := metricKey(name, labels)
	c.mu.Lock()
	defer c.mu.Unlock()
	if v, ok := c.counters[key]; ok {
		return v
	}
	v := &AtomicCounter{}
	c.counters[key] = v
	return v
}

func (c *AtomicCollector) Gauge(name string, labels Labels) Gauge {
	key := metricKey(name, labels)
	c.mu.Lock()
	defer c.mu.Unlock()
	if v, ok := c.gauges[key]; ok {
		return v
	}
	v := &AtomicGauge{}
	c.gauges[key] = v
	return v
}

func (c *AtomicCollector) Histogram(name string, labels Labels) Histogram {
	key := metricKey(name, labels)
	c.mu.Lock()
	defer c.mu.Unlock()
	if v, ok := c.histograms[key]; ok {
		return v
	}
	v := &AtomicHistogram{}
	c.histograms[key] = v
	return v
}

func (c *AtomicCollector) Snapshot() Snapshot {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := Snapshot{
		Counters:   make(map[string]float64, len(c.counters)),
		Gauges:     make(map[string]float64, len(c.gauges)),
		Histograms: make(map[string]HistogramSnapshot, len(c.histograms)),
	}
	for k, v := range c.counters {
		out.Counters[k] = v.Value()
	}
	for k, v := range c.gauges {
		out.Gauges[k] = v.Value()
	}
	for k, v := range c.histograms {
		out.Histograms[k] = v.Snapshot()
	}
	return out
}

type Snapshot struct {
	Counters   map[string]float64
	Gauges     map[string]float64
	Histograms map[string]HistogramSnapshot
}

type HistogramSnapshot struct {
	Count float64
	Sum   float64
}
