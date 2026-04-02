package ack

import "time"

var DefaultFactory = func() *factory {
	f := NewFactory()
	f.Register("strict", func(cfg map[string]any) (AckTracker, error) {
		return NewStrictAckTracker(), nil
	})
	f.Register("timeout", func(cfg map[string]any) (AckTracker, error) {
		ms, _ := cfg["timeout_ms"].(int)
		return NewTimeoutAckTracker(time.Duration(ms) * time.Millisecond), nil
	})
	f.Register("window", func(cfg map[string]any) (AckTracker, error) {
		size, _ := cfg["window_size"].(int)
		return NewWindowAckTracker(size), nil
	})
	f.Register("partitioned", func(cfg map[string]any) (AckTracker, error) {
		n, _ := cfg["partitions"].(int)
		if n <= 0 {
			n = 1
		}
		list := make([]AckTracker, n)
		for i := 0; i < n; i++ {
			list[i] = NewStrictAckTracker()
		}
		return NewPartitionedAckTracker(list), nil
	})
	return f
}()

