package engine

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/kordar/goetl"
	"github.com/kordar/goetl/checkpoint"
	sinkdispatcher "github.com/kordar/goetl/dispatcher/sink"
	workpooldispatcher "github.com/kordar/goetl/dispatcher/workpool"
)

type testSource struct {
	name       string
	n          int
	checkpoint checkpoint.CheckpointStore
}

type testMemCheckpoint struct {
	mu sync.Mutex
	m  map[string]checkpoint.Cursor
}

func (c *testMemCheckpoint) Load(ctx context.Context, key string) (checkpoint.Cursor, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.m == nil {
		c.m = map[string]checkpoint.Cursor{}
	}
	return c.m[key], nil
}

func (c *testMemCheckpoint) Save(ctx context.Context, key string, cur checkpoint.Cursor) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.m == nil {
		c.m = map[string]checkpoint.Cursor{}
	}
	c.m[key] = cur
	fmt.Println("save", cur)
	return nil
}

func (s *testSource) Name() string { return s.name }

func (s *testSource) Start(ctx context.Context, out chan<- goetl.Message) error {
	go func() {
		defer close(out)
		start := 0
		if s.checkpoint != nil {
			if cur, err := s.checkpoint.Load(ctx, s.name); err == nil && len(cur.Values) > 0 {
				switch v := cur.Values[0].(type) {
				case int:
					start = v + 1
				case int64:
					start = int(v) + 1
				}
			}
		}
		for i := start; i < s.n; i++ {
			select {
			case <-ctx.Done():
				return
			default:
			}
			rec := goetl.NewRecord(map[string]any{
				"i": i,
			}).WithSource(s.name)
			out <- goetl.Message{Record: rec}
			// if s.checkpoint != nil {
			// 	_ = s.checkpoint.Save(ctx, s.name, checkpoint.Cursor{Values: []any{i}})
			// }
		}
	}()
	return nil
}

func TestEngine_WithSource_BasicFlow(t *testing.T) {
	eng := NewEngine().WithSource(&testSource{name: "src1", n: 100})

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	outCh, errCh := eng.Start(ctx)
	defer eng.Stop()

	var got int
loop:
	for {
		select {
		case msg, ok := <-outCh:
			if !ok {
				break loop
			}
			if msg.Record == nil {
				t.Fatalf("nil record")
			}
			got++
			fmt.Println("========", msg.Record)
			if got == 3 {
				break loop
			}
		case err := <-errCh:
			if err != nil {
				t.Fatalf("unexpected err: %v", err)
			}
		case <-ctx.Done():
			t.Fatalf("timeout waiting for messages, got %d", got)
		}
	}
	if got != 3 {
		t.Fatalf("expected 3 messages, got %d", got)
	}
}

type memSink struct {
	mu         sync.Mutex
	batches    [][]goetl.Message
	checkpoint checkpoint.CheckpointStore
}

func (s *memSink) Name() string { return "mem" }

func (s *memSink) WriteBatch(ctx context.Context, messages []goetl.Message) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := make([]goetl.Message, len(messages))
	copy(cp, messages)
	s.batches = append(s.batches, cp)
	if s.checkpoint != nil {
		_ = s.checkpoint.Save(ctx, s.Name(), checkpoint.Cursor{Values: []any{len(s.batches)}})
	}
	return nil
}

func TestEngine_Dispatcher_Sink_Batch(t *testing.T) {
	c := &testMemCheckpoint{}

	ms := &memSink{checkpoint: c}
	d := sinkdispatcher.NewBatchSinkDispatcher(ms).
		WithBatchSize(1).
		WithFlushInterval(10 * time.Millisecond).
		WithQueueBuffer(10).
		WithBlocking(true)

	n := 10
	// 为 sink dispatcher 增加两个 chain（示例：不改变消息）
	c1 := goetl.NewChain(
		goetl.NewMapTransform("identity1", func(r *goetl.Record) (*goetl.Record, error) {
			fmt.Println("======== identity1 ============", r)
			r.Data = map[string]any{
				"i": "CCCCCC",
			}
			return r, nil
		}),
		goetl.NewFilterTransform("keep", func(r *goetl.Record) bool {
			fmt.Println("======== keep ============", r)
			return true
		}),
	)

	eng := NewEngine().
		WithSource(&testSource{name: "src1", n: n, checkpoint: c}).
		WithDispatcher(d).WithChain(c1)

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	outCh, errCh := eng.Start(ctx)
	defer eng.Stop()

	consumed := 0
loop:
	for {
		select {
		case _, ok := <-outCh:
			if !ok {
				break loop
			}
			consumed++
			if consumed == 5 {
				break loop
			}
		case err := <-errCh:
			if err != nil {
				t.Errorf("unexpected err: %v", err)
			}
		case <-ctx.Done():
			t.Fatalf("timeout waiting for messages")
		}
	}
	// wait a bit to allow flush by ticker
	time.Sleep(50 * time.Millisecond)

	ms.mu.Lock()
	defer ms.mu.Unlock()
	total := 0
	for _, b := range ms.batches {
		total += len(b)
	}
	if total != n {
		t.Fatalf("expected sink to receive %d messages, got %d (batches=%d)", n, total, len(ms.batches))
	}
}

func TestEngine_Dispatcher_Workpool(t *testing.T) {
	n := 10
	th := workpooldispatcher.NewTaskHandle(2, 32)
	th.AddTask("t", func(ctx context.Context, msg goetl.Message) error {
		fmt.Println("========", msg.Record)
		return nil
	})
	d := &workpooldispatcher.WorkpoolDispatcher{
		TH: th,
		TaskIDFunc: func(msg goetl.Message) string {
			return "t"
		},
	}

	eng := NewEngine().
		WithSource(&testSource{name: "src1", n: 99}).
		WithDispatcher(d.WithBlocking(true))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	outCh, errCh := eng.Start(ctx)
	defer eng.Stop()

	consumed := 0
loop:
	for {
		select {
		case _, ok := <-outCh:
			if !ok {
				break loop
			}
			consumed++
			if consumed == n {
				break loop
			}
		case err := <-errCh:
			if err != nil {
				t.Fatalf("unexpected err: %v", err)
			}
		case <-ctx.Done():
			t.Fatalf("timeout waiting for messages")
		}
	}
	// allow workers to drain
	time.Sleep(100 * time.Millisecond)

}
