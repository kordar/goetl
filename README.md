# goetl

一个轻量、可插拔的 ETL 处理引擎，支持 Source→Chain→Dispatcher（Sink/Workpool）流水线，内置 Checkpoint 与 Ack 跟踪机制。

## 组件概览

- Source（数据源）：拉取/扫描数据并产出 Message
- Chain（算子链）：对 Record 做映射、过滤、扁平化等处理
- Dispatcher（分发器）：
  - Sink（批量写）：面向外部 IO 的批量提交（数据库、消息队列等）
  - Workpool（任务池）：按 TaskID 路由到 Handler 并发处理
- Checkpoint/Ack：游标存储与在途消息确认，计算安全提交边界

## 核心类型

- 接口定义见 [interfaces.go](./interfaces.go)
  - Source、Dispatcher、Sink
- 消息与记录见 [types.go](./types.go)
  - Message 包含 Record、Partition、Attrs；Record 包含 Data 与 Meta

## Engine 用法

```go
eng := engine.NewEngine().
    WithSource(mySource, 4).        // 可选并行度
    WithDispatcher(myDispatcher).   // Sink 或 Workpool
    WithChain(goetl.NewChain(       // 算子链
        goetl.NewMapTransform("m", func(r *goetl.Record) (*goetl.Record, error) { return r, nil }),
        goetl.NewFilterTransform("f", func(r *goetl.Record) bool { return true }),
    )).
    WithSourceChanBuffer(1024)      // Source 内部输出通道缓冲（默认 256）

outCh, errCh := eng.Start(ctx)
defer eng.Stop()

for msg := range outCh { /* 消费消息 */ }
for err := range errCh { /* 处理错误（非阻塞上报） */ }
```

异步运行（独立协程）：

```go
eng.Run(ctx, func(m goetl.Message) { /* onMsg */ }, func(err error) { /* onErr */ })
```

## Source

实现接口：

```go
type Source interface {
    Name() string
    Start(ctx context.Context, out chan<- Message) error
}
```

Engine 会为每个 Source 创建一个内部通道（缓冲可通过 `WithSourceChanBuffer` 配置），并在 goroutine 中运行 Source。

## Chain

构建与使用：

```go
c := goetl.NewChain(
    goetl.NewMapTransform("normalize", func(r *goetl.Record) (*goetl.Record, error) { return r, nil }),
    goetl.NewFilterTransform("keep", func(r *goetl.Record) bool { return true }),
)
eng := engine.NewEngine().WithChain(c)
```

Engine 在派发前先对消息应用 Chain（支持 fan-out）。

## Dispatcher：Sink（批量）

构建：

```go
d := sinkdispatcher.NewBatchSinkDispatcher(mySink).
    WithBatchSize(100).
    WithFlushInterval(1 * time.Second).
    WithQueueBuffer(1024).
    WithRetries(3).
    WithBlocking(true) // 默认开启阻塞，队列满时背压到上游
```

扩展：

- 分发结束回调（每次批量 flush 后触发）：`WithDeliverFinishCallback(func(ctx context.Context, msg ...goetl.Message) error)`
- 可与 Checkpoint/Ack 结合，进行批量成功后的统一提交

## Dispatcher：Workpool（任务池）

构建：

```go
th := workpooldispatcher.NewTaskHandle(2, 256)
th.AddTask("t", func(ctx context.Context, msg goetl.Message) error { return nil })

d := &workpooldispatcher.WorkpoolDispatcher{
    TH: th,
    TaskIDFunc: func(msg goetl.Message) string { return "t" },
}.WithBlocking(true) // 默认开启阻塞
```

扩展：

- 分发结束回调（每次 Deliver 投递后触发）：`WithDeliverFinishCallback(func(ctx context.Context, msg ...goetl.Message) error)`

## Checkpoint 与 Ack

- Cursor 与 Store 接口见 [checkpoint/store.go](./checkpoint/store.go)
- 内存存储实现见 [checkpoint/memory.go](./checkpoint/memory.go)
  - `Load` 不存在时返回 `ErrNotFound`
- Ack tracker 策略见 [checkpoint/ack](./checkpoint/ack)
  - strict：严格顺序提交
  - timeout：队头超时自动视为 ack，防卡死
  - window：轻量门控（当前语义等同 strict）
  - partitioned：多分区并行，所有分区可提交时返回游标

结合 Sink 的示例（批量成功后统一 ack → commit → save）：

```go
f := ack.DefaultFactory
tracker, _ := f.Create("strict", nil)
store := checkpoint.NewMemoryCheckpoint()
key := "job:partition-0"

d := sinkdispatcher.NewBatchSinkDispatcher(mySink).
    WithBatchSize(100).
    WithBlocking(true).
    WithDeliverFinishCallback(func(ctx context.Context, msgs ...goetl.Message) error {
        for _, m := range msgs {
            id := m.Record.Meta.TraceID
            cur := &checkpoint.Cursor{Values: []any{m.Record.Meta.Offset}}
            tracker.Add(id, cur)
            tracker.Ack(id)
        }
        if c, ok := tracker.Commit(); ok && c != nil {
            _ = store.Save(ctx, key, *c)
        }
        return nil
    })
```

## 阻塞与丢弃策略

- Engine 的 outChan：阻塞背压（通道满阻塞）
- Sink/Workpool 的内部队列：
  - 默认阻塞（`WithBlocking(true)`）：队列满时背压到上游
  - 非阻塞（`WithBlocking(false)`）：队列满时丢弃并上报错误（ErrDispatchQueueFull / ErrTaskQueueFull）

根据需求选择一致性或弹性优先，并配合回调监控/告警。

## 运行测试

```bash
go test ./...
go vet ./...
```

