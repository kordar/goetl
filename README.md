# goetl

goetl 是一个可嵌入的 Go ETL 内核，提供：

- 插件化 Source / Transform / Sink / Checkpoint
- 可控并发 Worker Pool
- 有界队列背压
- 顺序 checkpoint 提交
- 动态加载与替换 Source（引擎级）

## 核心架构

数据流：

```text
Source -> raw queue -> sequencer -> worker queue -> worker pool -> sink
                                               \-> ack -> checkpoint committer
```

模块结构（当前实现）：

```text
engine/
  run.go                    运行主流程
  options.go                Engine Option API
  load_source.go            运行时动态加载/卸载 source
  checkpoint.go             checkpoint 顺序提交
  worker_pool.go            engine 与 worker 子包桥接
  worker/                   worker 子系统（pool/worker/batcher/retry/timer/ack）
  managed_source/           source 聚合与热替换（按 name）
```

## 快速开始

### 1) 代码方式构建并运行

```go
package main

import (
	"context"

	"github.com/kordar/goetl"
	"github.com/kordar/goetl/components/builtin"
	"github.com/kordar/goetl/config"
	"github.com/kordar/goetl/engine"
)

func main() {
	builtin.Register()

	job := config.Job{
		Source: config.Component{
			Type: "memory_sequence",
			Settings: map[string]any{
				"checkpoint_key": "seq",
				"total":          1000,
			},
		},
		Transforms: []config.Component{
			{Type: "trim_strings"},
		},
		Sink:       config.Component{Type: "stdout"},
		Checkpoint: config.Component{Type: "memory"},
	}

	eng, err := engine.Build(context.Background(), job, goetl.Runtime{})
	if err != nil {
		panic(err)
	}

	eng.SetWorkers(8)
	if err := eng.Run(context.Background()); err != nil {
		panic(err)
	}
}
```

### 2) 手动创建 Engine

```go
eng := engine.NewEngine(
	sink,
	engine.WithPipeline(goetl.NewPipeline(transforms...)),
	engine.WithMetrics(metricCollector),
	engine.WithLogger(logger),
	engine.WithCheckpoints(checkpointStore),
	engine.WithWorkers(1, 16, 4),
)

_ = eng.LoadSource("main", source)
_ = eng.Run(ctx)
```

## 配置模型

`config.Job` 主要字段：

- `source`: 单个 source 组件定义
- `transforms`: transform 列表
- `sink`: sink 组件定义
- `checkpoint`: checkpoint 存储组件定义
- `queue.buffer`: raw/work 队列容量
- `workers.min/max`: worker 范围（initial 默认取 max）

示例：

```json
{
  "source": {
    "type": "memory_sequence",
    "settings": {
      "checkpoint_key": "seq",
      "partition": "p0",
      "total": 1000,
      "delay_ms": 0
    }
  },
  "transforms": [
    { "type": "trim_strings" }
  ],
  "sink": { "type": "stdout" },
  "checkpoint": { "type": "memory" },
  "queue": { "buffer": 1024 },
  "workers": { "min": 1, "max": 16 }
}
```

## 动态 Source（两种方式）

### 1) 引擎级动态加载

运行中可以动态增删 source：

```go
_ = eng.LoadSource("users", srcA)
_ = eng.LoadSource("users", srcB) // 同名覆盖替换
_ = eng.UnloadSource("users")
```

### 2) 组件级 dynamic_multi

内置 `dynamic_multi` source（`components/source/dynamic`）可按 provider 周期刷新子 source 列表。
该能力由 source 组件实现，不依赖 engine 特殊开关。

## 内置组件

`components/builtin.Register()` 默认注册：

- Checkpoint: `memory`
- Source:
  - `memory_sequence`
  - `multi`
  - `dynamic_multi`
- Transform: `trim_strings`
- Sink: `stdout`

## Checkpoint 语义

- worker 写入 sink 成功后才发送 ack
- committer 按 partition 内 seq 顺序提交 checkpoint
- 避免并发完成顺序导致 checkpoint 回退或跳跃

## 指标

默认使用 `metrics.NopCollector`。可替换为自定义 Collector。引擎侧会记录：

- `etl_records_total`
- `etl_records_error_total`
- `etl_record_latency_ms`

## 运行验证

```bash
go test ./...
go vet ./...
```
