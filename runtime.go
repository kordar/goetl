package etl

import (
	"log/slog"

	"github.com/kordar/go-etl/checkpoint"
	"github.com/kordar/go-etl/metrics"
)

type Runtime struct {
	Logger      *slog.Logger
	Metrics     metrics.Collector
	Checkpoints checkpoint.Store
}
