package goetl

import (
	"log/slog"

	"github.com/kordar/goetl/checkpoint"
	"github.com/kordar/goetl/metrics"
)

type Runtime struct {
	Logger      *slog.Logger
	Metrics     metrics.Collector
	Checkpoints checkpoint.Store
}
