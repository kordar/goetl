package engine

import "github.com/kordar/goetl"

type ack struct {
	partition  string
	seq        uint64
	checkpoint *goetl.Checkpoint
}
