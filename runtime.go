package goetl

import (
	"fmt"

	"github.com/kordar/goetl/checkpoint"
	"github.com/kordar/goetl/checkpoint/ack"
)

type ComponentConfig struct {
	Type   string
	Config map[string]any
}

type RuntimeConfig struct {
	Checkpoint ComponentConfig
	Ack        ComponentConfig
}

type RuntimeFactory struct {
	CPFactory  checkpoint.CheckpointFactory
	AckFactory ack.AckTrackerFactory
}

type Runtime struct {
	CP  checkpoint.CheckpointStore
	Ack ack.AckTracker
}

func (f *RuntimeFactory) Create(cfg RuntimeConfig) (*Runtime, error) {
	var cp checkpoint.CheckpointStore
	if cfg.Checkpoint.Type != "" {
		if f.CPFactory == nil {
			return nil, fmt.Errorf("checkpoint factory is nil")
		}
		var err error
		cp, err = f.CPFactory.Create(cfg.Checkpoint.Type, cfg.Checkpoint.Config)
		if err != nil {
			return nil, err
		}
	}

	var a ack.AckTracker
	if cfg.Ack.Type != "" {
		if f.AckFactory == nil {
			return nil, fmt.Errorf("ack factory is nil")
		}
		var err error
		a, err = f.AckFactory.Create(cfg.Ack.Type, cfg.Ack.Config)
		if err != nil {
			return nil, err
		}
	}

	return &Runtime{CP: cp, Ack: a}, nil
}
