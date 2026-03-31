package ack

import (
	"errors"

	"github.com/kordar/goetl/checkpoint"
)

type AckTracker interface {
	Add(id string, cursor *checkpoint.Cursor)
	Ack(id string)
	Commit() (*checkpoint.Cursor, bool)
	Close() error
}

var ErrNotFound = errors.New("ack tracker not found")
