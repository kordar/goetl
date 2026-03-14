package goetl

import "time"

type Record struct {
	ID        string
	Timestamp time.Time
	Source    string
	Data      map[string]any
}

type Checkpoint struct {
	Key   string
	Value string
}

type Message struct {
	Record     *Record
	Checkpoint *Checkpoint
	Partition  string
}
