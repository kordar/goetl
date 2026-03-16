package managed_source

import (
	"context"
	"sync"
	"time"

	"github.com/kordar/goetl"
)

type ManagedSource struct {
	mu      sync.Mutex
	started bool
	ctx     context.Context
	out     chan<- goetl.Message

	children map[string]*ManagedChild
	exts     []ManagedSourceExtension
}

type ManagedChild struct {
	gen     uint64
	src     goetl.Source
	running bool
	cancel  context.CancelFunc
	done    chan struct{}
}

type ManagedSourceHost interface {
	Load(name string, src goetl.Source) error
	LoadWithDrain(name string, src goetl.Source, drain time.Duration) error
	Unload(name string) error
	UnloadWithDrain(name string, drain time.Duration) error
}

type ManagedSourceExtension interface {
	Start(ctx context.Context, host ManagedSourceHost) error
}

func NewManagedSource() *ManagedSource {
	return &ManagedSource{
		children: map[string]*ManagedChild{},
	}
}

func (s *ManagedSource) Name() string { return "managed_source" }
