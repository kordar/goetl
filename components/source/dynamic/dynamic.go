package dynamic

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/kordar/go-etl"
	"github.com/kordar/go-etl/config"
)

type BuildChildFunc func(ctx context.Context, c config.Component) (etl.Source, error)

type Provider interface {
	List(ctx context.Context) ([]config.Component, error)
}

type fileProvider struct {
	path string
	mu   sync.Mutex
}

func NewFileProvider(path string) Provider {
	return &fileProvider{path: path}
}

func (p *fileProvider) List(ctx context.Context) ([]config.Component, error) {
	_ = ctx
	p.mu.Lock()
	defer p.mu.Unlock()
	f, err := os.Open(p.path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	dec := json.NewDecoder(f)
	dec.UseNumber()
	var list []config.Component
	if err := dec.Decode(&list); err != nil {
		return nil, err
	}
	return list, nil
}

type Source struct {
	NameValue      string
	Provider       Provider
	ReloadInterval time.Duration
	Strategy       string // "graceful" | "immediate"
	DrainTimeout   time.Duration
	DedupLatest    bool
	BuildChild     BuildChildFunc
}

func (s *Source) Name() string {
	if s.NameValue != "" {
		return s.NameValue
	}
	return "dynamic_multi"
}

type child struct {
	cancel func()
	done   chan struct{}
	hash   string
}

func (s *Source) Start(ctx context.Context, out chan<- etl.Message) error {
	if s.Provider == nil || s.BuildChild == nil {
		return errors.New("dynamic source requires Provider and BuildChild")
	}
	reloadInt := s.ReloadInterval
	if reloadInt <= 0 {
		reloadInt = 5 * time.Second
	}
	drain := s.DrainTimeout
	if drain <= 0 {
		drain = 5 * time.Second
	}
	graceful := s.Strategy == "" || s.Strategy == "graceful"

	current := map[string]child{}
	mu := sync.Mutex{}

	apply := func(ctx context.Context) error {
		list, err := s.Provider.List(ctx)
		if err != nil {
			return err
		}
		desired := indexByName(list, s.DedupLatest)
		mu.Lock()
		defer mu.Unlock()

		// stop removed or changed
		for id, ch := range current {
			c, ok := desired[id]
			if !ok || hashComponent(c) != ch.hash {
				if graceful {
					ch.cancel()
					select {
					case <-ch.done:
					case <-time.After(drain):
					}
				} else {
					ch.cancel()
					<-ch.done
				}
				delete(current, id)
			}
		}
		// start new / changed
		for id, comp := range desired {
			h := hashComponent(comp)
			ch, ok := current[id]
			if ok && ch.hash == h {
				continue
			}
			if ok {
				// already stopped above, or needs replacement
				// ensure stopped
				ch.cancel()
				<-ch.done
				delete(current, id)
			}
			src, err := s.BuildChild(ctx, comp)
			if err != nil {
				return err
			}
			childCtx, cancel := context.WithCancel(ctx)
			done := make(chan struct{})
			go func(src etl.Source) {
				_ = src.Start(childCtx, out)
				close(done)
			}(src)
			current[id] = child{cancel: cancel, done: done, hash: h}
		}
		return nil
	}

	if err := apply(ctx); err != nil {
		return err
	}
	t := time.NewTicker(reloadInt)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			mu.Lock()
			for _, ch := range current {
				ch.cancel()
			}
			for _, ch := range current {
				<-ch.done
			}
			mu.Unlock()
			return ctx.Err()
		case <-t.C:
			_ = apply(ctx)
		}
	}
}

func indexByName(list []config.Component, latest bool) map[string]config.Component {
	m := map[string]config.Component{}
	for i, c := range list {
		name := c.Name
		if name == "" {
			name = c.Type + ":" + sha(hashComponent(c)+":"+itoa(i))
		}
		if _, ok := m[name]; ok && !latest {
			continue
		}
		cc := c
		cc.Name = name
		m[name] = cc
	}
	return m
}

func hashComponent(c config.Component) string {
	b, _ := json.Marshal(c)
	return sha(string(b))
}

func sha(s string) string {
	h := sha1.New()
	h.Write([]byte(s))
	return hex.EncodeToString(h.Sum(nil))
}

func itoa(i int) string {
	return strconv.Itoa(i)
}
