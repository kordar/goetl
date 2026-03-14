package dynamic

import (
	"errors"
	"sync"

	"github.com/kordar/goetl/config"
)

var ErrProviderNotRegistered = errors.New("provider type not registered")

type ProviderFactory func(settings map[string]any) (Provider, error)

var providerMu sync.RWMutex
var providerFactories = map[string]ProviderFactory{}

func RegisterProvider(typeName string, f ProviderFactory) {
	if typeName == "" || f == nil {
		return
	}
	providerMu.Lock()
	providerFactories[typeName] = f
	providerMu.Unlock()
}

func BuildProvider(typeName string, settings map[string]any) (Provider, error) {
	providerMu.RLock()
	f, ok := providerFactories[typeName]
	providerMu.RUnlock()
	if !ok {
		return nil, ErrProviderNotRegistered
	}
	return f(settings)
}

func init() {
	RegisterProvider("file", func(settings map[string]any) (Provider, error) {
		var s struct {
			Path string `json:"path"`
		}
		if err := config.DecodeSettings(settings, &s); err != nil {
			return nil, err
		}
		return NewFileProvider(s.Path), nil
	})
}
