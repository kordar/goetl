package config

import (
	"encoding/json"
	"io"
)

type Component struct {
	Type     string         `json:"type"`
	Name     string         `json:"name,omitempty"`
	Settings map[string]any `json:"settings,omitempty"`
}

type Job struct {
	Source     Component   `json:"source"`
	Transforms []Component `json:"transforms,omitempty"`
	Sink       Component   `json:"sink"`
	Checkpoint Component   `json:"checkpoint,omitempty"`

	Queue struct {
		Buffer int `json:"buffer,omitempty"`
	} `json:"queue,omitempty"`

	Workers struct {
		Min int `json:"min,omitempty"`
		Max int `json:"max,omitempty"`
	} `json:"workers,omitempty"`
}

func LoadJobJSON(r io.Reader) (Job, error) {
	var cfg Job
	dec := json.NewDecoder(r)
	dec.UseNumber()
	if err := dec.Decode(&cfg); err != nil {
		return Job{}, err
	}
	return cfg, nil
}

func DecodeSettings(settings map[string]any, out any) error {
	if settings == nil {
		return nil
	}
	b, err := json.Marshal(settings)
	if err != nil {
		return err
	}
	return json.Unmarshal(b, out)
}
