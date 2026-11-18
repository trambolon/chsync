package config

import (
	"fmt"
	"os"

	toml "github.com/pelletier/go-toml/v2"
)

// Load reads path, interpolates ${ENV} strictly, parses TOML, applies
// defaults and validates. Any failure is fatal by design: a partially
// configured snapshot daemon is worse than a dead one.
func Load(path string) (*Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}
	expanded, err := ExpandEnv(string(raw), os.LookupEnv)
	if err != nil {
		return nil, fmt.Errorf("config %s: %w", path, err)
	}
	var cfg Config
	if err := toml.Unmarshal([]byte(expanded), &cfg); err != nil {
		return nil, fmt.Errorf("parse config %s: %w", path, err)
	}
	cfg.ApplyDefaults()
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &cfg, nil
}
