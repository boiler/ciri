package config

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"

	"github.com/BurntSushi/toml"
)

type Config struct {
	Listen                   string `toml:"listen"`
	SnapshotPath             string `toml:"snapshot_path"`
	AuthToken                string `toml:"auth_token"`
	DefaultPoolMaxSize       int    `toml:"default_pool_max_size"`
	Pool                     map[string]*ConfigPool
	MetricsPrefix            string   `toml:"metrics_prefix"`
	MetricsUpdateInterval    uint     `toml:"metrics_update_interval"`
	WorkerCustomGaugeMetrics []string `toml:"worker_custom_gauge_metrics"`
	WorkerCustomCountMetrics []string `toml:"worker_custom_count_metrics"`
}
type ConfigPool struct {
	MaxSize int `toml:"max_size"`
}

func NewConfig() *Config {
	myName := filepath.Base(os.Args[0])
	cfg := &Config{
		Listen:                ":8080",
		DefaultPoolMaxSize:    8,
		MetricsPrefix:         myName,
		MetricsUpdateInterval: 60,
	}
	path := fmt.Sprintf("%s.conf", myName)
	if _, err := os.Stat(path); err != nil {
		log.Printf("config file '%s' not found", path)
		return cfg
	}
	configBodyBytes, err := ioutil.ReadFile(path)
	if err != nil {
		log.Fatal(err)
	}
	configBody := string(configBodyBytes)
	if _, err := toml.Decode(configBody, cfg); err != nil {
		log.Fatal(err)
	}

	return cfg
}

func (cfg *Config) Parse() error {
	return nil
}

func (cfg *Config) GetPoolMaxSize(pool string) int {
	if p, ok := cfg.Pool[pool]; ok {
		return p.MaxSize
	}
	return cfg.DefaultPoolMaxSize
}
