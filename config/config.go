package config

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/BurntSushi/toml"
)

type Config struct {
	Listen             string `toml:"listen"`
	SnapshotPath       string `toml:"snapshot_path"`
	AuthToken          string `toml:"auth_token"`
	DefaultPoolMaxSize int    `toml:"default_pool_max_size"`
	MetricsPrefix      string `toml:"metrics_prefix"`
	Pool               map[string]*ConfigPool
	WorkerCustomMetric map[string]*ConfigWorkerCustomMetric `toml:"worker_custom_metric"`
}
type ConfigPool struct {
	MaxSize int `toml:"max_size"`
}
type ConfigWorkerCustomMetric struct {
	Type   string   `toml:"type"`
	Labels []string `toml:"labels"`
}

func NewConfig() *Config {
	return &Config{
		Listen:             ":8080",
		DefaultPoolMaxSize: 8,
		MetricsPrefix:      filepath.Base(os.Args[0]),
	}
}

func (cfg *Config) ReadConfig() error {
	myName := filepath.Base(os.Args[0])
	path := os.Getenv(strings.ToUpper(myName) + "_CONFIG_PATH")
	if path == "" {
		path = fmt.Sprintf("%s.conf", myName)
	}
	if _, err := os.Stat(path); err != nil {
		return fmt.Errorf("config file not found: %s", path)
	} else {
		log.Printf("config file: %s", path)
	}
	configBodyBytes, err := ioutil.ReadFile(path)
	if err != nil {
		return err
	}
	configBody := string(configBodyBytes)
	if _, err := toml.Decode(configBody, cfg); err != nil {
		return err
	}
	return nil
}

func (cfg *Config) GetPoolMaxSize(pool string) int {
	if p, ok := cfg.Pool[pool]; ok {
		return p.MaxSize
	}
	return cfg.DefaultPoolMaxSize
}
