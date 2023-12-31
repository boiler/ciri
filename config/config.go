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
}
type ConfigPool struct {
	MaxSize int `toml:"max_size"`
}

func NewConfig() *Config {
	myName := filepath.Base(os.Args[0])
	cfg := &Config{
		Listen:             ":8080",
		DefaultPoolMaxSize: 8,
		MetricsPrefix:      myName,
	}
	path := os.Getenv(strings.ToUpper(myName) + "_CONFIG_PATH")
	if path == "" {
		path = fmt.Sprintf("%s.conf", myName)
	}
	if _, err := os.Stat(path); err != nil {
		log.Printf("config file not found: %s", path)
		return cfg
	} else {
		log.Printf("config file: %s", path)
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

func (cfg *Config) GetPoolMaxSize(pool string) int {
	if p, ok := cfg.Pool[pool]; ok {
		return p.MaxSize
	}
	return cfg.DefaultPoolMaxSize
}
