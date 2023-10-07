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
	Listen       string `toml:"listen"`
	SnapshotPath string `toml:"snapshot_path"`
}

func NewConfig() *Config {
	cfg := &Config{
		Listen: ":8080",
	}
	name := filepath.Base(os.Args[0])
	path := fmt.Sprintf("%s.conf", name)
	if _, err := os.Stat(path); err != nil {
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
