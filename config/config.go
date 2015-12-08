package config

import (
	"github.com/BurntSushi/toml"
	"github.com/lostz/Aegis/logging"
)

var configLogger = logging.GetLogger("config")

type Config struct {
	Password string
	Addr     string
	User     string
	Pidfile  string
	DBAddr   string
	DB       string
	DBUser   string
	DBPasswd string
}

func LoadConfig(conffile string) (*Config, error) {
	config, err := LoadConfigFile(conffile)
	return config, err

}

func LoadConfigFile(file string) (*Config, error) {
	var config Config
	if _, err := toml.DecodeFile(file, &config); err != nil {
		return &config, err
	}
	return &config, nil

}
