package main

import (
	"io/ioutil"
	"gopkg.in/yaml.v2"
)

type Server struct {
	Host string `yaml:"host"`
	Port uint16 `yaml:"port"`
	User string `yaml:"user"`
	Pass string `yaml:"pass"`
}

type Columns map[string]string

type Table struct {
	View           bool `yaml:"view"`
	Materialized   bool `yaml:"materialized"`
	Populate       bool `yaml:"populate"`
	Columns        Columns `yaml:"columns"`
	Engine         string `yaml:"engine"`
	AsAnotherTable string `yaml:"as_table"`
	AsSelect       string `yaml:"as_select"`
}

type Database struct {
	Name   string `yaml:"name"`
	Tables map[string]Table `yaml:"tables"`
}

type Config struct {
	Servers   []Server `yaml:"servers"`
	Databases []Database `yaml:"databases"`
}

func ParseConfig(path string) *Config {
	cfg, err := ioutil.ReadFile(path)
	if err != nil {
		panic(err)
	}

	config := &Config{}
	err = yaml.Unmarshal(cfg, config)
	if err != nil {
		panic(err)
	}

	return config
}