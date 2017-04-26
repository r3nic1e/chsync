package main

import "flag"

var sync bool
var config string
var debug, dropColumns bool

func init() {
	flag.StringVar(&config, "config", "config.yml", "config path")
	flag.BoolVar(&sync, "sync", false, "sync schema with config")
	flag.BoolVar(&dropColumns, "drop-columns", false, "drop excess columns")
	flag.BoolVar(&debug, "debug", false, "debug output")
}

func main() {
	flag.Parse()
	config := ParseConfig(config)

	synchronizer := NewSynchronizer(config)
	synchronizer.SetFix(sync)
	synchronizer.SetDropColumns(dropColumns)

	synchronizer.SetupLogger(debug)
	err := synchronizer.Connect()
	if err != nil {
		panic(err)
	}

	synchronizer.Check()

	err = synchronizer.Close()
	if err != nil {
		panic(err)
	}
}
