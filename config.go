package main

import (
	"fmt"
	"log"
	"os"

	flag "github.com/spf13/pflag"

	"influx/src/conf"
)

type configFile struct {
	Version bool
	Server  struct {
		Host     string
		Port     string
		User     string
		Password string
	}
	Database struct {
		Drop bool
		Name string
	}
	Import struct {
		Path string
	}
	Debug struct {
		active bool
		Path   string
	}
}

var currentConfig *configFile

func loadConfig() (*configFile, error) {
	var configuration = configFile{} //var options commandLineOptions
	var err error

	var cfg = conf.New()
	f := flag.NewFlagSet("Config", flag.ExitOnError)

	cfg.LoadDefault(map[string]interface{}{
		"server.host":     "192.168.65.26",
		"server.port":     "8086",
		"server.user":     "admin",
		"server.password": "UEPVwPC4KNv9ZGyx4qJE",
		"database.drop":   false,
		"database.name":   "winsol",
		"import.path":     "/tmp",
		"debug.active":    false,
		"debug.path":      "/tmp",
	})

	cfgFile := *f.String("configFile", "./config.yaml", "Config File")
	f.String("server.host", "influx", "influxdb server name")
	f.String("server.port", "8086", "influxdb port")
	f.String("server.user", "", "influxdb user")
	f.String("server.password", "", "password")
	f.Bool("database.drop", false, "drop and recreate db")
	f.String("database.name", "winsol", "database name")
	f.String("import.path", ".", "import path (csv)")
	f.Bool("debug.active", false, "enable debugging")
	f.String("debug.path", ".", "debug path")
	f.Bool("version", false, "print version and exit")

	if err := f.Parse(os.Args[1:]); err != nil {
		log.Printf("%v", err)
	}

	// Load the Config files provided in the config file.
	if err := cfg.LoadYaml(cfgFile); err != nil {
		log.Printf("error loading file: %v", err)
	}

	// Load the Config files provided in the commandline.
	if err := cfg.LoadFlag(f); err != nil {
		log.Printf("error loading Config: %v", err)
	}

	if err = cfg.Unmarshal("", &configuration); err != nil {
		return nil, fmt.Errorf("unable to decode the Config file into struct, %v", err)
	}

	return &configuration, err
}

func init() {
	var err error
	if currentConfig, err = loadConfig(); err != nil {
		panic(err)
	}

	if currentConfig.Version {
		fmt.Println(influxVersion)
		os.Exit(0)
	}

	return
}
