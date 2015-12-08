package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"strconv"

	"github.com/lostz/Aegis/config"
	"github.com/lostz/Aegis/logging"
	"github.com/lostz/Aegis/server"
)

var Version = "No Version Provided"
var BuildStamp = "No Builds Tamp"
var CommitId = "No Commit Id"

var logger = logging.GetLogger("main")

func main() {
	conf := resolveConfig()
	if err := start(conf); err != nil {
		os.Exit(1)
	}

}

func resolveConfig() (conf *config.Config) {
	conffile := flag.String("conf", "aegis.conf", "Config file path (Configs in this file are over-written by command line options)")
	flag.Parse()
	conf, confErr := config.LoadConfig(*conffile)
	if confErr != nil {
		logger.Criticalf("Failed to load the config file: %s", confErr)
		os.Exit(1)
	}
	return

}

func createPidFile(pidfile string) error {
	if pidString, err := ioutil.ReadFile(pidfile); err == nil {
		if pid, err := strconv.Atoi(string(pidString)); err == nil {
			if _, err := os.Stat(fmt.Sprintf("/proc/%d/", pid)); err == nil {
				return fmt.Errorf("Pidfile found, try stopping another running mackerel-agent or delete %s", pidfile)
			} else {
				logger.Warningf("Pidfile found, but there seems no another process of aegis. Ignoring %s", pidfile)
			}
		} else {
			logger.Warningf("Malformed pidfile found. Ignoring %s", pidfile)
		}
	}

	file, err := os.Create(pidfile)
	if err != nil {
		logger.Criticalf("Failed to create a pidfile: %s", err)
		return err
	}
	defer file.Close()

	_, err = fmt.Fprintf(file, "%d", os.Getpid())
	return err
}

func removePidFile(pidfile string) {
	if err := os.Remove(pidfile); err != nil {
		logger.Errorf("Failed to remove the pidfile: %s: %s", pidfile, err)
	}
}

func start(conf *config.Config) error {
	if err := createPidFile(conf.Pidfile); err != nil {
		return err
	}
	s := server.NewServer(conf)
	s.Version = Version
	s.BuildStamp = BuildStamp
	s.CommitId = CommitId
	s.Start()
	return nil

}
