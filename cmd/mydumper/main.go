/*
 * go-mydumper
 * xelabs.org
 *
 * Copyright (c) XeLabs
 * GPL License
 *
 */

package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/harryge00/go-mydumper/pkg/common"
	"github.com/harryge00/go-mydumper/pkg/config"
	"github.com/xelabs/go-mysqlstack/xlog"
)

var (
	flagConfig string

	log = xlog.NewStdLog(xlog.Level(xlog.INFO))
)

func init() {
	flag.StringVar(&flagConfig, "c", "", "config file")
}

func usage() {
	fmt.Println("Usage: " + os.Args[0] + " -c conf/mydumper.ini.sample")
	flag.PrintDefaults()
}

func main() {
	flag.Usage = func() { usage() }
	flag.Parse()

	if flagConfig == "" {
		usage()
		os.Exit(0)
	}

	args, err := config.ParseDumperConfig(flagConfig)
	common.AssertNil(err)

	common.Dumper(log, args)
}
