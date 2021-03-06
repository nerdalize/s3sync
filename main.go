package main

import (
	"fmt"
	"os"

	"github.com/nerdalize/s3sync/command"

	"github.com/mitchellh/cli"
)

var (
	name    = "s3sync"
	version = "build.from.src"
)

func main() {
	c := cli.NewCLI(name, version)
	c.Args = os.Args[1:]
	c.Commands = map[string]cli.CommandFactory{
		"push": command.PushFactory(),
	}

	status, err := c.Run()
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s: %s", name, err)
	}

	os.Exit(status)
}
