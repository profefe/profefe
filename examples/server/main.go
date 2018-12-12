package main

import (
	"fmt"
	"log"
	"time"

	"github.com/profefe/profefe/agent"
	"github.com/profefe/profefe/version"
)

func main() {
	agent.Start(
		"example_server",
		agent.WithCPUProfile(20*time.Second),
		agent.WithCollector(agent.DefaultCollectorAddr),
		agent.WithLogger(agentLogger),
		agent.WithLabels(
			"az", "fra",
			"host", "localhost",
			"instance", "1",
			"version", version.Version,
			"commit", version.Commit,
			"build-date", version.BuildTime,
		),
	)
	defer agent.Stop()

	time.Sleep(2 * time.Minute)
}

func agentLogger(format string, v ...interface{}) {
	log.Println(fmt.Sprintf(format, v...))
}
