package main

import (
	"os"
	"time"

	"github.com/profefe/profefe/agent"
)

func main() {
	stopper := agent.Start(
		"adjust_server",
		agent.WithCollector(agent.DefaultCollectorAddr),
		agent.WithLabels("host", "localhost", "instance", "1", "zone", "fra", "version", "1.0"),
	)
	defer stopper.Stop()

	time.Sleep(2 * time.Minute)

	os.Exit(1)
}
