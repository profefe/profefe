package main

import (
	"fmt"
	"log"
	"time"

	"github.com/profefe/profefe/agent"
)

func main() {
	stopper := agent.Start(
		"adjust_server",
		agent.WithCollector(agent.DefaultCollectorAddr),
		agent.WithLogger(agentLogger),
		agent.WithLabels("host", "localhost", "instance", "1", "zone", "fra", "version", "1.0"),
	)
	defer stopper.Stop()

	time.Sleep(2 * time.Minute)
}

func agentLogger(format string, v ...interface{}) {
	log.Println(fmt.Sprintf(format, v...))
}
