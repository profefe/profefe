package main

import (
	"fmt"
	"log"
	"time"

	"github.com/profefe/profefe/agent"
)

func main() {
	agent.Start(
		"adjust_server",
		agent.WithCollector(agent.DefaultCollectorAddr),
		agent.WithLogger(agentLogger),
		agent.WithLabels("az", "fra", "host", "localhost", "instance", "1", "version", "1.0"),
	)
	defer agent.Stop()

	time.Sleep(2 * time.Minute)
}

func agentLogger(format string, v ...interface{}) {
	log.Println(fmt.Sprintf(format, v...))
}
