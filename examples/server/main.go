package main

import (
	"fmt"
	"log"
	"time"

	"github.com/profefe/profefe/agent"
)

const pffCollectorAddr = "http://localhost:10100"

func main() {
	pffAgent, err := agent.Start(
		pffCollectorAddr,
		"example_server",
		agent.WithCPUProfile(10*time.Second),
		agent.WithLogger(agentLogger),
		agent.WithLabels(
			"az", "fra",
			"host", "localhost",
			"instance", "1",
		),
	)
	if err != nil {
		log.Fatalln(err)
	}
	defer pffAgent.Stop()

	time.Sleep(2 * time.Minute)
}

func agentLogger(format string, v ...interface{}) {
	log.Println(fmt.Sprintf(format, v...))
}
