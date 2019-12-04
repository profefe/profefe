package main

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/profefe/profefe/agent"
)

const pffCollectorAddr = "http://localhost:10100"

var instance = os.Getenv("INSTANCE_ID")

func init() {
	if instance == "" {
		instance = "0"
	}
}

func main() {
	pffAgent, err := agent.Start(
		pffCollectorAddr,
		"example_server",
		agent.WithCPUProfile(10*time.Second),
		agent.WithHeapProfile(),
		agent.WithBlockProfile(),
		agent.WithMutexProfile(),
		agent.WithGoroutineProfile(),
		agent.WithThreadcreateProfile(),
		agent.WithLogger(agentLogger),
		agent.WithLabels(
			"region", "europe-west3",
			"dc", "fra",
			"host", "localhost",
			"instance", instance,
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
