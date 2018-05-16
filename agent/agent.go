package agent

import (
	"bytes"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	"os"
	"runtime/pprof"
	"strconv"
	"sync"
	"time"
)

const (
	CollectorAddr = "http://localhost:10100"
)

type AgentOptions func(a *agent)

func WithCollector(addr string) AgentOptions {
	return func(a *agent) {
		a.collectorAddr = addr
	}
}

func WithLabels(args ...string) AgentOptions {
	if len(args)%2 != 0 {
		panic("agent.WithLabels: uneven number of arguments, expected key-value pairs")
	}
	return func(a *agent) {
		for i := 0; i+1 < len(args); i += 2 {
			a.labels[args[i]] = args[i+1]
		}
	}
}

var globalAgent *agent

var globalAgentOnce sync.Once

func Start(name string, opts ...AgentOptions) Stopper {
	globalAgentOnce.Do(func() {
		globalAgent = newAgent(name, opts...)
	})
	return globalAgent
}

type Stopper interface {
	Stop() error
}

const (
	defaultDuration = 20 * time.Second
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

type agent struct {
	CPUProfile  bool
	HeapProfile bool
	MuxProfile  bool
	Duration    time.Duration

	service      string
	buildID      string
	buildVersion string

	labels map[string]string

	rawClient     client
	collectorAddr string

	wg       sync.WaitGroup
	tick     time.Duration
	stopping chan struct{}
}

func newAgent(service string, opts ...AgentOptions) *agent {
	a := &agent{
		CPUProfile: true,
		Duration:   defaultDuration,

		labels: make(map[string]string),

		rawClient: http.DefaultClient,

		tick:     10 * time.Second,
		stopping: make(chan struct{}),
	}

	a.service = service
	a.buildID = calcBuildID()
	a.buildVersion = strconv.FormatInt(rand.Int63(), 10)

	for _, opt := range opts {
		opt(a)
	}

	a.wg.Add(1)
	go a.collectAndSend()

	return a
}

func calcBuildID() string {
	f, err := os.Open(os.Args[0])
	if err != nil {
		return "x0"
	}
	defer f.Close()

	h := sha1.New()
	if _, err := io.Copy(h, f); err != nil {
		return "x0"
	}
	return hex.EncodeToString(h.Sum(nil))
}

func (a *agent) collectProfiles(buf *bytes.Buffer) error {
	switch {
	case a.CPUProfile:
		log.Println("collecting cpu profile...")
		err := pprof.StartCPUProfile(buf)
		if err != nil {
			return fmt.Errorf("failed to start CPU profile: %v", err)
		}
		sleep(a.Duration, a.stopping)
		pprof.StopCPUProfile()
	case a.HeapProfile:
	case a.MuxProfile:
	}

	return nil
}

type client interface {
	Do(req *http.Request) (*http.Response, error)
}

var bodyPool = sync.Pool{
	New: func() interface{} {
		return &bytes.Buffer{}
	},
}

func (a *agent) sendProfiles(buf *bytes.Buffer) error {
	log.Println("sending profiles...")

	reqBody := struct {
		Meta map[string]string
		Data []byte
	}{
		Meta: map[string]string{
			"name":    a.service,
			"id":      a.buildID,
			"version": a.buildVersion,
			"time":    strconv.FormatInt(time.Now().UTC().UnixNano(), 10),
		},
		Data: buf.Bytes(),
	}

	for k, v := range a.labels {
		if _, ok := reqBody.Meta[k]; !ok {
			reqBody.Meta[k] = v
		}
	}

	body := bodyPool.Get().(*bytes.Buffer)
	body.Reset()
	defer bodyPool.Put(body)

	if err := json.NewEncoder(body).Encode(reqBody); err != nil {
		return err
	}

	surl := a.collectorAddr + "/api/v1/profile"
	req, err := http.NewRequest(http.MethodPost, surl, body)
	if err != nil {
		return err
	}

	resp, err := a.rawClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	_, err = io.Copy(ioutil.Discard, resp.Body)
	return err
}

func (a *agent) collectAndSend() {
	defer a.wg.Done()

	timer := time.NewTimer(a.tick)

	var buf bytes.Buffer
	for {
		select {
		case <-a.stopping:
			if !timer.Stop() {
				<-timer.C
			}
			return
		case <-timer.C:
			if err := a.collectProfiles(&buf); err != nil {
				log.Printf("failed to collect profiles: %v\n", err)
			} else if err := a.sendProfiles(&buf); err != nil {
				log.Printf("failed to send profiles to collector: %v\n", err)
			}

			buf.Reset()
			timer.Reset(a.tick)
		}
	}
}

func (a *agent) Stop() error {
	close(a.stopping)
	a.wg.Wait()
	return nil
}

var timersPool = sync.Pool{}

func sleep(d time.Duration, cancel <-chan struct{}) {
	t, _ := timersPool.Get().(*time.Timer)
	if t == nil {
		t = time.NewTimer(d)
	} else {
		t.Reset(d)
	}

	select {
	case <-t.C:
	case <-cancel:
		if !t.Stop() {
			<-t.C
		}
	}

	timersPool.Put(t)
}
