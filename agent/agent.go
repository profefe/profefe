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

	"github.com/profefe/profefe/pkg/profile"
)

const (
	CollectorAddr = "http://localhost:10100"
)

type Option func(a *agent)

func WithCollector(addr string) Option {
	return func(a *agent) {
		a.collectorAddr = addr
	}
}

func WithLabels(args ...string) Option {
	if len(args)%2 != 0 {
		panic("agent.WithLabels: uneven number of arguments, expected key-value pairs")
	}
	return func(a *agent) {
		for i := 0; i+1 < len(args); i += 2 {
			a.labels[args[i]] = args[i+1]
		}
	}
}

func WithClient(c *http.Client) Option {
	return func(a *agent) {
		a.rawClient = c
	}
}

var (
	globalAgent     *agent
	globalAgentOnce sync.Once
)

func Start(name string, opts ...Option) Stopper {
	globalAgentOnce.Do(func() {
		globalAgent = newAgent(name, opts...)
		globalAgent.Start()
	})
	return globalAgent
}

type Stopper interface {
	Stop() error
}

const (
	defaultDuration = 20 * time.Second
	backoffMinDelay = time.Minute
	backoffMaxDelay = 30 * time.Minute
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

type client interface {
	Do(req *http.Request) (*http.Response, error)
}

type agent struct {
	ProfileDuration time.Duration
	CPUProfile      bool
	HeapProfile     bool
	MuxProfile      bool

	labels map[string]string

	backoff       *backoff
	rawClient     client
	collectorAddr string

	tick time.Duration
	wg   sync.WaitGroup
	stop chan struct{}
}

func newAgent(service string, opts ...Option) *agent {
	a := &agent{
		ProfileDuration: defaultDuration,
		CPUProfile:      true,

		labels: make(map[string]string),

		backoff:   newBackoff(backoffMinDelay, backoffMaxDelay, 0),
		rawClient: http.DefaultClient,

		tick: 10 * time.Second,
		stop: make(chan struct{}),
	}

	for _, opt := range opts {
		opt(a)
	}

	a.labels["service"] = service
	a.labels["build_id"] = calcBuildID()
	a.labels["build_version"] = calcBuildVersion()

	return a
}

func calcBuildID() string {
	f, err := os.Open(os.Args[0])
	if err != nil {
		return "x1"
	}
	defer f.Close()

	h := sha1.New()
	if _, err := io.Copy(h, f); err != nil {
		return "x2"
	}
	return hex.EncodeToString(h.Sum(nil))
}

func calcBuildVersion() string {
	now := time.Now().UTC()
	tm := now.Unix()*1000 + int64(now.Nanosecond()/int(time.Millisecond))
	return strconv.FormatUint(uint64(tm), 36)
}

func (a *agent) Start() {
	a.wg.Add(1)
	go a.collectAndSend()
}

func (a *agent) Stop() error {
	close(a.stop)
	a.wg.Wait()
	return nil
}

func (a *agent) collectProfile(ptype profile.ProfileType, buf *bytes.Buffer) error {
	switch ptype {
	case profile.CPUProfile:
		err := pprof.StartCPUProfile(buf)
		if err != nil {
			return fmt.Errorf("failed to start CPU profile: %v", err)
		}
		sleep(a.ProfileDuration, a.stop)
		pprof.StopCPUProfile()
	case profile.HeapProfile:
		fallthrough
	case profile.BlockProfile:
		fallthrough
	case profile.MutexProfile:
		fallthrough
	default:
		return fmt.Errorf("expected profile type %v", ptype)
	}

	return nil
}

type profileReq struct {
	Meta map[string]string
	Data []byte
}

var bodyPool = sync.Pool{
	New: func() interface{} {
		return &bytes.Buffer{}
	},
}

func (a *agent) sendProfile(ptype profile.ProfileType, buf *bytes.Buffer) error {
	preq := &profileReq{
		Meta: make(map[string]string, len(a.labels)),
		Data: buf.Bytes(),
	}

	for k, v := range a.labels {
		if _, ok := preq.Meta[k]; !ok {
			preq.Meta[k] = v
		}
	}

	ptypeStr, err := ptype.MarshalString()
	if err != nil {
		return fmt.Errorf("could not marshal profile type %v: %v", ptype, err)
	}
	preq.Meta["type"] = ptypeStr
	preq.Meta["time"] = time.Now().UTC().Format(time.RFC3339)

	body := bodyPool.Get().(*bytes.Buffer)
	body.Reset()
	defer bodyPool.Put(body)

	if err := json.NewEncoder(body).Encode(preq); err != nil {
		return err
	}

	surl := a.collectorAddr + "/api/v1/profile"
	req, err := http.NewRequest(http.MethodPost, surl, body)
	if err != nil {
		return err
	}

	return a.backoff.Do(func() error {
		resp, err := a.rawClient.Do(req)
		if err != nil {
			return err
		}
		defer resp.Body.Close()

		if resp.StatusCode >= 500 {
			respBody, err := ioutil.ReadAll(resp.Body)
			if err != nil {
				return fmt.Errorf("unexpected respose %s: %v", resp.Status, err)
			}
			return fmt.Errorf("unexpected respose from collector %s: %q", resp.Status, respBody)
		} else if resp.StatusCode >= 400 {
			return Cancel(fmt.Errorf("bad client request: collector respond with %s", resp.Status))
		}

		_, err = io.Copy(ioutil.Discard, resp.Body)

		return err
	})
}

func (a *agent) collectAndSend() {
	defer a.wg.Done()

	timer := time.NewTimer(a.tick)

	var buf bytes.Buffer
	for {
		select {
		case <-a.stop:
			if !timer.Stop() {
				<-timer.C
			}
			return
		case <-timer.C:
			ptype := profile.CPUProfile // hardcoded for now

			if err := a.collectProfile(ptype, &buf); err != nil {
				log.Printf("failed to collect profiles: %v\n", err)
			} else if err := a.sendProfile(ptype, &buf); err != nil {
				log.Printf("failed to send profiles to collector: %v\n", err)
			}

			buf.Reset()
			timer.Reset(a.tick)
		}
	}
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
