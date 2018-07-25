package agent

import (
	"bytes"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"runtime/pprof"
	"sync"
	"time"

	"github.com/profefe/profefe/pkg/profile"
	"github.com/profefe/profefe/pkg/retry"
	"github.com/rs/xid"
)

const (
	DefaultCollectorAddr = "http://localhost:10100"
)

const (
	defaultDuration     = 20 * time.Second
	defaultTickInterval = 5 * time.Second
	backoffMinDelay     = time.Minute
	backoffMaxDelay     = 30 * time.Minute
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

func WithHTTPClient(c *http.Client) Option {
	return func(a *agent) {
		a.rawClient = c
	}
}

func WithLogger(logf func(string, ...interface{})) Option {
	return func(a *agent) {
		a.logf = logf
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

type client interface {
	Do(req *http.Request) (*http.Response, error)
}

type agent struct {
	ProfileDuration time.Duration
	CPUProfile      bool
	HeapProfile     bool
	MuxProfile      bool

	labels map[string]string

	logf func(format string, v ...interface{})

	rawClient     client
	collectorAddr string
	// mu protects reqBody which is used to encode JSON payload
	mu      sync.Mutex
	reqBody bytes.Buffer
	enc     *json.Encoder

	tick time.Duration
	stop chan struct{} // stop signals the beginning of stop
	done chan struct{} // done is closed when stop is done
}

func newAgent(service string, opts ...Option) *agent {
	a := &agent{
		ProfileDuration: defaultDuration,
		CPUProfile:      true,

		labels:    make(map[string]string),
		rawClient: http.DefaultClient,

		logf: func(format string, v ...interface{}) {},

		tick: defaultTickInterval,
		stop: make(chan struct{}),
		done: make(chan struct{}),
	}

	for _, opt := range opts {
		opt(a)
	}

	a.labels[profile.LabelService] = service
	a.labels[profile.LabelID] = calcBuildID(a)
	a.labels[profile.LabelGeneration] = calcGeneration()

	a.enc = json.NewEncoder(&a.reqBody)

	return a
}

func calcBuildID(a *agent) string {
	prog := os.Args[0]
	f, err := os.Open(prog)
	if err != nil {
		a.logf("failed to read binary %s: %v", prog, err)
		return "x1"
	}
	defer f.Close()

	h := sha1.New()
	if _, err := io.Copy(h, f); err != nil {
		a.logf("failed to generate sha1 from binary %s: %v", prog, err)
		return "x2"
	}
	return hex.EncodeToString(h.Sum(nil))
}

func calcGeneration() string {
	guid := xid.New()
	return guid.String()
}

func (a *agent) Start() {
	go a.collectAndSend()
}

func (a *agent) Stop() error {
	close(a.stop)
	<-a.done
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
	Meta map[string]string `json:"meta"`
	Data json.RawMessage   `json:"data"`
}

func (a *agent) sendProfile(ptype profile.ProfileType, ts time.Time, buf *bytes.Buffer) error {
	preq := &profileReq{
		Meta: make(map[string]string, len(a.labels)),
		Data: buf.Bytes(),
	}

	for k, v := range a.labels {
		if _, ok := preq.Meta[k]; !ok {
			preq.Meta[k] = v
		}
	}

	preq.Meta[profile.LabelType] = ptype.MarshalString()
	preq.Meta[profile.LabelTime] = ts.Format(time.RFC3339)

	a.mu.Lock()
	defer a.mu.Unlock()

	a.reqBody.Reset()

	if err := a.enc.Encode(preq); err != nil {
		return err
	}

	surl := a.collectorAddr + "/api/v1/profile"
	req, err := http.NewRequest(http.MethodPost, surl, &a.reqBody)
	if err != nil {
		return err
	}

	return retry.Do(backoffMinDelay, backoffMaxDelay, func() error {
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
			return retry.Cancel(fmt.Errorf("bad client request: collector responded with %s", resp.Status))
		}

		return err
	})
}

func (a *agent) collectAndSend() {
	defer close(a.done)

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
			ts := time.Now().UTC()

			if err := a.collectProfile(ptype, &buf); err != nil {
				a.logf("failed to collect profiles: %v", err)
			} else if err := a.sendProfile(ptype, ts, &buf); err != nil {
				a.logf("failed to send profiles to collector: %v", err)
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
