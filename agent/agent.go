package agent

import (
	"bytes"
	"context"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"runtime/pprof"
	"sync"
	"time"

	"github.com/profefe/profefe/pkg/profile"
	"github.com/profefe/profefe/pkg/retry"
	"github.com/rs/xid"
)

const DefaultCollectorAddr = "http://localhost:10100"

const (
	defaultDuration     = 20 * time.Second
	defaultTickInterval = 5 * time.Second

	backoffMinDelay = time.Minute
	backoffMaxDelay = 30 * time.Minute
)

var (
	globalAgent     *agent
	globalAgentOnce sync.Once
)

func Start(name string, opts ...Option) {
	globalAgentOnce.Do(func() {
		globalAgent = New(name, opts...)
		globalAgent.Start(context.Background())
	})
}

func Stop() (err error) {
	if globalAgent != nil {
		err = globalAgent.Stop()
	}
	return err
}

type httpClient interface {
	Do(req *http.Request) (*http.Response, error)
}

type agent struct {
	ProfileDuration time.Duration
	CPUProfile      bool
	HeapProfile     bool
	MuxProfile      bool

	labels map[string]string

	logf func(format string, v ...interface{})

	rawClient     httpClient
	collectorAddr string
	// mu protects reqBody which is to encode JSON payload
	mu      sync.Mutex
	reqBody bytes.Buffer
	reqEnc  *json.Encoder

	tick time.Duration
	stop chan struct{} // signals the beginning of stop
	done chan struct{} // closed when stopping is done
}

func New(service string, opts ...Option) *agent {
	a := &agent{
		ProfileDuration: defaultDuration,

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

	a.reqEnc = json.NewEncoder(&a.reqBody)

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

func (a *agent) Start(ctx context.Context) {
	go a.collectAndSend(ctx)
}

func (a *agent) Stop() error {
	close(a.stop)
	<-a.done
	return nil
}

func (a *agent) collectProfile(ctx context.Context, ptype profile.ProfileType, buf *bytes.Buffer) error {
	switch ptype {
	case profile.CPUProfile:
		err := pprof.StartCPUProfile(buf)
		if err != nil {
			return fmt.Errorf("failed to start CPU profile: %v", err)
		}
		sleep(a.ProfileDuration, ctx.Done())
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
	Data []byte            `json:"data"`
}

func (a *agent) sendProfile(ctx context.Context, ptype profile.ProfileType, ts time.Time, buf *bytes.Buffer) error {
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

	if err := a.reqEnc.Encode(preq); err != nil {
		return err
	}

	surl := a.collectorAddr + "/api/v0/profile"
	req, err := http.NewRequest(http.MethodPost, surl, &a.reqBody)
	if err != nil {
		return err
	}
	req = req.WithContext(ctx)

	return retry.Do(
		backoffMinDelay,
		backoffMaxDelay,
		func() error { return a.doRequest(req) },
	)
}

func (a *agent) doRequest(req *http.Request) error {
	resp, err := a.rawClient.Do(req)
	if err, ok := err.(*url.Error); ok && err.Err == context.Canceled {
		return retry.Cancel(err)
	} else if err != nil {
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
	return nil
}

func (a *agent) collectAndSend(ctx context.Context) {
	defer close(a.done)

	timer := time.NewTimer(a.tick)

	ctx, cancel := context.WithCancel(ctx)
	go func() {
		select {
		case <-a.stop:
			cancel()
		}
	}()

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

			if err := a.collectProfile(ctx, ptype, &buf); err != nil {
				a.logf("failed to collect profiles: %v", err)
			} else if err := a.sendProfile(ctx, ptype, ts, &buf); err != nil {
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
