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
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"runtime/pprof"
	"sync"
	"time"

	"github.com/profefe/profefe/pkg/profile"
	"github.com/profefe/profefe/pkg/retry"
)

const DefaultCollectorAddr = "http://localhost:10100"

const (
	defaultDuration     = 10 * time.Second
	defaultTickInterval = time.Minute

	backoffMinDelay = time.Minute
	backoffMaxDelay = 30 * time.Minute
)

var (
	globalAgent     *agent
	globalAgentOnce sync.Once
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

func Start(name string, opts ...Option) (err error) {
	globalAgentOnce.Do(func() {
		globalAgent = New(name, opts...)
		err = globalAgent.Start(context.Background())
	})
	return err
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

	service    string
	buildID    string
	agentToken string
	labels     map[string]string

	logf func(format string, v ...interface{})

	rawClient     httpClient
	collectorAddr string

	tick time.Duration
	stop chan struct{} // signals the beginning of stop
	done chan struct{} // closed when stopping is done
}

func New(service string, opts ...Option) *agent {
	a := &agent{
		ProfileDuration: defaultDuration,

		service:   service,
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

	return a
}

func (a *agent) Start(ctx context.Context) error {
	a.buildID = a.getBuildID()

	if err := a.registerAgent(ctx); err != nil {
		return fmt.Errorf("failed to register agent: %v", err)
	}

	go a.collectAndSend(ctx)

	return nil
}

func (a *agent) Stop() error {
	close(a.stop)
	<-a.done
	return nil
}

func (a *agent) getBuildID() string {
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

func (a *agent) registerAgent(ctx context.Context) error {
	var labels bytes.Buffer
	{
		first := true
		for k, v := range a.labels {
			if !first {
				labels.WriteByte(',')
			}
			first = false
			labels.WriteString(url.QueryEscape(k))
			labels.WriteByte('=')
			labels.WriteString(url.QueryEscape(v))
		}
	}

	q := url.Values{}
	q.Set("service", a.service)
	q.Set("id", a.buildID)
	q.Set("labels", labels.String())

	surl := a.collectorAddr + "/api/0/profile?" + q.Encode()
	req, err := http.NewRequest(http.MethodPut, surl, nil)
	if err != nil {
		return err
	}

	var rawResp bytes.Buffer

	req = req.WithContext(ctx)
	err = retry.Do(
		backoffMinDelay,
		backoffMaxDelay,
		func() error { return a.doRequest(req, &rawResp) },
	)
	if err != nil {
		return err
	}

	resp := struct {
		Token string `json:"token"`
	}{}
	if err := json.NewDecoder(&rawResp).Decode(&resp); err != nil {
		return fmt.Errorf("could not decode response: %v", err)
	}
	if resp.Token == "" {
		return fmt.Errorf("empty token in response: %s", rawResp.String())
	}

	a.agentToken = resp.Token

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
		return fmt.Errorf("unknown profile type %v", ptype)
	}

	return nil
}

func (a *agent) sendProfile(ctx context.Context, ptype profile.ProfileType, buf *bytes.Buffer) error {
	q := url.Values{}
	q.Set("id", a.buildID)
	q.Set("token", a.agentToken)
	q.Set("type", ptype.String())

	surl := a.collectorAddr + "/api/0/profile?" + q.Encode()
	req, err := http.NewRequest(http.MethodPost, surl, buf)
	if err != nil {
		return err
	}
	req = req.WithContext(ctx)

	return retry.Do(
		backoffMinDelay,
		backoffMaxDelay,
		func() error { return a.doRequest(req, nil) },
	)
}

func (a *agent) doRequest(req *http.Request, v io.Writer) error {
	resp, err := a.rawClient.Do(req)
	if err, ok := err.(*url.Error); ok && err.Err == context.Canceled {
		return retry.Cancel(err)
	} else if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		respBody, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return fmt.Errorf("unexpected respose %s: %v", resp.Status, err)
		}
		if resp.StatusCode >= 500 {
			return fmt.Errorf("unexpected respose from collector %s: %s", resp.Status, respBody)
		}
		return retry.Cancel(fmt.Errorf("bad client request: collector responded with %s: %s", resp.Status, respBody))
	}

	if v != nil {
		_, err := io.Copy(v, resp.Body)
		return err
	}

	return nil
}

func (a *agent) collectAndSend(ctx context.Context) {
	defer close(a.done)

	ctx, cancel := context.WithCancel(ctx)
	go func() {
		select {
		case <-a.stop:
			cancel()
		}
	}()

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

			if err := a.collectProfile(ctx, ptype, &buf); err != nil {
				a.logf("failed to collect profiles: %v", err)
			} else if err := a.sendProfile(ctx, ptype, &buf); err != nil {
				a.logf("failed to send profiles: %v", err)
			}

			buf.Reset()

			// add extra up to 10 seconds to sleep to dis-align profiles
			noise := time.Duration(rand.Intn(10)) * time.Second
			tick := a.tick + noise
			timer.Reset(tick)
		}
	}
}

var timersPool = sync.Pool{}

func sleep(d time.Duration, cancel <-chan struct{}) {
	timer, _ := timersPool.Get().(*time.Timer)
	if timer == nil {
		timer = time.NewTimer(d)
	} else {
		timer.Reset(d)
	}

	select {
	case <-timer.C:
	case <-cancel:
		if !timer.Stop() {
			<-timer.C
		}
	}

	timersPool.Put(timer)
}
