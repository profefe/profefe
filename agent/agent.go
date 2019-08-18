package agent

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"runtime/pprof"
	"strings"
	"sync"
	"time"

	"github.com/profefe/profefe/pkg/profile"
)

const (
	defaultDuration     = 10 * time.Second
	defaultTickInterval = time.Minute

	defaultProfileType = profile.CPUProfile

	backoffMinDelay = time.Minute
	backoffMaxDelay = 30 * time.Minute
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

func Start(addr, service string, opts ...Option) (*Agent, error) {
	agent := New(addr, service, opts...)
	if err := agent.Start(context.Background()); err != nil {
		return nil, err
	}
	return agent, nil
}

type httpClient interface {
	Do(req *http.Request) (*http.Response, error)
}

type Agent struct {
	CPUProfile         bool
	CPUProfileDuration time.Duration
	HeapProfile        bool
	BlockProfile       bool
	MutexProfile       bool
	GoroutineProfile   bool

	service    string
	instanceID profile.InstanceID
	rawLabels  strings.Builder

	logf func(format string, v ...interface{})

	rawClient     httpClient
	collectorAddr string

	tick time.Duration
	stop chan struct{} // signals the beginning of stop
	done chan struct{} // closed when stopping is done
}

func New(addr, service string, opts ...Option) *Agent {
	a := &Agent{
		CPUProfile:         true, // enable CPU profiling by default
		CPUProfileDuration: defaultDuration,

		collectorAddr: addr,
		service:       service,

		rawClient: http.DefaultClient,
		logf:      func(format string, v ...interface{}) {},

		tick: defaultTickInterval,
		stop: make(chan struct{}),
		done: make(chan struct{}),
	}

	for _, opt := range opts {
		opt(a)
	}

	a.instanceID = a.getInstanceID()

	return a
}

func (a *Agent) Start(ctx context.Context) error {
	if a.collectorAddr == "" {
		return fmt.Errorf("failed to start agent: collector address is empty")
	}

	go a.collectAndSend(ctx)

	return nil
}

func (a *Agent) Stop() error {
	close(a.stop)
	<-a.done
	return nil
}

func (a *Agent) getInstanceID() profile.InstanceID {
	prog := os.Args[0]
	f, err := os.Open(prog)
	if err != nil {
		a.logf("failed to read binary %s: %v", prog, err)
		return profile.NewInstanceID()
	}
	defer f.Close()

	h := md5.New()
	if _, err := io.Copy(h, f); err != nil {
		a.logf("failed to generate hash from binary %s: %v", prog, err)
		return profile.NewInstanceID()
	}
	return profile.InstanceID(hex.EncodeToString(h.Sum(nil)))
}

func (a *Agent) collectProfile(ctx context.Context, ptype profile.ProfileType, buf *bytes.Buffer) error {
	switch ptype {
	case profile.CPUProfile:
		err := pprof.StartCPUProfile(buf)
		if err != nil {
			return fmt.Errorf("failed to start CPU profile: %v", err)
		}
		sleep(a.CPUProfileDuration, ctx.Done())
		pprof.StopCPUProfile()
	case profile.HeapProfile:
		err := pprof.WriteHeapProfile(buf)
		if err != nil {
			return fmt.Errorf("failed to write heap profile: %v", err)
		}
	case profile.BlockProfile:
		return a.writeProfile("block", buf)
	case profile.MutexProfile:
		return a.writeProfile("mutex", buf)
	case profile.GoroutineProfile:
		return a.writeProfile("goroutine", buf)
	default:
		return fmt.Errorf("unknown profile type %v", ptype)
	}

	return nil
}

func (a *Agent) writeProfile(name string, w io.Writer) error {
	err := pprof.Lookup(name).WriteTo(w, 0)
	if err != nil {
		err = fmt.Errorf("failed to write %s profile: %v", name, err)
	}
	return err
}

func (a *Agent) sendProfile(ctx context.Context, ptype profile.ProfileType, buf *bytes.Buffer) error {
	q := url.Values{}
	q.Set("service", a.service)
	q.Set("instance_id", a.instanceID.String())
	q.Set("labels", a.rawLabels.String())
	q.Set("type", ptype.String())

	surl := a.collectorAddr + "/api/0/profiles?" + q.Encode()
	a.logf("send profile: %s", surl)
	req, err := http.NewRequest(http.MethodPost, surl, buf)
	if err != nil {
		return err
	}
	req = req.WithContext(ctx)

	return Do(
		backoffMinDelay,
		backoffMaxDelay,
		func() error { return a.doRequest(req, nil) },
	)
}

func (a *Agent) doRequest(req *http.Request, v io.Writer) error {
	resp, err := a.rawClient.Do(req)
	if err, ok := err.(*url.Error); ok && err.Err == context.Canceled {
		return Cancel(err)
	}
	if err != nil {
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
		return Cancel(fmt.Errorf("bad request: collector responded with %s: %s", resp.Status, respBody))
	}

	if v != nil {
		_, err := io.Copy(v, resp.Body)
		return err
	}

	return nil
}

func (a *Agent) collectAndSend(ctx context.Context) {
	defer close(a.done)

	ctx, cancel := context.WithCancel(ctx)
	go func() {
		select {
		case <-a.stop:
			cancel()
		}
	}()

	var (
		ptype = a.nextProfileType(profile.UnknownProfile)
		timer = time.NewTimer(tickInterval(0))

		buf bytes.Buffer
	)

	for {
		select {
		case <-a.stop:
			if !timer.Stop() {
				<-timer.C
			}
			return
		case <-timer.C:
			if err := a.collectProfile(ctx, ptype, &buf); err != nil {
				a.logf("failed to collect profiles: %v", err)
			} else if err := a.sendProfile(ctx, ptype, &buf); err != nil {
				a.logf("failed to send profiles: %v", err)
			}

			buf.Reset()

			ptype = a.nextProfileType(ptype)

			var tick time.Duration
			if ptype == defaultProfileType {
				// we took the full set of profiles, sleep for the whole tick
				tick = a.tick
			}

			timer.Reset(tickInterval(tick))
		}
	}
}

func (a *Agent) nextProfileType(ptype profile.ProfileType) profile.ProfileType {
	// special case to choose initial profile type on the first call
	if ptype == profile.UnknownProfile {
		return defaultProfileType
	}

	for {
		switch ptype {
		case profile.CPUProfile:
			ptype = profile.HeapProfile
			if a.HeapProfile {
				return ptype
			}
		case profile.HeapProfile:
			ptype = profile.BlockProfile
			if a.BlockProfile {
				return ptype
			}
		case profile.BlockProfile:
			ptype = profile.MutexProfile
			if a.MutexProfile {
				return ptype
			}
		case profile.MutexProfile:
			ptype = profile.GoroutineProfile
			if a.GoroutineProfile {
				return ptype
			}
		case profile.GoroutineProfile:
			ptype = profile.CPUProfile
			if a.CPUProfile {
				return ptype
			}
		}
	}
}

func tickInterval(d time.Duration) time.Duration {
	// add up to extra 10 seconds to sleep to dis-align profiles of different instances
	noise := time.Second + time.Duration(rand.Intn(9))*time.Second
	return d + noise
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
