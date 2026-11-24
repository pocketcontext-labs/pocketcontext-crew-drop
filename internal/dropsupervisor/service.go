package dropsupervisor

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"sync"
	"time"
)

var appID = regexp.MustCompile("^d-[a-z0-9]{15}$")

type service struct {
	disk                                                    *volumeBarrier // published before marking the backend ready
	writer                                                  *os.Process    // protected by mu
	frozen                                                  *os.Process    // only lifecycleEvent changes this reference
	app, root, fileReplica, bucket, region, backend, domain string
	hookAddress                                             string
	bootstrap                                               bool
	token                                                   [32]byte
	wait, hookTimeout                                       time.Duration
	proxy                                                   *httputil.ReverseProxy
	replica                                                 *replica

	mu                                                             sync.Mutex
	changed                                                        chan struct{}
	gates                                                          map[string]chan struct{}
	phases                                                         map[string]time.Time
	mode, dataState, replicaState, lastDataError, lastReplicaError string
	waiters, hookWaiters, active, recoveries, restores             int
	draining                                                       bool
	lifecycle                                                      chan struct{}
}

func env(name, fallback string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return fallback
}

func duration(name, fallback string) (time.Duration, error) {
	d, err := time.ParseDuration(env(name, fallback))
	if err != nil || d <= 0 || d > 2*time.Minute {
		return 0, fmt.Errorf("invalid %s", name)
	}
	return d, nil
}

func fromEnvironment() (*service, error) {
	s := &service{app: os.Getenv("CREW_DROP_APP_ID"), root: env("NIBRUN_DATA_DIR", "/app/data"), fileReplica: os.Getenv("CREW_DROP_FILE_REPLICA"), bucket: os.Getenv("CREW_DROP_BUCKET"), region: os.Getenv("AWS_REGION"), domain: os.Getenv("POCKETCONTEXT_DOMAIN"), backend: env("CREW_DROP_BACKEND_ADDR", "127.0.0.1:18080"), bootstrap: os.Getenv("CREW_DROP_BOOTSTRAP") == "1",
		changed: make(chan struct{}), gates: map[string]chan struct{}{}, phases: map[string]time.Time{"process_start": time.Now().UTC()}, lifecycle: make(chan struct{}, 1), dataState: "starting", replicaState: "pending"}
	if !appID.MatchString(s.app) || !filepath.IsAbs(s.root) {
		return nil, errors.New("valid CREW_DROP_APP_ID and absolute NIBRUN_DATA_DIR required")
	}
	if s.fileReplica != "" && !filepath.IsAbs(s.fileReplica) {
		return nil, errors.New("CREW_DROP_FILE_REPLICA must be absolute")
	}
	if s.fileReplica == "" && (s.bucket == "" || s.region == "") {
		return nil, errors.New("CREW_DROP_BUCKET and AWS_REGION required")
	}
	token := os.Getenv("NIBRUN_HOOK_TOKEN")
	if token == "" {
		return nil, errors.New("NIBRUN_HOOK_TOKEN required")
	}
	s.token = sha256.Sum256([]byte(token))
	port := env("NIBRUN_HOOK_PORT", "8081")
	number, err := strconv.ParseUint(port, 10, 16)
	if err != nil || number == 0 {
		return nil, errors.New("NIBRUN_HOOK_PORT must be a TCP port between 1 and 65535")
	}
	s.hookAddress = net.JoinHostPort("", port)
	// Local smoke tests restrict the listener to loopback on the runner-selected port.
	if addr := os.Getenv("CREW_DROP_HOOK_ADDR"); addr != "" {
		_, addrPort, err := net.SplitHostPort(addr)
		if err != nil || addrPort != port {
			return nil, errors.New("CREW_DROP_HOOK_ADDR must use NIBRUN_HOOK_PORT")
		}
		s.hookAddress = addr
	}
	host, _, err := net.SplitHostPort(s.backend)
	if err != nil || !net.ParseIP(host).IsLoopback() {
		return nil, errors.New("CREW_DROP_BACKEND_ADDR must be loopback")
	}
	if s.wait, err = duration("CREW_DROP_WAIT", "10s"); err != nil {
		return nil, err
	}
	if s.hookTimeout, err = duration("CREW_DROP_HOOK_TIMEOUT", "9s"); err != nil {
		return nil, err
	}
	for gate, v := range map[string]string{"hooks": "CREW_DROP_HOLD_HOOKS", "aws": "CREW_DROP_HOLD_AWS", "recovery": "CREW_DROP_HOLD_RECOVERY"} {
		if os.Getenv(v) == "1" {
			s.gates[gate] = make(chan struct{})
		}
	}
	target := &url.URL{Scheme: "http", Host: s.backend}
	s.proxy = &httputil.ReverseProxy{ModifyResponse: s.durableResponse, ErrorHandler: proxyError, Rewrite: func(p *httputil.ProxyRequest) {
		p.SetURL(target)
		p.Out.Host = p.In.Host
		for _, h := range []string{"CF-Connecting-IP", "X-Real-IP", "True-Client-IP", "Forwarded", "X-Forwarded-Host", "X-Forwarded-Proto", "X-Forwarded-For"} {
			p.Out.Header.Del(h)
		}
		if ip, _, err := net.SplitHostPort(p.In.RemoteAddr); err == nil {
			p.Out.Header.Set("X-Forwarded-For", ip)
		}
	}}
	return s, nil
}

func (s *service) notifyLocked() { close(s.changed); s.changed = make(chan struct{}) }
func (s *service) markLocked(key string) {
	if _, ok := s.phases[key]; !ok {
		s.phases[key] = time.Now().UTC()
	}
}
func (s *service) mark(key string) { s.mu.Lock(); defer s.mu.Unlock(); s.markLocked(key) }

func (s *service) data(state string, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.dataState = state
	s.lastDataError = ""
	if err != nil {
		s.lastDataError = err.Error()
	}
	s.notifyLocked()
}

func (s *service) replication(state string, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.replicaState = state
	s.lastReplicaError = ""
	if err != nil {
		s.lastReplicaError = err.Error()
	}
	if state == "ready" {
		s.markLocked("replica_ready")
	}
	s.notifyLocked()
}

func (s *service) gate(ctx context.Context, key string) error {
	s.mu.Lock()
	ch := s.gates[key]
	s.mu.Unlock()
	if ch == nil {
		return ctx.Err()
	}
	select {
	case <-ch:
		return ctx.Err()
	case <-ctx.Done():
		return ctx.Err()
	}
}

func wait(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

func (s *service) public(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), s.wait)
	defer cancel()
	s.mu.Lock()
	if s.dataState != "ready" && s.waiters < 64 && !s.draining {
		s.waiters++
		for s.dataState != "ready" && s.dataState != "failed" && !s.draining && ctx.Err() == nil {
			next := s.changed
			s.mu.Unlock()
			select {
			case <-next:
			case <-ctx.Done():
			}
			s.mu.Lock()
		}
		s.waiters--
	}
	if s.dataState != "ready" || s.draining || ctx.Err() != nil {
		s.mu.Unlock()
		w.Header().Set("Retry-After", "1")
		respond(w, 503, map[string]string{"error": "Crew is temporarily unavailable"})
		return
	}
	s.active++
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		s.active--
		if s.active == 0 {
			s.notifyLocked()
		}
		s.mu.Unlock()
	}()
	s.proxy.ServeHTTP(w, r)
}

func respond(w http.ResponseWriter, code int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(value)
}

// Run binds both interfaces before inspecting local storage or starting a child.
func Run(parent context.Context) error {
	s, err := fromEnvironment()
	if err != nil {
		return err
	}
	public, err := net.Listen("tcp", env("CREW_DROP_PUBLIC_ADDR", ":8080"))
	if err != nil {
		return err
	}
	defer public.Close()
	s.mark("public_listening")
	hooks, err := net.Listen("tcp", s.hookAddress)
	if err != nil {
		return err
	}
	defer hooks.Close()
	s.mark("hooks_listening")
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	// Use the service context for all requests, including waiters and active hooks.
	base := func(net.Listener) context.Context { return ctx }
	pub := &http.Server{Handler: http.HandlerFunc(s.public), BaseContext: base, ReadHeaderTimeout: 5 * time.Second, IdleTimeout: 120 * time.Second}
	priv := &http.Server{Handler: http.HandlerFunc(s.hook), BaseContext: base, ReadHeaderTimeout: 5 * time.Second, IdleTimeout: 30 * time.Second}
	stopped := make(chan error, 2)
	go func() { stopped <- pub.Serve(public) }()
	go func() { stopped <- priv.Serve(hooks) }()
	done := make(chan error, 1)
	go func() { done <- s.runWorkers(ctx) }()
	select {
	case err = <-done:
		cancel()
		pub.Close()
		priv.Close()
		return err
	case <-parent.Done():
	case err = <-stopped:
	}
	cancel()
	pub.Close()
	priv.Close()
	if workerErr := <-done; err == nil {
		err = workerErr
	}
	return err
}
