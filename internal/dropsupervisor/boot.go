package dropsupervisor

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

type pending struct {
	App   string `json:"app"`
	Nonce string `json:"nonce"`
	Mode  string `json:"mode"`
}

func storePending(path string, p pending) error {
	f, err := os.CreateTemp(filepath.Dir(path), ".pending-")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	err = json.NewEncoder(f).Encode(p)
	if err == nil {
		err = f.Sync()
	}
	if closeErr := f.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	if err = os.Rename(f.Name(), path); err != nil {
		return err
	}
	return syncFile(filepath.Dir(path))
}

func localDatabase(path string) (bool, error) {
	st, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		for _, end := range []string{"-wal", "-journal", "-shm"} {
			if _, sideErr := os.Lstat(path + end); !errors.Is(sideErr, os.ErrNotExist) {
				return false, errors.New("incomplete local SQLite files require review")
			}
		}
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if !st.Mode().IsRegular() {
		return false, errors.New("local database must be a regular file")
	}
	f, err := os.Open(path)
	if err != nil {
		return false, err
	}
	defer f.Close()
	buf := make([]byte, 16)
	if _, err := io.ReadFull(f, buf); err != nil || !bytes.Equal(buf, []byte("SQLite format 3\x00")) {
		return false, errors.New("invalid local SQLite header")
	}
	return true, nil
}

func (s *service) cloud(ctx context.Context) error {
	if err := s.gate(ctx, "aws"); err != nil {
		return err
	}
	if s.fileReplica != "" {
		return nil
	}
	if os.Getenv("AWS_ROLE_ARN") == "" || os.Getenv("AWS_WEB_IDENTITY_TOKEN_FILE") == "" {
		return errors.New("guest workload identity settings unavailable")
	}
	for _, name := range []string{"AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN", "AWS_PROFILE", "AWS_CONTAINER_CREDENTIALS_FULL_URI", "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI"} {
		if os.Getenv(name) != "" {
			return fmt.Errorf("unsupported credential source %s", name)
		}
	}
	// In async mode the runner publishes the token after the tenant starts.
	// Keep the same cold attempt open while that first token file is pending.
	for {
		st, err := os.Stat(os.Getenv("AWS_WEB_IDENTITY_TOKEN_FILE"))
		if err == nil && st.Size() > 0 {
			break
		}
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
		if err := wait(ctx, 50*time.Millisecond); err != nil {
			return err
		}
	}
	return nil
}

func (s *service) bootMode(ctx context.Context, record string) (string, error) {
	r := s.replica
	b, err := os.ReadFile(record)
	var p pending
	switch {
	case err == nil:
		if json.Unmarshal(b, &p) != nil || p.App != s.app || len(p.Nonce) != 32 {
			return "", errors.New("invalid pending startup record")
		}
		switch p.Mode {
		case "new", "recovered":
			return p.Mode, nil
		case "checking":
		default:
			return "", errors.New("unknown pending startup mode")
		}
	case errors.Is(err, os.ErrNotExist):
		exists, err := localDatabase(r.live)
		if err != nil {
			return "", err
		}
		if exists {
			return "local", nil
		}
		var id [16]byte
		if _, err := rand.Read(id[:]); err != nil {
			return "", err
		}
		p = pending{App: s.app, Nonce: hex.EncodeToString(id[:]), Mode: "checking"}
		if err := storePending(record, p); err != nil {
			return "", err
		}
	default:
		return "", err
	}
	s.data("recovering", nil)
	s.mu.Lock()
	s.recoveries++
	s.markLocked("recovery_started")
	s.mu.Unlock()
	if err := s.gate(ctx, "recovery"); err != nil {
		return "", err
	}
	if err := s.cloud(ctx); err != nil {
		return "", err
	}
	fresh := false
	if s.bootstrap {
		fresh, err = s.reserveNew(ctx, p.Nonce)
		if err != nil {
			return "", err
		}
	}
	if fresh {
		// A prior project snapshot with no live DB might still contain data:
		// require recovery or explicit operator reconciliation, never reinitialize it.
		if _, err := os.Lstat(r.project); !errors.Is(err, os.ErrNotExist) {
			return "", errors.New("cannot bootstrap over a preexisting project snapshot")
		}
		p.Mode = "new"
	} else {
		if err := r.prepare(); err != nil {
			return "", err
		}
		s.mu.Lock()
		s.restores++
		s.markLocked("restore_started")
		s.mu.Unlock()
		if err := r.restore(ctx); err != nil {
			return "", err
		}
		s.mark("restore_finished")
		p.Mode = "recovered"
	}
	if err := storePending(record, p); err != nil {
		return "", err
	}
	return p.Mode, nil
}

func exited(err error) error {
	if err == nil {
		return errors.New("child process exited")
	}
	return err
}

// Probe an actual JS-registered Crew route. Core's /up can respond before we
// know whether the executable's authenticated application handlers are usable.
func (s *service) probeCrew(ctx context.Context, client *http.Client) (bool, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://"+s.backend+"/api/crew/read", strings.NewReader(`{"collection":"notes"}`))
	if err != nil {
		return false, err
	}
	req.Host = s.domain
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return false, ctx.Err()
	} // no listener yet, or the operation was cancelled
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		return false, fmt.Errorf("Crew read route returned HTTP %d to an anonymous readiness probe, expected 401", resp.StatusCode)
	}
	return true, nil
}

func (s *service) runBackend(ctx context.Context, mode, record string) error {
	path, err := os.Executable()
	if err != nil {
		return err
	}
	childCtx, stop := context.WithCancel(ctx)
	defer stop()
	cmd := exec.CommandContext(childCtx, path, "drop-backend", mode)
	configureChild(cmd)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	s.mu.Lock()
	s.mode = mode
	s.markLocked("backend_starting")
	s.mu.Unlock()
	if err := cmd.Start(); err != nil {
		return err
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	joined := false
	defer func() {
		stop()
		if !joined {
			<-done
		}
	}()
	httpClient := http.Client{Timeout: 350 * time.Millisecond, Transport: &http.Transport{Proxy: nil, DisableKeepAlives: true}, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	probeCtx, cancel := context.WithTimeout(ctx, 90*time.Second)
	defer cancel()
	for {
		select {
		case err := <-done:
			joined = true
			return exited(err)
		case <-probeCtx.Done():
			return probeCtx.Err()
		default:
		}
		ready, err := s.probeCrew(probeCtx, &httpClient)
		if err != nil {
			return err
		}
		if ready {
			break
		}
		if err := wait(probeCtx, 15*time.Millisecond); err != nil {
			return err
		}
	}
	if mode != "local" {
		if err := verifyIdentity(s.replica.live, s.app); err != nil {
			return err
		}
		// The app, binding and SQLite WAL are durable before the pending
		// enrollment/restore record is removed or any request is admitted.
		if err := s.disk.wait(ctx); err != nil {
			return fmt.Errorf("persist initialized database: %w", err)
		}
		if err := os.Remove(record); err != nil {
			return err
		}
		if err := syncFile(filepath.Dir(record)); err != nil {
			return err
		}
	}
	s.mu.Lock()
	s.writer = cmd.Process
	s.mu.Unlock()
	defer func() { s.mu.Lock(); s.writer = nil; s.mu.Unlock() }()
	s.mark("data_ready")
	s.data("ready", nil)
	replCtx, stopReplication := context.WithCancel(ctx)
	replicationDone := make(chan struct{})
	go func() { s.replicate(replCtx); close(replicationDone) }()
	select {
	case err = <-done:
		joined = true
	case <-ctx.Done():
		err = ctx.Err()
	}
	// Block new proxy requests before restarting the backend.
	s.data("starting", nil)
	stopReplication()
	<-replicationDone
	return exited(err)
}

func (s *service) replicate(ctx context.Context) {
	r := s.replica
	for ctx.Err() == nil {
		s.replication("starting", nil)
		err := s.cloud(ctx)
		if err == nil {
			err = r.prepare()
		}
		if err == nil {
			err = s.runLitestream(ctx)
		}
		s.replication("retrying", err)
		if ctx.Err() == nil {
			log.Printf("CrewDrop replication unavailable: %v", err)
		}
		if wait(ctx, time.Second) != nil {
			return
		}
	}
}

func (s *service) runLitestream(ctx context.Context) error {
	r := s.replica
	if err := os.Remove(r.socket); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	repCtx, stop := context.WithCancel(ctx)
	defer stop()
	cmd := r.command(repCtx, "replicate", "-config", r.cfg)
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		return err
	}
	s.mark("litestream_started")
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	joined := false
	defer func() {
		stop()
		if !joined {
			<-done
		}
	}()
	startCtx, cancel := context.WithTimeout(ctx, 45*time.Second)
	defer cancel()
	for {
		select {
		case err := <-done:
			joined = true
			return exited(err)
		default:
		}
		if st, err := os.Stat(r.socket); err == nil && st.Mode()&os.ModeSocket != 0 {
			break
		}
		if err := wait(startCtx, 30*time.Millisecond); err != nil {
			return err
		}
	}
	if err := r.sync(startCtx); err != nil {
		return err
	}
	s.replication("ready", nil)
	select {
	case err := <-done:
		joined = true
		return exited(err)
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *service) runWorkers(ctx context.Context) error {
	dir := filepath.Join(s.root, "crew")
	for _, sub := range []string{"private", "project", "tools"} {
		if err := os.MkdirAll(filepath.Join(dir, sub), 0700); err != nil {
			return err
		}
	}
	lock, err := lockVolume(filepath.Join(dir, "drop.lock"))
	if err != nil {
		return err
	}
	defer lock.Close()
	tmp, err := os.MkdirTemp("", "crew-drop-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(tmp)
	prefix := s.app + "/crew-drop/sqlite"
	destination := "s3://" + s.bucket + "/" + prefix
	if s.fileReplica != "" {
		u := url.URL{Scheme: "file", Path: filepath.Join(s.fileReplica, prefix)}
		destination = u.String()
	}
	s.replica = &replica{cfg: filepath.Join(tmp, "ls.yml"), socket: filepath.Join(tmp, "sync.sock"), live: filepath.Join(dir, "private", "live", "data.db"), project: filepath.Join(dir, "project", "crew.db"), binDir: filepath.Join(dir, "tools"), app: s.app, url: destination, region: s.region}
	// Open the filesystem holding the authoritative database, not TMPDIR or
	// the snapshot directory, which may be different mounts.
	liveDir := filepath.Dir(s.replica.live)
	if err := os.MkdirAll(liveDir, 0700); err != nil {
		return err
	}
	volume, err := os.Open(liveDir)
	if err != nil {
		return err
	}
	defer volume.Close()
	s.disk = &volumeBarrier{flush: func() error { return syncVolume(volume) }}
	record := filepath.Join(dir, "pending-start.json")
	for ctx.Err() == nil {
		s.mu.Lock()
		for s.draining && ctx.Err() == nil {
			next := s.changed
			s.mu.Unlock()
			select {
			case <-next:
			case <-ctx.Done():
			}
			s.mu.Lock()
		}
		s.mu.Unlock()
		if ctx.Err() != nil {
			return nil
		}
		s.data("starting", nil)
		attempt, cancel := context.WithTimeout(ctx, 2*time.Minute)
		mode, err := s.bootMode(attempt, record)
		cancel()
		if err == nil {
			err = s.runBackend(ctx, mode, record)
		}
		if ctx.Err() != nil {
			return nil
		}
		s.data("failed", err)
		log.Printf("CrewDrop backend unavailable: %v", err)
		if wait(ctx, time.Second) != nil {
			return nil
		}
	}
	return nil
}
