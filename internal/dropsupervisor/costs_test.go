//go:build linux

package dropsupervisor

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

func TestReplicaSyncIntervalSetting(t *testing.T) {
	for raw, want := range map[string]time.Duration{
		"": defaultSyncInterval, "1s": time.Second, "10s": 10 * time.Second, "5m": 300 * time.Second,
		"999ms": defaultSyncInterval, "301s": defaultSyncInterval, "0": defaultSyncInterval, "-5s": defaultSyncInterval, "ten": defaultSyncInterval,
	} {
		t.Setenv("DROP_REPLICA_SYNC_INTERVAL", raw)
		if got := replicaSyncInterval(); got != want {
			t.Errorf("%q: got %s, want %s", raw, got, want)
		}
	}
	// An invalid value never prevents the guest from starting.
	t.Setenv("DROP_REPLICA_SYNC_INTERVAL", "1h")
	s := testService(t)
	if s.syncInterval != defaultSyncInterval || s.status()["replica_sync_interval"] != "10s" {
		t.Fatalf("service interval %s", s.syncInterval)
	}
}

func TestLitestreamConfigHistorySettings(t *testing.T) {
	r := &replica{socket: "/tmp/s.sock", live: "/data/live.db", url: "s3://bucket/d-1234567890abcde/crew-drop/sqlite", region: "eu-north-1", syncInterval: 30 * time.Second}
	want := `levels:
  - interval: 5m
  - interval: 4h
snapshot:
  interval: 24h
  retention: 72h
l0-retention-check-interval: 4h
socket:
  enabled: true
  path: "/tmp/s.sock"
dbs:
  - path: "/data/live.db"
    replica:
      url: "s3://bucket/d-1234567890abcde/crew-drop/sqlite"
      sync-interval: 30s
      region: "eu-north-1"
`
	if got := r.litestreamConfig(); got != want {
		t.Fatalf("config:\n%s", got)
	}
	r = &replica{url: "file:///replica"}
	if got := r.litestreamConfig(); !strings.Contains(got, "sync-interval: 10s\n") || strings.Contains(got, "region") {
		t.Fatalf("default config:\n%s", got)
	}
}

// fakeLitestream installs a script that answers "litestream sync" the way the
// pinned binary does, or fails.
func fakeLitestream(t *testing.T, s *service, body string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "litestream")
	if err := os.WriteFile(path, []byte("#!/bin/sh\n"+body+"\n"), 0700); err != nil {
		t.Fatal(err)
	}
	s.replica.bin = path
}

func stopped(t *testing.T, p *os.Process) bool {
	t.Helper()
	var info unix.Siginfo
	if err := unix.Waitid(unix.P_PID, p.Pid, &info, unix.WSTOPPED|unix.WNOWAIT|unix.WNOHANG, nil); err != nil {
		t.Fatal(err)
	}
	return info.Signo != 0
}

func TestLifecycleHooksConfirmReplicationAndThawCorrectly(t *testing.T) {
	s := testService(t)
	writer := exec.Command("sleep", "60")
	if err := writer.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { writer.Process.Signal(syscall.SIGCONT); writer.Process.Kill(); writer.Wait() }()
	s.writer = writer.Process
	s.replication("ready", nil)
	outcome := func(path string, want int) hookOutcome {
		t.Helper()
		code, body := hookReq(s, path, "test-only-token")
		var out hookOutcome
		if err := json.Unmarshal([]byte(body), &out); err != nil || code != want {
			t.Fatalf("%s=%d %s", path, code, body)
		}
		return out
	}

	fakeLitestream(t, s, `echo '{"db_path":"x","txid":7,"replica_txid":7,"duration_ms":1}'`)
	out := outcome("/hooks/suspend", 200)
	if !out.OK || out.Replication != "confirmed" || out.TXID != 7 || out.ReplicaTXID != 7 || s.draining || s.frozen != nil || stopped(t, writer.Process) {
		t.Fatalf("suspend %+v draining=%v", out, s.draining)
	}
	// Unconfirmed: the replica is behind, the CLI fails, or the socket hangs.
	for _, script := range []string{
		`echo '{"txid":9,"replica_txid":8}'`,
		`echo '{"txid":9}'`,
		`echo 'failed to connect to control socket' >&2; exit 1`,
	} {
		fakeLitestream(t, s, script)
		for _, hook := range []string{"/hooks/suspend", "/hooks/terminate", "/control/sync"} {
			out = outcome(hook, 503)
			if out.OK || out.Replication != "unconfirmed" || out.Stage != "sync" || s.draining || s.frozen != nil || stopped(t, writer.Process) {
				t.Fatalf("%s with %q: %+v", hook, script, out)
			}
		}
	}
	if last := s.status()["last_hook"].(*hookOutcome); last.Replication != "unconfirmed" {
		t.Fatalf("status last hook %+v", last)
	}
	s.hookTimeout = 1500 * time.Millisecond
	fakeLitestream(t, s, `sleep 5`)
	start := time.Now()
	if out = outcome("/hooks/terminate", 503); out.Stage != "sync" || time.Since(start) > 1400*time.Millisecond+5*time.Second {
		t.Fatalf("hung sync: %+v after %s", out, time.Since(start))
	}
	s.hookTimeout = 9 * time.Second

	fakeLitestream(t, s, `echo '{"txid":12,"replica_txid":12}'`)
	out = outcome("/hooks/terminate", 200)
	if out.Replication != "confirmed" || !s.draining || s.frozen != writer.Process || !stopped(t, writer.Process) {
		t.Fatalf("terminate %+v", out)
	}
	// A repeated terminate reuses the confirmed cut of the stopped writer.
	fakeLitestream(t, s, `exit 1`)
	if out = outcome("/hooks/terminate", 200); out.Replication != "confirmed" {
		t.Fatalf("repeat terminate %+v", out)
	}
	// Resume thaws the same writer once and never starts another.
	for range 2 {
		if out = outcome("/hooks/resume", 200); !out.OK || out.Replication != "" || s.draining || s.frozen != nil || s.writer != writer.Process || stopped(t, writer.Process) {
			t.Fatalf("resume %+v", out)
		}
	}
}
