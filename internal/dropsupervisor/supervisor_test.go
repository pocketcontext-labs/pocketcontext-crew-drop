//go:build linux

package dropsupervisor

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func testService(t *testing.T) *service {
	t.Helper()
	t.Setenv("CREW_DROP_APP_ID", "d-1234567890abcde")
	t.Setenv("NIBRUN_DATA_DIR", t.TempDir())
	t.Setenv("CREW_DROP_FILE_REPLICA", t.TempDir())
	t.Setenv("CREW_DROP_HOOK_TOKEN", "test-only-token")
	s, err := fromEnvironment()
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	r := &replica{app: s.app, binDir: dir, cfg: filepath.Join(dir, "ls.yml"), socket: filepath.Join(dir, "sync.sock"), live: filepath.Join(s.root, "data.db"), project: filepath.Join(s.root, "project.db"), url: "file://" + filepath.Join(s.fileReplica, s.app, "crew-drop", "sqlite")}
	s.replica = r
	s.disk = &volumeBarrier{flush: func() error { return nil }}
	return s
}

func makeDB(t *testing.T, path, app string) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	if _, err := db.Exec("PRAGMA journal_mode=WAL; CREATE TABLE _crew_drop_identity (app_id TEXT PRIMARY KEY); CREATE TABLE notes (body TEXT);"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("INSERT INTO _crew_drop_identity VALUES (?)", app); err != nil {
		t.Fatal(err)
	}
	return db
}

func hookReq(s *service, path, token string) (int, string) {
	r := httptest.NewRequest("POST", path, nil)
	r.Header.Set("Authorization", "Bearer "+token)
	w := httptest.NewRecorder()
	s.hook(w, r)
	return w.Code, w.Body.String()
}

func TestExistingLiveDoesNotNeedCloud(t *testing.T) {
	s := testService(t)
	makeDB(t, s.replica.live, s.app)
	s.gates["aws"] = make(chan struct{})
	s.gates["hooks"] = make(chan struct{})
	s.gates["recovery"] = make(chan struct{})
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	mode, err := s.bootMode(ctx, filepath.Join(s.root, "pending"))
	if err != nil || mode != "local" {
		t.Fatalf("mode=%q err=%v", mode, err)
	}
}

func TestUnreadyRequestQueueIsBoundedAndCancels(t *testing.T) {
	s := testService(t)
	s.wait = 35 * time.Millisecond
	r := httptest.NewRequest("GET", "/api/crew/read", nil)
	s.waiters = 64
	w := httptest.NewRecorder()
	s.public(w, r)
	if w.Code != 503 || s.waiters != 64 {
		t.Fatalf("full queue: status=%d waiters=%d", w.Code, s.waiters)
	}
	s.waiters = 0
	w = httptest.NewRecorder()
	s.public(w, r)
	if w.Code != 503 || s.waiters != 0 {
		t.Fatalf("timeout: status=%d waiters=%d", w.Code, s.waiters)
	}
}

func TestHookIsolationAndPublicHost(t *testing.T) {
	var host, header, cf string
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		host = r.Host
		header = r.Header.Get("X-Forwarded-For")
		cf = r.Header.Get("CF-Connecting-IP")
		w.WriteHeader(204)
	}))
	defer backend.Close()
	u, _ := url.Parse(backend.URL)
	t.Setenv("CREW_DROP_BACKEND_ADDR", u.Host)
	s := testService(t)
	s.data("ready", nil)
	s.gates["hooks"] = make(chan struct{})
	hookDone := make(chan int, 1)
	go func() { code, _ := hookReq(s, "/hooks/start", "test-only-token"); hookDone <- code }()
	r := httptest.NewRequest("POST", "http://crew.example/api/crew/write", nil)
	r.RemoteAddr = "192.0.2.12:1234"
	r.Header.Set("X-Forwarded-For", "127.0.0.1")
	r.Header.Set("CF-Connecting-IP", "127.0.0.1")
	w := httptest.NewRecorder()
	s.public(w, r)
	if w.Code != 204 || host != "crew.example" || header != "192.0.2.12" || cf != "" {
		t.Fatalf("upstream status=%d host=%q forwarded=%q cf=%q", w.Code, host, header, cf)
	}
	if status, _ := hookReq(s, "/control/release?gate=hooks", "incorrect"); status != 401 {
		t.Fatal(status)
	}
	select {
	case <-hookDone:
		t.Fatal("held hook completed")
	default:
	}
	if status, _ := hookReq(s, "/control/release?gate=hooks", "test-only-token"); status != 200 {
		t.Fatal(status)
	}
	if code := <-hookDone; code != 200 {
		t.Fatal(code)
	}
	s.hookTimeout = 30 * time.Millisecond
	if status, _ := hookReq(s, "/hooks/terminate", "test-only-token"); status != 503 || s.draining {
		t.Fatalf("failed drain status=%d draining=%v", status, s.draining)
	}
}

func TestUnsafePendingOrIncompleteSQLiteFailsClosed(t *testing.T) {
	s := testService(t)
	p := filepath.Join(s.root, "pending")
	if err := storePending(p, pending{App: "other", Nonce: strings.Repeat("f", 32), Mode: "new"}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.bootMode(context.Background(), p); err == nil {
		t.Fatal("accepted another app's pending record")
	}
	if err := os.Remove(p); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(s.replica.live+"-wal", []byte("stranded"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := s.bootMode(context.Background(), p); err == nil {
		t.Fatal("bootstrapped over a stranded WAL")
	}
}

func TestPinnedLitestreamFileReplica(t *testing.T) {
	if _, err := packaged.ReadFile("assets/litestream-linux-amd64.gz"); err != nil {
		t.Skip("drop/build.sh stages the release payload")
	}
	s := testService(t)
	s.bootstrap = true
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	record := filepath.Join(s.root, "pending")
	mode, err := s.bootMode(ctx, record)
	if err != nil || mode != "new" {
		t.Fatalf("enroll: mode=%q err=%v", mode, err)
	}
	// If the supervisor crashes after enrollment but before first /up, the
	// exact pending record can finish without a second prefix claim.
	s.bootstrap = false
	if mode, err := s.bootMode(ctx, record); err != nil || mode != "new" {
		t.Fatalf("pending new: mode=%q err=%v", mode, err)
	}
	db := makeDB(t, s.replica.live, s.app)
	writer := exec.Command("sleep", "60")
	if err := writer.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { writer.Process.Kill(); writer.Wait() }()
	s.writer = writer.Process
	if err := s.replica.prepare(); err != nil {
		t.Fatal(err)
	}
	repCtx, stop := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- s.runLitestream(repCtx) }()
	defer func() { stop(); <-done }()
	if err := s.readiness(ctx, false); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("INSERT INTO notes VALUES ('write after initial snapshot')"); err != nil {
		t.Fatal(err)
	}
	if status, body := hookReq(s, "/hooks/terminate", "test-only-token"); status != 200 || !s.draining {
		t.Fatalf("terminate=%d %s", status, body)
	}
	if status, body := hookReq(s, "/hooks/resume", "test-only-token"); status != 200 || s.draining {
		t.Fatalf("resume=%d %s", status, body)
	}
	if err := s.replica.restore(ctx); err != nil {
		t.Fatal(err)
	}
	restored, err := sql.Open("sqlite3", "file:"+s.replica.project+"?mode=ro")
	if err != nil {
		t.Fatal(err)
	}
	var body string
	if err := restored.QueryRow("SELECT body FROM notes").Scan(&body); err != nil || body != "write after initial snapshot" {
		t.Fatalf("restore body=%q err=%v", body, err)
	}
	restored.Close()
	before, err := os.ReadFile(s.replica.project)
	if err != nil {
		t.Fatal(err)
	}
	r := *s.replica
	r.app = "d-another-app"
	if err := r.restore(ctx); err == nil {
		t.Fatal("restored wrong app identity")
	}
	after, err := os.ReadFile(r.project)
	if err != nil || sha256.Sum256(before) != sha256.Sum256(after) {
		t.Fatal("failed restore changed snapshot")
	}
}

func TestEnrollmentCanResumeButAnotherVolumeCannotEnrollTwice(t *testing.T) {
	s := testService(t)
	for _, tc := range []struct {
		nonce string
		want  bool
	}{{"first", true}, {"first", true}, {"second", false}} {
		fresh, err := s.reserveNew(context.Background(), tc.nonce)
		if err != nil || fresh != tc.want {
			t.Fatalf("reserve %q = %t,%v", tc.nonce, fresh, err)
		}
	}
}

func TestLocalBootWithAbsentAsyncToken(t *testing.T) {
	s := testService(t)
	s.fileReplica = ""
	t.Setenv("AWS_ROLE_ARN", "arn:aws:iam::123456789012:role/test")
	t.Setenv("AWS_WEB_IDENTITY_TOKEN_FILE", filepath.Join(s.root, "pending-token"))
	for _, key := range []string{"AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN", "AWS_PROFILE", "AWS_CONTAINER_CREDENTIALS_FULL_URI", "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI"} {
		t.Setenv(key, "")
	}
	makeDB(t, s.replica.live, s.app)
	timeout, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	mode, err := s.bootMode(timeout, filepath.Join(s.root, "pending"))
	if err != nil || mode != "local" {
		t.Fatalf("local mode %q, %v", mode, err)
	}
	if err := s.cloud(timeout); err != context.DeadlineExceeded {
		t.Fatalf("cloud attempt with missing token: %v", err)
	}
}

// /up and auth-mode are compiled into Core; neither proves the Crew router
// registered its JavaScript entry points. A public 200 would also be unsafe.
func TestProbeRequiresProtectedCrewRoute(t *testing.T) {
	s := testService(t)
	s.domain = "crew.example.test"
	for _, status := range []int{404, 200, 401} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/up" {
					w.WriteHeader(200)
					return
				}
				if r.Method != http.MethodPost || r.URL.Path != "/api/crew/read" || r.Host != s.domain || r.Header.Get("Authorization") != "" {
					t.Errorf("unexpected probe: %s %s host=%q", r.Method, r.URL.Path, r.Host)
				}
				w.WriteHeader(status)
			}))
			defer server.Close()
			target, _ := url.Parse(server.URL)
			s.backend = target.Host
			ready, err := s.probeCrew(context.Background(), server.Client())
			want := status == 401
			if ready != want || (err == nil) != want {
				t.Fatalf("handler %d: ready=%t err=%v", status, ready, err)
			}
		})
	}
}
