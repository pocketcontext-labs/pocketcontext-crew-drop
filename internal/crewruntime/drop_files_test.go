package crewruntime

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"io"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pocketcontext-labs/pocketcontext-crew/internal/dropfiles"
)

type testDropObjects struct {
	store         map[string][]byte
	gets          int
	failPut       bool
	blockIdentity chan struct{}
	entered       chan struct{}
}

func (m *testDropObjects) Prepare(ctx context.Context) error {
	if m.blockIdentity == nil {
		return nil
	}
	close(m.entered)
	select {
	case <-m.blockIdentity:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (m *testDropObjects) Put(_ context.Context, digest string, size int64, body io.ReadSeeker) error {
	if m.failPut {
		return errors.New("synthetic upload failure")
	}
	var b bytes.Buffer
	if err := dropfiles.Verify(&b, body, digest, size); err != nil {
		return err
	}
	m.store[digest] = b.Bytes()
	return nil
}

func (m *testDropObjects) Get(_ context.Context, digest string) (io.ReadCloser, error) {
	m.gets++
	data, ok := m.store[digest]
	if !ok {
		return nil, os.ErrNotExist
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

func TestDropAttachmentDurabilityAndAuthorizedRepair(t *testing.T) {
	a := newTestApp(t)
	m := &testDropObjects{store: map[string][]byte{}}
	bindDropFiles(a.app, m)
	page := []byte("<p>Synthetic protected preview</p>")
	task := multipartRequest(t, a, "/api/crew/write", a.token, envelope("task.create", map[string]any{"title": "Synthetic preview"}), []testFile{{"preview.html", page}}, 200)
	att := task["attachments"].([]any)[0].(map[string]any)
	id := att["id"].(string)
	hash := att["sha256"].(string)
	if !bytes.Equal(m.store[hash], page) {
		t.Fatal("upload acknowledged without durable bytes")
	}
	record, err := findAttachment(a.app, id)
	if err != nil {
		t.Fatal(err)
	}
	filename := filepath.Join(a.app.DataDir(), "storage", filepath.FromSlash(record.BaseFilesPath()), record.GetString("file"))
	remove := func() {
		t.Helper()
		if err := os.Remove(filename); err != nil {
			t.Fatal(err)
		}
	}
	remove()
	attachmentGet(t, a, "id="+id, "", 401)
	attachmentGet(t, a, "id="+id+"&preview=invalid", "", 401)
	a.request(t, "POST", "/api/crew/attachment-read", "", `{"id":"`+id+`"}`, 401)
	a.request(t, "POST", "/api/crew/attachment-read", a.token, `{"id":"`+id+`","variant":"unexpected"}`, 400)
	if m.gets != 0 {
		t.Fatal("unauthorized or invalid requests fetched bytes")
	}
	crewWrite(t, a, false, "attachment.archive", id, 1, map[string]any{}, 200)
	if got := attachmentGet(t, a, "id="+id, a.token, 200); !bytes.Equal(got.Body.Bytes(), page) {
		t.Fatal("archived download lost original")
	}
	crewWrite(t, a, false, "attachment.restore", id, 2, map[string]any{}, 200)
	query := previewQuery(t, previewLink(t, a, a.token, id, 200)["url"].(string))
	remove()
	if res := attachmentGet(t, a, query, "", 200); !bytes.Equal(res.Body.Bytes(), page) || !strings.Contains(res.Header().Get("Content-Security-Policy"), "sandbox allow-scripts;") {
		t.Fatal("preview did not restore protected bytes")
	}
	if err := os.WriteFile(filename, bytes.Repeat([]byte{'!'}, len(page)), 0600); err != nil {
		t.Fatal(err)
	}
	body := a.request(t, "POST", "/api/crew/attachment-read", a.token, `{"id":"`+id+`","variant":"original","offset":3,"max_bytes":8}`, 200)
	if body["content_base64"] != base64.StdEncoding.EncodeToString(page[3:11]) {
		t.Fatalf("wrong work-client bytes: %v", body)
	}
	if m.gets != 3 {
		t.Fatalf("expected three verified recoveries, got %d", m.gets)
	}
	remove()
	m.store[hash] = bytes.Repeat([]byte{'?'}, len(page))
	res := attachmentGet(t, a, "id="+id, a.token, 503)
	if bytes.Contains(res.Body.Bytes(), m.store[hash]) {
		t.Fatal("corrupt object served")
	}
	if _, err := os.Stat(filename); !os.IsNotExist(err) {
		t.Fatal("corrupt object published", err)
	}
	m.store[hash] = page
	attachmentGet(t, a, "id="+id, a.token, 200)
	delete(m.store, hash)
	if got := attachmentGet(t, a, "id="+id, a.token, 200); !bytes.Equal(got.Body.Bytes(), page) {
		t.Fatal("local first read failed")
	}
}

func TestDropFileFailedUploadRollsBackAndRetries(t *testing.T) {
	a := newTestApp(t)
	m := &testDropObjects{store: map[string][]byte{}, failPut: true}
	bindDropFiles(a.app, m)
	files := []testFile{{"synthetic.log", []byte("synthetic-log")}}
	task := envelope("task.create", map[string]any{"title": "Synthetic retry"})
	task["idempotency_key"] = "same-retry"
	multipartRequest(t, a, "/api/crew/write", a.token, task, files, 503)
	for _, collection := range []string{"crew_tasks", "crew_attachments"} {
		if count, err := a.app.CountRecords(collection); err != nil || count != 0 {
			t.Fatalf("%s count %d: %v", collection, count, err)
		}
	}
	m.failPut = false
	out := multipartRequest(t, a, "/api/crew/write", a.token, task, files, 200)
	if len(m.store) != 1 {
		t.Fatal("retried upload not stored")
	}
	link := crewWrite(t, a, false, "upload_link.create", "", 0, map[string]any{"task": out["id"]}, 200)
	batch := []testFile{{"synthetic.png", tinyPNG(t, 6, 5)}}
	uploadLinkRequest(t, a, strings.Repeat("A", 43), nil, batch, 404)
	if len(m.store) != 1 {
		t.Fatal("unknown upload link stored content")
	}
	uploadLinkRequest(t, a, link["token"].(string), nil, batch, 200)
	if got := m.store[digest(batch[0].data)]; !bytes.Equal(got, batch[0].data) {
		t.Fatal("upload-link bytes not durable")
	}
}

func TestDropPublishedSnapshotUsesAuthorizedImmutableOriginal(t *testing.T) {
	a := newTestApp(t)
	m := &testDropObjects{store: map[string][]byte{}}
	bindDropFiles(a.app, m)
	shot := tinyPNG(t, 25, 12)
	draft := crewWrite(t, a, true, "artifact.create", "", 0, map[string]any{"title": "Synthetic chart", "body": "![chart](attachment:new-1)", "files": []any{map[string]any{"name": "chart.png", "content_base64": base64.StdEncoding.EncodeToString(shot)}}}, 200)
	id := draft["id"].(string)
	crewWrite(t, a, true, "artifact.publish", id, 1, map[string]any{}, 403)
	published := crewWrite(t, a, false, "artifact.publish", id, 1, map[string]any{}, 200)
	token := published["token"].(string)
	att := published["published_files"].([]any)[0].(string)
	record, err := findAttachment(a.app, att)
	if err != nil {
		t.Fatal(err)
	}
	filename := filepath.Join(a.app.DataDir(), "storage", filepath.FromSlash(record.BaseFilesPath()), record.GetString("file"))
	if err := os.Remove(filename); err != nil {
		t.Fatal(err)
	}
	get := func(token, index string, code int) []byte {
		t.Helper()
		req := httptest.NewRequest("GET", "/a/"+token+"/files/"+index, nil)
		res := httptest.NewRecorder()
		a.handler.ServeHTTP(res, req)
		if res.Code != code {
			t.Fatalf("artifact file: %d, want %d (%s)", res.Code, code, res.Body.String())
		}
		return res.Body.Bytes()
	}
	get(strings.Repeat("X", 32), "1", 404)
	get(token, "2", 404)
	if m.gets != 0 {
		t.Fatal("unpublished file was accessed")
	}
	if !bytes.Equal(get(token, "1", 200), shot) {
		t.Fatal("published bytes changed")
	}
	crewWrite(t, a, false, "artifact.revoke", id, int(published["revision"].(float64)), map[string]any{}, 200)
	if err := os.Remove(filename); err != nil {
		t.Fatal(err)
	}
	get(token, "1", 404)
	if m.gets != 1 {
		t.Fatal("revoked artifact fetched bytes")
	}
}

func TestDropPlainWriteWhileFileIdentityIsPending(t *testing.T) {
	a := newTestApp(t)
	m := &testDropObjects{store: map[string][]byte{}, blockIdentity: make(chan struct{}), entered: make(chan struct{})}
	bindDropFiles(a.app, m)
	var release sync.Once
	unblock := func() { release.Do(func() { close(m.blockIdentity) }) }
	defer unblock()
	send := func(body string) <-chan int {
		done := make(chan int, 1)
		go func() {
			req := httptest.NewRequest("POST", "/api/crew/agent-write", strings.NewReader(body))
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("Authorization", a.token)
			rec := httptest.NewRecorder()
			a.handler.ServeHTTP(rec, req)
			done <- rec.Code
		}()
		return done
	}
	attachment := send(`{"action":"task.create","data":{"title":"Upload waits for identity","files":[{"name":"synthetic.txt","content_base64":"dGVzdA=="}]}}`)
	select {
	case <-m.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("attachment never reached identity preflight")
	}
	plain := send(`{"action":"task.create","data":{"title":"Independent plain task"}}`)
	select {
	case code := <-plain:
		if code != 200 {
			t.Fatal("plain write failed with status", code)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("pending credentials blocked a plain Crew write")
	}
	unblock()
	select {
	case code := <-attachment:
		if code != 200 {
			t.Fatal("attachment failed after identity arrived", code)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("attachment never completed")
	}
}
