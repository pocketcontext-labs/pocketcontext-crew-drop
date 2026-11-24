//go:build linux

package dropsupervisor

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
)

func TestMutationResponseWaitsForCommitAndOwnDiskRound(t *testing.T) {
	began := make(chan string, 2)
	finishFirst := make(chan struct{})
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/crew/read" {
			w.WriteHeader(200)
			return
		}
		w.Header().Set("Set-Cookie", "session=fixture")
		w.WriteHeader(201)
		if r.URL.Query().Get("seq") == "first" {
			// Some upstream handlers can flush headers before performing the
			// final work. The guest must withhold them until that work is done.
			w.(http.Flusher).Flush()
			began <- "headers only"
			<-finishFirst
		}
		io.WriteString(w, "committed "+r.URL.Query().Get("seq"))
	}))
	defer backend.Close()
	u, _ := url.Parse(backend.URL)
	t.Setenv("CREW_DROP_BACKEND_ADDR", u.Host)
	s := testService(t)
	s.data("ready", nil)
	rounds := make(chan chan error, 3)
	s.disk = &volumeBarrier{flush: func() error {
		result := make(chan error, 1)
		rounds <- result
		return <-result
	}}
	guest := httptest.NewServer(http.HandlerFunc(s.public))
	defer guest.Close()
	type reply struct {
		code               int
		token, retry, body string
		err                error
	}
	send := func(path string) <-chan reply {
		got := make(chan reply, 1)
		go func() {
			resp, err := http.Post(guest.URL+path, "application/json", strings.NewReader("{}"))
			if err != nil {
				got <- reply{err: err}
				return
			}
			defer resp.Body.Close()
			b, err := io.ReadAll(resp.Body)
			got <- reply{resp.StatusCode, resp.Header.Get("Set-Cookie"), resp.Header.Get("Retry-After"), string(b), err}
		}()
		return got
	}
	first := send("/mcp?seq=first")
	<-began
	select {
	case round := <-rounds:
		round <- nil
		close(finishFirst)
		t.Fatal("durability began before the response body finished")
	case <-time.After(30 * time.Millisecond):
	}
	close(finishFirst)
	one := <-rounds
	second := send("/api/crew/write?seq=second")
	// Ensure the second operation really committed while the first flush was
	// in flight; it cannot be acknowledged by that earlier syncfs call.
	timeout := time.Now().Add(3 * time.Second)
	for {
		s.disk.mu.Lock()
		queued := s.disk.pending != nil
		s.disk.mu.Unlock()
		if queued {
			break
		}
		if time.Now().After(timeout) {
			one <- nil
			t.Fatal("second commit never reached its durability wait")
		}
		time.Sleep(time.Millisecond)
	}
	read := <-send("/api/crew/read")
	if read.err != nil || read.code != 200 {
		one <- nil
		t.Fatalf("read blocked on disk: %+v", read)
	}
	one <- nil
	two := <-rounds
	two <- errors.New("device unavailable")
	a, b := <-first, <-second
	if a.err != nil || a.code != 201 || a.body != "committed first" || a.token == "" {
		t.Fatalf("successful barrier lost upstream reply: %+v", a)
	}
	if b.err != nil || b.code != 503 || b.retry != "1" || b.token != "" || strings.Contains(b.body, "committed second") {
		t.Fatalf("barrier failure leaked a successful response: %+v", b)
	}
}

func TestDurabilityRouteClassification(t *testing.T) {
	for _, test := range []struct {
		method, path string
		durable      bool
	}{
		{"GET", "/api/crew/attachment", false}, {"HEAD", "/api/crew/read", false},
		{"POST", "/api/crew/read", false}, {"POST", "/api/crew/search", false}, {"POST", "/api/crew/query", false},
		{"POST", "/api/crew/attachment-read", false}, {"POST", "/api/crew/upload-link/info", false},
		{"POST", "/api/crew/write", true}, {"POST", "/api/crew/agent-write", true},
		{"POST", "/api/crew/upload-link/upload", true}, {"POST", "/mcp", true}, {"POST", "/oauth/token", true},
		{"PATCH", "/future", true}, {"DELETE", "/future", true},
	} {
		r := httptest.NewRequest(test.method, test.path, nil)
		if requiresDurability(r) != test.durable {
			t.Errorf("%s %s: expected durable=%v", test.method, test.path, test.durable)
		}
	}
}

func TestHoldResponseSpillsWithoutLosingBytes(t *testing.T) {
	body := strings.Repeat("abcdef", 1<<15)
	replay, err := holdResponse(strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer replay.Close()
	got, err := io.ReadAll(replay)
	if err != nil || string(got) != body {
		t.Fatalf("spilled %d of %d bytes, error %v", len(got), len(body), err)
	}
}

func TestCanceledDiskWaitNeverCompletesAsSuccess(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	gate := make(chan struct{})
	start := make(chan struct{})
	b := &volumeBarrier{flush: func() error { close(start); <-gate; return nil }}
	result := make(chan error, 1)
	go func() { result <- b.wait(ctx) }()
	<-start
	cancel()
	if err := <-result; err != context.Canceled {
		t.Fatal(err)
	}
	close(gate)
}
