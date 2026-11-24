package dropsupervisor

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"sync"
)

// A round starts after every commit whose caller joined it. Requests arriving
// during syncfs must wait for a later round, even when the first one succeeds.
type syncRound struct {
	done chan struct{}
	err  error
}

type volumeBarrier struct {
	flush   func() error
	mu      sync.Mutex
	pending *syncRound
	running bool
}

func (b *volumeBarrier) wait(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	b.mu.Lock()
	if b.pending == nil {
		b.pending = &syncRound{done: make(chan struct{})}
	}
	round := b.pending
	if !b.running {
		b.running = true
		go b.run()
	}
	b.mu.Unlock()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-round.done:
		return round.err
	}
}

func (b *volumeBarrier) run() {
	for {
		b.mu.Lock()
		round := b.pending
		b.pending = nil
		if round == nil {
			b.running = false
			b.mu.Unlock()
			return
		}
		b.mu.Unlock()
		round.err = b.flush()
		close(round.done)
	}
}

func requiresDurability(r *http.Request) bool {
	switch r.Method {
	case http.MethodGet, http.MethodHead, http.MethodOptions:
		return false
	case http.MethodPost:
		switch r.URL.Path {
		case "/api/crew/read", "/api/crew/search", "/api/crew/query", "/api/crew/attachment-read", "/api/crew/attachment-preview", "/api/crew/upload-link/info":
			return false
		}
	}
	// Includes new routes, custom writes, upload links, OAuth and MCP tool calls.
	return true
}

// Hold the whole successful response. In particular, a legacy MCP batch may
// produce an SSE-formatted POST result before the last operation has finished.
// GET subscriptions and the explicit business reads above still stream.
func holdResponse(body io.Reader) (io.ReadCloser, error) {
	const memoryLimit = 64 << 10
	prefix, err := io.ReadAll(io.LimitReader(body, memoryLimit+1))
	if err != nil {
		return nil, err
	}
	if len(prefix) <= memoryLimit {
		return io.NopCloser(bytes.NewReader(prefix)), nil
	}
	f, err := os.CreateTemp("", "crew-response-*")
	if err != nil {
		return nil, err
	}
	// This is a Linux guest. An open, unlinked descriptor survives only as long
	// as the request, including when the supervisor itself is killed.
	if err = os.Remove(f.Name()); err == nil {
		_, err = f.Write(prefix)
	}
	if err == nil {
		_, err = io.Copy(f, body)
	}
	if err == nil {
		_, err = f.Seek(0, io.SeekStart)
	}
	if err != nil {
		f.Close()
		return nil, err
	}
	return f, nil
}

var errDurability = errors.New("local durability barrier failed")

func (s *service) durableResponse(resp *http.Response) error {
	if resp.StatusCode < 200 || resp.StatusCode >= 400 || !requiresDurability(resp.Request) {
		return nil
	}
	upstream := resp.Body
	replay, err := holdResponse(upstream)
	upstream.Close()
	if err != nil {
		return fmt.Errorf("read complete backend response: %w", err)
	}
	if err := s.disk.wait(resp.Request.Context()); err != nil {
		replay.Close()
		return fmt.Errorf("%w: %v", errDurability, err)
	}
	resp.Body = replay
	return nil
}

func proxyError(w http.ResponseWriter, r *http.Request, err error) {
	if r.Context().Err() != nil {
		return
	}
	if errors.Is(err, errDurability) {
		log.Printf("CrewDrop local response durability: %v", err)
		w.Header().Set("Retry-After", "1")
		respond(w, http.StatusServiceUnavailable, map[string]string{"error": "local durability unconfirmed"})
		return
	}
	http.Error(w, "Bad Gateway", http.StatusBadGateway)
}
