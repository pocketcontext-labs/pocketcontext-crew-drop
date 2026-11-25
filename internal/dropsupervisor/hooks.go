package dropsupervisor

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"errors"
	"log"
	"net/http"
	"os"
	"strings"
	"time"
)

func (s *service) status() map[string]any {
	s.mu.Lock()
	defer s.mu.Unlock()
	phases := make(map[string]any, len(s.phases))
	for k, v := range s.phases {
		phases[k] = v
	}
	held := map[string]bool{}
	for key := range s.gates {
		held[key] = true
	}
	return map[string]any{"boot_mode": s.mode, "data_state": s.dataState, "replication_state": s.replicaState, "data_error": s.lastDataError, "replication_error": s.lastReplicaError, "data_waiters": s.waiters, "hook_waiters": s.hookWaiters, "in_flight": s.active, "draining": s.draining, "restore_attempts": s.restores, "recovery_attempts": s.recoveries, "held": held, "phases": phases, "litestream": "0.5.18", "replica_sync_interval": s.syncInterval.String(), "last_hook": s.lastHook}
}

func (s *service) readiness(ctx context.Context, drain bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for ctx.Err() == nil {
		if s.replicaState == "ready" && (!drain || s.active == 0) {
			return nil
		}
		next := s.changed
		s.mu.Unlock()
		select {
		case <-next:
		case <-ctx.Done():
		}
		s.mu.Lock()
	}
	return ctx.Err()
}

func (s *service) setDraining(value bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.draining = value
	s.notifyLocked()
}

// hookOutcome is the body of a lifecycle or sync answer and the last outcome
// in /status. NibRunner records only the HTTP status: 200 means the drain,
// a confirmed replica sync and the local flush all completed.
type hookOutcome struct {
	Event       string    `json:"event"`
	OK          bool      `json:"ok"`
	Completed   string    `json:"completed,omitempty"`   // the event, on success
	Replication string    `json:"replication,omitempty"` // confirmed or unconfirmed
	Stage       string    `json:"stage,omitempty"`       // first step that failed
	Error       string    `json:"error,omitempty"`
	TXID        uint64    `json:"txid,omitempty"`
	ReplicaTXID uint64    `json:"replica_txid,omitempty"`
	At          time.Time `json:"at"`
}

func (s *service) lifecycleEvent(ctx context.Context, event string) (out hookOutcome) {
	out.Event = event
	fail := func(stage string, err error) hookOutcome {
		out.Stage, out.Error = stage, err.Error()
		if event != "resume" {
			out.Replication = "unconfirmed"
		}
		return out
	}
	select {
	case s.lifecycle <- struct{}{}:
	case <-ctx.Done():
		return fail("lifecycle", ctx.Err())
	}
	defer func() { <-s.lifecycle }()
	if event == "resume" {
		// Resume only thaws a writer that terminate froze. It never starts a
		// backend; runWorkers owns the single writer under the volume lock.
		if s.frozen != nil {
			if err := resumeWriter(s.frozen); err != nil && !errors.Is(err, os.ErrProcessDone) {
				return fail("resume", err)
			}
			s.frozen = nil
		}
		s.setDraining(false)
	}
	if event != "sync" {
		if err := s.gate(ctx, "hooks"); err != nil {
			return fail("hold", err)
		}
	}
	if event == "resume" {
		out.OK = true
		return out
	}
	if s.frozen != nil {
		// The prior cut already confirmed the unchanged remote state.
		if err := s.disk.wait(ctx); err != nil {
			return fail("flush", err)
		}
		out.OK, out.Replication = true, "confirmed"
		return out
	}
	quiesce := event == "suspend" || event == "terminate"
	if quiesce {
		s.setDraining(true)
	}
	stage := "drain"
	err := s.readiness(ctx, quiesce)
	var frozen *os.Process
	if err == nil && quiesce {
		// Stop Crew's background jobs too, so nothing commits between the
		// replicated cut and the answer. Terminate keeps the writer stopped
		// for handoff; suspend thaws it before answering so a snapshot wake
		// serves without waiting for the resume hook.
		stage = "freeze"
		s.mu.Lock()
		writer := s.writer
		s.mu.Unlock()
		if writer == nil {
			err = errors.New("no backend writer")
		} else if err = freezeWriter(ctx, writer); err == nil {
			frozen = writer
		}
	}
	var result syncResult
	if err == nil {
		stage = "sync"
		result, err = s.replica.sync(ctx)
		out.TXID = result.TXID
		if result.ReplicaTXID != nil {
			out.ReplicaTXID = *result.ReplicaTXID
		}
	}
	if err == nil {
		// After remote sync, also push the guest's dirty pages and ext4
		// journal through its own block device before confirming the cut.
		stage = "flush"
		err = s.disk.wait(ctx)
	}
	if frozen != nil {
		if err == nil && event == "terminate" {
			s.frozen = frozen
		} else {
			_ = resumeWriter(frozen)
		}
	}
	if quiesce && (event == "suspend" || err != nil) {
		s.setDraining(false)
	}
	if err != nil {
		return fail(stage, err)
	}
	out.OK, out.Replication = true, "confirmed"
	return out
}

func (s *service) hook(w http.ResponseWriter, r *http.Request) {
	token, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
	digest := sha256.Sum256([]byte(token))
	if !ok || subtle.ConstantTimeCompare(s.token[:], digest[:]) != 1 {
		respond(w, 401, map[string]string{"error": "unauthorized"})
		return
	}
	if r.Method == "GET" && r.URL.Path == "/status" {
		respond(w, 200, s.status())
		return
	}
	if r.Method != "POST" {
		http.NotFound(w, r)
		return
	}
	switch r.URL.Path {
	case "/control/hold", "/control/release":
		key := r.URL.Query().Get("gate")
		if key != "aws" && key != "hooks" && key != "recovery" {
			respond(w, 400, map[string]string{"error": "unknown gate"})
			return
		}
		s.mu.Lock()
		ch := s.gates[key]
		if r.URL.Path == "/control/hold" && ch == nil {
			s.gates[key] = make(chan struct{})
		}
		if r.URL.Path == "/control/release" && ch != nil {
			delete(s.gates, key)
			close(ch)
		}
		s.mu.Unlock()
		respond(w, 200, s.status())
		return
	}
	event := strings.TrimPrefix(r.URL.Path, "/hooks/")
	switch r.URL.Path {
	case "/control/sync":
		event = "sync"
	case "/hooks/suspend", "/hooks/resume", "/hooks/terminate":
	default:
		http.NotFound(w, r)
		return
	}
	timeout := s.hookTimeout
	if event == "suspend" {
		// Finish the drain (or reopen ingress on failure) before the default 10s host deadline.
		timeout = min(timeout, 9*time.Second)
	}
	ctx, cancel := context.WithTimeout(r.Context(), timeout)
	defer cancel()
	s.mu.Lock()
	s.hookWaiters++
	s.markLocked("hook_" + event + "_started")
	s.mu.Unlock()
	defer func() { s.mu.Lock(); s.hookWaiters--; s.mu.Unlock() }()
	out := s.lifecycleEvent(ctx, event)
	out.At = time.Now().UTC()
	s.mu.Lock()
	s.lastHook = &out
	s.mu.Unlock()
	if !out.OK {
		log.Printf("CrewDrop %s did not complete at %s; replication %s: %s", event, out.Stage, out.Replication, out.Error)
		respond(w, 503, out)
		return
	}
	s.mark("hook_" + event + "_completed")
	out.Completed = event
	respond(w, 200, out)
}
