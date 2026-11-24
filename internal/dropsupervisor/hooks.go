package dropsupervisor

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"errors"
	"net/http"
	"os"
	"strings"
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
	return map[string]any{"boot_mode": s.mode, "data_state": s.dataState, "replication_state": s.replicaState, "data_error": s.lastDataError, "replication_error": s.lastReplicaError, "data_waiters": s.waiters, "hook_waiters": s.hookWaiters, "in_flight": s.active, "draining": s.draining, "restore_attempts": s.restores, "recovery_attempts": s.recoveries, "held": held, "phases": phases, "litestream": "0.5.18"}
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

func (s *service) lifecycleEvent(ctx context.Context, event string) error {
	select {
	case s.lifecycle <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}
	defer func() { <-s.lifecycle }()
	if event == "resume" {
		if s.frozen != nil {
			if err := resumeWriter(s.frozen); err != nil && !errors.Is(err, os.ErrProcessDone) {
				return err
			}
			s.frozen = nil
		}
		s.setDraining(false)
	}
	if event != "sync" {
		if err := s.gate(ctx, "hooks"); err != nil {
			return err
		}
	}
	if event == "start" || event == "resume" {
		return nil
	}
	if s.frozen != nil {
		return nil
	} // repeated lifecycle event after a successful final cut
	quiesce := event == "suspend" || event == "terminate"
	if quiesce {
		s.setDraining(true)
	}
	err := s.readiness(ctx, quiesce)
	if err == nil && event == "terminate" {
		s.mu.Lock()
		writer := s.writer
		s.mu.Unlock()
		if writer == nil {
			err = errors.New("no backend writer")
		} else {
			err = freezeWriter(ctx, writer)
			if err == nil {
				s.frozen = writer
			}
		}
	}
	if err == nil {
		err = s.replica.sync(ctx)
	}
	if err != nil && s.frozen != nil {
		_ = resumeWriter(s.frozen)
		s.frozen = nil
	}
	if quiesce && (event == "suspend" || err != nil) {
		s.setDraining(false)
	}
	if err != nil {
		return errors.New("drain or replica sync did not complete")
	}
	return nil
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
	if r.Method == "GET" && r.URL.Path == "/health" {
		respond(w, 200, map[string]bool{"listening": true})
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
	case "/hooks/start", "/hooks/suspend", "/hooks/resume", "/hooks/terminate":
	default:
		http.NotFound(w, r)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), s.hookTimeout)
	defer cancel()
	s.mu.Lock()
	s.hookWaiters++
	s.markLocked("hook_" + event + "_started")
	s.mu.Unlock()
	defer func() { s.mu.Lock(); s.hookWaiters--; s.mu.Unlock() }()
	if err := s.lifecycleEvent(ctx, event); err != nil {
		respond(w, 503, map[string]string{"error": err.Error()})
		return
	}
	s.mark("hook_" + event + "_completed")
	respond(w, 200, map[string]string{"completed": event})
}
