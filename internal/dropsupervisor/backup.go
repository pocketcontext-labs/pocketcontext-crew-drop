package dropsupervisor

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strconv"

	"github.com/pocketcontext-labs/pocketcontext-crew/internal/dropfiles"
	"github.com/pocketcontext-labs/pocketcontext-crew/internal/workspacecopy"
)

var backupJob = regexp.MustCompile("^[a-zA-Z0-9_-]{1,64}$")
var peerChallenge = regexp.MustCompile("^[a-fA-F0-9]{64}$")

func (s *service) backupPeer(w http.ResponseWriter, r *http.Request) {
	challenge := r.Header.Get("X-Drop-Challenge")
	if !peerChallenge.MatchString(challenge) {
		http.Error(w, "invalid challenge", 400)
		return
	}
	mac := hmac.New(sha256.New, []byte(os.Getenv("NIBRUN_HOOK_TOKEN")))
	io.WriteString(mac, "drop-backup-peer/v1:"+challenge)
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Drop-Proof", hex.EncodeToString(mac.Sum(nil)))
	w.WriteHeader(200)
}
func (s *service) backupHook(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	root := filepath.Join(s.root, "crew")
	p := workspacecopy.Platform{Root: root, Live: filepath.Join(root, "private", "live", "data.db"), Project: filepath.Join(root, "project", "crew.db"), AppID: s.app}
	ctx := r.Context()
	job := r.Header.Get("X-Drop-Job")
	s.mu.Lock()
	ready := s.dataState == "ready" && !s.draining
	s.mu.Unlock()
	if r.URL.Path != "/control/backup" {
		if !backupJob.MatchString(job) {
			http.Error(w, "invalid backup job", 400)
			return
		}
		if ready {
			applied, err := p.Applied(ctx, job)
			if err != nil {
				http.Error(w, "reading restore status failed", 503)
				return
			}
			if applied {
				respond(w, 200, map[string]string{"state": "applied"})
				return
			}
		}
		pending, err := os.ReadFile(filepath.Join(root, "owner-restore", "platform-job"))
		if string(pending) == job {
			respond(w, 202, map[string]string{"state": "staged"})
			return
		}
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			http.Error(w, "reading restore status failed", 503)
			return
		}
		if r.URL.Path == "/control/restore-status" {
			if !ready {
				http.Error(w, "backend restarting", 503)
				return
			}
			http.NotFound(w, r)
			return
		}
	}
	if !ready {
		http.Error(w, "backend unavailable", 503)
		return
	}
	select {
	case s.lifecycle <- struct{}{}:
		defer func() { <-s.lifecycle }()
	default:
		http.Error(w, "another operation is active", 409)
		return
	}
	if r.URL.Path == "/control/backup" {
		objects, err := dropfiles.FromEnvironment()
		if err != nil {
			http.Error(w, "storage unavailable", 503)
			return
		}
		p.Objects = objects
		started := false
		err = p.WithArchive(ctx, func(f *os.File) error {
			st, err := f.Stat()
			if err != nil {
				return err
			}
			if st.Size() > workspacecopy.MaxPlatformArchive {
				return workspacecopy.ErrPlatformArchiveLimit
			}
			w.Header().Set("Content-Type", "application/zip")
			w.Header().Set("Content-Length", strconv.FormatInt(st.Size(), 10))
			started = true
			_, err = io.Copy(w, f)
			return err
		})
		if err != nil {
			log.Printf("CrewDrop platform backup failed: %v", err)
			if !started {
				if errors.Is(err, workspacecopy.ErrPlatformArchiveLimit) {
					http.Error(w, err.Error(), 413)
					return
				}
				http.Error(w, "could not capture backup", 503)
			}
		}
		return
	}
	source := r.Header.Get("X-Drop-Source-App")
	if !appID.MatchString(source) {
		http.Error(w, "invalid source app", 400)
		return
	}
	// Hold ingress while the verified restore is staged on the destination snapshot.
	s.setDraining(true)
	defer s.setDraining(false)
	if err := s.readiness(ctx, true); err != nil {
		http.Error(w, "destination unavailable", 503)
		return
	}
	if err := p.StagePlatform(ctx, job, source, r.Body); err != nil {
		if errors.Is(err, workspacecopy.ErrPlatformArchiveLimit) {
			http.Error(w, err.Error(), 413)
			return
		}
		log.Printf("CrewDrop platform restore failed: %v", err)
		http.Error(w, "could not verify and stage backup", 422)
		return
	}
	s.data("starting", nil)
	respond(w, 202, map[string]string{"state": "staged"})
}
