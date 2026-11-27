package workspacecopy

import (
	"errors"
	"os"
	"path/filepath"
)

// Apply runs under the supervisor's exclusive volume lock with the backend
// and replicator stopped. Filesystem moves are repeatable after interruption.
func Apply(root, project, live string, validate func(string) error, markRecovered func(string) error) error {
	job := filepath.Join(root, "owner-restore")
	raw, err := os.ReadFile(filepath.Join(job, "id"))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	id := string(raw)
	if !copyID.MatchString(id) {
		return errors.New("invalid pending workspace restore")
	}
	source := filepath.Join(job, "data.db")
	staged, err := exists(source)
	if err != nil {
		return err
	}
	if staged {
		if err = validate(source); err != nil {
			return err
		}
		prev := filepath.Join(job, "previous-live")
		saved, err := exists(prev)
		if err != nil {
			return err
		}
		if !saved {
			if err = os.Rename(filepath.Dir(live), prev); err != nil {
				return err
			}
			if err = synced(job); err != nil {
				return err
			}
			if err = synced(filepath.Dir(filepath.Dir(live))); err != nil {
				return err
			}
		}
		if err = os.MkdirAll(filepath.Dir(live), 0700); err != nil {
			return err
		}
		if err = os.Link(project, filepath.Join(job, "previous-project.db")); err != nil && !errors.Is(err, os.ErrExist) && !errors.Is(err, os.ErrNotExist) {
			return err
		}
		if err = os.Rename(source, project); err != nil {
			return err
		}
	}
	if err = synced(filepath.Dir(project)); err != nil {
		return err
	}
	if err = validate(project); err != nil {
		return err
	}
	if err = markRecovered(id); err != nil {
		return err
	}
	// A different private directory for each application of a copy keeps the
	// pre-restore working set available if startup or a subsequent migration fails.
	old, err := os.MkdirTemp(root, ".before-restore-")
	if err != nil {
		return err
	}
	if err = os.Rename(job, filepath.Join(old, "workspace")); err != nil {
		return err
	}
	if err = synced(old); err != nil {
		return err
	}
	return synced(root)
}
func exists(path string) (bool, error) {
	_, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	return err == nil, err
}
