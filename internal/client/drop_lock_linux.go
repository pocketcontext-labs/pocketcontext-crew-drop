//go:build linux && crew_drop

package client

import (
	"fmt"
	"os"
	"path/filepath"

	"golang.org/x/sys/unix"
)

func lockDropVolume(cache string) (func(), error) {
	f, err := os.OpenFile(filepath.Join(cache, "drop-backend.lock"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	if err := unix.Flock(int(f.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		f.Close()
		return nil, fmt.Errorf("lock Drop backend volume: %w", err)
	}
	return func() { f.Close() }, nil
}
