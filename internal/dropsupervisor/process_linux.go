//go:build linux

package dropsupervisor

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

// A kernel-held lock disappears after a crash. A stale pathname never locks
// the data volume and a surviving second supervisor cannot start another writer.
func lockVolume(path string) (*os.File, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	if err := unix.Flock(int(f.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		f.Close()
		return nil, fmt.Errorf("volume already locked: %w", err)
	}
	return f, nil
}

func configureChild(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Pdeathsig: syscall.SIGKILL}
	cmd.Cancel = func() error { return cmd.Process.Signal(syscall.SIGTERM) }
	cmd.WaitDelay = 5 * time.Second
}

// Wait for the kernel's group-stop notification. At that point all backend
// threads, including Crew jobs, are quiescent. Wait does not consume child
// exit status owned by exec.Cmd. A timeout rolls back the stop.
func freezeWriter(ctx context.Context, p *os.Process) error {
	if err := p.Signal(syscall.SIGSTOP); err != nil {
		return err
	}
	for {
		var info unix.Siginfo
		err := unix.Waitid(unix.P_PID, p.Pid, &info, unix.WSTOPPED|unix.WNOWAIT|unix.WNOHANG, nil)
		if err == nil && info.Signo != 0 {
			return nil
		}
		if err != nil && err != unix.EINTR {
			_ = resumeWriter(p)
			return err
		}
		if err := wait(ctx, 4*time.Millisecond); err != nil {
			_ = resumeWriter(p)
			return err
		}
	}
}
func resumeWriter(p *os.Process) error { return p.Signal(syscall.SIGCONT) }

// Keep the same descriptor for the serving lifetime. Linux then also reports
// asynchronous writeback errors since the preceding sync on that descriptor.
func syncVolume(volume *os.File) error {
	conn, err := volume.SyscallConn()
	if err != nil {
		return err
	}
	var flushErr error
	err = conn.Control(func(fd uintptr) {
		for {
			flushErr = unix.Syncfs(int(fd))
			if flushErr != unix.EINTR {
				break
			}
		}
	})
	if err != nil {
		return err
	}
	return flushErr
}
