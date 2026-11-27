//go:build linux && crew_drop

package client

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/pocketcontext-labs/pocketcontext-crew/internal/dropsupervisor"
)

// Only the test binary can use the synthetic mailbox. The guest, startup,
// replication, lifecycle, authentication and copy routes otherwise run normally.
func init() {
	mailbox := os.Getenv("CREW_COPY_TEST_MAILBOX")
	if mailbox == "" || len(os.Args) < 2 {
		return
	}
	var err error
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
	switch os.Args[1] {
	case "drop-serve":
		err = dropsupervisor.Run(ctx)
	case "drop-backend":
		err = serveDropBackend(ctx, os.Args[2], func(code string) error { return os.WriteFile(mailbox, []byte(code), 0600) })
	default:
		stop()
		return
	}
	wasRunning := ctx.Err() == nil
	stop()
	if err != nil && wasRunning {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	os.Exit(0)
}
func TestGuestWorkspaceCopyRecovery(t *testing.T) {
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command("python3", filepath.Join("..", "..", "drop", "copies_test.py"), binary)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("%v: %s", err, output)
	}
}
