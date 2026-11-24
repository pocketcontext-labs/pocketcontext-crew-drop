//go:build linux && crew_drop

package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/pocketcontext-labs/pocketcontext-crew/internal/client"
	"github.com/pocketcontext-labs/pocketcontext-crew/internal/dropsupervisor"
)

func runDrop(args []string) (bool, int) {
	if len(args) > 0 && args[0] != "drop-serve" && args[0] != "drop-backend" {
		return false, 0
	}
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	var err error
	switch {
	case len(args) == 0 || (len(args) == 1 && args[0] == "drop-serve"):
		err = dropsupervisor.Run(ctx)
	case len(args) == 2 && args[0] == "drop-backend":
		err = client.ServeDropBackend(ctx, args[1])
	default:
		err = errors.New("usage: pocketcontext-crew-drop drop-serve")
	}
	if err != nil && !errors.Is(err, context.Canceled) {
		fmt.Fprintln(os.Stderr, err)
		return true, 1
	}
	return true, 0
}
