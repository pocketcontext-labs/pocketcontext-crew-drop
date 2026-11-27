//go:build linux && crew_drop

package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/pocketcontext-labs/pocketcontext-crew/internal/buildinfo"
	"github.com/pocketcontext-labs/pocketcontext-crew/internal/client"
	"github.com/pocketcontext-labs/pocketcontext-crew/internal/dropsupervisor"
)

func runDrop(args []string) (bool, int) {
	if len(args) > 0 && args[0] == "--drop-manifest" {
		if len(args) != 1 {
			fmt.Fprintln(os.Stderr, "usage: pocketcontext-crew-drop --drop-manifest")
			return true, 2
		}
		err := json.NewEncoder(os.Stdout).Encode(map[string]any{
			"schema":    "drop.manifest/v1",
			"app":       "crew",
			"version":   buildinfo.Version,
			"http_port": 8080,
			"hook_port": 8081,
			"health":    "tcp",
			"hooks":     []string{"suspend", "resume", "terminate"},
			"requires":  []string{"persistent_volume", "verified_owner_email", "sign_in_email"},
			"optional":  []string{"s3_replica"},
			"minimum":   map[string]int{"memory_mib": 256, "cpu_quarters": 1, "disk_mib": 1024},
		})
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			return true, 1
		}
		return true, 0
	}
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
