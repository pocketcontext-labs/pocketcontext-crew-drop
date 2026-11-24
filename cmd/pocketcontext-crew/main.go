// Command pocketcontext-crew is the PocketContext Crew server and client in one binary.
package main

import (
	"os"

	"github.com/pocketcontext-labs/pocketcontext-crew/internal/client"
)

func main() {
	if handled, code := runDrop(os.Args[1:]); handled {
		os.Exit(code)
	}
	os.Exit(client.Run(os.Args[1:]))
}
