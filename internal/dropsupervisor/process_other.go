//go:build !linux

package dropsupervisor

import (
	"context"
	"errors"
	"os"
	"os/exec"
)

func lockVolume(string) (*os.File, error) { return nil, errors.New("CrewDrop requires Linux") }
func configureChild(*exec.Cmd)            {}

func freezeWriter(context.Context, *os.Process) error { return errors.New("CrewDrop requires Linux") }
func resumeWriter(*os.Process) error                  { return errors.New("CrewDrop requires Linux") }

func syncVolume(*os.File) error { return errors.New("CrewDrop requires Linux") }
