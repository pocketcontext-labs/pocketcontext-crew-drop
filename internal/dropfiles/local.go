package dropfiles

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
)

// localObjects is used by the supervisor's explicitly local replica tests. It
// exercises the same content-addressed writes and never contacts AWS.
type localObjects string

func (s localObjects) Put(ctx context.Context, digest string, size int64, body io.ReadSeeker) error {
	if _, err := digestBytes(digest); err != nil {
		return err
	}
	if err := os.MkdirAll(string(s), 0700); err != nil {
		return err
	}
	staging, err := os.CreateTemp(string(s), ".incoming-")
	if err != nil {
		return err
	}
	defer os.Remove(staging.Name())
	err = Verify(staging, body, digest, size)
	if err == nil {
		err = staging.Sync()
	}
	if err = errors.Join(err, staging.Close(), ctx.Err()); err != nil {
		return err
	}
	err = os.Link(staging.Name(), filepath.Join(string(s), digest))
	if !errors.Is(err, os.ErrExist) {
		return err
	}
	reader, err := s.Get(ctx, digest)
	if err != nil {
		return err
	}
	defer reader.Close()
	return Verify(io.Discard, reader, digest, size)
}

func (s localObjects) Get(ctx context.Context, digest string) (io.ReadCloser, error) {
	if _, err := digestBytes(digest); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return os.Open(filepath.Join(string(s), digest))
}
