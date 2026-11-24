// Package dropsupervisor runs the two-listener Crew process used in a Drop guest.
package dropsupervisor

import (
	"compress/gzip"
	"context"
	"crypto/sha256"
	"database/sql"
	"embed"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"

	_ "github.com/mattn/go-sqlite3"
)

// Release builds stage the pinned binary here. The license also lets ordinary
// upstream Crew builds and tests compile without fetching the guest payload.
//
//go:embed assets/*
var packaged embed.FS

const litestreamDigest = "f2d452c1527be81f06d5f135655b81b0e8d9a338cb4c2825cf2243134bc2dbd6"

type replica struct {
	bin, binDir, cfg, socket, live, project, app, url, region string
}

func (r *replica) prepare() error {
	if r.bin != "" {
		return nil
	}
	if runtime.GOOS != "linux" || runtime.GOARCH != "amd64" {
		return errors.New("CrewDrop requires a Linux amd64 guest")
	}
	p := filepath.Join(r.binDir, "litestream-0.5.18")
	if err := unpackLitestream(p); err != nil {
		return err
	}
	config := fmt.Sprintf("levels:\n  - interval: 4h\nl0-retention-check-interval: 4h\nsocket:\n  enabled: true\n  path: %q\ndbs:\n  - path: %q\n    replica:\n      url: %q\n      sync-interval: 1s\n", r.socket, r.live, r.url)
	if strings.HasPrefix(r.url, "s3://") {
		config += fmt.Sprintf("      region: %q\n", r.region)
	}
	if err := os.WriteFile(r.cfg, []byte(config), 0600); err != nil {
		return err
	}
	r.bin = p
	return nil
}

// Reuse a verified local copy after restart. Publish a new executable atomically
// so a cancelled or interrupted unpack cannot leave an executable partial file.
func unpackLitestream(p string) error {
	if st, err := os.Lstat(p); err == nil && st.Mode().IsRegular() && st.Mode().Perm() == 0700 {
		if f, err := os.Open(p); err == nil {
			h := sha256.New()
			_, err = io.Copy(h, f)
			f.Close()
			if err == nil && fmt.Sprintf("%x", h.Sum(nil)) == litestreamDigest {
				return nil
			}
		}
	}
	compressed, err := packaged.Open("assets/litestream-linux-amd64.gz")
	if err != nil {
		return errors.New("release was built without the pinned Litestream payload")
	}
	defer compressed.Close()
	raw, err := gzip.NewReader(compressed)
	if err != nil {
		return err
	}
	defer raw.Close()
	f, err := os.CreateTemp(filepath.Dir(p), ".litestream-")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	h := sha256.New()
	_, err = io.Copy(io.MultiWriter(f, h), raw)
	if closeErr := f.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	if fmt.Sprintf("%x", h.Sum(nil)) != litestreamDigest {
		return errors.New("embedded Litestream checksum mismatch")
	}
	if err := os.Chmod(f.Name(), 0700); err != nil {
		return err
	}
	return os.Rename(f.Name(), p)
}

func (r *replica) command(ctx context.Context, args ...string) *exec.Cmd {
	cmd := exec.CommandContext(ctx, r.bin, args...)
	configureChild(cmd)
	cmd.Env = append(os.Environ(), "AWS_EC2_METADATA_DISABLED=true", "AWS_SHARED_CREDENTIALS_FILE="+os.DevNull, "AWS_CONFIG_FILE="+os.DevNull)
	return cmd
}

func (r *replica) sync(ctx context.Context) error {
	return r.command(ctx, "sync", "-wait", "-socket", r.socket, "-timeout", "30", r.live).Run()
}

func verifyIdentity(path, app string) error {
	u := url.URL{Scheme: "file", Path: path}
	q := u.Query()
	q.Set("mode", "ro")
	u.RawQuery = q.Encode()
	db, err := sql.Open("sqlite3", u.String())
	if err != nil {
		return err
	}
	defer db.Close()
	var id string
	var n int
	if err := db.QueryRow("SELECT count(*), coalesce(min(app_id),'') FROM _crew_drop_identity").Scan(&n, &id); err != nil {
		return fmt.Errorf("read database identity: %w", err)
	}
	if n != 1 || id != app {
		return errors.New("database belongs to a different app")
	}
	return nil
}

// Litestream validates the entire restored database before publication. The
// existing project snapshot is untouched on a failed restore or identity check.
func (r *replica) restore(ctx context.Context) error {
	dir, err := os.MkdirTemp(filepath.Dir(r.project), ".restore-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)
	tmp := filepath.Join(dir, "crew.db")
	cmd := r.command(ctx, "restore", "-config", r.cfg, "-integrity-check", "full", "-o", tmp, r.live)
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("Litestream could not restore the app replica: %w", err)
	}
	if err := verifyIdentity(tmp, r.app); err != nil {
		return err
	}
	if err := syncFile(tmp); err != nil {
		return err
	}
	if err := os.Rename(tmp, r.project); err != nil {
		return err
	}
	return syncFile(filepath.Dir(r.project))
}

func syncFile(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}
