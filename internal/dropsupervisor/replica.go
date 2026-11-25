// Package dropsupervisor runs the two-listener Crew process used in a Drop guest.
package dropsupervisor

import (
	"compress/gzip"
	"context"
	"crypto/sha256"
	"database/sql"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

// Release builds stage the pinned binary here. The license also lets ordinary
// upstream Crew builds and tests compile without fetching the guest payload.
//
//go:embed assets/*
var packaged embed.FS

const litestreamDigest = "f2d452c1527be81f06d5f135655b81b0e8d9a338cb4c2825cf2243134bc2dbd6"

// Replication cost settings. S3 PUTs scale with write-active seconds divided
// by the sync interval; restore egress scales with the snapshot plus the
// compacted history after it.
const (
	defaultSyncInterval = 10 * time.Second
	minSyncInterval     = time.Second
	maxSyncInterval     = 300 * time.Second
)

// replicaSyncInterval reads DROP_REPLICA_SYNC_INTERVAL. An invalid value is
// logged and replaced by the default so the guest still opens its port.
func replicaSyncInterval() time.Duration {
	raw := os.Getenv("DROP_REPLICA_SYNC_INTERVAL")
	if raw == "" {
		return defaultSyncInterval
	}
	d, err := time.ParseDuration(raw)
	if err != nil || d < minSyncInterval || d > maxSyncInterval {
		log.Printf("CrewDrop ignored DROP_REPLICA_SYNC_INTERVAL=%q: use a Go duration from %s to %s; using %s", raw, minSyncInterval, maxSyncInterval, defaultSyncInterval)
		return defaultSyncInterval
	}
	return d
}

type replica struct {
	bin, binDir, cfg, socket, live, project, app, url, region string
	syncInterval                                              time.Duration
}

// litestreamConfig renders the Litestream 0.5.18 configuration. Keys follow
// cmd/litestream/main.go at v0.5.18: levels are L1..Ln compaction intervals,
// the top-level snapshot block sets the L9 snapshot interval and retention.
// A 5-minute L1 keeps restores from replaying hours of per-sync L0 files and
// the daily snapshot bounds restore size; 72 hours of snapshots keep three
// days of point-in-time history.
func (r *replica) litestreamConfig() string {
	interval := r.syncInterval
	if interval == 0 {
		interval = defaultSyncInterval
	}
	config := fmt.Sprintf("levels:\n  - interval: 5m\n  - interval: 4h\nsnapshot:\n  interval: 24h\n  retention: 72h\nl0-retention-check-interval: 4h\nsocket:\n  enabled: true\n  path: %q\ndbs:\n  - path: %q\n    replica:\n      url: %q\n      sync-interval: %s\n", r.socket, r.live, r.url, interval)
	if strings.HasPrefix(r.url, "s3://") {
		config += fmt.Sprintf("      region: %q\n", r.region)
	}
	return config
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
	if err := os.WriteFile(r.cfg, []byte(r.litestreamConfig()), 0600); err != nil {
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

// syncResult is Litestream's confirmation of a waited sync.
type syncResult struct {
	TXID        uint64  `json:"txid"`
	ReplicaTXID *uint64 `json:"replica_txid"`
}

// sync asks the running Litestream, over its local control socket, to copy
// the live WAL to the replica and waits for the upload. It succeeds only when
// Litestream reports a replica position at or beyond the local position. The
// client timeout ends before the caller's deadline so a hook still answers.
func (r *replica) sync(ctx context.Context) (syncResult, error) {
	seconds := 30
	if deadline, ok := ctx.Deadline(); ok {
		seconds = max(1, int(time.Until(deadline)/time.Second))
	}
	out, err := r.command(ctx, "sync", "-wait", "-json", "-socket", r.socket, "-timeout", strconv.Itoa(seconds), r.live).Output()
	var result syncResult
	if err != nil {
		var exit *exec.ExitError
		if errors.As(err, &exit) && len(exit.Stderr) > 0 {
			msg := strings.TrimSpace(string(exit.Stderr))
			if len(msg) > 200 {
				msg = msg[len(msg)-200:]
			}
			return result, fmt.Errorf("litestream sync: %w: %s", err, msg)
		}
		return result, fmt.Errorf("litestream sync: %w", err)
	}
	if err := json.Unmarshal(out, &result); err != nil {
		return result, fmt.Errorf("litestream sync: unreadable confirmation: %w", err)
	}
	if result.ReplicaTXID == nil || *result.ReplicaTXID < result.TXID {
		return result, errors.New("litestream sync: replica position behind the local database")
	}
	return result, nil
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
