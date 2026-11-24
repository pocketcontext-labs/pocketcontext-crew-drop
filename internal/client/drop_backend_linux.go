//go:build linux && crew_drop

package client

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/netip"
	"os"
	"path/filepath"
	"regexp"
	"time"

	"ganigeorgiev.com/pocketbase/dbx"
	"ganigeorgiev.com/pocketbase/pocketbase/core"
	"github.com/pocketcontext-labs/pocketcontext-core/appserver"
	"github.com/pocketcontext-labs/pocketcontext-core/sqliteuri"
	"github.com/pocketcontext-labs/pocketcontext-crew/internal/appconfig"
	"github.com/pocketcontext-labs/pocketcontext-crew/internal/crewruntime"
)

var dropAppID = regexp.MustCompile(`^d-[a-z0-9]{15}$`)

// ServeDropBackend is the loopback hosted server owned by the guest supervisor.
// Core exclusively opens the private working database. SQLite and its WAL stay together.
func ServeDropBackend(ctx context.Context, mode string) error {
	if mode != "local" && mode != "new" && mode != "recovered" {
		return errors.New("invalid Drop backend boot mode")
	}
	appID := os.Getenv("CREW_DROP_APP_ID")
	if !dropAppID.MatchString(appID) {
		return errors.New("invalid CREW_DROP_APP_ID")
	}
	root := os.Getenv("NIBRUN_DATA_DIR")
	if root == "" {
		root = "/app/data"
	}
	if !filepath.IsAbs(root) {
		return errors.New("NIBRUN_DATA_DIR must be absolute")
	}
	addr := os.Getenv("CREW_DROP_BACKEND_ADDR")
	if addr == "" {
		addr = "127.0.0.1:18080"
	}
	ap, err := netip.ParseAddrPort(addr)
	if err != nil || !ap.Addr().IsLoopback() || ap.Port() == 0 {
		return errors.New("CREW_DROP_BACKEND_ADDR must be a loopback IP and port")
	}

	hooks := crewruntime.Hooks()
	onStart := hooks.OnServeStart
	hooks.OnServeStart = func(e *core.ServeEvent, p *appserver.Project) error {
		if err := bindDropDatabase(e.App, appID, mode == "new"); err != nil {
			return err
		}
		return onStart(e, p)
	}
	rt, err := appconfig.NewRuntime(hooks)
	if err != nil {
		return err
	}
	projectDir := filepath.Join(root, "crew", "project")
	if err := os.MkdirAll(projectDir, 0700); err != nil {
		return err
	}
	// Guest init may launch the tenant without a login home. Keep Core discovery
	// and any library cache inside the private guest volume.
	if err := os.Setenv("XDG_CACHE_HOME", filepath.Join(root, "crew", "private")); err != nil {
		return err
	}
	p, err := rt.Resolve(filepath.Join(projectDir, "crew.db"))
	if err != nil {
		return err
	}
	p.Cache = filepath.Join(root, "crew", "private")
	if p.Config.Instance != "" && p.Config.Instance != appID {
		return errors.New("Drop volume belongs to another application")
	}
	p.Config.Instance = appID
	// The backend can be invoked independently. Serialize all pre-Core inspection and
	// snapshot recovery as well as the serving lifetime.
	if err := os.MkdirAll(p.Cache, 0700); err != nil {
		return err
	}
	unlock, err := lockDropVolume(p.Cache)
	if err != nil {
		return err
	}
	defer unlock()
	init, err := prepareDropBoot(p, appID, mode)
	if err != nil {
		return err
	}
	proxy := &appserver.HostedProxy{Header: appserver.HostedProxyXForwardedFor, Networks: []netip.Prefix{netip.PrefixFrom(ap.Addr(), ap.Addr().BitLen())}}
	return appserver.ServeContext(ctx, p, appserver.ServeOptions{
		Hosted: true, Headless: true, MCP: true, MCPHandler: newMCPHandler,
		HTTPSDomain: os.Getenv("POCKETCONTEXT_DOMAIN"), HostedHTTPAddress: addr, HostedProxy: proxy,
		HostedInit: init, HostedRestore: mode == "recovered",
	})
}

func bindDropDatabase(app core.App, appID string, initialize bool) error {
	return app.RunInTransaction(func(tx core.App) error {
		if initialize {
			if _, err := tx.DB().NewQuery("CREATE TABLE IF NOT EXISTS _crew_drop_identity (app_id TEXT PRIMARY KEY NOT NULL)").Execute(); err != nil {
				return err
			}
			// A transaction either seeds an empty binding or checks the existing one.
			if _, err := tx.DB().NewQuery("INSERT INTO _crew_drop_identity (app_id) SELECT {:id} WHERE NOT EXISTS (SELECT 1 FROM _crew_drop_identity)").Bind(dbx.Params{"id": appID}).Execute(); err != nil {
				return err
			}
		}
		var ids []string
		if err := tx.DB().NewQuery("SELECT app_id FROM _crew_drop_identity").Column(&ids); err != nil {
			return fmt.Errorf("read Drop database identity: %w", err)
		}
		if len(ids) != 1 || ids[0] != appID {
			return errors.New("unexpected Drop database identity")
		}
		return nil
	})
}

func prepareDropBoot(p *appserver.Project, appID, mode string) (bool, error) {
	store := appserver.NewStore(p)
	live, err := regularDropFile(store.LivePath())
	if err != nil {
		return false, err
	}
	snapshot, err := regularDropFile(p.DB)
	if err != nil {
		return false, err
	}
	if !live {
		if mode == "local" || (mode == "new" && snapshot) || (mode == "recovered" && !snapshot) {
			return false, errors.New("unexpected Drop project layout for boot mode")
		}
		return mode == "new", nil
	}

	// Read the WAL too. An immutable SQLite URI would hide acknowledged WAL commits.
	u := sqliteuri.File(store.LivePath())
	u.RawQuery = "mode=ro&_busy_timeout=5000"
	db, err := sql.Open("sqlite3", u.String())
	if err != nil {
		return false, err
	}
	defer db.Close()
	var identities, users int
	if err := db.QueryRow("SELECT count(*) FROM sqlite_master WHERE type='table' AND name='_crew_drop_identity'").Scan(&identities); err != nil {
		return false, err
	}
	if identities > 0 {
		var count, matching int
		if err := db.QueryRow("SELECT count(*),count(CASE WHEN app_id=? THEN 1 END) FROM _crew_drop_identity", appID).Scan(&count, &matching); err != nil {
			return false, err
		}
		if count != 1 || matching != 1 {
			return false, errors.New("local SQLite identity differs from Drop placement")
		}
	} else if mode != "new" {
		return false, errors.New("local SQLite database is missing its Drop identity")
	}
	if mode == "new" {
		if err := db.QueryRow("SELECT count(*) FROM sqlite_master WHERE type='table' AND name='users'").Scan(&users); err != nil {
			return false, err
		}
		if users > 0 {
			if err := db.QueryRow("SELECT count(*) FROM users").Scan(&users); err != nil {
				return false, err
			}
		}
	}
	if err := db.Close(); err != nil {
		return false, err
	}
	if mode == "new" && users == 0 {
		// Only the supervisor's durable pending-new claim permits retrying unfinished initialization.
		if snapshot {
			if err := os.Rename(p.DB, p.DB+".interrupted-init"); err != nil {
				return false, err
			}
			store.Changed()
		}
		return true, nil
	}
	if !snapshot {
		// The process can die after accepting a write but before Core's first
		// periodic write-back. Rebuild the absent snapshot from the live WAL
		// using the same atomic write-back path used by a graceful shutdown.
		store.Changed()
		if err := store.Close(); err != nil {
			return false, fmt.Errorf("materialize private working copy: %w", err)
		}
	}
	if snapshot {
		matches, err := matchesCoreSnapshotState(p)
		if err != nil {
			return false, err
		}
		if !matches {
			// Drop exclusively owns this volume and has just checked the live app ID.
			// Core may have been killed between the snapshot rename and state.json.
			// Preserve the unmatched snapshot and use Core's keep-local recovery.
			store.Changed()
			if err := store.Recover(true); err != nil {
				return false, fmt.Errorf("reconcile project snapshot with live SQLite: %w", err)
			}
		}
	}
	return false, nil
}

// Core v0.3.0 also uses size and mtime to identify its last atomic write-back.
// Reading this small file avoids hashing or copying a healthy database on restart.
func matchesCoreSnapshotState(p *appserver.Project) (bool, error) {
	var state struct {
		Base *struct {
			Size  int64     `json:"size"`
			Mtime time.Time `json:"mtime"`
		} `json:"base"`
	}
	b, err := os.ReadFile(filepath.Join(p.LiveDir(), "state.json"))
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if err := json.Unmarshal(b, &state); err != nil {
		return false, fmt.Errorf("read Core working-copy state: %w", err)
	}
	if state.Base == nil {
		return false, nil
	}
	info, err := os.Stat(p.DB)
	if err != nil {
		return false, err
	}
	return info.Size() == state.Base.Size && info.ModTime().Equal(state.Base.Mtime), nil
}

func regularDropFile(path string) (bool, error) {
	st, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if !st.Mode().IsRegular() {
		return false, fmt.Errorf("expected regular database file at %s", path)
	}
	return true, nil
}
