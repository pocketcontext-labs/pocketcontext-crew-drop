//go:build linux && crew_drop

package client

import (
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	"github.com/pocketcontext-labs/pocketcontext-core/appserver"
	"github.com/pocketcontext-labs/pocketcontext-core/sqliteuri"
	"github.com/pocketcontext-labs/pocketcontext-crew/internal/appconfig"
)

func TestDropBootUsesCommittedWALBeforeFirstProjectSnapshot(t *testing.T) {
	const appID = "d-000000000000001"
	p, err := appconfig.Runtime.Resolve(filepath.Join(t.TempDir(), "crew.db"))
	if err != nil {
		t.Fatal(err)
	}
	p.Cache = t.TempDir()
	if err := os.MkdirAll(p.LiveDir(), 0700); err != nil {
		t.Fatal(err)
	}
	u := sqliteuri.File(appserver.NewStore(p).LivePath())
	writer, err := sql.Open("sqlite3", u.String())
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()
	writer.SetMaxOpenConns(1)
	if _, err := writer.Exec("PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0; CREATE TABLE _crew_drop_identity(app_id TEXT PRIMARY KEY); INSERT INTO _crew_drop_identity VALUES (?); CREATE TABLE synthetic_note(body TEXT); INSERT INTO synthetic_note VALUES ('acknowledged in WAL')", appID); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(appserver.NewStore(p).LivePath() + "-wal"); err != nil {
		t.Fatal(err)
	}

	if _, err := prepareDropBoot(p, "d-000000000000002", "local"); err == nil {
		t.Fatal("accepted wrong application")
	}
	if _, err := os.Stat(p.DB); !os.IsNotExist(err) {
		t.Fatalf("wrong identity changed project snapshot: %v", err)
	}
	initialize, err := prepareDropBoot(p, appID, "local")
	if err != nil || initialize {
		t.Fatalf("existing local boot: init=%v err=%v", initialize, err)
	}
	// Inspect a copy to preserve the project's sync-safe contract.
	target := filepath.Join(t.TempDir(), "private-verifier.db")
	data, err := os.ReadFile(p.DB)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(target, data, 0600); err != nil {
		t.Fatal(err)
	}
	readURI := sqliteuri.File(target)
	readURI.RawQuery = "mode=ro"
	replica, err := sql.Open("sqlite3", readURI.String())
	if err != nil {
		t.Fatal(err)
	}
	defer replica.Close()
	var body string
	if err := replica.QueryRow("SELECT body FROM synthetic_note").Scan(&body); err != nil || body != "acknowledged in WAL" {
		t.Fatalf("snapshot content = %q, err=%v", body, err)
	}
}

func TestDropBootRequiresExplicitNewOrRecoveredInput(t *testing.T) {
	p, err := appconfig.Runtime.Resolve(filepath.Join(t.TempDir(), "crew.db"))
	if err != nil {
		t.Fatal(err)
	}
	p.Cache = t.TempDir()
	for _, tc := range []struct {
		mode             string
		initialize, fail bool
	}{{"new", true, false}, {"local", false, true}, {"recovered", false, true}} {
		initialize, err := prepareDropBoot(p, "d-000000000000001", tc.mode)
		if (err != nil) != tc.fail || initialize != tc.initialize {
			t.Fatalf("%s init=%v err=%v", tc.mode, initialize, err)
		}
	}
}

func TestDropUnmatchedSnapshotNeverDisplacesVerifiedLiveDatabase(t *testing.T) {
	const id = "d-000000000000001"
	p, err := appconfig.Runtime.Resolve(filepath.Join(t.TempDir(), "crew.db"))
	if err != nil {
		t.Fatal(err)
	}
	p.Cache = t.TempDir()
	if err := os.MkdirAll(p.LiveDir(), 0700); err != nil {
		t.Fatal(err)
	}
	store := appserver.NewStore(p)
	u := sqliteuri.File(store.LivePath())
	live, err := sql.Open("sqlite3", u.String())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := live.Exec("CREATE TABLE _crew_drop_identity(app_id TEXT PRIMARY KEY); INSERT INTO _crew_drop_identity VALUES (?); CREATE TABLE synthetic_note(body TEXT); INSERT INTO synthetic_note VALUES ('older')", id); err != nil {
		t.Fatal(err)
	}
	store.Changed()
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := live.Exec("UPDATE synthetic_note SET body='latest acknowledged'"); err != nil {
		t.Fatal(err)
	}
	if err := live.Close(); err != nil {
		t.Fatal(err)
	}
	// Simulate an interrupted write-back that replaced the snapshot while its
	// state metadata still describes the old one.
	if err := os.Remove(filepath.Join(p.LiveDir(), "state.json")); err != nil {
		t.Fatal(err)
	}
	if _, err := prepareDropBoot(p, id, "local"); err != nil {
		t.Fatal(err)
	}
	if err := appserver.NewStore(p).Open(); err != nil {
		t.Fatal(err)
	}
	check, err := sql.Open("sqlite3", u.String())
	if err != nil {
		t.Fatal(err)
	}
	defer check.Close()
	var body string
	if err := check.QueryRow("SELECT body FROM synthetic_note").Scan(&body); err != nil || body != "latest acknowledged" {
		t.Fatalf("Core opened %q, err=%v", body, err)
	}
	prior, err := filepath.Glob(p.DB + ".replaced-*.db")
	if err != nil || len(prior) != 1 {
		t.Fatalf("preserved snapshots: %v, err=%v", prior, err)
	}
}
