// Package workspacecopy implements owner-selected recovery copies for a Drop guest.
// Copy metadata travels with SQLite replication. Copy bytes are immutable and
// are also kept on the local volume for recovery without a funded object store.
package workspacecopy

import (
	"archive/zip"
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	"ganigeorgiev.com/pocketbase/dbx"
	"ganigeorgiev.com/pocketbase/pocketbase/apis"
	"ganigeorgiev.com/pocketbase/pocketbase/core"
	_ "github.com/mattn/go-sqlite3"
	"github.com/pocketcontext-labs/pocketcontext-core/sqliteuri"
)

type Objects interface {
	Put(context.Context, string, int64, io.ReadSeeker) error
	Get(context.Context, string) (io.ReadCloser, error)
}
type File struct {
	Path, SHA256 string
	Size         int64
}
type Config struct {
	Root, AppID, IdentityTable string
	Objects                    Objects
	Confirm                    func(context.Context) error
	Files                      func(context.Context, *sql.DB) ([]File, error)
	Materialize                func(context.Context, File) error
}
type copyInfo struct {
	ID        string   `json:"id"`
	Created   string   `json:"created"`
	Bytes     int64    `json:"bytes"`
	Remote    bool     `json:"remote"`
	Digest    string   `json:"digest"`
	Available bool     `json:"available,omitempty"`
	Parts     []string `json:"parts,omitempty"`
}

const catalog = "CREATE TABLE IF NOT EXISTS _drop_workspace_copies(id TEXT PRIMARY KEY, info TEXT NOT NULL)"

var copyID = regexp.MustCompile("^[0-9a-f]{32}$")
var digestID = regexp.MustCompile("^[0-9a-f]{64}$")

const chunkSize = 4 << 20

var ErrPending = errors.New("a workspace restore is already pending")

func openCopy(p string, readOnly bool) (*sql.DB, error) {
	u := sqliteuri.File(p)
	if readOnly {
		u.RawQuery = "mode=ro&immutable=1"
	}
	return sql.Open("sqlite3", u.String())
}
func verifiedFile(dst io.Writer, src io.Reader, size int64, digest string) error {
	if size < 0 || !digestID.MatchString(digest) {
		return errors.New("invalid file description")
	}
	h := sha256.New()
	n, err := io.CopyN(io.MultiWriter(dst, h), src, size)
	if err != nil {
		return err
	}
	var extra [1]byte
	count, e := io.ReadFull(src, extra[:])
	if n != size || count != 0 || e != io.EOF || hex.EncodeToString(h.Sum(nil)) != digest {
		return errors.New("copy size or digest mismatch")
	}
	return nil
}
func checkedPath(key string) bool {
	return key != "" && key != "." && filepath.IsLocal(key) && path.Clean(key) == key && !strings.Contains(key, "\\")
}
func synced(p string) error {
	f, err := os.Open(p)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}
func zipFile(z *zip.Writer, name, source string, expected *File) error {
	f, err := os.Open(source)
	if err != nil {
		return err
	}
	defer f.Close()
	entry, err := z.CreateHeader(&zip.FileHeader{Name: name, Method: zip.Store})
	if err != nil {
		return err
	}
	if expected != nil {
		return verifiedFile(entry, f, expected.Size, expected.SHA256)
	}
	_, err = io.Copy(entry, f)
	return err
}
func (c Config) dbFiles(ctx context.Context, p string) ([]File, error) {
	db, err := openCopy(p, true)
	if err != nil {
		return nil, err
	}
	defer db.Close()
	var check string
	if err = db.QueryRowContext(ctx, "PRAGMA integrity_check").Scan(&check); err != nil || check != "ok" {
		return nil, errors.New("copy failed SQLite integrity verification")
	}
	if c.IdentityTable != "_crew_drop_identity" && c.IdentityTable != "_atlas_drop_identity" {
		return nil, errors.New("unknown workspace identity table")
	}
	var count int
	var appID string
	if err = db.QueryRowContext(ctx, "SELECT count(*),coalesce(min(app_id),'') FROM "+c.IdentityTable).Scan(&count, &appID); err != nil || count != 1 || appID != c.AppID {
		return nil, errors.New("recovery copy belongs to another workspace")
	}
	return c.Files(ctx, db)
}
func (c Config) create(ctx context.Context, app core.App, id string) (copyInfo, error) {
	info := copyInfo{ID: id, Created: time.Now().UTC().Format(time.RFC3339Nano), Remote: c.Objects != nil}
	dir := filepath.Join(c.Root, "copies")
	if err := os.MkdirAll(dir, 0700); err != nil {
		return info, err
	}
	tmp, err := os.MkdirTemp(dir, ".create-")
	if err != nil {
		return info, err
	}
	defer os.RemoveAll(tmp)
	database := filepath.Join(tmp, "data.db")
	if _, err = app.ConcurrentDB().NewQuery("VACUUM INTO {:path}").Bind(dbx.Params{"path": database}).WithContext(ctx).Execute(); err != nil {
		return info, err
	}
	files, err := c.dbFiles(ctx, database)
	if err != nil {
		return info, err
	}
	archive := filepath.Join(tmp, "copy.zip")
	out, err := os.Create(archive)
	if err != nil {
		return info, err
	}
	defer out.Close()
	z := zip.NewWriter(out)
	if err = zipFile(z, "data.db", database, nil); err != nil {
		return info, err
	}
	for _, file := range files {
		if !checkedPath(file.Path) {
			return info, errors.New("invalid storage key in database")
		}
		if c.Materialize != nil {
			if err = c.Materialize(ctx, file); err != nil {
				return info, err
			}
		}
		if err = zipFile(z, "storage/"+file.Path, filepath.Join(app.DataDir(), "storage", file.Path), &file); err != nil {
			return info, err
		}
	}
	if err = z.Close(); err != nil {
		return info, err
	}
	if err = out.Sync(); err != nil {
		return info, err
	}
	if _, err = out.Seek(0, 0); err != nil {
		return info, err
	}
	h := sha256.New()
	buf := make([]byte, chunkSize)
	for {
		n, readErr := io.ReadFull(out, buf)
		if readErr != nil && readErr != io.EOF && readErr != io.ErrUnexpectedEOF {
			return info, readErr
		}
		if n > 0 {
			h.Write(buf[:n])
			info.Bytes += int64(n)
			if c.Objects != nil {
				digest := fmt.Sprintf("%x", sha256.Sum256(buf[:n]))
				if err = c.Objects.Put(ctx, digest, int64(n), bytes.NewReader(buf[:n])); err != nil {
					return info, err
				}
				info.Parts = append(info.Parts, digest)
			}
		}
		if readErr != nil {
			break
		}
	}
	info.Digest = hex.EncodeToString(h.Sum(nil))
	if err = os.Rename(archive, filepath.Join(dir, id+".zip")); err != nil {
		return info, err
	}
	if err = synced(dir); err != nil {
		return info, err
	}
	encoded, _ := json.Marshal(info)
	_, err = app.DB().NewQuery("INSERT INTO _drop_workspace_copies(id,info) VALUES ({:id},{:info})").Bind(dbx.Params{"id": id, "info": string(encoded)}).Execute()
	return info, err
}
func (c Config) archive(ctx context.Context, info copyInfo, dest string) error {
	if !copyID.MatchString(info.ID) || info.Bytes <= 0 {
		return errors.New("invalid recovery copy")
	}
	out, err := os.OpenFile(dest, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
	if err != nil {
		return err
	}
	defer out.Close()
	local, err := os.Open(filepath.Join(c.Root, "copies", info.ID+".zip"))
	if err == nil {
		err = verifiedFile(out, local, info.Bytes, info.Digest)
		local.Close()
		if err == nil {
			return out.Sync()
		}
	}
	if !info.Remote || c.Objects == nil {
		return errors.New("verified copy unavailable on this volume; remote access requires funded storage")
	}
	if int64(len(info.Parts)) != (info.Bytes-1)/chunkSize+1 {
		return errors.New("invalid recovery copy part count")
	}
	if err = out.Truncate(0); err != nil {
		return err
	}
	if _, err = out.Seek(0, 0); err != nil {
		return err
	}
	h := sha256.New()
	remaining := info.Bytes
	for _, part := range info.Parts {
		r, e := c.Objects.Get(ctx, part)
		if e != nil {
			return e
		}
		size := min(int64(chunkSize), remaining)
		e = verifiedFile(io.MultiWriter(out, h), r, size, part)
		r.Close()
		if e != nil {
			return e
		}
		remaining -= size
	}
	if hex.EncodeToString(h.Sum(nil)) != info.Digest {
		return errors.New("recovery copy digest mismatch")
	}
	return out.Sync()
}

type metadataRow struct {
	Info string `db:"info"`
}

func list(app core.App) ([]copyInfo, error) {
	var rows []metadataRow
	if err := app.DB().NewQuery("SELECT info FROM _drop_workspace_copies ORDER BY json_extract(info,'$.created') DESC").All(&rows); err != nil {
		return nil, err
	}
	result := make([]copyInfo, 0, len(rows))
	for _, row := range rows {
		var info copyInfo
		if err := json.Unmarshal([]byte(row.Info), &info); err != nil {
			return nil, err
		}
		result = append(result, info)
	}
	return result, nil
}

// Stage validates the complete archive before making the restore request visible
// to the supervisor. Immutable originals can be installed before switching DBs.
func (c Config) stage(ctx context.Context, app core.App, info copyInfo, copies []copyInfo) error {
	tmp, err := os.MkdirTemp(c.Root, ".restore-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(tmp)
	source := filepath.Join(tmp, "copy.zip")
	if err = c.archive(ctx, info, source); err != nil {
		return err
	}
	z, err := zip.OpenReader(source)
	if err != nil {
		return err
	}
	defer z.Close()
	entries := map[string]*zip.File{}
	for _, item := range z.File {
		if !checkedPath(item.Name) || entries[item.Name] != nil {
			return errors.New("invalid copy entry")
		}
		entries[item.Name] = item
	}
	dbEntry := entries["data.db"]
	if dbEntry == nil {
		return errors.New("copy is missing its database")
	}
	dbPath := filepath.Join(tmp, "data.db")
	r, err := dbEntry.Open()
	if err != nil {
		return err
	}
	f, err := os.OpenFile(dbPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		r.Close()
		return err
	}
	_, err = io.Copy(f, r)
	err = errors.Join(err, r.Close(), f.Close())
	if err != nil {
		return err
	}
	files, err := c.dbFiles(ctx, dbPath)
	if err != nil {
		return err
	}
	if len(files)+1 != len(entries) {
		return errors.New("attachment inventory differs from archive")
	}
	for _, file := range files {
		entry := entries["storage/"+file.Path]
		if !checkedPath(file.Path) || entry == nil {
			return errors.New("missing attachment in archive")
		}
		dest := filepath.Join(app.DataDir(), "storage", file.Path)
		dir := filepath.Dir(dest)
		if err = os.MkdirAll(dir, 0700); err != nil {
			return err
		}
		if err = installOriginal(entry, file, dir, dest); err != nil {
			return err
		}
	}
	db, err := openCopy(dbPath, false)
	if err != nil {
		return err
	}
	// Preserve copy selection history when rolling the workspace back.
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		db.Close()
		return err
	}
	defer db.Close()
	defer tx.Rollback()
	for _, item := range copies {
		raw, _ := json.Marshal(item)
		if _, err = tx.Exec("INSERT OR REPLACE INTO _drop_workspace_copies(id,info) VALUES (?,?)", item.ID, string(raw)); err != nil {
			return err
		}
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	if _, err = db.Exec("PRAGMA journal_mode=DELETE"); err != nil {
		return err
	}
	if err = db.Close(); err != nil {
		return err
	}
	if err = synced(dbPath); err != nil {
		return err
	}
	if err = os.Remove(source); err != nil {
		return err
	}
	if err = os.WriteFile(filepath.Join(tmp, "id"), []byte(info.ID), 0600); err != nil {
		return err
	}
	if err = synced(filepath.Join(tmp, "id")); err != nil {
		return err
	}
	if err = synced(tmp); err != nil {
		return err
	}
	if err = os.Rename(tmp, filepath.Join(c.Root, "owner-restore")); err != nil {
		return err
	}
	return synced(c.Root)
}
func installOriginal(entry *zip.File, file File, dir, dest string) error {
	src, err := entry.Open()
	if err != nil {
		return err
	}
	defer src.Close()
	out, err := os.CreateTemp(dir, ".original-")
	if err != nil {
		return err
	}
	defer os.Remove(out.Name())
	err = verifiedFile(out, src, file.Size, file.SHA256)
	if err == nil {
		err = out.Sync()
	}
	err = errors.Join(err, out.Close())
	if err != nil {
		return err
	}
	if err = os.Rename(out.Name(), dest); err != nil {
		return err
	}
	return synced(dir)
}

type Request struct {
	ID      string `json:"id"`
	Confirm string `json:"confirm"`
}

// Register belongs only to the single-owner Drop backend. The app public policy
// allows these exact paths for the reader; the MCP route policy omits them.
func Register(e *core.ServeEvent, c Config) error {
	if c.AppID == "" || !filepath.IsAbs(c.Root) || c.Files == nil {
		return errors.New("invalid workspace copy configuration")
	}
	if _, err := e.App.DB().NewQuery(catalog).Execute(); err != nil {
		return err
	}
	var mu sync.Mutex
	const base = "/api/workspace/copies"
	require := apis.RequireAuth("users")
	e.Router.GET(base, func(re *core.RequestEvent) error {
		re.Response.Header().Set("Cache-Control", "no-store")
		items, err := list(re.App)
		if err != nil {
			return err
		}
		for i := range items {
			items[i].Parts = nil
			local, err := os.Stat(filepath.Join(c.Root, "copies", items[i].ID+".zip"))
			items[i].Available = (err == nil && local.Mode().IsRegular()) || (items[i].Remote && c.Objects != nil)
		}
		return re.JSON(200, map[string]any{"copies": items, "funded": c.Objects != nil})
	}).Bind(require)
	for _, op := range []string{"create", "restore"} {
		route := base
		if op == "restore" {
			route += "/restore"
		}
		e.Router.POST(route, func(re *core.RequestEvent) error {
			if !mu.TryLock() {
				return re.Error(409, "Another copy operation is in progress", nil)
			}
			defer mu.Unlock()
			var q Request
			if re.BindBody(&q) != nil || !copyID.MatchString(q.ID) {
				return re.BadRequestError("Invalid copy ID", nil)
			}
			if _, err := os.Stat(filepath.Join(c.Root, "owner-restore")); err == nil {
				return re.Error(409, ErrPending.Error(), nil)
			} else if !errors.Is(err, os.ErrNotExist) {
				return err
			}
			items, err := list(re.App)
			if err != nil {
				return err
			}
			var selected *copyInfo
			for i := range items {
				if q.ID == items[i].ID {
					selected = &items[i]
					break
				}
			}
			if op == "restore" {
				if q.Confirm != q.ID {
					return re.BadRequestError("Confirm the selected copy before replacing this workspace", nil)
				}
				if selected == nil {
					return re.NotFoundError("Copy unavailable", nil)
				}
				if err = c.stage(re.Request.Context(), re.App, *selected, items); err != nil {
					return re.Error(503, "Could not verify and stage this copy; the current workspace is still selected", err)
				}
				return re.JSON(202, map[string]string{"state": "restoring"})
			}
			if selected == nil {
				info, err := c.create(re.Request.Context(), re.App, q.ID)
				if err != nil {
					return re.Error(503, "Copy did not finish. Check storage availability and retry.", err)
				}
				selected = &info
			}
			if selected.Remote && c.Confirm != nil {
				if err = c.Confirm(re.Request.Context()); err != nil {
					return re.Error(503, "Copy saved; replication confirmation is pending. Retry the same operation.", err)
				}
			}
			result := *selected
			result.Parts = nil
			return re.JSON(200, result)
		}).Bind(require)
	}
	return nil
}
