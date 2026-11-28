// Package workspacecopy also implements the data-only archive used by Drop.
// A restore overlays Crew records onto a snapshot of the destination database,
// so the destination owns its authentication, integrations and app identity.
package workspacecopy

import (
	"archive/zip"
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

const portableFormat = "crew-content/v1"
const MaxPlatformArchive = 90 << 20

var ErrPlatformArchiveLimit = errors.New("platform backup exceeds 90 MiB")

type limitedArchiveWriter struct {
	dst       io.Writer
	remaining int64
}

func (w *limitedArchiveWriter) Write(b []byte) (int, error) {
	if int64(len(b)) > w.remaining {
		return 0, ErrPlatformArchiveLimit
	}
	n, err := w.dst.Write(b)
	w.remaining -= int64(n)
	return n, err
}

// All names are constants, never taken from the archive. Runtime accounts,
// integrations, executable migrations, upload grants and copy catalogs stay local.
var contentTables = []string{"crew_agents", "crew_buckets", "crew_notes", "crew_note_revisions", "crew_tasks", "crew_events", "crew_reviews", "crew_artifacts", "crew_settings", "crew_session_revisions", "crew_attachments"}

type Platform struct {
	Root, Live, Project, AppID string
	Objects                    Objects
}

func (p Platform) scratch() (string, error) { return os.MkdirTemp(p.Root, ".platform-backup-") }

func sqliteSnapshot(ctx context.Context, live, dest string) error {
	db, err := openCopy(live, false)
	if err != nil {
		return err
	}
	defer db.Close()
	_, err = db.ExecContext(ctx, "VACUUM INTO ?", dest)
	return err
}

// WithArchive supplies the finished file while its temporary directory exists.
func (p Platform) WithArchive(ctx context.Context, send func(*os.File) error) error {
	dir, err := p.scratch()
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)
	data := filepath.Join(dir, "content.db")
	db, err := openCopy(data, false)
	if err != nil {
		return err
	}
	defer db.Close()
	conn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	if _, err = conn.ExecContext(ctx, "ATTACH DATABASE ? AS source", p.Live); err != nil {
		return err
	}
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var source, collection string
	if err = tx.QueryRowContext(ctx, "SELECT app_id FROM source._crew_drop_identity").Scan(&source); err != nil || source != p.AppID {
		return errors.New("source database identity mismatch")
	}
	if err = tx.QueryRowContext(ctx, "SELECT id FROM source._collections WHERE name='crew_attachments'").Scan(&collection); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, "CREATE TABLE origin(format TEXT, app TEXT);"); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, "INSERT INTO origin VALUES(?,?)", portableFormat, source); err != nil {
		return err
	}
	for _, name := range contentTables {
		if _, err = tx.ExecContext(ctx, "CREATE TABLE main."+name+" AS SELECT * FROM source."+name); err != nil {
			return err
		}
	}
	if _, err = tx.ExecContext(ctx, "UPDATE crew_artifacts SET token='', status='revoked' WHERE token<>'' OR status='published'"); err != nil {
		return err
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	if _, err = conn.ExecContext(ctx, "DETACH source"); err != nil {
		return err
	}
	// VACUUM removes previous cell contents including revoked public link tokens.
	if _, err = conn.ExecContext(ctx, "VACUUM"); err != nil {
		return err
	}
	out, err := os.CreateTemp(dir, "*.zip")
	if err != nil {
		return err
	}
	defer out.Close()
	z := zip.NewWriter(&limitedArchiveWriter{dst: out, remaining: MaxPlatformArchive})
	if err = zipFile(z, "content.db", data, nil); err != nil {
		return err
	}
	files, err := attachmentList(ctx, conn)
	if err != nil {
		return err
	}
	for _, f := range files {
		entry, err := z.CreateHeader(&zip.FileHeader{Name: "originals/" + f.Path, Method: zip.Store})
		if err != nil {
			return err
		}
		local, err := os.Open(filepath.Join(filepath.Dir(p.Project), "storage", collection, f.Path))
		var reader io.ReadCloser
		if err == nil {
			reader = local
		} else if errors.Is(err, os.ErrNotExist) && p.Objects != nil {
			reader, err = p.Objects.Get(ctx, f.SHA256)
		}
		if err != nil {
			return err
		}
		err = verifiedFile(entry, reader, f.Size, f.SHA256)
		reader.Close()
		if err != nil {
			return err
		}
	}
	if err = z.Close(); err != nil {
		return err
	}
	if _, err = out.Seek(0, 0); err != nil {
		return err
	}
	return send(out)
}

func attachmentList(ctx context.Context, conn *sql.Conn) ([]File, error) {
	rows, err := conn.QueryContext(ctx, "SELECT id,file,sha256,size FROM crew_attachments ORDER BY id")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []File
	for rows.Next() {
		var id, name string
		var f File
		if err = rows.Scan(&id, &name, &f.SHA256, &f.Size); err != nil {
			return nil, err
		}
		if !checkedPath(id) || !checkedPath(name) || strings.Contains(id+name, "/") || !digestID.MatchString(f.SHA256) || f.Size < 0 {
			return nil, errors.New("invalid attachment metadata")
		}
		f.Path = id + "/" + name
		result = append(result, f)
	}
	return result, rows.Err()
}

func platformID(job string) string { return fmt.Sprintf("%x", sha256.Sum256([]byte(job)))[:32] }

// The staging directory becomes visible to the supervisor only after all originals
// and the replacement SQLite database have passed verification.
func (p Platform) StagePlatform(ctx context.Context, job, source string, body io.Reader) error {
	dir, err := p.scratch()
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)
	input := filepath.Join(dir, "download.zip")
	f, err := os.Create(input)
	if err != nil {
		return err
	}
	n, err := io.Copy(f, io.LimitReader(body, MaxPlatformArchive+1))
	err = errors.Join(err, f.Close())
	if err != nil {
		return err
	}
	if n > MaxPlatformArchive {
		return ErrPlatformArchiveLimit
	}
	z, err := zip.OpenReader(input)
	if err != nil {
		return err
	}
	defer z.Close()
	entries := map[string]*zip.File{}
	for _, item := range z.File {
		if item.Method != zip.Store || !checkedPath(item.Name) || entries[item.Name] != nil {
			return errors.New("unexpected portable archive entry")
		}
		entries[item.Name] = item
	}
	item := entries["content.db"]
	if item == nil {
		return errors.New("missing content database")
	}
	content := filepath.Join(dir, "content.db")
	r, err := item.Open()
	if err != nil {
		return err
	}
	out, err := os.Create(content)
	if err != nil {
		r.Close()
		return err
	}
	_, err = io.Copy(out, r)
	err = errors.Join(err, r.Close(), out.Close())
	if err != nil {
		return err
	}

	target := filepath.Join(dir, "data.db")
	if err = sqliteSnapshot(ctx, p.Live, target); err != nil {
		return err
	}
	db, err := openCopy(target, false)
	if err != nil {
		return err
	}
	defer db.Close()
	conn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	if _, err = conn.ExecContext(ctx, "PRAGMA trusted_schema=OFF"); err != nil {
		return err
	}
	if _, err = conn.ExecContext(ctx, "ATTACH DATABASE ? AS incoming", content); err != nil {
		return err
	}
	// Only inert SQLite tables are accepted. Never install the incoming schema.
	var unsafe int
	if err = conn.QueryRowContext(ctx, "SELECT count(*) FROM incoming.sqlite_schema WHERE type<>'table' OR upper(sql) LIKE '%VIRTUAL TABLE%'").Scan(&unsafe); err != nil || unsafe != 0 {
		return errors.New("unexpected portable database schema")
	}
	var format, origin, collection, targetApp string
	if err = conn.QueryRowContext(ctx, "SELECT format,app FROM incoming.origin").Scan(&format, &origin); err != nil || format != portableFormat || origin != source {
		return errors.New("unexpected portable archive identity")
	}
	if err = conn.QueryRowContext(ctx, "SELECT app_id FROM main._crew_drop_identity").Scan(&targetApp); err != nil || targetApp != p.AppID {
		return errors.New("unexpected destination identity")
	}
	if err = conn.QueryRowContext(ctx, "SELECT id FROM main._collections WHERE name='crew_attachments'").Scan(&collection); err != nil {
		return err
	}
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, "DROP TRIGGER IF EXISTS _crew_drop_trial_upload; CREATE TABLE IF NOT EXISTS _crew_drop_pending_files(id TEXT PRIMARY KEY); DELETE FROM _crew_drop_pending_files; DELETE FROM crew_upload_links"); err != nil {
		return err
	}
	for _, name := range contentTables {
		if _, err = tx.ExecContext(ctx, "DELETE FROM main."+name); err != nil {
			return err
		}
		if _, err = tx.ExecContext(ctx, "INSERT INTO main."+name+" SELECT * FROM incoming."+name); err != nil {
			return err
		}
	}
	if _, err = tx.ExecContext(ctx, "UPDATE crew_artifacts SET token='', status='revoked' WHERE token<>'' OR status='published'; INSERT INTO _crew_drop_pending_files SELECT id FROM crew_attachments"); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, "CREATE TABLE IF NOT EXISTS _crew_drop_restore_receipts (job TEXT PRIMARY KEY); INSERT OR IGNORE INTO _crew_drop_restore_receipts(job) VALUES(?)", job); err != nil {
		return err
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	if _, err = conn.ExecContext(ctx, "DETACH incoming"); err != nil {
		return err
	}
	files, err := attachmentList(ctx, conn)
	if err != nil {
		return err
	}
	if len(files)+1 != len(entries) {
		return errors.New("attachment inventory differs from archive")
	}
	for _, f := range files {
		entry := entries["originals/"+f.Path]
		if entry == nil {
			return errors.New("missing original")
		}
		dest := filepath.Join(filepath.Dir(p.Project), "storage", collection, f.Path)
		folder := filepath.Dir(dest)
		if err = os.MkdirAll(folder, 0700); err != nil {
			return err
		}
		// An existing immutable original can be shared only when its digest matches.
		if local, e := os.Open(dest); e == nil {
			e = verifiedFile(io.Discard, local, f.Size, f.SHA256)
			local.Close()
			if e != nil {
				return errors.New("attachment conflicts with existing original")
			}
		} else if !errors.Is(e, os.ErrNotExist) {
			return e
		}
		if err = installOriginal(entry, f, folder, dest); err != nil {
			return err
		}
	}
	if _, err = conn.ExecContext(ctx, "PRAGMA journal_mode=DELETE"); err != nil {
		return err
	}
	conn.Close()
	db.Close()
	z.Close()
	if err = os.Remove(content); err != nil {
		return err
	}
	if err = os.Remove(input); err != nil {
		return err
	}
	if err = synced(target); err != nil {
		return err
	}
	for name, value := range map[string]string{"id": platformID(job), "platform-job": job} {
		path := filepath.Join(dir, name)
		if err = os.WriteFile(path, []byte(value), 0600); err != nil {
			return err
		}
		if err = synced(path); err != nil {
			return err
		}
	}
	if err = synced(dir); err != nil {
		return err
	}
	if err = os.Rename(dir, filepath.Join(p.Root, "owner-restore")); err != nil {
		return err
	}
	return synced(p.Root)
}

func (p Platform) Applied(ctx context.Context, job string) (bool, error) {
	db, err := sql.Open("sqlite3", p.Live+"?mode=ro")
	if err != nil {
		return false, err
	}
	defer db.Close()
	var n int
	if err = db.QueryRowContext(ctx, "SELECT count(*) FROM sqlite_schema WHERE name='_crew_drop_restore_receipts'").Scan(&n); err != nil || n == 0 {
		return false, err
	}
	err = db.QueryRowContext(ctx, "SELECT count(*) FROM _crew_drop_restore_receipts WHERE job=?", job).Scan(&n)
	return n != 0, err
}
