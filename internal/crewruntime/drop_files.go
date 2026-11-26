package crewruntime

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"ganigeorgiev.com/pocketbase/dbx"
	"ganigeorgiev.com/pocketbase/pocketbase/apis"
	"ganigeorgiev.com/pocketbase/pocketbase/core"
	"ganigeorgiev.com/pocketbase/pocketbase/tools/filesystem"
	"github.com/pocketcontext-labs/pocketcontext-crew/internal/dropfiles"
	"golang.org/x/sync/singleflight"
)

const dropFileStore = "crew.drop.attachmentStore"

type dropAttachmentStore struct {
	objects dropfiles.Objects
	repairs singleflight.Group
}

func configureDropFiles(app core.App) error {
	objects, err := dropfiles.FromEnvironment()
	if err != nil {
		return err
	}
	if objects != nil {
		bindDropFiles(app, objects)
	}
	return nil
}

// prepareDropUpload is called by the existing JS file collection step, after
// owner or upload-link authentication and before the SQLite writer transaction.
func prepareDropUpload(e *core.RequestEvent, result map[string]any) (map[string]any, error) {
	store, ok := e.App.Store().Get(dropFileStore).(*dropAttachmentStore)
	if !ok || result["error"] != nil {
		return result, nil
	}
	identity, ok := store.objects.(interface{ Prepare(context.Context) error })
	if !ok {
		return result, nil
	}
	files, _ := result["files"].([]map[string]any)
	for _, entry := range files {
		if entry["file"] != nil {
			ctx, cancel := context.WithTimeout(e.Request.Context(), time.Minute)
			defer cancel()
			if err := identity.Prepare(ctx); err != nil {
				return nil, e.Error(http.StatusServiceUnavailable, "Crew file storage is unavailable; retry the upload", nil)
			}
			break
		}
	}
	return result, nil
}

func bindDropFiles(app core.App, objects dropfiles.Objects) {
	store := &dropAttachmentStore{objects: objects}
	app.Store().Set(dropFileStore, store)
	// All supported file-carrying routes eventually create one crew_attachments
	// record per original. This hook runs after validation, before the DB insert.
	app.OnRecordCreateExecute("crew_attachments").BindFunc(func(e *core.RecordEvent) error {
		if e.App.Settings().S3.Enabled {
			return errors.New("Drop requires local PocketBase file storage")
		}
		files := e.Record.GetUnsavedFiles("file")
		if len(files) != 1 || files[0].Size != e.Record.GetInt64("size") {
			return errors.New("Drop attachment requires an original matching its recorded size")
		}
		r, err := files[0].Reader.Open()
		if err != nil {
			return err
		}
		defer r.Close()
		ctx, cancel := context.WithTimeout(e.Context, time.Minute)
		defer cancel()
		if err := store.objects.Put(ctx, e.Record.GetString("sha256"), files[0].Size, r); err != nil {
			return apis.NewApiError(http.StatusServiceUnavailable, "Crew could not durably store the attachment. Retry the upload.", nil)
		}
		return e.Next()
	})
}

// attachmentFilesystem is called only after the existing route has authorized
// this attachment. Thumbnails remain derived local files; the original is what
// ties an acknowledged upload, archived reference or published snapshot to S3.
func attachmentFilesystem(re *core.RequestEvent, rec *core.Record) (*filesystem.System, error) {
	fsys, err := re.App.NewFilesystem()
	if err != nil {
		return nil, re.InternalServerError("Storage unavailable", nil)
	}
	store, ok := re.App.Store().Get(dropFileStore).(*dropAttachmentStore)
	if !ok {
		return fsys, nil
	}
	if re.App.Settings().S3.Enabled {
		fsys.Close()
		return nil, re.InternalServerError("Storage unavailable", nil)
	}
	key := rec.BaseFilesPath() + "/" + rec.GetString("file")
	digest, size := rec.GetString("sha256"), rec.GetInt64("size")
	ctx, cancel := context.WithTimeout(re.Request.Context(), time.Minute)
	defer cancel()
	_, err, _ = store.repairs.Do(key, func() (any, error) {
		return nil, store.materialize(ctx, fsys, key, digest, size)
	})
	if err != nil {
		fsys.Close()
		return nil, re.Error(http.StatusServiceUnavailable, "Crew attachment storage unavailable. Retry the request.", nil)
	}
	fsys.SetContext(re.Request.Context())
	return fsys, nil
}

func (s *dropAttachmentStore) materialize(ctx context.Context, fsys *filesystem.System, key, digest string, size int64) error {
	fsys.SetContext(ctx)
	local, err := fsys.GetReader(key)
	if err == nil {
		err = dropfiles.Verify(io.Discard, local, digest, size)
		local.Close()
		if err == nil {
			return nil
		}
	}
	remote, err := s.objects.Get(ctx, digest)
	if err != nil {
		return err
	}
	defer remote.Close()
	// Stage outside PocketBase's visible file tree. Verification must finish before
	// even the first byte is made available to range reads or preview generation.
	staging, err := os.CreateTemp("", "crew-attachment-*")
	if err != nil {
		return err
	}
	defer os.Remove(staging.Name())
	err = dropfiles.Verify(staging, remote, digest, size)
	closeErr := staging.Close()
	if err != nil || closeErr != nil {
		return errors.Join(err, closeErr)
	}
	file, err := filesystem.NewFileFromPath(staging.Name())
	if err != nil {
		return err
	}
	return fsys.UploadFile(file, key)
}

// Keep each trial upload in the same SQLite transaction as its attachment.
func PrepareTrialAttachments(ctx context.Context, app core.App) error {
	if dropfiles.LocalOnly() {
		return app.RunInTransaction(func(tx core.App) error {
			if _, err := tx.DB().NewQuery("CREATE TABLE IF NOT EXISTS _crew_drop_pending_files (id TEXT PRIMARY KEY)").Execute(); err != nil {
				return err
			}
			_, err := tx.DB().NewQuery("CREATE TRIGGER IF NOT EXISTS _crew_drop_trial_upload AFTER INSERT ON crew_attachments BEGIN INSERT INTO _crew_drop_pending_files(id) VALUES (NEW.id); END").Execute()
			return err
		})
	}
	var exists int
	if err := app.DB().NewQuery("SELECT count(*) FROM sqlite_master WHERE name='_crew_drop_pending_files' AND type='table'").Row(&exists); err != nil || exists == 0 {
		return err
	}
	if _, err := app.DB().NewQuery("DROP TRIGGER IF EXISTS _crew_drop_trial_upload").Execute(); err != nil {
		return err
	}
	objects, err := dropfiles.FromEnvironment()
	if err != nil {
		return err
	}
	if objects == nil {
		return errors.New("attachment replica is unavailable")
	}
	for {
		var ids []string
		if err := app.DB().NewQuery("SELECT id FROM _crew_drop_pending_files ORDER BY id LIMIT 32").Column(&ids); err != nil {
			return err
		}
		if len(ids) == 0 {
			return nil
		}
		for _, id := range ids {
			record, err := app.FindRecordById("crew_attachments", id)
			if err != nil {
				return err
			}
			if err := promoteTrialAttachment(ctx, app, objects, record); err != nil {
				return fmt.Errorf("replicate trial attachment %s: %w", id, err)
			}
			if _, err := app.DB().NewQuery("DELETE FROM _crew_drop_pending_files WHERE id={:id}").Bind(dbx.Params{"id": id}).Execute(); err != nil {
				return err
			}
		}
	}
}

func promoteTrialAttachment(ctx context.Context, app core.App, objects dropfiles.Objects, record *core.Record) error {
	root, err := os.OpenRoot(filepath.Join(app.DataDir(), "storage"))
	if err != nil {
		return err
	}
	defer root.Close()
	f, err := root.Open(filepath.Join(record.BaseFilesPath(), record.GetString("file")))
	if err != nil {
		return err
	}
	defer f.Close()
	size, hash := record.GetInt64("size"), record.GetString("sha256")
	if err := dropfiles.Verify(io.Discard, f, hash, size); err != nil {
		return err
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return err
	}
	upload, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	return objects.Put(upload, hash, size, f)
}
