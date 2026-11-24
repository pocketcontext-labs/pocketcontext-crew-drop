package crewruntime

import (
	"context"
	"errors"
	"io"
	"net/http"
	"os"
	"time"

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
