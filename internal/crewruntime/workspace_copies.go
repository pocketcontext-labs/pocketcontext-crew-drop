package crewruntime

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"ganigeorgiev.com/pocketbase/pocketbase/core"
	"github.com/pocketcontext-labs/pocketcontext-crew/internal/dropfiles"
	"github.com/pocketcontext-labs/pocketcontext-crew/internal/workspacecopy"
)

func RegisterWorkspaceCopies(e *core.ServeEvent, root, appID string, recovered bool) error {
	objects, err := dropfiles.FromEnvironment()
	if err != nil {
		return err
	}
	files := func(ctx context.Context, db *sql.DB) ([]workspacecopy.File, error) {
		rows, err := db.QueryContext(ctx, "SELECT c.id || '/' || a.id || '/' || a.file, a.sha256, a.size FROM crew_attachments AS a JOIN _collections AS c ON c.name='crew_attachments' ORDER BY a.id")
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		var files []workspacecopy.File
		for rows.Next() {
			var f workspacecopy.File
			if err := rows.Scan(&f.Path, &f.SHA256, &f.Size); err != nil {
				return nil, err
			}
			files = append(files, f)
		}
		return files, rows.Err()
	}
	cfg := workspacecopy.Config{Root: root, AppID: appID, IdentityTable: "_crew_drop_identity", Files: files, Objects: objects, Confirm: confirmCopyReplication}
	if objects != nil {
		store := dropAttachmentStore{objects: objects}
		materialize := func(ctx context.Context, f workspacecopy.File) error {
			fsys, err := e.App.NewFilesystem()
			if err != nil {
				return err
			}
			defer fsys.Close()
			return store.materialize(ctx, fsys, f.Path, f.SHA256, f.Size)
		}
		cfg.Materialize = materialize
		if recovered {
			// The account may run out of credit immediately after this recovery.
			// Hydrate every immutable original before allowing the first local read.
			offset := 0
			for {
				records, err := e.App.FindRecordsByFilter("crew_attachments", "", "id", 100, offset)
				if err != nil {
					return err
				}
				for _, r := range records {
					f := workspacecopy.File{Path: filepath.ToSlash(filepath.Join(r.BaseFilesPath(), r.GetString("file"))), Size: r.GetInt64("size"), SHA256: r.GetString("sha256")}
					ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
					err = materialize(ctx, f)
					cancel()
					if err != nil {
						return fmt.Errorf("recover attachment %s: %w", r.Id, err)
					}
				}
				if len(records) < 100 {
					break
				}
				offset += len(records)
			}
		}
	}
	return workspacecopy.Register(e, cfg)
}
func confirmCopyReplication(ctx context.Context) error {
	port := os.Getenv("NIBRUN_HOOK_PORT")
	if port == "" {
		port = "8081"
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://"+net.JoinHostPort("127.0.0.1", port)+"/control/sync", nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+os.Getenv("NIBRUN_HOOK_TOKEN"))
	client := http.Client{Timeout: 10 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	res, err := client.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode != 200 {
		return errors.New("workspace copy replication not confirmed")
	}
	return nil
}
