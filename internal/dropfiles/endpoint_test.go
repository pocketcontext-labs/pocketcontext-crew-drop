package dropfiles

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestHostEndpointUsesPlaceholderKeysPathStyle(t *testing.T) {
	const app = "d-aaaaaaaaaaaaaaa"
	data := []byte("synthetic attachment")
	h := sha256.Sum256(data)
	digest := hex.EncodeToString(h[:])
	var stored []byte
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/synthetic-bucket/"+app+"/files/"+digest || !strings.Contains(r.Header.Get("Authorization"), "Credential=placeholder-key/") ||
			r.Header.Get("X-Amz-Security-Token") != "" {
			t.Errorf("request %s %s %q", r.Method, r.URL.Path, r.Header.Get("Authorization"))
			w.WriteHeader(403)
			return
		}
		if r.Method == "PUT" {
			stored, _ = io.ReadAll(r.Body)
			return
		}
		w.Write(stored)
	}))
	defer server.Close()
	t.Setenv("CREW_DROP_FILE_REPLICA", "")
	t.Setenv("CREW_DROP_APP_ID", app)
	t.Setenv("CREW_DROP_BUCKET", "synthetic-bucket")
	t.Setenv("AWS_REGION", "eu-north-1")
	t.Setenv("AWS_ROLE_ARN", "")
	t.Setenv("AWS_WEB_IDENTITY_TOKEN_FILE", "")
	t.Setenv("AWS_ACCESS_KEY_ID", "placeholder-key")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "placeholder-secret")
	t.Setenv(EnvHostEndpoint, server.URL)
	objects, err := FromEnvironment()
	if err != nil {
		t.Fatal(err)
	}
	if err := objects.(interface{ Prepare(context.Context) error }).Prepare(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := objects.Put(t.Context(), digest, int64(len(data)), bytes.NewReader(data)); err != nil {
		t.Fatal(err)
	}
	r, err := objects.Get(t.Context(), digest)
	if err != nil {
		t.Fatal(err)
	}
	got, _ := io.ReadAll(r)
	if !bytes.Equal(got, data) {
		t.Fatal("download differs")
	}
	for _, bad := range []string{"https://127.0.0.1:9000", "http://10.0.0.1:9000", "http://s3.amazonaws.com:80", "http://127.0.0.1", "http://user@127.0.0.1:9000"} {
		t.Setenv(EnvHostEndpoint, bad)
		if _, err := FromEnvironment(); err == nil {
			t.Errorf("accepted %s", bad)
		}
	}
	t.Setenv(EnvHostEndpoint, server.URL)
	t.Setenv("AWS_SECRET_ACCESS_KEY", "")
	if _, err := FromEnvironment(); err == nil {
		t.Error("accepted a host endpoint without placeholder keys")
	}
}
