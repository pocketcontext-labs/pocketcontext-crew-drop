package dropfiles

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func TestS3ImmutableWriteAndVerifiedReplay(t *testing.T) {
	const app = "d-aaaaaaaaaaaaaaa"
	data := []byte("synthetic receipt")
	h := sha256.Sum256(data)
	digest := hex.EncodeToString(h[:])
	var remote []byte
	var reads int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.URL.Path; got != "/synthetic-bucket/"+app+"/files/"+digest {
			t.Errorf("unexpected path %s", got)
			w.WriteHeader(403)
			return
		}
		switch r.Method {
		case "PUT":
			if r.Header.Get("If-None-Match") != "*" || r.Header.Get("X-Amz-Checksum-Sha256") != base64.StdEncoding.EncodeToString(h[:]) {
				t.Error("PUT omitted conditional write or checksum")
			}
			if remote != nil {
				w.Header().Set("Content-Type", "application/xml")
				w.WriteHeader(412)
				io.WriteString(w, "<Error><Code>PreconditionFailed</Code></Error>")
				return
			}
			var err error
			remote, err = io.ReadAll(r.Body)
			if err != nil {
				t.Error(err)
			}
		case "GET":
			reads++
			w.Write(remote)
		default:
			t.Error("unexpected request", r.Method)
			w.WriteHeader(405)
		}
	}))
	defer server.Close()
	client := s3.NewFromConfig(aws.Config{Region: "us-east-1", Credentials: aws.AnonymousCredentials{}}, func(o *s3.Options) {
		o.UsePathStyle = true
		o.BaseEndpoint = aws.String(server.URL)
	})
	s := &s3Objects{client: client, bucket: "synthetic-bucket", prefix: app + "/files/"}
	put := func() error { return s.Put(t.Context(), digest, int64(len(data)), bytes.NewReader(data)) }
	if err := put(); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(remote, data) {
		t.Fatal("stored object differs")
	}
	if err := put(); err != nil || reads != 1 {
		t.Fatal("replay not verified", err, reads)
	}
	remote = bytes.Repeat([]byte("x"), len(data))
	if err := put(); err == nil {
		t.Fatal("corrupt existing object acknowledged")
	}
	for _, malformed := range []string{"../other", strings.ToUpper(digest)} {
		if _, err := s.Get(t.Context(), malformed); err == nil {
			t.Fatal("invalid digest accepted", malformed)
		}
	}
}

func TestLocalReplicaSeparatesApplicationsAndVerifiesContent(t *testing.T) {
	root := t.TempDir()
	t.Setenv("CREW_DROP_BUCKET", "")
	t.Setenv("CREW_DROP_FILE_REPLICA", root)
	t.Setenv("CREW_DROP_APP_ID", "d-aaaaaaaaaaaaaaa")
	objects, err := FromEnvironment()
	if err != nil {
		t.Fatal(err)
	}
	data := []byte("verified content")
	sum := sha256.Sum256(data)
	hash := hex.EncodeToString(sum[:])
	put := func(payload []byte) error {
		return objects.Put(t.Context(), hash, int64(len(data)), bytes.NewReader(payload))
	}
	if err := put(data[:len(data)-1]); err == nil {
		t.Fatal("partial content published")
	}
	if _, err := objects.Get(t.Context(), hash); err == nil {
		t.Fatal("partial object visible")
	}
	if err := put(data); err != nil {
		t.Fatal(err)
	}
	if err := put(data); err != nil {
		t.Fatal("replay", err)
	}
	t.Setenv("CREW_DROP_APP_ID", "d-bbbbbbbbbbbbbbb")
	peer, err := FromEnvironment()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := peer.Get(context.Background(), hash); err == nil {
		t.Fatal("read from peer prefix")
	}
	t.Setenv("CREW_DROP_APP_ID", "../peer")
	if _, err := FromEnvironment(); err == nil {
		t.Fatal("accepted path in app ID")
	}
	t.Setenv("CREW_DROP_APP_ID", "d-aaaaaaaaaaaaaaa")
	t.Setenv("CREW_DROP_BUCKET", "synthetic-bucket")
	if _, err := FromEnvironment(); err == nil {
		t.Fatal("accepted mixed local/AWS configuration")
	}
	t.Setenv("CREW_DROP_FILE_REPLICA", "")
	t.Setenv("AWS_ROLE_ARN", "arn:aws:iam::111122223333:role/synthetic")
	t.Setenv("AWS_REGION", "us-east-1")
	t.Setenv("AWS_WEB_IDENTITY_TOKEN_FILE", filepath.Join(root, "not-created"))
	if _, err := FromEnvironment(); err != nil {
		t.Fatal("startup waited for identity", err)
	}
}
