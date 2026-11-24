// Package dropfiles stores immutable Crew attachment originals in one Drop app's S3 prefix.
package dropfiles

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials/stscreds"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sts"
)

var appID = regexp.MustCompile("^d-[a-z0-9]{15}$")

type Objects interface {
	Put(context.Context, string, int64, io.ReadSeeker) error
	Get(context.Context, string) (io.ReadCloser, error)
}

type s3Objects struct {
	client      *s3.Client
	credentials aws.CredentialsProvider
	bucket      string
	prefix      string
}

// FromEnvironment constructs clients without opening the token file or contacting AWS.
// Ordinary Crew installations continue to use their configured PocketBase filesystem.
func FromEnvironment() (Objects, error) {
	id, bucket := os.Getenv("CREW_DROP_APP_ID"), os.Getenv("CREW_DROP_BUCKET")
	if root := os.Getenv("CREW_DROP_FILE_REPLICA"); root != "" {
		if !appID.MatchString(id) || !filepath.IsAbs(root) || bucket != "" {
			return nil, errors.New("local Drop files require an absolute CREW_DROP_FILE_REPLICA, literal app ID and no AWS bucket")
		}
		return localObjects(filepath.Join(root, id, "files")), nil
	}
	if id == "" && bucket == "" {
		return nil, nil
	}
	if !appID.MatchString(id) || bucket == "" {
		return nil, errors.New("Drop file storage requires a literal CREW_DROP_APP_ID and CREW_DROP_BUCKET")
	}
	region, role, file := os.Getenv("AWS_REGION"), os.Getenv("AWS_ROLE_ARN"), os.Getenv("AWS_WEB_IDENTITY_TOKEN_FILE")
	if region == "" || role == "" || file == "" {
		return nil, errors.New("Drop file storage requires AWS_REGION, AWS_ROLE_ARN and AWS_WEB_IDENTITY_TOKEN_FILE")
	}
	cfg := aws.Config{Region: region, Credentials: aws.AnonymousCredentials{}}
	provider := stscreds.NewWebIdentityRoleProvider(sts.NewFromConfig(cfg), role, stscreds.IdentityTokenFile(file), func(o *stscreds.WebIdentityRoleOptions) {
		o.RoleSessionName = "crew-files-" + id
	})
	cfg.Credentials = aws.NewCredentialsCache(workloadIdentity{file, provider}, func(o *aws.CredentialsCacheOptions) {
		o.ExpiryWindow = time.Minute
	})
	return &s3Objects{client: s3.NewFromConfig(cfg), credentials: cfg.Credentials, bucket: bucket, prefix: id + "/files/"}, nil
}

type workloadIdentity struct {
	tokenFile string
	provider  aws.CredentialsProvider
}

func (w workloadIdentity) Retrieve(ctx context.Context) (aws.Credentials, error) {
	// The runner creates the guest token asynchronously. Only operations that need
	// S3 wait for it. The SDK cache can detach cancellation from this shared call.
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	for {
		_, err := os.Stat(w.tokenFile)
		if !errors.Is(err, os.ErrNotExist) {
			if err != nil {
				return aws.Credentials{}, err
			}
			return w.provider.Retrieve(ctx)
		}
		select {
		case <-ctx.Done():
			return aws.Credentials{}, ctx.Err()
		case <-time.After(50 * time.Millisecond):
		}
	}
}

func digestBytes(digest string) ([]byte, error) {
	b, err := hex.DecodeString(digest)
	if err != nil || len(b) != sha256.Size || strings.ToLower(digest) != digest {
		return nil, errors.New("invalid attachment SHA-256")
	}
	return b, nil
}

// Prepare resolves the initial asynchronous identity outside the Crew database
// writer transaction. The S3 client and this preflight share the same cache.
func (s *s3Objects) Prepare(ctx context.Context) error {
	_, err := s.credentials.Retrieve(ctx)
	return err
}

func (s *s3Objects) Put(ctx context.Context, digest string, size int64, body io.ReadSeeker) error {
	hash, err := digestBytes(digest)
	if err != nil {
		return err
	}
	_, err = s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: &s.bucket, Key: aws.String(s.prefix + digest), Body: body, ContentLength: &size,
		IfNoneMatch: aws.String("*"), ChecksumSHA256: aws.String(base64.StdEncoding.EncodeToString(hash)),
	})
	var httpErr interface{ HTTPStatusCode() int }
	if !errors.As(err, &httpErr) || httpErr.HTTPStatusCode() != 412 {
		return err
	}
	// A concurrent upload or a retry can find existing bytes. Verify them before
	// acknowledging the reference. ETags aren't SHA-256 checksums.
	r, err := s.Get(ctx, digest)
	if err != nil {
		return err
	}
	defer r.Close()
	return Verify(io.Discard, r, digest, size)
}

func (s *s3Objects) Get(ctx context.Context, digest string) (io.ReadCloser, error) {
	if _, err := digestBytes(digest); err != nil {
		return nil, err
	}
	result, err := s.client.GetObject(ctx, &s3.GetObjectInput{Bucket: &s.bucket, Key: aws.String(s.prefix + digest)})
	if err != nil {
		return nil, err
	}
	return result.Body, nil
}

// Verify copies the expected number of bytes and checks the digest stored in Crew's database.
// The caller publishes the destination only after this succeeds.
func Verify(dst io.Writer, src io.Reader, digest string, size int64) error {
	if _, err := digestBytes(digest); err != nil {
		return err
	}
	if size <= 0 || size > 20<<20 {
		return errors.New("invalid Crew attachment size")
	}
	hash := sha256.New()
	n, err := io.Copy(io.MultiWriter(dst, hash), io.LimitReader(src, size+1))
	if err != nil {
		return err
	}
	if n != size || hex.EncodeToString(hash.Sum(nil)) != digest {
		return errors.New("attachment differs from its recorded size or SHA-256")
	}
	return nil
}
