package dropfiles

import (
	"errors"
	"net"
	"net/url"
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// EnvHostEndpoint is set by NibRunner when the node serves the guest's S3
// through host services: a loopback endpoint the host relays, re-signs and
// confines to the app's prefix. The guest then holds only placeholder keys
// (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY) and no workload identity.
const EnvHostEndpoint = "AWS_ENDPOINT_URL_S3"

// HostEndpoint returns the host services endpoint, or "" when the guest uses
// workload identity. Only plain HTTP on a loopback address is accepted, with
// both placeholder keys present.
func HostEndpoint() (string, error) {
	raw := os.Getenv(EnvHostEndpoint)
	if raw == "" {
		return "", nil
	}
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "http" || u.User != nil || (u.Path != "" && u.Path != "/") || u.RawQuery != "" || u.Port() == "" {
		return "", errors.New("AWS_ENDPOINT_URL_S3 must be the host services loopback endpoint")
	}
	if ip := net.ParseIP(u.Hostname()); ip == nil || !ip.IsLoopback() {
		return "", errors.New("AWS_ENDPOINT_URL_S3 must be the host services loopback endpoint")
	}
	if os.Getenv("AWS_ACCESS_KEY_ID") == "" || os.Getenv("AWS_SECRET_ACCESS_KEY") == "" {
		return "", errors.New("host services require the placeholder AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY")
	}
	return raw, nil
}

// NewHostClient returns an S3 client for the host services endpoint:
// path-style, the placeholder keys, no session token.
func NewHostClient(region, endpoint string) *s3.Client {
	cfg := aws.Config{Region: region, Credentials: credentials.NewStaticCredentialsProvider(os.Getenv("AWS_ACCESS_KEY_ID"), os.Getenv("AWS_SECRET_ACCESS_KEY"), "")}
	return s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(endpoint)
		o.UsePathStyle = true
	})
}
