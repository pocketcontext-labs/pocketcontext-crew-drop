package dropsupervisor

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials/stscreds"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/aws/smithy-go"
)

func (s *service) reserveNew(ctx context.Context, nonce string) (bool, error) {
	if s.fileReplica != "" {
		dir := filepath.Join(s.fileReplica, s.app, "crew-drop")
		if err := os.MkdirAll(dir, 0700); err != nil {
			return false, err
		}
		p := filepath.Join(dir, "enrollment")
		contents, err := os.ReadFile(p)
		if err == nil {
			return string(contents) == nonce, nil
		}
		if !errors.Is(err, os.ErrNotExist) {
			return false, err
		}
		entries, err := os.ReadDir(dir)
		if err != nil || len(entries) > 0 {
			return false, err
		}
		f, err := os.OpenFile(p, os.O_WRONLY|os.O_EXCL|os.O_CREATE, 0600)
		if errors.Is(err, os.ErrExist) {
			return false, nil
		}
		if err != nil {
			return false, err
		}
		_, err = f.WriteString(nonce)
		if err == nil {
			err = f.Sync()
		}
		if closeErr := f.Close(); err == nil {
			err = closeErr
		}
		if err == nil {
			err = syncFile(dir)
		}
		return err == nil, err
	}
	arn, token := os.Getenv("AWS_ROLE_ARN"), os.Getenv("AWS_WEB_IDENTITY_TOKEN_FILE")
	if arn == "" || token == "" {
		return false, errors.New("workload identity settings unavailable")
	}
	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(s.region), config.WithSharedConfigFiles([]string{}), config.WithSharedCredentialsFiles([]string{}), config.WithCredentialsProvider(aws.AnonymousCredentials{}))
	if err != nil {
		return false, err
	}
	cfg.Credentials = aws.NewCredentialsCache(stscreds.NewWebIdentityRoleProvider(sts.NewFromConfig(cfg), arn, stscreds.IdentityTokenFile(token)))
	client := s3.NewFromConfig(cfg)
	prefix := s.app + "/crew-drop/"
	key := prefix + "enrollment"
	objects, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: &s.bucket, Prefix: &prefix, MaxKeys: aws.Int32(1)})
	if err != nil {
		return false, fmt.Errorf("list enrollment: %s", errorCode(err))
	}
	if len(objects.Contents) > 0 {
		obj, err := client.GetObject(ctx, &s3.GetObjectInput{Bucket: &s.bucket, Key: &key})
		if errorCode(err) == "NoSuchKey" {
			return false, nil
		}
		if err != nil {
			return false, fmt.Errorf("read enrollment: %s", errorCode(err))
		}
		defer obj.Body.Close()
		contents, err := io.ReadAll(io.LimitReader(obj.Body, 128))
		return string(contents) == nonce, err
	}
	_, err = client.PutObject(ctx, &s3.PutObjectInput{Bucket: &s.bucket, Key: &key, Body: strings.NewReader(nonce), IfNoneMatch: aws.String("*")})
	if errorCode(err) == "PreconditionFailed" {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("reserve enrollment: %s", errorCode(err))
	}
	return true, nil
}

func errorCode(err error) string {
	var api smithy.APIError
	if errors.As(err, &api) {
		return api.ErrorCode()
	}
	return "request_failed"
}
