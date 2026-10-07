package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/datazip-inc/olake-helm/worker/constants"
	"github.com/datazip-inc/olake-helm/worker/types"
	"github.com/spf13/viper"
)

var (
	s3Client *s3.Client
	s3Bucket string
)

// S3Object is an S3 object listing entry.
type S3Object struct {
	Key          string
	LastModified time.Time
}

// Init initializes the shared S3 client when storage mode is S3. No-op for NFS.
func Init(ctx context.Context) error {
	if Mode() != constants.StorageModeS3 {
		return nil
	}

	configOpts := []func(*config.LoadOptions) error{}
	if region := viper.GetString(constants.EnvS3Region); region != "" {
		configOpts = append(configOpts, config.WithRegion(region))
	}

	accessKey := viper.GetString(constants.EnvS3AccessKeyID)
	secretKey := viper.GetString(constants.EnvS3SecretAccessKey)
	if accessKey != "" && secretKey != "" {
		configOpts = append(configOpts, config.WithCredentialsProvider(
			credentials.StaticCredentialsProvider{Value: aws.Credentials{AccessKeyID: accessKey, SecretAccessKey: secretKey}},
		))
	}

	awsCfg, err := config.LoadDefaultConfig(ctx, configOpts...)
	if err != nil {
		return fmt.Errorf("failed to load AWS config: %s", err)
	}

	var s3Opts []func(*s3.Options)
	if endpoint := viper.GetString(constants.EnvS3Endpoint); endpoint != "" {
		// Path-style is required for MinIO and other S3-compatible endpoints.
		s3Opts = append(s3Opts, func(o *s3.Options) {
			o.BaseEndpoint = aws.String(endpoint)
			o.UsePathStyle = true
			// SDK-default CRC32 integrity checksums (service/s3 >= v1.73) are not
			// implemented by several S3-compatible services (R2, older MinIO, GCS interop)
			o.RequestChecksumCalculation = aws.RequestChecksumCalculationWhenRequired
			o.ResponseChecksumValidation = aws.ResponseChecksumValidationWhenRequired
		})
	}

	s3Client = s3.NewFromConfig(awsCfg, s3Opts...)
	s3Bucket = viper.GetString(constants.EnvS3Bucket)
	if s3Bucket == "" {
		return fmt.Errorf("s3 bucket is required when storage mode is s3")
	}
	return ensureS3Bucket(ctx, s3Client, s3Bucket)
}

// ensureS3Bucket verifies the configured bucket exists. For S3-compatible endpoints
// (MinIO), it creates the bucket when missing and retries while the server starts.
func ensureS3Bucket(ctx context.Context, client *s3.Client, bucket string) error {
	customEndpoint := viper.GetString(constants.EnvS3Endpoint) != ""

	prefix := strings.Trim(viper.GetString(constants.EnvS3Prefix), "/")
	if !customEndpoint {
		_, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:  aws.String(bucket),
			Prefix:  aws.String(prefix),
			MaxKeys: aws.Int32(1),
		})
		if err != nil {
			return fmt.Errorf("s3 bucket %q is not accessible: %s", bucket, err)
		}
		return nil
	}

	const maxAttempts = 60
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		if _, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:  aws.String(bucket),
			Prefix:  aws.String(prefix),
			MaxKeys: aws.Int32(1),
		}); err == nil {
			return nil
		}

		_, err := client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)})
		if err == nil {
			return nil
		}

		var alreadyExists *s3types.BucketAlreadyExists
		var alreadyOwned *s3types.BucketAlreadyOwnedByYou
		if errors.As(err, &alreadyExists) || errors.As(err, &alreadyOwned) {
			return nil
		}

		if attempt == maxAttempts {
			return fmt.Errorf("failed to ensure s3 bucket %q after %d attempts: %s", bucket, maxAttempts, err)
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(5 * time.Second):
		}
	}

	return nil
}

// getS3Client returns the shared S3 client initialized by InitStorage.
func getS3Client() (*s3.Client, string, error) {
	if s3Client == nil {
		return nil, "", fmt.Errorf("s3 storage not initialized")
	}
	return s3Client, s3Bucket, nil
}

// writeFilesS3 writes job configs to the S3 bucket.
func writeFilesS3(ctx context.Context, workDir string, configs []types.JobConfig) error {
	client, bucket, err := getS3Client()
	if err != nil {
		return err
	}

	for _, jobConfig := range configs {
		key, err := S3Key(workDir, jobConfig.Name, false)
		if err != nil {
			return err
		}

		_, err = client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: &bucket,
			Key:    &key,
			Body:   strings.NewReader(jobConfig.Data),
		})
		if err != nil {
			return fmt.Errorf("failed to upload %s to s3://%s/%s: %s", jobConfig.Name, bucket, key, err)
		}
	}

	return nil
}

// readFileS3 reads a file from the S3 bucket.
func readFileS3(ctx context.Context, workDir, relativePath string, validateJSON bool) (string, error) {
	key, err := S3Key(workDir, relativePath, false)
	if err != nil {
		return "", err
	}

	client, bucket, err := getS3Client()
	if err != nil {
		return "", err
	}

	out, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: &bucket,
		Key:    &key,
	})
	if err != nil {
		var noSuchKey *s3types.NoSuchKey
		if errors.As(err, &noSuchKey) {
			return "", fmt.Errorf("s3://%s/%s: %w", bucket, key, fs.ErrNotExist)
		}
		return "", fmt.Errorf("failed to download %s from s3://%s/%s: %s", relativePath, bucket, key, err)
	}
	defer out.Body.Close()

	body, err := io.ReadAll(out.Body)
	if err != nil {
		return "", fmt.Errorf("failed to read %s from s3://%s/%s: %s", relativePath, bucket, key, err)
	}

	if validateJSON {
		ref := fmt.Sprintf("s3://%s/%s", bucket, key)
		var result map[string]interface{}
		if err := json.Unmarshal(body, &result); err != nil {
			return "", fmt.Errorf("failed to read %s: failed to parse JSON from %s: %s", relativePath, ref, err)
		}
	}

	return string(body), nil
}

// ListS3Objects lists S3 objects under the given prefix, including LastModified.
func ListS3Objects(ctx context.Context, prefix string) ([]S3Object, error) {
	client, bucket, err := getS3Client()
	if err != nil {
		return nil, err
	}

	var s3Objects []S3Object
	paginator := s3.NewListObjectsV2Paginator(client, &s3.ListObjectsV2Input{
		Bucket: &bucket,
		Prefix: &prefix,
	})
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to list objects in s3://%s/%s: %s", bucket, prefix, err)
		}
		for _, obj := range page.Contents {
			s3Objects = append(s3Objects, S3Object{
				Key:          aws.ToString(obj.Key),
				LastModified: aws.ToTime(obj.LastModified),
			})
		}
	}
	return s3Objects, nil
}

// DeleteS3Object deletes a single object from the configured S3 bucket.
func DeleteS3Object(ctx context.Context, key string) error {
	client, bucket, err := getS3Client()
	if err != nil {
		return err
	}

	_, err = client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: &bucket,
		Key:    &key,
	})
	if err != nil {
		return fmt.Errorf("failed to delete s3://%s/%s: %s", bucket, key, err)
	}
	return nil
}

// S3Key mirrors the NFS layout as an S3 object key.
// With isDirectory true, returns a directory prefix ending with "/".
// Otherwise returns <prefix>/<workflow-dir>/<relativePath> as an object key without a trailing slash.
func S3Key(workDir, relativePath string, isDirectory bool) (string, error) {
	workRel, err := filepath.Rel(ConfigDir(), workDir)
	if err != nil {
		return "", fmt.Errorf("failed to resolve storage path for %s: %s", workDir, err)
	}

	prefix := strings.Trim(viper.GetString(constants.EnvS3Prefix), "/")
	key := path.Join(prefix, workRel, relativePath)
	if isDirectory {
		return strings.TrimSuffix(key, "/") + "/", nil
	}
	return key, nil
}

// PutS3Object uploads body to the object key (a full key, as returned by S3Key).
func PutS3Object(ctx context.Context, key string, body io.Reader) error {
	client, bucket, err := getS3Client()
	if err != nil {
		return err
	}

	_, err = client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: &bucket,
		Key:    &key,
		Body:   body,
	})
	return err
}
