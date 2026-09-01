package utils

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
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
	"github.com/datazip-inc/olake-helm/worker/utils/storagemode"
	"github.com/spf13/viper"
)

var (
	s3Client *s3.Client
	s3Bucket string
)

// InitStorage initializes the shared S3 client when storage mode is S3. No-op for NFS.
func InitStorage(ctx context.Context) error {
	if storagemode.Get() != constants.StorageModeS3 {
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
			credentials.NewStaticCredentialsProvider(accessKey, secretKey, viper.GetString(constants.EnvS3SessionToken)),
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

	if !customEndpoint {
		_, err := client.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String(bucket)})
		if err != nil {
			return fmt.Errorf("s3 bucket %q is not accessible: %s", bucket, err)
		}
		return nil
	}

	const maxAttempts = 60
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		if _, err := client.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String(bucket)}); err == nil {
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
		time.Sleep(5 * time.Second)
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

// WriteConfigFiles writes job configs to the active shared storage backend (NFS or S3).
func WriteConfigFiles(ctx context.Context, workDir string, configs []types.JobConfig) error {
	if len(configs) == 0 {
		return nil
	}

	switch storagemode.Get() {
	case constants.StorageModeS3:
		return WriteFilesToS3(ctx, workDir, configs)
	default:
		return WriteFilesToNFS(workDir, configs)
	}
}

// WriteFilesToNFS writes job configs to the local filesystem.
func WriteFilesToNFS(workDir string, configs []types.JobConfig) error {
	for _, jobConfig := range configs {
		filePath := filepath.Join(workDir, jobConfig.Name)
		if err := WriteFile(filePath, []byte(jobConfig.Data)); err != nil {
			return fmt.Errorf("failed to write %s: %s", jobConfig.Name, err)
		}
	}
	return nil
}

// ReadFileFromNFS reads a file from the local filesystem.
func ReadFileFromNFS(workDir, relativePath string) (string, error) {
	filePath := filepath.Join(workDir, relativePath)
	return ReadFile(filePath)
}

// WriteFilesToS3 writes job configs to the S3 bucket.
func WriteFilesToS3(ctx context.Context, workDir string, configs []types.JobConfig) error {
	client, bucket, err := getS3Client()
	if err != nil {
		return err
	}

	for _, jobConfig := range configs {
		key, err := configStorageKey(workDir, jobConfig.Name, false)
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

// ReadFileFromS3 reads a file from the S3 bucket.
func ReadFileFromS3(ctx context.Context, workDir, relativePath string, validateJSON bool) (string, error) {
	var key string
	var err error
	if workDir == "" {
		prefix := strings.Trim(viper.GetString(constants.EnvS3Prefix), "/")
		key = path.Join(prefix, path.Clean(strings.Trim(relativePath, "/")))
	} else {
		key, err = configStorageKey(workDir, relativePath, false)
		if err != nil {
			return "", err
		}
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

// workflowConnectorLogsExistInS3 mirrors the NFS check for logs/sync_*/olake.log:
// true only when connector log chunks have been uploaded for this workflow.
// Worker retries before the first chunk is uploaded still look like a first launch.
func workflowConnectorLogsExistInS3(ctx context.Context, workDir string) bool {
	logsPath, err := configStorageKey(workDir, "logs", true)
	if err != nil {
		return false
	}

	s3Objects, err := listS3Objects(ctx, logsPath)
	if err != nil {
		return false
	}

	for _, s3object := range s3Objects {
		parts := strings.Split(strings.TrimPrefix(s3object.Key, logsPath), "/")
		if len(parts) != 2 {
			continue
		}
		if strings.HasPrefix(parts[0], constants.ConnectorLogDirPrefix) && strings.HasPrefix(parts[1], constants.PodLogFilenamePref) {
			return true
		}
	}
	return false
}

// deleteS3Object deletes a single object from the configured S3 bucket.
func deleteS3Object(ctx context.Context, key string) error {
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

// configStorageKey mirrors the NFS layout as an S3 object key.
// With isDirectory true, returns a directory prefix ending with "/".
// Otherwise returns <prefix>/<workflow-dir>/<relativePath> as an object key without a trailing slash.
func configStorageKey(workDir, relativePath string, isDirectory bool) (string, error) {
	workRel, err := filepath.Rel(GetConfigDir(), workDir)
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
