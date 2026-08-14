package utils

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/datazip-inc/olake-helm/worker/constants"
	"github.com/datazip-inc/olake-helm/worker/types"
	"github.com/spf13/viper"
)

var (
	s3Client *s3.Client
	s3Bucket string
)

// InitStorage initializes the shared S3 client when storage mode is S3. No-op for NFS.
func InitStorage(ctx context.Context) error {
	if constants.GetStorageMode() != constants.StorageModeS3 {
		return nil
	}

	configOpts := []func(*config.LoadOptions) error{}
	if region := envFirst(constants.EnvS3Region, "AWS_REGION"); region != "" {
		configOpts = append(configOpts, config.WithRegion(region))
	}

	accessKey := envFirst(constants.EnvS3AccessKeyID, "AWS_ACCESS_KEY_ID")
	secretKey := envFirst(constants.EnvS3SecretAccessKey, "AWS_SECRET_ACCESS_KEY")
	if accessKey != "" && secretKey != "" {
		sessionToken := envFirst(constants.EnvS3SessionToken, "AWS_SESSION_TOKEN")
		configOpts = append(configOpts, config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(accessKey, secretKey, sessionToken),
		))
	}

	awsCfg, err := config.LoadDefaultConfig(ctx, configOpts...)
	if err != nil {
		return fmt.Errorf("failed to load AWS config: %s", err)
	}

	var s3Opts []func(*s3.Options)
	if endpoint := envFirst(constants.EnvS3Endpoint, "AWS_ENDPOINT_URL"); endpoint != "" {
		// Path-style is required for MinIO and other S3-compatible endpoints.
		s3Opts = append(s3Opts, func(o *s3.Options) {
			o.BaseEndpoint = aws.String(endpoint)
			o.UsePathStyle = true
		})
	}

	s3Client = s3.NewFromConfig(awsCfg, s3Opts...)
	s3Bucket = viper.GetString(constants.EnvS3Bucket)
	return nil
}

// envFirst returns the first non-empty environment variable value from the given keys.
func envFirst(keys ...string) string {
	for _, key := range keys {
		if value := os.Getenv(key); value != "" {
			return value
		}
	}
	return ""
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

	switch constants.GetStorageMode() {
	case constants.StorageModeS3:
		return WriteFilesToS3(ctx, workDir, configs)
	case constants.StorageModeNFS:
		return WriteFilesToNFS(workDir, configs)
	default:
		return fmt.Errorf("unsupported storage mode: %s", constants.GetStorageMode())
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
func readFileFromNFS(workDir, relativePath string) (string, error) {
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
func readFileFromS3(ctx context.Context, workDir, relativePath string, validateJSON bool) (string, error) {
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

	keys, err := listS3ObjectKeys(ctx, logsPath)
	if err != nil {
		return false
	}

	for _, key := range keys {
		parts := strings.Split(strings.TrimPrefix(key, logsPath), "/")
		if len(parts) != 2 {
			continue
		}
		if strings.HasPrefix(parts[0], constants.ConnectorLogDirPrefix) && strings.HasPrefix(parts[1], constants.PodLogFilenamePref) {
			return true
		}
	}
	return false
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
