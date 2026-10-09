package utils

import (
	"context"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/datazip-inc/olake-helm/worker/constants"
	"github.com/datazip-inc/olake-helm/worker/utils/logger"
	"github.com/datazip-inc/olake-helm/worker/utils/storagemode"
	"github.com/robfig/cron"
	"github.com/spf13/viper"
)

// starts a log cleaner that removes old logs from the specified directory based on the retention period
func InitLogCleaner(ctx context.Context, logDir string, retentionPeriod int) {
	c := cron.New()

	err := c.AddFunc("@midnight", func() {
		switch storagemode.Get() {
		case constants.StorageModeS3:
			cleanS3OldLogs(ctx, retentionPeriod)
		default:
			cleanNFSOldLogs(logDir, retentionPeriod)
		}
	})
	if err != nil {
		logger.Errorf("failed to start log cleaner: %s", err)
		return
	}

	c.Start()
}

func shouldCheckModTime(fileName string) bool {
	return strings.HasSuffix(fileName, ".log") ||
		strings.HasSuffix(fileName, ".log.gz") ||
		fileName == "streams.json"
}

func cleanNFSOldLogs(logDir string, retentionPeriod int) {
	logger.Info("running log cleaner...")
	cutoff := time.Now().AddDate(0, 0, -retentionPeriod)

	// check if old logs are present
	shouldDelete := func(path string, cutoff time.Time) bool {
		entries, _ := os.ReadDir(path)
		if len(entries) == 0 {
			return true
		}

		var foundOldLog bool
		_ = filepath.Walk(path, func(filePath string, info os.FileInfo, _ error) error {
			if info == nil || info.IsDir() {
				return nil
			}

			if shouldCheckModTime(filepath.Base(filePath)) && info.ModTime().Before(cutoff) {
				foundOldLog = true
				return filepath.SkipDir
			}
			return nil
		})
		return foundOldLog
	}

	entries, err := os.ReadDir(logDir)
	if err != nil {
		logger.Errorf("failed to read log dir: %s", err)
		return
	}
	// delete dir if old logs are found or is empty
	for _, entry := range entries {
		if !entry.IsDir() || entry.Name() == "telemetry" {
			continue
		}
		dirPath := filepath.Join(logDir, entry.Name())
		if toDelete := shouldDelete(dirPath, cutoff); toDelete {
			logger.Infof("deleting folder: %s", dirPath)
			_ = os.RemoveAll(dirPath)
		}
	}
}

// cleanS3OldLogs matches NFS: if any *.log / *.log.gz / streams.json in a workflow
// prefix has LastModified before LOG_RETENTION_PERIOD, delete the entire prefix.
func cleanS3OldLogs(ctx context.Context, retentionPeriod int) {
	logger.Info("running log cleaner...")
	cutoff := time.Now().AddDate(0, 0, -retentionPeriod)

	s3path := strings.Trim(viper.GetString(constants.EnvS3Prefix), "/")
	if s3path != "" {
		s3path += "/"
	}

	s3Objects, err := listS3Objects(ctx, s3path)
	if err != nil {
		logger.Errorf("failed to list s3 objects: %s", err)
		return
	}

	groups := map[string][]s3Object{}

	for _, s3object := range s3Objects {
		relativePath := strings.TrimPrefix(s3object.Key, s3path)
		workflowDir, _, ok := strings.Cut(relativePath, "/")
		if !ok || workflowDir == "" || workflowDir == "telemetry" {
			continue
		}
		groups[workflowDir] = append(groups[workflowDir], s3object)
	}

	for _, group := range groups {
		for _, s3object := range group {
			if !shouldCheckModTime(path.Base(s3object.Key)) || !s3object.LastModified.Before(cutoff) {
				continue
			}
			for _, s3object := range group {
				if err := deleteS3Object(ctx, s3object.Key); err != nil {
					logger.Errorf("failed to delete s3 object %s: %s", s3object.Key, err)
				}
			}
			break
		}
	}
}
