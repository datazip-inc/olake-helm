package utils

import (
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/datazip-inc/olake-helm/worker/utils/logger"
	"github.com/robfig/cron"
)

// starts a log cleaner that removes old logs from the specified directory based on the retention period
func InitLogCleaner(logDir string, retentionPeriod int) {
	logger.Info("log cleaner started...")
	go cleanOldLogs(logDir, retentionPeriod) // catchup missed cycles if any
	c := cron.New()
	err := c.AddFunc("@midnight", func() {
		cleanOldLogs(logDir, retentionPeriod)
	})
	if err != nil {
		logger.Errorf("failed to start log cleaner: %s", err)
		return
	}
	c.Start()
}

func cleanOldLogs(logDir string, retentionPeriod int) {
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
			if (strings.HasSuffix(filePath, ".log") || strings.HasSuffix(filePath, ".log.gz")) &&
				info.ModTime().Before(cutoff) {
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
