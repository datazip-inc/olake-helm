package utils

import (
	"fmt"
	"os"

	"github.com/datazip-inc/olake-helm/worker/constants"
)

func SetupWorkDirectory(workDirPath string) error {
	if err := os.MkdirAll(workDirPath, constants.DefaultDirPermissions); err != nil {
		return fmt.Errorf("failed to create work directory: %s", err)
	}
	return nil
}

// CreateDirectory creates a directory with the specified permissions if it doesn't exist
func CreateDirectory(dirPath string) error {
	if _, err := os.Stat(dirPath); os.IsNotExist(err) {
		if err := os.MkdirAll(dirPath, constants.DefaultDirPermissions); err != nil {
			return fmt.Errorf("failed to create directory %s: %s", dirPath, err)
		}
	}
	return nil
}

func DeleteDirectory(dirPath string) error {
	if err := os.RemoveAll(dirPath); err != nil {
		return fmt.Errorf("failed to delete directory %s: %s", dirPath, err)
	}
	return nil
}
