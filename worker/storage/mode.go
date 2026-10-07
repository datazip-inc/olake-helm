package storage

import (
	"strings"

	"github.com/datazip-inc/olake-helm/worker/constants"
	"github.com/spf13/viper"
)

// TODO: add support for GCS and Azure Blob storage modes.
// Mode returns OLAKE_STORAGE_MODE from the environment, defaulting to nfs.
func Mode() string {
	mode := strings.ToLower(strings.TrimSpace(viper.GetString(constants.EnvStorageMode)))
	if mode == "" {
		return constants.StorageModeNFS
	}
	return mode
}

// ConfigDir is the directory shared with the connector: the NFS mount, or the root that maps to
// the S3 prefix.
func ConfigDir() string {
	if viper.GetString(constants.EnvKubernetesServiceHost) != "" {
		return constants.K8sPersistentDir
	}
	return constants.DockerPersistentDir
}
