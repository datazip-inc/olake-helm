package storagemode

import (
	"github.com/datazip-inc/olake-helm/worker/constants"
	"github.com/spf13/viper"
	"strings"
)

// Get returns OLAKE_STORAGE_MODE from the environment, defaulting to nfs.
func Get() string {
	mode := strings.ToLower(strings.TrimSpace(viper.GetString(constants.EnvStorageMode)))
	if mode == "" {
		return constants.StorageModeNFS
	}
	return mode
}
