package config

import (
	"fmt"

	"github.com/datazip-inc/olake-helm/worker/constants"
	"github.com/datazip-inc/olake-helm/worker/types"
	"github.com/datazip-inc/olake-helm/worker/utils"
	"github.com/spf13/viper"
)

func Init() error {
	viper.AutomaticEnv()

	setDefaults()

	if err := requiredEnvVars(); err != nil {
		return fmt.Errorf("failed to initialize config: %v", err)
	}

	return nil
}

// setDefaults sets default values for configuration
func setDefaults() {
	// Temporal defaults
	viper.SetDefault("TEMPORAL_ADDRESS", "temporal:7233")

	// Worker defaults
	viper.SetDefault("LOG_RETENTION_PERIOD", 30)

	// Kubernetes defaults
	viper.SetDefault("WORKER_NAMESPACE", "default")

	// Logging defaults
	viper.SetDefault("LOG_LEVEL", "info")
	viper.SetDefault("LOG_FORMAT", "console")

	// API defaults
	viper.SetDefault("OLAKE_CALLBACK_URL", "http://olake-ui:8000/internal/worker/callback")

	// database defaults
	viper.SetDefault("DB_HOST", "postgresql")
	viper.SetDefault("DB_PORT", 5432)
	viper.SetDefault("DB_USER", "temporal")
	viper.SetDefault("DB_PASSWORD", "temporal")
	viper.SetDefault("DB_NAME", "postgres")
	viper.SetDefault("DB_SSLMODE", "disable")
	viper.SetDefault("RUN_MODE", "dev")
}

// checks for required environment variables
func requiredEnvVars() error {
	// Common required env vars
	requiredEnv := []string{
		constants.EnvCallbackURL,
	}

	if viper.IsSet(constants.EnvDatabaseURL) && viper.GetString(constants.EnvDatabaseURL) != "" {
		requiredEnv = append(requiredEnv, constants.EnvDatabaseURL)
	} else {
		requiredEnv = append(requiredEnv, constants.EnvDatabaseDatabase)
		requiredEnv = append(requiredEnv, constants.EnvDatabaseHost)
		requiredEnv = append(requiredEnv, constants.EnvDatabasePassword)
		requiredEnv = append(requiredEnv, constants.EnvDatabasePort)
		requiredEnv = append(requiredEnv, constants.EnvDatabaseSSLMode)
		requiredEnv = append(requiredEnv, constants.EnvDatabaseUser)
	}

	// k8s required
	k8sRequiredEnv := []string{
		constants.EnvNamespace,
		constants.EnvStoragePVCName,
		constants.EnvPodName,
		constants.EnvKubernetesServiceHost,
	}

	// Docker required
	dockerRequiredEnv := []string{
		constants.EnvHostPersistentDir,
	}

	execEnv := utils.GetExecutorEnvironment()
	if execEnv == string(types.Docker) {
		requiredEnv = append(requiredEnv, dockerRequiredEnv...)
	} else {
		requiredEnv = append(requiredEnv, k8sRequiredEnv...)
	}

	var missing []string
	for _, key := range requiredEnv {
		if !viper.IsSet(key) || viper.GetString(key) == "" {
			missing = append(missing, key)
		}
	}

	if len(missing) > 0 {
		return fmt.Errorf("missing required environment variables: %v", missing)
	}

	return nil
}
