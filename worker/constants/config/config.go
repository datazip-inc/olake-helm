package config

import (
	"fmt"

	"github.com/datazip-inc/olake-helm/worker/constants"
	environment "github.com/datazip-inc/olake-helm/worker/executor/enviroment"
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
	viper.SetDefault("OLAKE_CALLBACK_URL", "http://host.docker.internal:8000/internal/worker/callback")

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
	var missing []string

	// Common required env vars
	required := []string{
		constants.EnvCallbackURL,
		constants.EnvDatabaseDatabase,
		constants.EnvDatabaseHost,
		constants.EnvDatabasePassword,
		constants.EnvDatabasePort,
		constants.EnvDatabaseRunMode,
		constants.EnvDatabaseSSLMode,
		constants.EnvDatabaseUser,
	}
	for _, key := range required {
		if !viper.IsSet(key) || viper.GetString(key) == "" {
			missing = append(missing, key)
		}
	}

	execEnv := environment.GetExecutorEnvironment()

	if execEnv == "kubernetes" {
		k8sRequired := []string{
			constants.EnvNamespace,
			constants.EnvStoragePVCName,
			constants.EnvPodName,
			constants.EnvKubernetesServiceHost,
		}
		for _, key := range k8sRequired {
			if !viper.IsSet(key) || viper.GetString(key) == "" {
				missing = append(missing, key)
			}
		}
	}

	if execEnv == "docker" {
		dockerRequired := []string{
			constants.EnvHostPersistentDir,
		}
		for _, key := range dockerRequired {
			if !viper.IsSet(key) || viper.GetString(key) == "" {
				missing = append(missing, key)
			}
		}
	}

	if len(missing) > 0 {
		return fmt.Errorf("missing required environment variables: %v", missing)
	}

	return nil
}
