package config

import (
	"github.com/datazip-inc/olake-helm/worker/logger"
	"github.com/spf13/viper"
)

func Init() {
	viper.AutomaticEnv()

	setDefaults()

	logger.Infof("Initialized config")

	// TODO: check if we need this
	// if err := requiredEnvVars(); err != nil {
	// 	logger.Fatalf("Failed to initialize config: %v", err)
	// }
}

// setDefaults sets default values for configuration
func setDefaults() {
	// Temporal defaults
	viper.SetDefault("TEMPORAL_ADDRESS", "localhost:7233")

	// Worker defaults
	viper.SetDefault("EXECUTOR_ENVIRONMENT", "docker")
	viper.SetDefault("HEALTH_PORT", 8090)
	viper.SetDefault("LOG_RETENTION_PERIOD", 30)

	// Kubernetes defaults
	viper.SetDefault("WORKER_NAMESPACE", "default")

	// Logging defaults
	viper.SetDefault("LOG_LEVEL", "info")
	viper.SetDefault("LOG_FORMAT", "console")

	// API defaults
	viper.SetDefault("OLAKE_CALLBACK_URL", "http://host.docker.internal:8000/internal/worker/callback")

	// Docker defaults
	viper.SetDefault("DOCKER_IMAGE_PREFIX", "olakego/source")

	// database
	viper.SetDefault("DB_HOST", "postgresql")
	viper.SetDefault("DB_PORT", 5432)
	viper.SetDefault("DB_USER", "temporal")
	viper.SetDefault("DB_PASSWORD", "temporal")
	viper.SetDefault("DB_NAME", "postgres")
	viper.SetDefault("DB_SSLMODE", "disable")
	viper.SetDefault("RUN_MODE", "dev")
}

// TODO: check if we need this
// func requiredEnvVars() error {
// 	// Common required env vars
// 	required := []string{
// 		constants.EnvTemporalAddress,
// 		constants.EnvExecutorEnvironment,
// 		constants.EnvContainerPersistentDir,
// 	}

// 	for _, key := range required {
// 		if !viper.IsSet(key) || viper.GetString(key) == "" {
// 			return fmt.Errorf("required config %q is not set", key)
// 		}
// 	}

// 	// Environment-specific requirements
// 	executorEnv := viper.GetString(constants.EnvExecutorEnvironment)

// 	if executorEnv == "kubernetes" {
// 		k8sRequired := []string{
// 			constants.EnvNamespace,
// 			constants.EnvStoragePVCName,
// 		}
// 		for _, key := range k8sRequired {
// 			if !viper.IsSet(key) || viper.GetString(key) == "" {
// 				return fmt.Errorf("kubernetes mode requires %q to be set", key)
// 			}
// 		}
// 	}

// 	if executorEnv == "docker" {
// 		dockerRequired := []string{
// 			constants.EnvHostPersistentDir,
// 		}

// 		for _, key := range dockerRequired {
// 			if !viper.IsSet(key) || viper.GetString(key) == "" {
// 				return fmt.Errorf("docker mode requires %q to be set", key)
// 			}
// 		}

// 	}

// 	return nil
// }
