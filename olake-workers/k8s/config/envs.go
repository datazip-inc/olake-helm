package config

import (
	"github.com/spf13/viper"
)

var globalViper *viper.Viper

// InitConfig initializes the global Viper instance
func InitConfig() {
	globalViper = viper.New()
	globalViper.AutomaticEnv()
}

// LoadConfig loads configuration using Viper from environment variables
func LoadConfig() (*Config, error) {
	if globalViper == nil {
		InitConfig()
	}

	// Bind environment variables to structured config
	bindEnvironmentVariables(globalViper)

	// Unmarshal into struct
	var config Config
	if err := globalViper.Unmarshal(&config); err != nil {
		return nil, err
	}

	return &config, nil
}

// bindEnvironmentVariables binds environment variables to config paths
func bindEnvironmentVariables(v *viper.Viper) {
	// Database bindings
	_ = v.BindEnv("database.url", "DATABASE_URL")
	_ = v.BindEnv("database.host", "DB_HOST")
	_ = v.BindEnv("database.port", "DB_PORT")
	_ = v.BindEnv("database.user", "DB_USER")
	_ = v.BindEnv("database.password", "DB_PASSWORD")
	_ = v.BindEnv("database.database", "DB_NAME")
	_ = v.BindEnv("database.ssl_mode", "DB_SSLMODE")
	_ = v.BindEnv("database.run_mode", "RUN_MODE")
	_ = v.BindEnv("database.max_open_conns", "DB_MAX_OPEN_CONNS")
	_ = v.BindEnv("database.max_idle_conns", "DB_MAX_IDLE_CONNS")
	_ = v.BindEnv("database.conn_max_lifetime", "DB_CONN_MAX_LIFETIME")

	// Temporal bindings
	_ = v.BindEnv("temporal.address", "TEMPORAL_ADDRESS")
	_ = v.BindEnv("temporal.task_queue", "TEMPORAL_TASK_QUEUE")
	_ = v.BindEnv("temporal.timeout", "TEMPORAL_TIMEOUT")

	// Kubernetes bindings
	_ = v.BindEnv("kubernetes.namespace", "WORKER_NAMESPACE")
	_ = v.BindEnv("kubernetes.image_registry", "IMAGE_REGISTRY")
	_ = v.BindEnv("kubernetes.image_pull_policy", "IMAGE_PULL_POLICY")
	_ = v.BindEnv("kubernetes.service_account", "SERVICE_ACCOUNT")
	_ = v.BindEnv("kubernetes.storage_pvc_name", "OLAKE_STORAGE_PVC_NAME")
	_ = v.BindEnv("kubernetes.job_service_account", "JOB_SERVICE_ACCOUNT_NAME")
	_ = v.BindEnv("kubernetes.job_mapping_raw", "OLAKE_JOB_MAPPING")
	_ = v.BindEnv("kubernetes.secret_key", "OLAKE_SECRET_KEY")
	_ = v.BindEnv("kubernetes.labels.version", "WORKER_VERSION")

	// Worker bindings
	_ = v.BindEnv("worker.max_concurrent_activities", "MAX_CONCURRENT_ACTIVITIES")
	_ = v.BindEnv("worker.max_concurrent_workflows", "MAX_CONCURRENT_WORKFLOWS")
	_ = v.BindEnv("worker.heartbeat_interval", "HEARTBEAT_INTERVAL")
	_ = v.BindEnv("worker.worker_identity", "POD_NAME")

	// Timeout bindings
	_ = v.BindEnv("timeouts.activity.discover", "TIMEOUT_ACTIVITY_DISCOVER")
	_ = v.BindEnv("timeouts.activity.test", "TIMEOUT_ACTIVITY_TEST")
	_ = v.BindEnv("timeouts.activity.sync", "TIMEOUT_ACTIVITY_SYNC")

	// Logging bindings
	_ = v.BindEnv("logging.level", "LOG_LEVEL")
	_ = v.BindEnv("logging.format", "LOG_FORMAT")
}
