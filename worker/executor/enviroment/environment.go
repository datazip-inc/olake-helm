package environment

import (
	"github.com/datazip-inc/olake-helm/worker/constants"
	"github.com/spf13/viper"
)

type ExecutorEnvironment string

const (
	Kubernetes ExecutorEnvironment = "kubernetes"
	Docker     ExecutorEnvironment = "docker"
)

func GetExecutorEnvironment() string {
	if viper.GetString(constants.EnvKubernetesServiceHost) != "" {
		return string(Kubernetes)
	}
	return string(Docker)
}
