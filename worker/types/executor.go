package types

import (
	"context"
	"time"
)

type ExecutorEnvironment string

const (
	Kubernetes ExecutorEnvironment = "kubernetes"
	Docker     ExecutorEnvironment = "docker"
)

type ExecutionRequest struct {
	Type          string        `json:"type"`
	Command       Command       `json:"command"`
	ConnectorType string        `json:"connector_type"`
	Version       string        `json:"version"`
	Args          []string      `json:"args"`
	Configs       []JobConfig   `json:"configs"`
	WorkflowID    string        `json:"workflow_id"`
	JobID         int           `json:"job_id"`
	ProjectID     string        `json:"project_id"`
	Timeout       time.Duration `json:"timeout"`
	OutputFile    string        `json:"output_file"`
	TempPath      string        `json:"temp_path"`

	// k8s specific fields
	HeartbeatFunc func(context.Context, ...interface{}) `json:"-"`
}

type ExecutorResponse struct {
	Response string `json:"response"`
}
