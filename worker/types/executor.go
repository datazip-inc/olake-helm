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
	Command       Command       `json:"command"`
	ConnectorType string        `json:"connector_type"`
	Version       string        `json:"version"`
	Args          []string      `json:"args"`
	Configs       []JobConfig   `json:"configs"`
	WorkflowID    string        `json:"workflow_id"`
	ProjectID     string        `json:"project_id"`
	JobID         int           `json:"job_id"`
	Timeout       time.Duration `json:"timeout"`
	OutputFile    string        `json:"output_file"` // to get the output file from the workflow

	Options *ExecutionOptions `json:"options,omitempty"`

	// k8s specific fields
	HeartbeatFunc func(context.Context, ...interface{}) `json:"-"`
}

// ExecutionOptions are optional parameters for the execution request
// used to customize the execution behavior
type ExecutionOptions struct {
	TempPath      string `json:"temp_path,omitempty"`
	UseEmptyState bool   `json:"use_empty_state,omitempty"`
}

type ExecutorResponse struct {
	Response string `json:"response"`
}
