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
	// IndexRequired mirrors the job's `index_required` advanced setting. Only a
	// job that asks for it gets an index volume, so a deployment that does not
	// use the Iceberg delete path provisions no storage at all.
	IndexRequired bool `json:"index_required"`

	// k8s specific fields
	HeartbeatFunc func(context.Context, ...interface{}) `json:"-"`
}

type ExecutorResponse struct {
	Response string `json:"response"`
}
