package docker

import (
	"context"
	"crypto/sha256"
	"fmt"
	"path/filepath"

	"github.com/datazip-inc/olake-helm/worker/constants"
	"github.com/datazip-inc/olake-helm/worker/executor"
	"github.com/datazip-inc/olake-helm/worker/utils"
	"github.com/docker/docker/client"
)

type DockerExecutor struct {
	client     *client.Client
	workingDir string
}

func NewDockerExecutor() (*DockerExecutor, error) {
	client, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		return nil, fmt.Errorf("failed to create docker client: %s", err)
	}

	return &DockerExecutor{client: client, workingDir: constants.DefaultConfigDir}, nil
}

func (d *DockerExecutor) Execute(ctx context.Context, req *executor.ExecutionRequest) (map[string]interface{}, error) {
	subDir := utils.Ternary(req.Command == "sync", fmt.Sprintf("%x", sha256.Sum256([]byte(req.WorkflowID))), req.WorkflowID).(string)
	workDir, err := d.SetupWorkDirectory(subDir)
	if err != nil {
		return nil, err
	}

	if len(req.Configs) > 0 {
		if err := WriteConfigFiles(workDir, req.Configs); err != nil {
			return nil, err
		}
		defer DeleteConfigFiles(workDir, req.Configs)
	}

	rawLogs, err := d.RunContainer(ctx, req, workDir)
	if err != nil {
		return nil, err
	}

	if req.Command == "sync" {
		stateFile := filepath.Join(workDir, "state.json")
		if err := UpdateStateFile(req.JobID, stateFile); err != nil {
			return nil, err
		}
	}

	// if output file is specified, return it
	if req.OutputFile != "" {
		response, err := ReadJSONFile(filepath.Join(workDir, req.OutputFile))
		if err != nil {
			return nil, fmt.Errorf("failed to parse JSON file: %s", err)
		}
		return map[string]interface{}{
			"response": response,
		}, nil
	}

	// Default: return raw logs
	return map[string]interface{}{
		"response": rawLogs,
	}, nil
}

func (d *DockerExecutor) Close() error {
	return d.client.Close()
}

func init() {
	executor.RegisteredExecutors[executor.Docker] = func() (executor.Executor, error) {
		return NewDockerExecutor()
	}
}
