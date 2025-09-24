package docker

import (
	"context"
	"fmt"
	"path/filepath"

	"github.com/datazip-inc/olake-helm/worker/constants"
	"github.com/datazip-inc/olake-helm/worker/executor"
	"github.com/docker/docker/client"
)

type DockerExecutor struct {
	client *client.Client
}

func NewDockerExecutor() (*DockerExecutor, error) {
	client, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		return nil, fmt.Errorf("failed to create docker client: %s", err)
	}

	return &DockerExecutor{client: client}, nil
}

func (d *DockerExecutor) Execute(ctx context.Context, req *executor.ExecutionRequest) (map[string]interface{}, error) {
	// TODO: check env for config path
	workDir, err := SetupWorkDirectory(constants.DefaultConfigDir, req.WorkflowID)
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
