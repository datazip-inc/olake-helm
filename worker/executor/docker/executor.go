package docker

import (
	"context"
	"crypto/sha256"
	"fmt"
	"path/filepath"

	"github.com/datazip-inc/olake-helm/worker/executor"
	"github.com/datazip-inc/olake-helm/worker/types"
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

	// TODO: Check if we can separate hostPersistence and containerConfigPath
	// TODO: check backward compatibility after changing ENV VARIABLE NAME
	return &DockerExecutor{client: client, workingDir: utils.GetConfigDir()}, nil
}

func (d *DockerExecutor) Execute(ctx context.Context, req *executor.ExecutionRequest) (map[string]interface{}, error) {
	subDir := utils.Ternary(req.Command == types.Sync, fmt.Sprintf("%x", sha256.Sum256([]byte(req.WorkflowID))), req.WorkflowID).(string)
	workDir, err := utils.SetupWorkDirectory(d.workingDir, subDir)
	if err != nil {
		return nil, err
	}

	if err := utils.WriteConfigFiles(workDir, req.Configs); err != nil {
		return nil, err
	}
	// Question: Telemetry requires streams.json, so cleaning up fails telemetry. Do we need cleanup?
	// defer utils.CleanupConfigFiles(workDir, req.Configs)

	out, err := d.RunContainer(ctx, req, workDir)
	if err != nil {
		return nil, err
	}

	if req.OutputFile != "" {
		fileContent, err := utils.ReadFile(filepath.Join(workDir, req.OutputFile))
		if err != nil {
			return nil, fmt.Errorf("failed to parse JSON file: %s", err)
		}
		return map[string]interface{}{
			"response": fileContent,
		}, nil
	}

	return map[string]interface{}{
		"response": out,
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
