package temporal

import (
	"context"
	"fmt"
	"time"

	"github.com/datazip-inc/olake-helm/worker/constants"
	"github.com/datazip-inc/olake-helm/worker/utils"
	"github.com/datazip-inc/olake-helm/worker/utils/logger"
	"github.com/spf13/viper"
	namespacepb "go.temporal.io/api/namespace/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"google.golang.org/protobuf/types/known/durationpb"
)

// Temporal provides methods to interact with Temporal
type Temporal struct {
	client client.Client
}

// NewClient creates a new Temporal client
func NewClient() (*Temporal, error) {
	var temporalClient *Temporal

	err := utils.RetryWithBackoff(func() error {
		client, err := client.Dial(client.Options{
			HostPort: viper.GetString(constants.EnvTemporalAddress),
			Logger:   logger.Log(context.Background()),
			Namespace: constants.DefaultTemporalNamespace,
		})
		if err != nil {
			return err
		}
		temporalClient = &Temporal{
			client: client,
		}
		return nil
	}, 3, time.Second)
	if err != nil {
		return nil, fmt.Errorf("failed to create Temporal client: %s", err)
	}

	return temporalClient, nil
}

// Close closes the Temporal client
func (t *Temporal) Close() {
	if t.client != nil {
		t.client.Close()
	}
}
func (t *Temporal) GetClient() client.Client {
	return t.client
}

// SetWorkflowRetentionPeriod sets the workflow execution retention period for the default namespace.
// This ensures workflow history is available for debugging (defaults to 7 days).
// Handles both fresh installs and upgrades from shorter retention periods.
// Fatal: worker fails to start if this fails.
func (t *Temporal) SetWorkflowRetentionPeriod(ctx context.Context) error {
	retentionPeriod, err := time.ParseDuration(viper.GetString(constants.EnvTemporalRetentionPeriod))
	if err != nil {
		return fmt.Errorf("failed to parse retention string: %s", err)
	}

	_, err = t.client.WorkflowService().UpdateNamespace(ctx, &workflowservice.UpdateNamespaceRequest{
		Namespace: constants.DefaultTemporalNamespace,
		Config: &namespacepb.NamespaceConfig{
			WorkflowExecutionRetentionTtl: durationpb.New(retentionPeriod),
		},
	})
	if err != nil {
		return fmt.Errorf("failed to update namespace %s retention: %s", constants.DefaultTemporalNamespace, err)
	}

	logger.Infof("namespace %s retention set to %s", constants.DefaultTemporalNamespace, retentionPeriod)
	return nil
}
