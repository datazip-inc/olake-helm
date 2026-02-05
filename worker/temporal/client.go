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

// UpdateRetention updates the workflow execution retention period for the default namespace.
func (t *Temporal) UpdateRetention(ctx context.Context, retentionString string) error {
	retentionPeriod, err := time.ParseDuration(retentionString)
	if err != nil {
		return fmt.Errorf("failed to parse retention string: %w", err)
	}

	_, err = t.client.WorkflowService().UpdateNamespace(ctx, &workflowservice.UpdateNamespaceRequest{
		Namespace: constants.DefaultTemporalNamespace,
		Config: &namespacepb.NamespaceConfig{
			WorkflowExecutionRetentionTtl: durationpb.New(retentionPeriod),
		},
	})
	if err != nil {
		return fmt.Errorf("failed to update namespace %q retention: %w", constants.DefaultTemporalNamespace, err)
	}

	logger.Infof("namespace %q retention set to %s", constants.DefaultTemporalNamespace, retentionPeriod)
	return nil
}
