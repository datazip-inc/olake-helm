package temporal

import (
	"context"
	"crypto/tls"
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

	namespace := utils.GetTemporalNamespace()

	err := utils.RetryWithBackoff(func() error {
		opts := client.Options{
			HostPort:  viper.GetString(constants.EnvTemporalAddress),
			Logger:    logger.Log(context.Background()),
			Namespace: namespace,
		}

		if viper.GetBool(constants.EnvTemporalEnableTLS) {
			opts.ConnectionOptions = client.ConnectionOptions{
				TLS: &tls.Config{},
			}
		}

		if apiKey := viper.GetString(constants.EnvTemporalAPIKey); apiKey != "" {
			opts.Credentials = client.NewAPIKeyStaticCredentials(apiKey)
		}

		client, err := client.Dial(opts)
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

// SetWorkflowRetentionPeriod sets the workflow execution retention period for the namespace.
// This ensures workflow history is available for debugging (defaults to 7 days).
// Handles both fresh installs and upgrades from shorter retention periods.
// Fatal: worker fails to start if this fails.
func (t *Temporal) SetWorkflowRetentionPeriod(ctx context.Context) error {
	retentionPeriod, err := time.ParseDuration(viper.GetString(constants.EnvTemporalRetentionPeriod))
	if err != nil {
		return fmt.Errorf("failed to parse retention string: %s", err)
	}

	namespace := constants.DefaultTemporalNamespace

	if utils.IsTemporalCloud() {
		externalClient, err := NewExternalClient()
		if err != nil {
			return fmt.Errorf("failed to create external Temporal client: %w", err)
		}
		defer externalClient.Close()

		namespace = utils.GetTemporalNamespace()
		retentionDays := int32(retentionPeriod.Hours() / 24)
		if retentionDays < 1 {
			retentionDays = 1
		}
		return externalClient.SetNamespaceRetention(ctx, namespace, retentionDays)
	}

	_, err = t.client.WorkflowService().UpdateNamespace(ctx, &workflowservice.UpdateNamespaceRequest{
		Namespace: namespace,
		Config: &namespacepb.NamespaceConfig{
			WorkflowExecutionRetentionTtl: durationpb.New(retentionPeriod),
		},
	})
	if err != nil {
		return fmt.Errorf("failed to update namespace %s retention: %s", namespace, err)
	}

	logger.Infof("namespace %s retention set to %s", namespace, retentionPeriod)
	return nil
}
