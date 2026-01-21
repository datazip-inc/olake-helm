package temporal

import (
	"context"
	"fmt"
	"time"

	"github.com/datazip-inc/olake-helm/worker/constants"
	"github.com/datazip-inc/olake-helm/worker/utils"
	"github.com/datazip-inc/olake-helm/worker/utils/logger"
	"github.com/spf13/viper"
	"go.temporal.io/sdk/client"
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
			Namespace: "default",
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
