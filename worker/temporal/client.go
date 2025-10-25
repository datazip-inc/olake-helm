package temporal

import (
	"fmt"

	"github.com/datazip-inc/olake-helm/worker/constants"
	"github.com/spf13/viper"
	"go.temporal.io/sdk/client"
)

// Temporal provides methods to interact with Temporal
type Temporal struct {
	client client.Client
}

// NewClient creates a new Temporal client
func NewClient() (*Temporal, error) {
	c, err := client.Dial(client.Options{
		HostPort: viper.GetString(constants.EnvTemporalAddress),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create Temporal client: %s", err)
	}

	return &Temporal{
		client: c,
	}, nil
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
