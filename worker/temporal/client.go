package temporal

import (
	"fmt"

	"github.com/datazip-inc/olake-helm/worker/constants"
	"github.com/spf13/viper"
	"go.temporal.io/sdk/client"
)

// Client provides methods to interact with Temporal
type Client struct {
	temporalClient client.Client
}

// NewClient creates a new Temporal client
func NewClient() (*Client, error) {
	c, err := client.Dial(client.Options{
		HostPort: viper.GetString(constants.EnvTemporalAddress),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create Temporal client: %s", err)
	}

	return &Client{
		temporalClient: c,
	}, nil
}

// Close closes the Temporal client
func (c *Client) Close() {
	if c.temporalClient != nil {
		c.temporalClient.Close()
	}
}
func (c *Client) GetClient() client.Client {
	return c.temporalClient
}
