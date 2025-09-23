package temporal

import (
	"fmt"

	"github.com/datazip-inc/olake-helm/worker/constants"
	"github.com/datazip-inc/olake-helm/worker/utils"
	"go.temporal.io/sdk/client"
)

var (
	TemporalHostPort string
)

func init() {
	TemporalHostPort = utils.GetEnv(constants.EnvTemporalAddress, constants.DefaultTemporalAddress)
}

// Client provides methods to interact with Temporal
type Client struct {
	temporalClient client.Client
}

// NewClient creates a new Temporal client
func NewClient() (*Client, error) {
	c, err := client.Dial(client.Options{
		HostPort: TemporalHostPort,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create Temporal client: %v", err)
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
