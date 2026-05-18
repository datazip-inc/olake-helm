package temporal

import (
	"context"
	"fmt"
	"time"

	"github.com/datazip-inc/olake-helm/worker/constants"
	"github.com/datazip-inc/olake-helm/worker/utils/logger"
	"github.com/spf13/viper"
	enums "go.temporal.io/api/enums/v1"
	cloudservice "go.temporal.io/cloud-sdk/api/cloudservice/v1"
	namespacepb "go.temporal.io/cloud-sdk/api/namespace/v1"
	operationpb "go.temporal.io/cloud-sdk/api/operation/v1"
	"go.temporal.io/cloud-sdk/cloudclient"
)

// ExternalClient wraps the Temporal Cloud API client
type ExternalClient struct {
	client *cloudclient.Client
}

// NewExternalClient creates a new Temporal Cloud API client
func NewExternalClient() (*ExternalClient, error) {
	apiKey := viper.GetString(constants.EnvTemporalAPIKey)
	if apiKey == "" {
		return nil, fmt.Errorf("TEMPORAL_API_KEY is required for external Temporal")
	}

	client, err := cloudclient.New(cloudclient.Options{
		APIKey: apiKey,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create external Temporal client: %w", err)
	}

	return &ExternalClient{
		client: client,
	}, nil
}

// Close closes the external Temporal API client
func (c *ExternalClient) Close() {
	if c.client != nil {
		c.client.Close()
	}
}

// waitForAsyncOperation polls the async operation until it completes
func (c *ExternalClient) waitForAsyncOperation(ctx context.Context, opID string) error {
	service := c.client.CloudService()
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			resp, err := service.GetAsyncOperation(ctx, &cloudservice.GetAsyncOperationRequest{
				AsyncOperationId: opID,
			})
			if err != nil {
				return fmt.Errorf("failed to get async operation status: %w", err)
			}

			switch state := resp.GetAsyncOperation().GetState(); state {
			case operationpb.AsyncOperation_STATE_FULFILLED:
				return nil
			case operationpb.AsyncOperation_STATE_FAILED:
				return fmt.Errorf("async operation failed: %s", resp.GetAsyncOperation().GetFailureReason())
			case operationpb.AsyncOperation_STATE_CANCELLED:
				return fmt.Errorf("async operation cancelled")
			}
			// Keep waiting for STATE_PENDING or STATE_IN_PROGRESS
		}
	}
}

// SetNamespaceRetention sets the workflow execution retention period via external Temporal API
func (c *ExternalClient) SetNamespaceRetention(ctx context.Context, namespace string, retentionDays int32) error {
	ns, spec, err := c.getNamespaceAndSpec(ctx, namespace)
	if err != nil {
		return err
	}

	if spec.GetRetentionDays() == retentionDays {
		logger.Infof("namespace %s retention is already %d days", namespace, retentionDays)
		return nil
	}

	spec.RetentionDays = retentionDays
	updateResp, err := c.client.CloudService().UpdateNamespace(ctx, &cloudservice.UpdateNamespaceRequest{
		Namespace:       namespace,
		Spec:            spec,
		ResourceVersion: ns.GetResourceVersion(),
	})
	if err != nil {
		return fmt.Errorf("failed to update namespace %s: %w", namespace, err)
	}

	opID := updateResp.GetAsyncOperation().GetId()
	logger.Infof("waiting for namespace %s update operation (id: %s) to complete", namespace, opID)
	if err := c.waitForAsyncOperation(ctx, opID); err != nil {
		return fmt.Errorf("namespace update operation failed: %w", err)
	}

	logger.Infof("namespace %s retention successfully set to %d days in external Temporal", namespace, retentionDays)
	return nil
}

// AddSearchAttributes adds custom search attributes via external Temporal API
func (c *ExternalClient) AddSearchAttributes(ctx context.Context, namespace string, searchAttributes map[string]enums.IndexedValueType) error {
	ns, spec, err := c.getNamespaceAndSpec(ctx, namespace)
	if err != nil {
		return err
	}

	if spec.SearchAttributes == nil {
		spec.SearchAttributes = make(map[string]namespacepb.NamespaceSpec_SearchAttributeType)
	}
	existingAttributes := spec.SearchAttributes

	needsUpdate := false

	for k, v := range searchAttributes {
		if _, exists := existingAttributes[k]; !exists {
			var externalType namespacepb.NamespaceSpec_SearchAttributeType
			switch v {
			case enums.INDEXED_VALUE_TYPE_TEXT:
				externalType = namespacepb.NamespaceSpec_SEARCH_ATTRIBUTE_TYPE_TEXT
			case enums.INDEXED_VALUE_TYPE_KEYWORD:
				externalType = namespacepb.NamespaceSpec_SEARCH_ATTRIBUTE_TYPE_KEYWORD
			case enums.INDEXED_VALUE_TYPE_INT:
				externalType = namespacepb.NamespaceSpec_SEARCH_ATTRIBUTE_TYPE_INT
			case enums.INDEXED_VALUE_TYPE_DOUBLE:
				externalType = namespacepb.NamespaceSpec_SEARCH_ATTRIBUTE_TYPE_DOUBLE
			case enums.INDEXED_VALUE_TYPE_BOOL:
				externalType = namespacepb.NamespaceSpec_SEARCH_ATTRIBUTE_TYPE_BOOL
			case enums.INDEXED_VALUE_TYPE_DATETIME:
				externalType = namespacepb.NamespaceSpec_SEARCH_ATTRIBUTE_TYPE_DATETIME
			case enums.INDEXED_VALUE_TYPE_KEYWORD_LIST:
				externalType = namespacepb.NamespaceSpec_SEARCH_ATTRIBUTE_TYPE_KEYWORD_LIST
			default:
				return fmt.Errorf("unsupported search attribute type for key %s: %v", k, v)
			}
			existingAttributes[k] = externalType
			needsUpdate = true
		}
	}

	if !needsUpdate {
		logger.Infof("search attributes already exist in namespace %s", namespace)
		return nil
	}

	updateResp, err := c.client.CloudService().UpdateNamespace(ctx, &cloudservice.UpdateNamespaceRequest{
		Namespace:       namespace,
		Spec:            spec,
		ResourceVersion: ns.GetResourceVersion(),
	})
	if err != nil {
		return fmt.Errorf("failed to update namespace %s: %w", namespace, err)
	}

	opID := updateResp.GetAsyncOperation().GetId()
	logger.Infof("waiting for namespace %s search attributes update operation (id: %s) to complete", namespace, opID)
	if err := c.waitForAsyncOperation(ctx, opID); err != nil {
		return fmt.Errorf("namespace update operation failed: %w", err)
	}

	logger.Infof("custom search attributes successfully added to namespace %s in external Temporal", namespace)
	return nil
}

func (c *ExternalClient) getNamespaceAndSpec(ctx context.Context, namespace string) (*namespacepb.Namespace, *namespacepb.NamespaceSpec, error) {
	namespaceResp, err := c.client.CloudService().GetNamespace(ctx, &cloudservice.GetNamespaceRequest{
		Namespace: namespace,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get namespace %s: %w", namespace, err)
	}
	ns := namespaceResp.GetNamespace()
	spec := ns.GetSpec()
	if spec == nil {
		return nil, nil, fmt.Errorf("namespace %s has no spec", namespace)
	}
	return ns, spec, nil
}
