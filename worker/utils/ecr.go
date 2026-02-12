package utils

import (
	"context"
	"encoding/base64"
	"fmt"
	"os/exec"
	"regexp"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/ecr"
)

// ParseECRDetails extracts account ID, region, and repository name from ECR URI
// Example:
//	Input:  "123456789012.dkr.ecr.us-west-2.amazonaws.com/olakego/source-mysql:latest"
//	Output: accountID = "123456789012"
//	        region    = "us-west-2"
//	        repoName  = "olakego/source-mysql:latest"
func ParseECRDetails(fullImageName string) (accountID, region, repoName string, err error) {
	// handle private and public ecr and china ecr
	privateRe := regexp.MustCompile(`^(\d+)\.dkr\.ecr\.([a-z0-9-]+)\.amazonaws\.com(\.cn)?/(.+)$`)
	publicRe := regexp.MustCompile(`^public\.ecr\.aws/(.+)$`)

	if matches := privateRe.FindStringSubmatch(fullImageName); len(matches) == 5 {
		return matches[1], matches[2], matches[4], nil
	}

	if matches := publicRe.FindStringSubmatch(fullImageName); len(matches) == 2 {
		// Public ECR doesn't have accountID/region
		return "public", "global", matches[1], nil
	}

	return "", "", "", fmt.Errorf("failed to parse ECR URI: %s", fullImageName)
}

// DockerLoginECR logs in to an AWS ECR repository using the AWS SDK
func DockerLoginECR(ctx context.Context, region, registryID string) error {
	// Load AWS credentials & config
	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(region))
	if err != nil {
		return fmt.Errorf("failed to load AWS config: %s", err)
	}

	client := ecr.NewFromConfig(cfg)

	// Get ECR authorization token
	authResp, err := client.GetAuthorizationToken(ctx, &ecr.GetAuthorizationTokenInput{
		RegistryIds: []string{registryID},
	})
	if err != nil {
		return fmt.Errorf("failed to get ECR authorization token: %s", err)
	}

	if len(authResp.AuthorizationData) == 0 {
		return fmt.Errorf("no authorization data received from ECR")
	}
	authData := authResp.AuthorizationData[0]

	// Decode token
	decodedToken, err := base64.StdEncoding.DecodeString(aws.ToString(authData.AuthorizationToken))
	if err != nil {
		return fmt.Errorf("failed to decode authorization token: %s", err)
	}

	parts := strings.SplitN(string(decodedToken), ":", 2)
	if len(parts) != 2 {
		return fmt.Errorf("invalid authorization token format")
	}

	username := parts[0]
	password := parts[1]
	registryURL := aws.ToString(authData.ProxyEndpoint) // e.g., https://678819669750.dkr.ecr.ap-south-1.amazonaws.com

	// Perform docker login
	cmd := exec.CommandContext(ctx, "docker", "login", "-u", username, "--password-stdin", registryURL)
	cmd.Stdin = strings.NewReader(password)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("docker login failed: %s\nOutput: %s", err, output)
	}

	return nil
}
