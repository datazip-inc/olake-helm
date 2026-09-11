package types

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

type Command string

const (
	Discover         Command = "discover"
	Spec             Command = "spec"
	Check            Command = "check"
	Sync             Command = "sync"
	ClearDestination Command = "clear-destination"
)

type JobConfig struct {
	Name string `json:"name"`
	Data string `json:"data"`
}

// FileConfig represents a configuration file to be written
type FileConfig struct {
	Name string
	Data string
}

// LoggingConfig contains logging settings
type LoggingConfig struct {
	Level  string `mapstructure:"level"`
	Format string `mapstructure:"format"`
}

type JobData struct {
	JobName            string
	ProjectID          string
	Source             string
	Destination        string
	Streams            string
	SelectedStreams    string
	State              string
	Version            string // source version
	Driver             string
	Frequency          string
	CreatedAt          time.Time
	DestinationVersion string
	SourceName         string
	DestinationName    string
	// AdvancedSettings is the job's advanced_settings jsonb column, read as text.
	// Empty when the column is NULL.
	AdvancedSettings string
}

// IndexRequired reports whether the job asked for the per-job Pebble index
// volume, via `index_required` in its advanced settings. Unparseable settings
// are an error rather than a false: a job that asked for an index and lost it to
// a typo would otherwise rebuild the index every run with nothing to say why.
func (j JobData) IndexRequired() (bool, error) {
	if strings.TrimSpace(j.AdvancedSettings) == "" {
		return false, nil
	}

	var settings struct {
		IndexRequired bool `json:"index_required"`
	}
	if err := json.Unmarshal([]byte(j.AdvancedSettings), &settings); err != nil {
		return false, fmt.Errorf("failed to parse job advanced settings: %s", err)
	}

	return settings.IndexRequired, nil
}

type WebhookNotificationArgs struct {
	JobID        int
	ProjectID    string
	LastRunTime  time.Time
	ErrorMessage string
}

type Result struct {
	OK      bool
	Message string
}

type ProjectSettings struct {
	ID              int
	ProjectID       string
	WebhookAlertURL string
}

// IndicatorRequest is GitOps-only: sent from olake-ui via IndicatorWorkflow to spawn/delete failure indicators.
type IndicatorRequest struct {
	Action       string `json:"action"`        // spawn | delete
	Name         string `json:"name"`          // pod/container name (DNS-1123)
	Namespace    string `json:"namespace"`     // K8s namespace; ignored in Docker mode
	Kind         string `json:"kind"`          // source | destination | job | streams
	ResourceName string `json:"resource_name"` // originating ConfigMap or Secret name
	Message      string `json:"message"`       // error text for spawn
}
