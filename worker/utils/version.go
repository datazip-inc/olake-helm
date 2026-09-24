package utils

import (
	"strings"

	"github.com/datazip-inc/olake-helm/worker/constants"
	"github.com/spf13/viper"
	"golang.org/x/mod/semver"
)

func CompareAtLeast(version, minVersion string) bool {
	version = strings.TrimSpace(version)
	if version == "" {
		return false
	}
	if !strings.HasPrefix(version, "v") {
		version = "v" + version
	}
	if !semver.IsValid(version) {
		return false
	}

	minVersion = strings.TrimSpace(minVersion)
	if !strings.HasPrefix(minVersion, "v") {
		minVersion = "v" + minVersion
	}
	if !semver.IsValid(minVersion) {
		return false
	}
	return semver.Compare(version, minVersion) >= 0
}

// CustomDriverVersion returns the custom driver version used to test OLake in development, or
// empty outside development. olake-ui reads the same variables, so both sides gate alike.
func CustomDriverVersion() string {
	if strings.EqualFold(strings.TrimSpace(viper.GetString(constants.EnvAppEnvironment)), constants.AppEnvDevelopment) {
		return viper.GetString(constants.EnvCustomDriverVersion)
	}
	return ""
}

// SupportsSplitStreams reports whether the source driver accepts the split catalog flags
// (--available-streams / --selected-streams). Older drivers only read streams.json.
func SupportsSplitStreams(version string) bool {
	return CustomDriverVersion() != "" || CompareAtLeast(version, constants.MinSplitStreamsVersion)
}
