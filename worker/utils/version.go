package utils

import (
	"strings"

	"github.com/datazip-inc/olake-helm/worker/constants"
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

// UseSchemaSplit reports whether schema_config should be mounted and --schema passed.
func UseSchemaSplit(version string) bool {
	return CompareAtLeast(version, constants.MinSchemaSplitVersion)
}
