package metrics

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

// SyncFileStats holds the values self-reported by the sync process in stats.json.
type SyncFileStats struct {
	SyncedRecords  int64
	BytesCommitted int64
	CPUUtilization float64 // 0-1 ratio; key absent on the first ticks
	MemoryBytes    int64   // parsed from "Memory" ("%d mb"); 0 if absent or unparseable
}

// ReadSyncStats parses stats.json, which the sync CLI rewrites every 2s.
func ReadSyncStats(workdir string) (*SyncFileStats, error) {
	data, err := os.ReadFile(filepath.Join(workdir, "stats.json"))
	if err != nil {
		return nil, err
	}
	var raw map[string]any
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, err
	}
	s := &SyncFileStats{}
	if v, ok := raw["Synced Records"].(float64); ok {
		s.SyncedRecords = int64(v)
	}
	if v, ok := raw["Bytes Committed"].(float64); ok {
		s.BytesCommitted = int64(v)
	}
	if v, ok := raw["CPU Utilization"].(float64); ok {
		s.CPUUtilization = v
	}
	if str, ok := raw["Memory"].(string); ok {
		var mb int64
		if _, err := fmt.Sscanf(str, "%d mb", &mb); err == nil {
			s.MemoryBytes = mb * 1024 * 1024
		}
	}
	return s, nil
}
