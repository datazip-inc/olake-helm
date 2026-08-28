package metrics

import (
	"context"
	"strconv"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// pollInterval paces the stats.json reads. The sync CLI rewrites the file
// every 2s; polling at the same cadence keeps gauges as fresh as the source
// allows, at the cost of one small file read per running sync per tick.
const pollInterval = 2 * time.Second

type SyncLabels struct {
	JobID           int
	JobName         string
	SourceName      string
	DestinationName string
}

func (l SyncLabels) promLabels() prometheus.Labels {
	return prometheus.Labels{
		"job_id":           strconv.Itoa(l.JobID),
		"job_name":         l.JobName,
		"source_name":      l.SourceName,
		"destination_name": l.DestinationName,
	}
}

type activeSync struct {
	labels  SyncLabels
	workdir string
	started time.Time
}

// Tracker maintains the per-job sync gauges. One central poll loop serves all
// in-flight syncs; Start/Finish bracket each run from SyncActivity, which
// blocks for the sync's entire lifetime.
type Tracker struct {
	mu     sync.Mutex
	active map[string]*activeSync // key: workflowID
}

func NewTracker(ctx context.Context) *Tracker {
	t := &Tracker{active: map[string]*activeSync{}}
	go t.pollLoop(ctx)
	return t
}

// Start registers a sync run and initialises its series. Called from
// SyncActivity right before executor.Execute.
func (t *Tracker) Start(workflowID, workdir string, labels SyncLabels) {
	now := time.Now()
	t.mu.Lock()
	t.active[workflowID] = &activeSync{labels: labels, workdir: workdir, started: now}
	t.mu.Unlock()

	lp := labels.promLabels()
	SyncStatus.With(lp).Set(StatusRunning)
	SyncStartTime.With(lp).Set(float64(now.Unix()))
	SyncDuration.With(lp).Set(0)
	SyncRecordsIngested.With(lp).Set(0)
	SyncBytesCommitted.With(lp).Set(0)
}

// Finish freezes the run's series at its final values. Called from
// SyncActivity after executor.Execute returns. The series persist (not
// deleted) so "did last night's run succeed" stays answerable at any time.
func (t *Tracker) Finish(workflowID string, succeeded bool) {
	t.mu.Lock()
	s, ok := t.active[workflowID]
	if ok {
		delete(t.active, workflowID)
	}
	t.mu.Unlock()
	if !ok {
		return
	}
	lp := s.labels.promLabels()
	t.updateFromFile(s, lp) // final read: freeze the last totals
	SyncDuration.With(lp).Set(time.Since(s.started).Seconds())
	if succeeded {
		SyncStatus.With(lp).Set(StatusSucceeded)
	} else {
		SyncStatus.With(lp).Set(StatusFailed)
	}
}

func (t *Tracker) pollLoop(ctx context.Context) {
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			t.pollActive()
		}
	}
}

// pollActive refreshes duration and stats.json-derived gauges for every
// in-flight sync.
func (t *Tracker) pollActive() {
	t.mu.Lock()
	snapshot := make([]*activeSync, 0, len(t.active))
	for _, s := range t.active {
		snapshot = append(snapshot, s)
	}
	t.mu.Unlock()
	for _, s := range snapshot {
		lp := s.labels.promLabels()
		SyncDuration.With(lp).Set(time.Since(s.started).Seconds())
		t.updateFromFile(s, lp)
	}
}

func (t *Tracker) updateFromFile(s *activeSync, lp prometheus.Labels) {
	stats, err := ReadSyncStats(s.workdir)
	if err != nil {
		return // stats.json not written yet, or transient rewrite race — keep last values
	}
	SyncRecordsIngested.With(lp).Set(float64(stats.SyncedRecords))
	SyncBytesCommitted.With(lp).Set(float64(stats.BytesCommitted))
	if stats.CPUUtilization > 0 {
		SyncCPURatio.With(lp).Set(stats.CPUUtilization)
	}
	if stats.MemoryBytes > 0 {
		SyncMemoryBytes.With(lp).Set(float64(stats.MemoryBytes))
	}
}
