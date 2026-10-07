package utils

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/acarl005/stripansi"
	"github.com/datazip-inc/olake-helm/worker/constants"
	"github.com/datazip-inc/olake-helm/worker/storage"
	"github.com/datazip-inc/olake-helm/worker/utils/logger"
)

const (
	chunkTimestampLayout = "2006-01-02T150405.999999999Z"
	logChunkSeqMarker    = "-seq"
)

type PodLogBuffer struct {
	path                  string
	s3LogDir              string // S3 key prefix for this log directory (storage.S3Key(workDir, logRelDir))
	filenamePrefix        string // chunk filename prefix, e.g. connector- or worker-
	counter               int
	lastLocalLogTimestamp time.Time // k8s/docker line timestamp for chunk naming
	lastLocalLogSeq       uint64    // seq of the last line in the buffered chunk (stream checkpoint)
}

// COMMENT: optimize per-line open/stat and synchronous PutObject on Flush (keep the file handle open, track size in memory, bounded async upload queue).
// resumePoint is the S3 snapshot used to continue log collection.
type resumePoint struct {
	logDir              string
	lastPodLogTimestamp time.Time
	lastPodLogSeq       uint64 // last seen seq of the latest chunk (stream checkpoint); 0 if none
	chunkCounter        int    // highest uploaded chunk number; 0 if none exist yet
}

// streamCheckpoint skips log lines that were already uploaded when a stream is re-read from
// its resume point (connector reconnect, final catch-up, resume after restart, worker recovery).
// First read (no checkpoint seq): every line is accepted. Otherwise lines are skipped until the
// checkpoint line (seq == checkpoint seq) shows up, or until log time passes the checkpoint
// timestamp + 1s if it is missing; then every line is accepted. There is no seq ordering check,
// so out-of-order lines after the checkpoint are kept.
// Accepted tradeoffs: an out-of-order line printed before the checkpoint is skipped, and with the
// checkpoint missing (e.g. log rotation) up to 1s of lines can be lost.
type streamCheckpoint struct {
	seq      uint64
	deadline time.Time
	reached  bool
}

func newStreamCheckpoint(seq uint64, timestamp time.Time) *streamCheckpoint {
	return &streamCheckpoint{
		seq:      seq,
		deadline: timestamp.Add(time.Second),
		reached:  seq == 0,
	}
}

// accept reports whether the line should be written.
func (c *streamCheckpoint) accept(normalizedLogLine podLogLineEntry) bool {
	if c.reached {
		return true
	}
	if normalizedLogLine.Seq == c.seq {
		// checkpoint line was already uploaded
		c.reached = true
		return false
	}
	if !normalizedLogLine.PodLogTimestamp.After(c.deadline) {
		return false
	}
	// checkpoint line not found within 1s: accept from here
	c.reached = true
	return true
}

// logChunkMetadata holds resume fields parsed from a chunked log filename.
type logChunkMetadata struct {
	counter   int
	timestamp time.Time
	seq       uint64
}

// podLogLineEntry is a parsed pod log line: JSON fields, k8s/docker timestamp, and normalized line text.
type podLogLineEntry struct {
	WorkflowID string `json:"workflowID"`
	Command    string `json:"command"`
	Seq        uint64 `json:"seq"`
	// PodLogTimestamp is the chunk/resume timestamp. Worker JSON fills it from
	// zerolog "time"; parsePodLogLine then overwrites it with the docker/k8s prefix.
	PodLogTimestamp   time.Time `json:"time"`
	normalizedLogLine string
}

// NewPodLogBuffer creates a local staging buffer for S3 log chunks.
func NewPodLogBuffer(workDir, logRelDir, filenamePrefix string, counter int) (*PodLogBuffer, error) {
	localDir := PodLogLocalDir(workDir)
	if err := CreateDirectory(localDir); err != nil {
		return nil, err
	}

	s3LogDir, err := storage.S3Key(workDir, logRelDir, false)
	if err != nil {
		return nil, err
	}
	// clear stale local pod log buffer so that we don't have to worry about deduplication
	path := filepath.Join(localDir, "buffer-"+strings.TrimSuffix(filenamePrefix, "-")+".log")
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return nil, fmt.Errorf("failed to clear stale local pod log buffer %s: %s", path, err)
	}
	return &PodLogBuffer{
		path:           path,
		s3LogDir:       s3LogDir,
		filenamePrefix: filenamePrefix,
		counter:        counter,
	}, nil
}

// parseLogChunkMetadata parses counter, timestamp, and seq from a chunk filename.
func parseLogChunkMetadata(name, filenamePrefix string) (logChunkMetadata, bool) {
	if !strings.HasPrefix(name, filenamePrefix) || !strings.HasSuffix(name, ".log") {
		return logChunkMetadata{}, false
	}
	body := strings.TrimSuffix(strings.TrimPrefix(name, filenamePrefix), ".log")

	dash := strings.Index(body, "-")
	if dash <= 0 {
		return logChunkMetadata{}, false
	}

	counter, err := strconv.Atoi(body[:dash])
	if err != nil {
		return logChunkMetadata{}, false
	}

	timestampBody := body[dash+1:]
	idx := strings.LastIndex(timestampBody, logChunkSeqMarker)
	if idx < 0 {
		return logChunkMetadata{}, false
	}

	seq, err := strconv.ParseUint(timestampBody[idx+len(logChunkSeqMarker):], 10, 64)
	if err != nil {
		return logChunkMetadata{}, false
	}

	chunkTimestamp, err := time.Parse(chunkTimestampLayout, timestampBody[:idx])
	if err != nil {
		return logChunkMetadata{}, false
	}

	return logChunkMetadata{
		counter:   counter,
		timestamp: chunkTimestamp,
		seq:       seq,
	}, true
}

// loadResumePoint lists chunks under logRelDir and returns the latest timestamp/seq/chunkCounter.
func loadResumePoint(ctx context.Context, workDir, logRelDir, prefix string) (resumePoint, error) {
	s3LogDir, err := storage.S3Key(workDir, logRelDir, true)
	if err != nil {
		return resumePoint{}, err
	}

	s3Objects, err := storage.ListS3Objects(ctx, s3LogDir)
	if err != nil {
		return resumePoint{}, err
	}

	resume := resumePoint{logDir: logRelDir}
	for _, s3object := range s3Objects {
		keySuffix := strings.TrimPrefix(s3object.Key, s3LogDir)
		if keySuffix == "" {
			continue
		}
		meta, ok := parseLogChunkMetadata(keySuffix, prefix)
		if !ok {
			continue
		}
		// Latest chunk wins: counter for the next chunk number, timestamp as the since cursor,
		// and its last seen seq as the checkpoint (0 means no seq: resume accepts every line from since).
		if meta.counter > resume.chunkCounter {
			resume.chunkCounter = meta.counter
			resume.lastPodLogTimestamp = meta.timestamp
			resume.lastPodLogSeq = meta.seq
		}
	}

	return resume, nil
}

// resolveCurrentLogDir returns the connector log session directory name (sync_*)
// under logs/ for the given workDir. It reuses an existing sync_* folder from S3
// when present; otherwise it returns a new sync_<timestamp> name for this run.
func resolveCurrentLogDir(ctx context.Context, workDir string) (string, error) {
	logsPrefix, err := storage.S3Key(workDir, "logs", true)
	if err != nil {
		return "", err
	}

	s3Objects, err := storage.ListS3Objects(ctx, logsPrefix)
	if err != nil {
		return "", err
	}

	var currentLogDir string
	for _, s3object := range s3Objects {
		pathWithinLogsDir := strings.TrimPrefix(s3object.Key, logsPrefix)
		logDir, _, ok := strings.Cut(pathWithinLogsDir, "/")
		if !ok || !strings.HasPrefix(logDir, constants.ConnectorLogDirPrefix) {
			continue
		}
		if currentLogDir == "" {
			currentLogDir = logDir
		}
	}

	if currentLogDir == "" {
		now := time.Now().UTC()
		return fmt.Sprintf("%s%d-%02d-%02d_%02d-%02d-%02d",
			constants.ConnectorLogDirPrefix,
			now.Year(), now.Month(), now.Day(),
			now.Hour(), now.Minute(), now.Second(),
		), nil
	}
	return currentLogDir, nil
}

func parsePodLogLine(rawLogLine string) (podLogLineEntry, bool) {
	normalizedLine, podLogTimestamp, ok := NormalizePodLogLine(rawLogLine)
	if !ok {
		return podLogLineEntry{}, false
	}
	var normalizedLogLine podLogLineEntry
	// Ignore type errors
	_ = json.Unmarshal([]byte(strings.TrimSpace(normalizedLine)), &normalizedLogLine)
	normalizedLogLine.PodLogTimestamp = podLogTimestamp
	normalizedLogLine.normalizedLogLine = normalizedLine
	return normalizedLogLine, true
}

// PodLogLocalDir returns a local staging directory for log chunk buffering before S3 upload.
func PodLogLocalDir(workDir string) string {
	return filepath.Join(os.TempDir(), "olake-pod-logs", filepath.Base(workDir))
}

// Flush uploads the local buffer file to S3 and removes the local file.
func (b *PodLogBuffer) Flush(ctx context.Context) error {
	data, err := os.ReadFile(b.path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	if err := b.upload(ctx, podLogLineEntry{
		PodLogTimestamp:   b.lastLocalLogTimestamp,
		Seq:               b.lastLocalLogSeq,
		normalizedLogLine: string(data),
	}); err != nil {
		return err
	}
	err = os.Remove(b.path)
	if err != nil {
		return err
	}
	b.lastLocalLogTimestamp = time.Time{}
	b.lastLocalLogSeq = 0
	return nil
}

// WriteLine appends a single parsed log line using the same chunking rules as connector log collection.
func (b *PodLogBuffer) WriteLine(ctx context.Context, normalizedLogLine podLogLineEntry) error {
	shouldFlush, err := b.appendLine(normalizedLogLine)
	if err != nil {
		logger.Warnf("failed to append line to pod log buffer with seq %d: %s", normalizedLogLine.Seq, err)
		if flushErr := b.Flush(ctx); flushErr != nil {
			return flushErr
		}
		return b.upload(ctx, normalizedLogLine)
	}
	if shouldFlush {
		return b.Flush(ctx)
	}
	return nil
}

func (b *PodLogBuffer) appendLine(normalizedLogLine podLogLineEntry) (shouldFlush bool, err error) {
	if err := b.writeLocal([]byte(normalizedLogLine.normalizedLogLine)); err != nil {
		return false, err
	}
	if !normalizedLogLine.PodLogTimestamp.IsZero() {
		b.lastLocalLogTimestamp = normalizedLogLine.PodLogTimestamp
	}
	// Chunk seq is the last seen seq, not the max: it is the checkpoint a resumed stream skips to.
	if normalizedLogLine.Seq > 0 {
		b.lastLocalLogSeq = normalizedLogLine.Seq
	}
	size, err := b.currentBufferSize()
	if err != nil {
		// line is already in the buffer; flush so Flush stays the only uploader
		logger.Warnf("failed to stat pod log buffer, flushing: %s", err)
		return true, nil
	}
	return size >= int64(b.currentThreshold()), nil
}

// currentThreshold returns the chunk size threshold for the current chunk counter.
func (b *PodLogBuffer) currentThreshold() int {
	if b.counter < len(constants.PodLogChunkThresholds) {
		return constants.PodLogChunkThresholds[b.counter]
	}
	return constants.PodLogChunkMaxBytes
}

// currentBufferSize returns the size of the local buffer file.
func (b *PodLogBuffer) currentBufferSize() (int64, error) {
	info, err := os.Stat(b.path)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}
	return info.Size(), nil
}

// writeLocal appends data to the local buffer file.
func (b *PodLogBuffer) writeLocal(data []byte) (err error) {
	f, err := os.OpenFile(b.path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, constants.DefaultFilePermissions)
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := f.Close(); err == nil {
			err = closeErr
		}
	}()

	_, err = f.Write(data)
	if err != nil {
		return err
	}
	return nil
}

func (b *PodLogBuffer) upload(ctx context.Context, normalizedLogLine podLogLineEntry) error {
	filename := b.nextFilename(normalizedLogLine)
	key := path.Join(b.s3LogDir, filename)

	return storage.PutS3Object(ctx, key, strings.NewReader(normalizedLogLine.normalizedLogLine))
}

// nextFilename returns the next S3 chunk filename and increments the counter.
func (b *PodLogBuffer) nextFilename(normalizedLogLine podLogLineEntry) string {
	b.counter++
	ts := strings.ReplaceAll(normalizedLogLine.PodLogTimestamp.UTC().Format(time.RFC3339Nano), ":", "")
	return fmt.Sprintf("%s%06d-%s%s%06d.log", b.filenamePrefix, b.counter, ts, logChunkSeqMarker, normalizedLogLine.Seq)
}

// NormalizePodLogLine strips docker/k8s prefixes and accepts only JSON log lines.
func NormalizePodLogLine(rawLogLine string) (string, time.Time, bool) {
	rawLogLine = strings.TrimRight(rawLogLine, "\r\n")
	rawLogLine = stripansi.Strip(rawLogLine)
	rawLogLine = strings.TrimSpace(rawLogLine)
	if rawLogLine == "" {
		return "", time.Time{}, false
	}

	jsonBody, podLogTimestamp, ok := ParsePodLogLineTimestamp(rawLogLine)
	if !ok {
		return "", time.Time{}, false
	}
	if jsonBody == "" || !json.Valid([]byte(jsonBody)) {
		return "", time.Time{}, false
	}
	normalizedLine := jsonBody + "\n"
	return normalizedLine, podLogTimestamp, true
}

func readPodLogStream(stream io.Reader, handleLine func(rawLogLine string) error) error {
	reader := bufio.NewReader(stream)
	for {
		lineBytes, err := reader.ReadBytes('\n')
		if len(lineBytes) > 0 {
			if err := handleLine(string(lineBytes)); err != nil {
				return err
			}
		}
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
	}
}

// ParsePodLogLineTimestamp parses the RFC3339 timestamp prefix from a kubectl-style log line.
// jsonBody is the line body after the timestamp prefix when ok is true.
func ParsePodLogLineTimestamp(rawLogLine string) (jsonBody string, ts time.Time, ok bool) {
	prefix, jsonBody, found := strings.Cut(rawLogLine, " ")
	if !found {
		return rawLogLine, time.Time{}, false
	}
	podLogTimestamp, podLogTimestampOK := parseRFC3339(prefix)
	if !podLogTimestampOK {
		return rawLogLine, time.Time{}, false
	}
	return strings.TrimSpace(jsonBody), podLogTimestamp, true
}

// parseRFC3339 parses RFC3339 timestamps in nano formats.
func parseRFC3339(value string) (time.Time, bool) {
	podLogTimestamp, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		return time.Time{}, false
	}
	return podLogTimestamp, true
}
