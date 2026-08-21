package utils

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/acarl005/stripansi"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/datazip-inc/olake-helm/worker/constants"
)

const (
	chunkTimestampLayout = "2006-01-02T150405.999999999Z"
	logChunkSeqMarker    = "-seq"
)

type PodLogBuffer struct {
	path                  string
	s3LogDir              string // S3 key prefix for this log directory (configStorageKey(workDir, logRelDir))
	filenamePrefix        string // chunk filename prefix, e.g. connector- or worker-
	counter               int
	lastLocalLogTimestamp time.Time // k8s/docker line timestamp for chunk naming
	lastLocalLogSeq       uint64    // last seq in the buffered chunk
	mu                    sync.Mutex
}

// logChunkMetadata holds resume fields parsed from a chunked log filename.
type logChunkMetadata struct {
	counter   int
	timestamp time.Time
	seq       uint64
}

// podLogLineEntry is a parsed pod log line: JSON fields, k8s/docker timestamp, and normalized line text.
type podLogLineEntry struct {
	WorkflowID        string `json:"workflowID"`
	Command           string `json:"command"`
	Seq               uint64 `json:"seq"`
	PodLogTimestamp   time.Time
	normalizedLogLine string
}

type s3Object struct {
	Key          string
	LastModified time.Time
}

// NewPodLogBuffer creates a new PodLogBuffer
func NewPodLogBuffer(localDir, workDir, logRelDir, filenamePrefix string, counter int) (*PodLogBuffer, error) {
	s3LogDir, err := configStorageKey(workDir, logRelDir, false)
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

// listS3Objects lists S3 objects under the given prefix, including LastModified.
func listS3Objects(ctx context.Context, prefix string) ([]s3Object, error) {
	client, bucket, err := getS3Client()
	if err != nil {
		return nil, err
	}

	var s3Objects []s3Object
	paginator := s3.NewListObjectsV2Paginator(client, &s3.ListObjectsV2Input{
		Bucket: &bucket,
		Prefix: &prefix,
	})
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to list objects in s3://%s/%s: %s", bucket, prefix, err)
		}
		for _, obj := range page.Contents {
			s3Objects = append(s3Objects, s3Object{
				Key:          aws.ToString(obj.Key),
				LastModified: aws.ToTime(obj.LastModified),
			})
		}
	}
	return s3Objects, nil
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

// newWorkerPodLogBufferForWorkDir creates a PodLogBuffer for worker logs.
func newWorkerPodLogBufferForWorkDir(ctx context.Context, workDir string) (*PodLogBuffer, time.Time, uint64, error) {
	localDir := PodLogLocalDir(workDir)
	if err := CreateDirectory(localDir); err != nil {
		return nil, time.Time{}, 0, err
	}

	lastLogTimestamp, lastLogSeq, chunkCounter, err := resolveLogChunkResumeState(ctx, workDir, constants.WorkerLogRelDir, constants.WorkerLogFilenamePref)
	if err != nil {
		return nil, time.Time{}, 0, err
	}

	buffer, err := NewPodLogBuffer(localDir, workDir, constants.WorkerLogRelDir, constants.WorkerLogFilenamePref, chunkCounter)
	if err != nil {
		return nil, time.Time{}, 0, err
	}
	return buffer, lastLogTimestamp, lastLogSeq, nil
}

// NewConnectorPodLogBuffer creates a new PodLogBuffer for the connector pod logs.
func newConnectorPodLogBufferForWorkDir(ctx context.Context, workDir, filenamePrefix string) (*PodLogBuffer, time.Time, uint64, error) {
	localDir := PodLogLocalDir(workDir)
	if err := CreateDirectory(localDir); err != nil {
		return nil, time.Time{}, 0, err
	}

	logRelDir, lastLogTimestamp, lastLogSeq, chunkCounter, err := resolveLogDirState(ctx, workDir, filenamePrefix)
	if err != nil {
		return nil, time.Time{}, 0, err
	}

	buffer, err := NewPodLogBuffer(localDir, workDir, logRelDir, filenamePrefix, chunkCounter)
	if err != nil {
		return nil, time.Time{}, 0, err
	}
	return buffer, lastLogTimestamp, lastLogSeq, nil
}

// resolveLogDirState lists log chunks for the current sync_* directory and returns the
// S3-relative log path, latest chunk timestamp/seq for resume, and highest chunk counter.
func resolveLogDirState(ctx context.Context, workDir, filenamePrefix string) (logRelDir string, lastLogTimestamp time.Time, lastLogSeq uint64, chunkCounter int, err error) {
	currentLogDir, err := resolveCurrentLogDir(ctx, workDir)
	if err != nil {
		return "", time.Time{}, 0, 0, err
	}
	logRelDir = path.Join("logs", currentLogDir)

	lastLogTimestamp, lastLogSeq, chunkCounter, err = resolveLogChunkResumeState(ctx, workDir, logRelDir, filenamePrefix)
	if err != nil {
		return "", time.Time{}, 0, 0, err
	}

	return logRelDir, lastLogTimestamp, lastLogSeq, chunkCounter, nil
}

// resolveLogChunkResumeState lists log chunks under logRelDir and returns resume metadata from the latest chunk.
func resolveLogChunkResumeState(ctx context.Context, workDir, logRelDir, filenamePrefix string) (lastLogTimestamp time.Time, lastLogSeq uint64, chunkCounter int, err error) {
	s3LogDir, err := configStorageKey(workDir, logRelDir, true)
	if err != nil {
		return time.Time{}, 0, 0, err
	}

	s3Objects, err := listS3Objects(ctx, s3LogDir)
	if err != nil {
		return time.Time{}, 0, 0, err
	}

	for _, s3object := range s3Objects {
		keySuffix := strings.TrimPrefix(s3object.Key, s3LogDir)
		if keySuffix == "" {
			continue
		}
		meta, ok := parseLogChunkMetadata(keySuffix, filenamePrefix)
		if !ok || meta.counter <= chunkCounter {
			continue
		}
		chunkCounter = meta.counter
		lastLogTimestamp = meta.timestamp
		lastLogSeq = meta.seq
	}

	return lastLogTimestamp, lastLogSeq, chunkCounter, nil
}

// resolveCurrentLogDir returns the connector log session directory name (sync_*)
// under logs/ for the given workDir. It reuses an existing sync_* folder from S3
// when present; otherwise it returns a new sync_<timestamp> name for this run.
func resolveCurrentLogDir(ctx context.Context, workDir string) (string, error) {
	logsPrefix, err := configStorageKey(workDir, "logs", true)
	if err != nil {
		return "", err
	}

	s3Objects, err := listS3Objects(ctx, logsPrefix)
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
	if err := json.Unmarshal([]byte(strings.TrimSpace(normalizedLine)), &normalizedLogLine); err != nil {
		return podLogLineEntry{}, false
	}
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
	b.mu.Lock()
	defer b.mu.Unlock()

	data, err := os.ReadFile(b.path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	if err := b.uploadTolockeds3(ctx, podLogLineEntry{
		PodLogTimestamp:   b.lastLocalLogTimestamp,
		Seq:               b.lastLocalLogSeq,
		normalizedLogLine: string(data),
	}); err != nil {
		return err
	}
	b.lastLocalLogTimestamp = time.Time{}
	b.lastLocalLogSeq = 0
	return os.Remove(b.path)
}

// WriteLine appends a single parsed log line using the same chunking rules as connector log collection.
func (b *PodLogBuffer) WriteLine(ctx context.Context, normalizedLogLine podLogLineEntry) error {
	shouldFlush, err := b.appendLine(normalizedLogLine)
	if err != nil {
		b.mu.Lock()
		defer b.mu.Unlock()
		return b.uploadTolockeds3(ctx, normalizedLogLine)
	}
	if shouldFlush {
		return b.Flush(ctx)
	}
	return nil
}

func (b *PodLogBuffer) appendLine(normalizedLogLine podLogLineEntry) (shouldFlush bool, err error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if !normalizedLogLine.PodLogTimestamp.IsZero() {
		b.lastLocalLogTimestamp = normalizedLogLine.PodLogTimestamp
	}
	if normalizedLogLine.Seq > 0 {
		b.lastLocalLogSeq = normalizedLogLine.Seq
	}
	if err := b.writeToLocalLockedFile([]byte(normalizedLogLine.normalizedLogLine)); err != nil {
		return false, err
	}
	size, err := b.currentLockedBufferSize()
	if err != nil {
		return false, err
	}

	var threshold int
	if b.counter < len(constants.PodLogChunkThresholds) {
		threshold = constants.PodLogChunkThresholds[b.counter]
	} else {
		threshold = constants.PodLogChunkMaxBytes
	}
	return size >= int64(threshold), nil
}

// currentLockedBufferSize returns the size of the local buffer file.
func (b *PodLogBuffer) currentLockedBufferSize() (int64, error) {
	info, err := os.Stat(b.path)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}
	return info.Size(), nil
}

// writeToLocalLockedFile writes the data to the local file and returns an error if the file cannot be opened or written.
func (b *PodLogBuffer) writeToLocalLockedFile(data []byte) (err error) {
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

// uploadTolockeds3 uploads the data to S3 with the filename.
func (b *PodLogBuffer) uploadTolockeds3(ctx context.Context, normalizedLogLine podLogLineEntry) error {
	filename := b.nextLockedFilename(normalizedLogLine)
	key := path.Join(b.s3LogDir, filename)

	client, bucket, err := getS3Client()
	if err != nil {
		return err
	}

	_, err = client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: &bucket,
		Key:    &key,
		Body:   bytes.NewReader([]byte(normalizedLogLine.normalizedLogLine)),
	})
	return err
}

// nextLockedFilename returns the next filename for the next chunk.
func (b *PodLogBuffer) nextLockedFilename(normalizedLogLine podLogLineEntry) string {
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
