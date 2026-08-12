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
)

type PodLogBuffer struct {
	path                  string
	s3LogDir              string // S3 key prefix for this log directory (configStorageKey(workDir, logRelDir))
	filenamePrefix        string // chunk filename prefix, e.g. connector- or worker-
	counter               int
	lastLocalLogTimestamp time.Time // k8s/docker line timestamp for chunk naming
	mu                    sync.Mutex
}

// NewPodLogBuffer creates a new PodLogBuffer
func NewPodLogBuffer(localDir, workDir, logRelDir, filenamePrefix string, counter int) (*PodLogBuffer, error) {
	s3LogDir, err := configStorageKey(workDir, logRelDir, false)
	if err != nil {
		return nil, err
	}
	return &PodLogBuffer{
		path:           filepath.Join(localDir, "buffer-"+strings.TrimSuffix(filenamePrefix, "-")+".log"),
		s3LogDir:       s3LogDir,
		filenamePrefix: filenamePrefix,
		counter:        counter,
	}, nil
}

// listS3ObjectKeys lists the S3 object keys for the given prefix.
func listS3ObjectKeys(ctx context.Context, prefix string) ([]string, error) {
	client, bucket, err := getS3Client()
	if err != nil {
		return nil, err
	}

	var s3ObjectKeys []string
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
			s3ObjectKeys = append(s3ObjectKeys, aws.ToString(obj.Key))
		}
	}
	return s3ObjectKeys, nil
}

// parseLogChunkCounter parses the counter from a log chunk name.
func parseLogChunkCounter(name, filenamePrefix string) (int, bool) {
	raw := strings.TrimSuffix(strings.TrimPrefix(name, filenamePrefix), ".log")
	dash := strings.Index(raw, "-")
	if dash <= 0 {
		return 0, false
	}
	counter, err := strconv.Atoi(raw[:dash])
	if err != nil {
		return 0, false
	}
	return counter, true
}


// to be resolved 

// NewWorkerPodLogBuffer creates a new PodLogBuffer for the worker pod logs.
func newWorkerPodLogBufferForWorkDir(ctx context.Context, workDir string) (*PodLogBuffer, time.Time, error) {
	localDir := PodLogLocalDir(workDir)
	if err := CreateDirectory(localDir); err != nil {
		return nil, time.Time{}, err
	}

	lastLogTimestamp, chunkCounter, err := resolveLogChunkState(ctx, workDir, constants.WorkerLogRelDir, constants.WorkerLogFilenamePref)
	if err != nil {
		return nil, time.Time{}, err
	}

	buffer, err := NewPodLogBuffer(localDir, workDir, constants.WorkerLogRelDir, constants.WorkerLogFilenamePref, chunkCounter)
	if err != nil {
		return nil, time.Time{}, err
	}
	return buffer, lastLogTimestamp, nil
}

// NewConnectorPodLogBuffer creates a new PodLogBuffer for the connector pod logs.
func newConnectorPodLogBufferForWorkDir(ctx context.Context, workDir, filenamePrefix string) (*PodLogBuffer, time.Time, error) {
	localDir := PodLogLocalDir(workDir)
	if err := CreateDirectory(localDir); err != nil {
		return nil, time.Time{}, err
	}

	logRelDir, lastLogTimestamp, chunkCounter, err := resolveLogDirState(ctx, workDir, filenamePrefix)
	if err != nil {
		return nil, time.Time{}, err
	}

	buffer, err := NewPodLogBuffer(localDir, workDir, logRelDir, filenamePrefix, chunkCounter)
	if err != nil {
		return nil, time.Time{}, err
	}
	return buffer, lastLogTimestamp, nil
}

// resolveLogDirState lists log chunks for the current sync_* directory and returns the
// S3-relative log path, latest chunk timestamp for resume/dedup, and highest chunk counter.
func resolveLogDirState(ctx context.Context, workDir, filenamePrefix string) (logRelDir string, lastLogTimestamp time.Time, chunkCounter int, err error) {
	currentLogDir, err := resolveCurrentLogDir(ctx, workDir)
	if err != nil {
		return "", time.Time{}, 0, err
	}
	logRelDir = path.Join("logs", currentLogDir)

	lastLogTimestamp, chunkCounter, err = resolveLogChunkState(ctx, workDir, logRelDir, filenamePrefix)
	if err != nil {
		return "", time.Time{}, 0, err
	}

	return logRelDir, lastLogTimestamp, chunkCounter, nil
}

// resolveLogChunkState lists log chunks under logRelDir and returns the latest chunk
// timestamp for resume/dedup and the highest chunk counter.
func resolveLogChunkState(ctx context.Context, workDir, logRelDir, filenamePrefix string) (lastLogTimestamp time.Time, chunkCounter int, err error) {
	s3LogDir, err := configStorageKey(workDir, logRelDir, true)
	if err != nil {
		return time.Time{}, 0, err
	}

	keys, err := listS3ObjectKeys(ctx, s3LogDir)
	if err != nil {
		return time.Time{}, 0, err
	}

	for _, key := range keys {
		keySuffix := strings.TrimPrefix(key, s3LogDir)
		if keySuffix == "" {
			continue
		}
		if counter, ok := parseLogChunkCounter(keySuffix, filenamePrefix); ok && counter > chunkCounter {
			chunkCounter = counter
		}
		if chunkTimestamp, ok := parseLogChunkTimestamp(keySuffix, filenamePrefix); ok && chunkTimestamp.After(lastLogTimestamp) {
			lastLogTimestamp = chunkTimestamp
		}
	}

	return lastLogTimestamp, chunkCounter, nil
}

// resolveCurrentLogDir returns the connector log session directory name (sync_*)
// under logs/ for the given workDir. It reuses an existing sync_* folder from S3
// when present; otherwise it returns a new sync_<timestamp> name for this run.
func resolveCurrentLogDir(ctx context.Context, workDir string) (string, error) {
	logsPrefix, err := configStorageKey(workDir, "logs", true)
	if err != nil {
		return "", err
	}

	keys, err := listS3ObjectKeys(ctx, logsPrefix)
	if err != nil {
		return "", err
	}

	var currentLogDir string
	for _, key := range keys {
		pathWithinLogsDir := strings.TrimPrefix(key, logsPrefix)
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

// parseLogChunkTimestamp parses the timestamp suffix from a chunked log filename.
func parseLogChunkTimestamp(name, filenamePrefix string) (time.Time, bool) {
	if !strings.HasPrefix(name, filenamePrefix) || !strings.HasSuffix(name, ".log") {
		return time.Time{}, false
	}
	raw := strings.TrimSuffix(strings.TrimPrefix(name, filenamePrefix), ".log")
	dash := strings.Index(raw, "-")
	if dash <= 0 {
		return time.Time{}, false
	}
	chunkTimestampStr := raw[dash+1:]
	chunkTimestamp, err := time.Parse(chunkTimestampLayout, chunkTimestampStr)
	if err != nil {
		return time.Time{}, false
	}
	return chunkTimestamp, true
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

	lastTS := b.lastLocalLogTimestamp
	if lastTS.IsZero() {
		lastTS, _ = lastLogChunkTimestamp(data)
	}
	if err := b.uploadTolockeds3(ctx, lastTS, data); err != nil {
		return err
	}
	b.lastLocalLogTimestamp = time.Time{}
	return os.Remove(b.path)
}

// lastLogChunkTimestamp returns the timestamp of the last log chunk.
func lastLogChunkTimestamp(data []byte) (time.Time, bool) {
	lines := strings.Split(string(data), "\n")
	for i := len(lines) - 1; i >= 0; i-- {
		line := strings.TrimSpace(lines[i])
		if line == "" {
			continue
		}
		if ts, ok := logLineTimestamp([]byte(line + "\n")); ok {
			return ts, true
		}
	}
	return time.Time{}, false
}

// logLineTimestamp parses the timestamp from a log line.
func logLineTimestamp(lineBytes []byte) (time.Time, bool) {
	if _, ts, ok := ParsePodLogLineTimestamp(string(lineBytes)); ok {
		return ts, true
	}
	var entry struct {
		Time string `json:"time"`
	}
	if err := json.Unmarshal(lineBytes, &entry); err != nil || entry.Time == "" {
		return time.Time{}, false
	}
	return parseRFC3339(entry.Time)
}

// WriteLine appends a single log line using the same chunking rules as connector log collection.
// lastPodLogTimestamp should be the k8s/docker log prefix timestamp when available (connector logs).
func (b *PodLogBuffer) WriteLine(ctx context.Context, lineBytes []byte, lastPodLogTimestamp time.Time) error {
	chunkTS := lastPodLogTimestamp
	if chunkTS.IsZero() {
		chunkTS, _ = logLineTimestamp(lineBytes)
	}

	shouldFlush, err := b.appendLine(lastPodLogTimestamp, lineBytes)
	if err != nil {
		b.mu.Lock()
		defer b.mu.Unlock()
		return b.uploadTolockeds3(ctx, chunkTS, lineBytes)
	}
	if shouldFlush {
		return b.Flush(ctx)
	}
	return nil
}

func (b *PodLogBuffer) appendLine(lastPodLogTimestamp time.Time, lineBytes []byte) (shouldFlush bool, err error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if !lastPodLogTimestamp.IsZero() {
		b.lastLocalLogTimestamp = lastPodLogTimestamp
	}
	if err := b.writeToLocalLockedFile(lineBytes); err != nil {
		return false, err
	}
	size, err := b.currentLockedBufferSize()
	if err != nil {
		return false, err
	}
	threshold := Ternary(b.counter < len(constants.PodLogChunkThresholds),constants.PodLogChunkThresholds[b.counter],constants.PodLogChunkMaxBytes).(int)
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
func (b *PodLogBuffer) uploadTolockeds3(ctx context.Context, lastLogTS time.Time, data []byte) error {
	filename := b.nextLockedFilename(lastLogTS)
	key := path.Join(b.s3LogDir, filename)

	client, bucket, err := getS3Client()
	if err != nil {
		return err
	}

	_, err = client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: &bucket,
		Key:    &key,
		Body:   bytes.NewReader(data),
	})
	return err
}

// nextLockedFilename returns the next filename for the next chunk.
func (b *PodLogBuffer) nextLockedFilename(lastLogTS time.Time) string {
	b.counter++
	ts := strings.ReplaceAll(lastLogTS.UTC().Format(time.RFC3339Nano), ":", "")
	return fmt.Sprintf("%s%06d-%s.log", b.filenamePrefix, b.counter, ts)
}

// NormalizePodLogLine strips docker/k8s prefixes and accepts only JSON log lines.
func NormalizePodLogLine(line string) (string, time.Time, bool) {
	line = strings.TrimRight(line, "\r\n")
	line = stripansi.Strip(line)
	line = strings.TrimSpace(line)
	if line == "" {
		return "", time.Time{}, false
	}

	rest, podLogTimestamp, ok := ParsePodLogLineTimestamp(line)
	if !ok {
		return "", time.Time{}, false
	}
	line = rest
	if line == "" || !json.Valid([]byte(line)) {
		return "", time.Time{}, false
	}
	line += "\n"
	return line, podLogTimestamp, true
}

func readPodLogStream(stream io.Reader, handleLine func(line string) error) error {
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
// rest is the line body after the timestamp prefix when ok is true.
func ParsePodLogLineTimestamp(line string) (rest string, ts time.Time, ok bool) {
	prefix, rest, found := strings.Cut(line, " ")
	if !found {
		return line, time.Time{}, false
	}
	podLogTimestamp, podLogTimestampOK := parseRFC3339(prefix)
	if !podLogTimestampOK {
		return line, time.Time{}, false
	}
	return strings.TrimSpace(rest), podLogTimestamp, true
}

// parseRFC3339 parses RFC3339 timestamps in nano formats.
func parseRFC3339(value string) (time.Time, bool) {
	podLogTimestamp, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		return time.Time{}, false
	}
	return podLogTimestamp, true
}
