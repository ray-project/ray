// Copyright 2026 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package logmonitor

import (
	"bufio"
	"context"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/ray-project/ray/go/pkg/log"
)

type Monitor struct {
	ip                     string
	logsDir                string
	publisher              Publisher
	tracked                map[string]struct{}
	openFiles              []*logFileInfo
	closedFiles            []*logFileInfo
	canOpenMoreFiles       bool
	maxFilesOpen           int
	manyFilesThreshold     int
	nameUpdateInterval     time.Duration
	maxLinesPerRead        int
	runtimeEnvToDriver     bool
	autoscalerV2Enabled    bool
	lastFilenameUpdateTime time.Time
	isProcAlive            func(pid int) bool
	sleep                  func(time.Duration)
}

// RuntimeConfig holds runtime configuration overrides for the log monitor.
type RuntimeConfig struct {
	RuntimeEnvToDriver  bool
	AutoscalerV2Enabled bool
	MaxFilesOpen        int
}

func New(ip, logsDir string, publisher Publisher) (*Monitor, error) {
	if logsDir == "" {
		return nil, fmt.Errorf("logsDir must not be empty")
	}
	if publisher == nil {
		return nil, fmt.Errorf("publisher must not be nil")
	}
	if info, err := os.Stat(logsDir); err != nil || !info.IsDir() {
		return nil, fmt.Errorf("logsDir must be an existing directory: %w", err)
	}
	if err := os.MkdirAll(filepath.Join(logsDir, "old"), 0o755); err != nil {
		return nil, fmt.Errorf("create old log directory: %w", err)
	}

	return &Monitor{
		ip:                 ip,
		logsDir:            logsDir,
		publisher:          publisher,
		tracked:            map[string]struct{}{},
		canOpenMoreFiles:   true,
		maxFilesOpen:       defaultMaxFilesOpen,
		manyFilesThreshold: defaultManyFilesThreshold,
		nameUpdateInterval: defaultNameUpdateInterval,
		maxLinesPerRead:    defaultMaxLinesPerRead,
		isProcAlive:        defaultIsProcAlive,
		sleep:              time.Sleep,
	}, nil
}

// NewWithConfig creates a Monitor with runtime configuration overrides.
func NewWithConfig(ip, logsDir string, publisher Publisher, cfg RuntimeConfig) (*Monitor, error) {
	m, err := New(ip, logsDir, publisher)
	if err != nil {
		return nil, err
	}
	if cfg.MaxFilesOpen > 0 {
		m.maxFilesOpen = cfg.MaxFilesOpen
	}
	m.runtimeEnvToDriver = cfg.RuntimeEnvToDriver
	m.autoscalerV2Enabled = cfg.AutoscalerV2Enabled
	m.writePythonStyleLog(183, fmt.Sprintf(
		"Starting log monitor with [max open files=%d], [is_autoscaler_v2=%s]",
		m.maxFilesOpen,
		pythonBool(m.autoscalerV2Enabled),
	))
	return m, nil
}

func (m *Monitor) Run(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if m.shouldUpdateFilenames(time.Now()) {
			if err := m.updateLogFilenames(); err != nil {
				return err
			}
			m.lastFilenameUpdateTime = time.Now()
		}

		if err := m.openClosedFiles(); err != nil {
			return err
		}
		published, err := m.checkLogFilesAndPublishUpdates()
		if err != nil {
			return err
		}
		if !published {
			m.sleep(defaultIdleSleep)
		}
	}
}

func (m *Monitor) shouldUpdateFilenames(now time.Time) bool {
	if len(m.tracked) < m.manyFilesThreshold {
		return true
	}
	return now.Sub(m.lastFilenameUpdateTime) > m.nameUpdateInterval
}

func (m *Monitor) updateLogFilenames() error {
	if len(m.tracked) == 0 {
		m.sleep(initialScanDelay)
	}
	tpuDirInfo, err := os.Stat(filepath.Join(m.logsDir, "tpu_logs"))
	tpuDirExists := err == nil && tpuDirInfo.IsDir()

	candidates, err := orderedLogPaths(m.logsDir, m.autoscalerV2Enabled, m.runtimeEnvToDriver, tpuDirExists)
	if err != nil {
		return err
	}
	for _, path := range candidates {
		info, err := os.Stat(path)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return err
		}
		if !info.Mode().IsRegular() && info.Mode()&fs.ModeSymlink == 0 {
			continue
		}
		if _, exists := m.tracked[path]; exists {
			continue
		}

		fileInfo := &logFileInfo{
			filename:     path,
			isErrFile:    isErrFile(path),
			componentPID: classifyComponentPID(path),
		}
		// Only parse PID from worker filenames, not JobID (to match Python behavior)
		if _, pid, ok := parseWorkerFilename(path); ok {
			fileInfo.workerPID = pid
		}
		if jobID, ok := parseRuntimeEnvFilename(path); ok {
			fileInfo.jobID = jobID
		}

		m.tracked[path] = struct{}{}
		m.closedFiles = append(m.closedFiles, fileInfo)
		m.writePythonStyleLog(305, fmt.Sprintf("Beginning to track file %s", filepath.Base(path)))
	}
	return nil
}

func (m *Monitor) openClosedFiles() error {
	if !m.canOpenMoreFiles {
		if err := m.closeAllFiles(); err != nil {
			return err
		}
	}

	remainingClosed := make([]*logFileInfo, 0, len(m.closedFiles))
	for _, fileInfo := range m.closedFiles {
		if len(m.openFiles) >= m.maxFilesOpen {
			m.canOpenMoreFiles = false
			remainingClosed = append(remainingClosed, fileInfo)
			continue
		}
		size, err := fileInfo.currentSize()
		if err != nil {
			if os.IsNotExist(err) {
				delete(m.tracked, fileInfo.filename)
				continue
			}
			return err
		}
		// Reopen only if the file grew since the last open.
		if size <= fileInfo.sizeWhenLastOpened {
			remainingClosed = append(remainingClosed, fileInfo)
			continue
		}
		if err := fileInfo.openAtCurrentPosition(); err != nil {
			if os.IsNotExist(err) {
				delete(m.tracked, fileInfo.filename)
				continue
			}
			return err
		}
		fileInfo.sizeWhenLastOpened = size
		m.openFiles = append(m.openFiles, fileInfo)
	}
	m.closedFiles = remainingClosed
	if len(m.openFiles) < m.maxFilesOpen {
		m.canOpenMoreFiles = true
	}
	return nil
}

func (m *Monitor) closeAllFiles() error {
	logger := log.WithName("logmonitor")
	reopenedClosed := make([]*logFileInfo, 0, len(m.openFiles))

	for _, fileInfo := range m.openFiles {
		if fileInfo.fileHandle != nil {
			_ = fileInfo.fileHandle.Close()
			fileInfo.fileHandle = nil
			fileInfo.reader = nil
		}

		alive := true
		if fileInfo.workerPID != nil {
			alive = m.isProcAlive(*fileInfo.workerPID)
		}

		if !alive {
			target := filepath.Join(m.logsDir, "old", filepath.Base(fileInfo.filename))
			if err := os.Rename(fileInfo.filename, target); err != nil && !os.IsNotExist(err) {
				return err
			}
			delete(m.tracked, fileInfo.filename)
			continue
		}

		reopenedClosed = append(reopenedClosed, fileInfo)
	}

	m.openFiles = nil
	m.closedFiles = append(m.closedFiles, reopenedClosed...)
	m.canOpenMoreFiles = true
	logger.Info("recycled open log files", "closed_count", len(reopenedClosed))
	return nil
}

func (m *Monitor) writePythonStyleLog(line int, message string) {
	logLine := fmt.Sprintf(
		"%s\tINFO log_monitor.py:%d -- %s\n",
		time.Now().Format("2006-01-02 15:04:05,000"),
		line,
		message,
	)
	if logPath := filepath.Join(m.logsDir, "log_monitor.log"); logPath != "" {
		if f, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600); err == nil {
			_, _ = f.WriteString(logLine)
			_ = f.Close()
		}
	}
	_, _ = os.Stderr.WriteString(logLine)
}

func pythonBool(v bool) string {
	if v {
		return "True"
	}
	return "False"
}

func (m *Monitor) checkLogFilesAndPublishUpdates() (bool, error) {
	anythingPublished := false

	for _, fileInfo := range m.openFiles {
		if err := fileInfo.reopenIfNecessary(); err != nil && !os.IsNotExist(err) {
			return false, err
		}

		pending := make([]string, 0, 16)
		flush := func() error {
			if len(pending) == 0 {
				return nil
			}
			if err := m.publisher.Publish(LogBatch{
				IP:        m.ip,
				PID:       fileInfo.publishPID(),
				JobID:     fileInfo.jobID,
				IsErr:     fileInfo.isErrFile,
				Lines:     append([]string(nil), pending...),
				ActorName: fileInfo.actorName,
				TaskName:  fileInfo.taskName,
			}); err != nil {
				anythingPublished = true
				pending = pending[:0]
				return nil
			}
			anythingPublished = true
			pending = pending[:0]
			return nil
		}

		// The reader is persistent across polls: lines left in its buffer
		// when maxLinesPerRead ends this poll stay available for the next
		// poll instead of being skipped.
		if fileInfo.reader == nil {
			fileInfo.reader = bufio.NewReader(fileInfo.fileHandle)
		}
		for i := 0; i < m.maxLinesPerRead; i++ {
			line, err := fileInfo.reader.ReadString('\n')
			if err != nil && line == "" {
				break
			}
			fileInfo.consumedBytes += int64(len(line))
			line = strings.TrimRight(line, "\r\n")
			switch {
			case strings.HasPrefix(line, logPrefixActorName):
				if err := flush(); err != nil {
					return false, err
				}
				fileInfo.actorName = strings.TrimPrefix(line, logPrefixActorName)
				fileInfo.taskName = ""
			case strings.HasPrefix(line, logPrefixTaskName):
				if err := flush(); err != nil {
					return false, err
				}
				fileInfo.taskName = strings.TrimPrefix(line, logPrefixTaskName)
			case strings.HasPrefix(line, logPrefixJobID):
				fileInfo.jobID = strings.TrimPrefix(line, logPrefixJobID)
			case line == windowsAccessViolationLine:
				if skipped, _ := fileInfo.reader.ReadString('\n'); skipped != "" {
					fileInfo.consumedBytes += int64(len(skipped))
				}
			case line == "":
				pending = append(pending, "")
			default:
				pending = append(pending, line)
			}
		}

		fileInfo.filePosition = fileInfo.consumedBytes
		if err := flush(); err != nil {
			return false, err
		}
	}

	return anythingPublished, nil
}
