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
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

type recordingPublisher struct {
	batches []LogBatch
	err     error
}

func (p *recordingPublisher) Publish(batch LogBatch) error {
	p.batches = append(p.batches, batch)
	return p.err
}

func createLogFile(t *testing.T, dir, name, contents string) string {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte(contents), 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
	return path
}

func TestUpdateLogFilenamesParity(t *testing.T) {
	logsDir := t.TempDir()
	createLogFile(t, logsDir, "worker-6df6d5dd-01000000-47660.out", "line\n")
	createLogFile(t, logsDir, "raylet.err", "line\n")
	createLogFile(t, logsDir, "gcs_server.1.err", "line\n")
	createLogFile(t, logsDir, "monitor.log", "line\n")

	m, err := New("127.0.0.1", logsDir, &recordingPublisher{})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	if err := m.updateLogFilenames(); err != nil {
		t.Fatalf("updateLogFilenames() error = %v", err)
	}

	if len(m.closedFiles) != 4 {
		t.Fatalf("len(closedFiles) = %d, want 4", len(m.closedFiles))
	}
	if len(m.tracked) != 4 {
		t.Fatalf("len(tracked) = %d, want 4", len(m.tracked))
	}
}

func TestUpdateLogFilenamesWorkerOrderingParity(t *testing.T) {
	logsDir := t.TempDir()
	createLogFile(t, logsDir, "worker-b-01000000-10002.out", "stdout-b\n")
	createLogFile(t, logsDir, "worker-b-01000000-10002.err", "stderr-b\n")
	createLogFile(t, logsDir, "worker-a-01000000-10001.out", "stdout-a\n")
	createLogFile(t, logsDir, "worker-a-01000000-10001.err", "stderr-a\n")

	m, err := New("127.0.0.1", logsDir, &recordingPublisher{})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	if err := m.updateLogFilenames(); err != nil {
		t.Fatalf("updateLogFilenames() error = %v", err)
	}

	got := make([]string, 0, len(m.closedFiles))
	for _, fileInfo := range m.closedFiles {
		got = append(got, filepath.Base(fileInfo.filename))
	}
	want := []string{
		"worker-b-01000000-10002.out",
		"worker-b-01000000-10002.err",
		"worker-a-01000000-10001.out",
		"worker-a-01000000-10001.err",
	}
	if len(got) != len(want) {
		t.Fatalf("closedFiles len = %d, want %d (%v)", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("closedFiles[%d] = %q, want %q; full order=%v", i, got[i], want[i], got)
		}
	}
}

func TestOpenClosedFilesParity(t *testing.T) {
	logsDir := t.TempDir()
	createLogFile(t, logsDir, "worker-6df6d5dd-01000000-47660.out", "line\n")
	createLogFile(t, logsDir, "raylet.err", "line\n")

	m, err := New("127.0.0.1", logsDir, &recordingPublisher{})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	m.maxFilesOpen = 1

	if err := m.updateLogFilenames(); err != nil {
		t.Fatalf("updateLogFilenames() error = %v", err)
	}
	if err := m.openClosedFiles(); err != nil {
		t.Fatalf("openClosedFiles() error = %v", err)
	}

	if len(m.openFiles) != 1 {
		t.Fatalf("len(openFiles) = %d, want 1", len(m.openFiles))
	}
	if m.canOpenMoreFiles {
		t.Fatal("canOpenMoreFiles = true, want false after open-file limit")
	}
}

func TestCloseAllFilesArchivesDeadWorkersParity(t *testing.T) {
	logsDir := t.TempDir()
	createLogFile(t, logsDir, "worker-6df6d5dd-01000000-11111.out", "line\n")
	createLogFile(t, logsDir, "worker-6df6d5dd-01000000-22222.err", "line\n")
	createLogFile(t, logsDir, "raylet.err", "line\n")

	m, err := New("127.0.0.1", logsDir, &recordingPublisher{})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	m.maxFilesOpen = 10
	m.isProcAlive = func(pid int) bool { return pid == 11111 }

	if err := m.updateLogFilenames(); err != nil {
		t.Fatalf("updateLogFilenames() error = %v", err)
	}
	if err := m.openClosedFiles(); err != nil {
		t.Fatalf("openClosedFiles() error = %v", err)
	}
	if err := m.closeAllFiles(); err != nil {
		t.Fatalf("closeAllFiles() error = %v", err)
	}

	if _, err := os.Stat(filepath.Join(logsDir, "old", "worker-6df6d5dd-01000000-22222.err")); err != nil {
		t.Fatalf("expected archived dead worker log: %v", err)
	}
}

func TestShouldUpdateFilenamesBackpressureParity(t *testing.T) {
	m := &Monitor{
		tracked:                map[string]struct{}{},
		manyFilesThreshold:     2,
		nameUpdateInterval:     500 * time.Millisecond,
		lastFilenameUpdateTime: time.Unix(0, 0),
	}
	m.tracked["a"] = struct{}{}
	if !m.shouldUpdateFilenames(time.Unix(0, 100)) {
		t.Fatal("expected updates when tracked count is below threshold")
	}
	m.tracked["b"] = struct{}{}
	m.tracked["c"] = struct{}{}
	if m.shouldUpdateFilenames(time.Unix(0, 100)) {
		t.Fatal("expected updates to be throttled before interval expires")
	}
	if !m.shouldUpdateFilenames(time.Unix(1, 0)) {
		t.Fatal("expected updates after interval expires")
	}
}

func TestPublishFailureDoesNotStopLoopParity(t *testing.T) {
	logsDir := t.TempDir()
	createLogFile(t, logsDir, "raylet.err", "line\n")

	publisher := &recordingPublisher{err: os.ErrInvalid}
	m, err := New("127.0.0.1", logsDir, publisher)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if err := m.updateLogFilenames(); err != nil {
		t.Fatalf("updateLogFilenames() error = %v", err)
	}
	if err := m.openClosedFiles(); err != nil {
		t.Fatalf("openClosedFiles() error = %v", err)
	}

	published, err := m.checkLogFilesAndPublishUpdates()
	if err != nil {
		t.Fatalf("checkLogFilesAndPublishUpdates() error = %v", err)
	}
	if !published {
		t.Fatal("expected published = true when new lines were consumed even if publish returns error")
	}
}

func TestUpdateLogFilenamesRemovesMissingFilesParity(t *testing.T) {
	logsDir := t.TempDir()
	path := createLogFile(t, logsDir, "raylet.err", "line\n")

	m, err := New("127.0.0.1", logsDir, &recordingPublisher{})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if err := m.updateLogFilenames(); err != nil {
		t.Fatalf("updateLogFilenames() error = %v", err)
	}

	if err := os.Remove(path); err != nil {
		t.Fatalf("remove log file: %v", err)
	}
	if err := m.openClosedFiles(); err != nil {
		t.Fatalf("openClosedFiles() error = %v", err)
	}

	if _, exists := m.tracked[path]; exists {
		t.Fatalf("tracked still contains removed file %q", path)
	}
}

func TestOpenClosedFilesSkipsUnchangedClosedFilesParity(t *testing.T) {
	logsDir := t.TempDir()
	createLogFile(t, logsDir, "raylet.err", "line\n")

	m, err := New("127.0.0.1", logsDir, &recordingPublisher{})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if err := m.updateLogFilenames(); err != nil {
		t.Fatalf("updateLogFilenames() error = %v", err)
	}
	if err := m.openClosedFiles(); err != nil {
		t.Fatalf("openClosedFiles() error = %v", err)
	}
	if len(m.openFiles) != 1 {
		t.Fatalf("len(openFiles) = %d, want 1", len(m.openFiles))
	}
	if err := m.closeAllFiles(); err != nil {
		t.Fatalf("closeAllFiles() error = %v", err)
	}
	if len(m.closedFiles) != 1 {
		t.Fatalf("len(closedFiles) = %d, want 1", len(m.closedFiles))
	}
	if err := m.openClosedFiles(); err != nil {
		t.Fatalf("second openClosedFiles() error = %v", err)
	}
	if len(m.openFiles) != 0 {
		t.Fatalf("len(openFiles) = %d, want 0 when file has not grown", len(m.openFiles))
	}
	if len(m.closedFiles) != 1 {
		t.Fatalf("len(closedFiles) = %d, want 1 when file has not grown", len(m.closedFiles))
	}
}

func TestNewWithConfigAppliesRuntimeOverrides(t *testing.T) {
	logsDir := t.TempDir()
	m, err := NewWithConfig("127.0.0.1", logsDir, &recordingPublisher{}, RuntimeConfig{
		RuntimeEnvToDriver:  true,
		AutoscalerV2Enabled: true,
		MaxFilesOpen:        77,
	})
	if err != nil {
		t.Fatalf("NewWithConfig() error = %v", err)
	}
	if !m.runtimeEnvToDriver || !m.autoscalerV2Enabled || m.maxFilesOpen != 77 {
		t.Fatalf("runtime config not applied: %#v", m)
	}

	logPath := filepath.Join(logsDir, "log_monitor.log")
	content, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("ReadFile(%q) error = %v", logPath, err)
	}
	if !strings.Contains(string(content), "Starting log monitor with [max open files=77], [is_autoscaler_v2=True]") {
		t.Fatalf("startup log missing: %q", string(content))
	}
}

func TestUpdateLogFilenamesLogsTrackedFiles(t *testing.T) {
	logsDir := t.TempDir()
	createLogFile(t, logsDir, "raylet.err", "line\n")

	m, err := New("127.0.0.1", logsDir, &recordingPublisher{})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if err := m.updateLogFilenames(); err != nil {
		t.Fatalf("updateLogFilenames() error = %v", err)
	}

	logPath := filepath.Join(logsDir, "log_monitor.log")
	content, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("ReadFile(%q) error = %v", logPath, err)
	}
	if !strings.Contains(string(content), "Beginning to track file raylet.err") {
		t.Fatalf("track-file log missing: %q", string(content))
	}
}
