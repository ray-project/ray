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
	"bytes"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"testing"
)

// TestRealSessionReplayParity verifies that the Go log monitor produces
// the same output as the Python log monitor when processing real session logs.
//
// This test requires LOGMONITOR_REAL_LOGS_DIR to be set to a directory
// containing captured Ray session logs.
func TestRealSessionReplayParity(t *testing.T) {
	realLogsDir := os.Getenv("LOGMONITOR_REAL_LOGS_DIR")
	if realLogsDir == "" {
		t.Skip("LOGMONITOR_REAL_LOGS_DIR not set, skipping real session replay test")
	}

	if _, err := os.Stat(realLogsDir); os.IsNotExist(err) {
		t.Skipf("Real logs directory does not exist: %s", realLogsDir)
	}

	// Create a temporary directory for the test
	tmpDir := t.TempDir()

	// Copy real logs to temp directory
	testLogsDir := filepath.Join(tmpDir, "logs")
	if err := os.MkdirAll(testLogsDir, 0o755); err != nil {
		t.Fatalf("Failed to create temp logs dir: %v", err)
	}

	// Copy a subset of log files for testing
	entries, err := os.ReadDir(realLogsDir)
	if err != nil {
		t.Fatalf("Failed to read real logs dir: %v", err)
	}

	copied := 0
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if !(filepath.Ext(name) == ".out" || filepath.Ext(name) == ".err") {
			continue
		}
		src := filepath.Join(realLogsDir, name)
		dst := filepath.Join(testLogsDir, name)
		data, err := os.ReadFile(src)
		if err != nil {
			t.Logf("Skipping %s: %v", name, err)
			continue
		}
		if err := os.WriteFile(dst, data, 0o644); err != nil {
			t.Fatalf("Failed to copy %s: %v", name, err)
		}
		copied++
	}

	if copied == 0 {
		t.Skip("No log files found to test")
	}

	t.Logf("Copied %d log files for testing", copied)

	// Run Python reference runner
	pythonRunner := runfilePath(t, "go/internal/logmonitor/testdata/real_session_replay_runner.py")
	referencePath := runfilePath(t, "python/ray/_private/log_monitor.py")
	pythonCmd := exec.Command("python3", pythonRunner, referencePath, testLogsDir)
	var pythonOut bytes.Buffer
	pythonCmd.Stdout = &pythonOut
	pythonCmd.Stderr = os.Stderr
	if err := pythonCmd.Run(); err != nil {
		t.Fatalf("Python runner failed: %v", err)
	}

	// Parse Python output
	var pythonEntries []LogBatch
	if err := json.Unmarshal(pythonOut.Bytes(), &pythonEntries); err != nil {
		t.Fatalf("Failed to parse Python output: %v", err)
	}

	// Run Go log monitor in one-shot mode
	publisher := &recordingPublisher{}
	m, err := NewWithConfig("127.0.0.1", testLogsDir, publisher, RuntimeConfig{
		RuntimeEnvToDriver:  false,
		AutoscalerV2Enabled: false,
		MaxFilesOpen:        100,
	})
	if err != nil {
		t.Fatalf("NewWithConfig() error = %v", err)
	}

	// Manually trigger file discovery and processing
	if err := m.updateLogFilenames(); err != nil {
		t.Fatalf("updateLogFilenames() error = %v", err)
	}
	if err := m.openClosedFiles(); err != nil {
		t.Fatalf("openClosedFiles() error = %v", err)
	}
	if _, err := m.checkLogFilesAndPublishUpdates(); err != nil {
		t.Fatalf("checkLogFilesAndPublishUpdates() error = %v", err)
	}

	goEntries := normalizeBatches(publisher.batches)
	pythonEntries = normalizeBatches(pythonEntries)
	if !reflect.DeepEqual(goEntries, pythonEntries) {
		t.Fatalf("Go batches != Python batches\nGo: %#v\nPython: %#v", goEntries, pythonEntries)
	}

	t.Logf("Parity check passed: %d entries matched", len(goEntries))
}
