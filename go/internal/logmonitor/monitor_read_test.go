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
	"testing"
)

func appendLog(t *testing.T, path, contents string) {
	t.Helper()
	file, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatalf("open append file: %v", err)
	}
	defer file.Close()
	if _, err := file.WriteString(contents); err != nil {
		t.Fatalf("append file: %v", err)
	}
}

func TestCheckLogFilesAndPublishUpdatesParity(t *testing.T) {
	logsDir := t.TempDir()
	publisher := &recordingPublisher{}
	logPath := createLogFile(t, logsDir, "worker-6df6d5dd-01000000-47660.out", "First line\n")

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
		t.Fatal("expected initial content to be published")
	}
	if got := len(publisher.batches); got != 1 {
		t.Fatalf("len(batches) = %d, want 1", got)
	}
	if publisher.batches[0].Lines[0] != "First line" {
		t.Fatalf("first batch line = %q, want %q", publisher.batches[0].Lines[0], "First line")
	}

	appendLog(t, logPath, logPrefixTaskName+"task\nline\n")
	published, err = m.checkLogFilesAndPublishUpdates()
	if err != nil {
		t.Fatalf("checkLogFilesAndPublishUpdates() second error = %v", err)
	}
	if !published {
		t.Fatal("expected appended content to be published")
	}

	last := publisher.batches[len(publisher.batches)-1]
	if last.TaskName != "task" {
		t.Fatalf("task name = %q, want %q", last.TaskName, "task")
	}
	if len(last.Lines) != 1 || last.Lines[0] != "line" {
		t.Fatalf("lines = %#v, want []string{\"line\"}", last.Lines)
	}
}

func TestMaxLinesPerReadParity(t *testing.T) {
	logsDir := t.TempDir()
	publisher := &recordingPublisher{}
	logPath := createLogFile(t, logsDir, "raylet.err", "")

	m, err := New("127.0.0.1", logsDir, publisher)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	m.maxLinesPerRead = 3

	if err := m.updateLogFilenames(); err != nil {
		t.Fatalf("updateLogFilenames() error = %v", err)
	}
	if err := m.openClosedFiles(); err != nil {
		t.Fatalf("openClosedFiles() error = %v", err)
	}

	appendLog(t, logPath, "1\n2\n3\n4\n5\n")
	if err := m.openClosedFiles(); err != nil {
		t.Fatalf("second openClosedFiles() error = %v", err)
	}

	// The first poll must stop at the per-read cap.
	published, err := m.checkLogFilesAndPublishUpdates()
	if err != nil {
		t.Fatalf("checkLogFilesAndPublishUpdates() error = %v", err)
	}
	if !published {
		t.Fatal("expected lines to be published")
	}
	last := publisher.batches[len(publisher.batches)-1]
	if len(last.Lines) != 3 {
		t.Fatalf("len(lines) = %d, want 3", len(last.Lines))
	}
	if last.Lines[0] != "1" || last.Lines[1] != "2" || last.Lines[2] != "3" {
		t.Fatalf("lines = %#v, want [1 2 3]", last.Lines)
	}
	if got := m.openFiles[0].filePosition; got != int64(len("1\n2\n3\n")) {
		t.Fatalf("filePosition = %d, want %d; unconsumed buffered lines must not advance the recorded position", got, len("1\n2\n3\n"))
	}

	// Lines beyond the cap must still arrive on later polls instead of
	// being skipped.
	published, err = m.checkLogFilesAndPublishUpdates()
	if err != nil {
		t.Fatalf("second checkLogFilesAndPublishUpdates() error = %v", err)
	}
	if !published {
		t.Fatal("expected lines left beyond the cap to be published on the next poll")
	}
	last = publisher.batches[len(publisher.batches)-1]
	if len(last.Lines) != 2 || last.Lines[0] != "4" || last.Lines[1] != "5" {
		t.Fatalf("lines = %#v, want [4 5]", last.Lines)
	}

	total := 0
	for _, batch := range publisher.batches {
		total += len(batch.Lines)
	}
	if total != 5 {
		t.Fatalf("total published lines = %d, want 5", total)
	}
}
