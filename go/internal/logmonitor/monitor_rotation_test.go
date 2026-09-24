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
)

func TestReopenIfNecessaryAfterReplacementParity(t *testing.T) {
	logsDir := t.TempDir()
	logPath := createLogFile(t, logsDir, "raylet.err", "line1\n")

	info := &logFileInfo{filename: logPath}
	if err := info.openAtCurrentPosition(); err != nil {
		t.Fatalf("openAtCurrentPosition() error = %v", err)
	}
	info.filePosition = int64(len("line1\nline2\n"))

	replacementPath := filepath.Join(logsDir, "replacement.err")
	if err := os.WriteFile(replacementPath, []byte("line2\n"), 0o644); err != nil {
		t.Fatalf("write replacement: %v", err)
	}
	if err := os.Rename(replacementPath, logPath); err != nil {
		t.Fatalf("replace file: %v", err)
	}

	if err := info.reopenIfNecessary(); err != nil {
		t.Fatalf("reopenIfNecessary() error = %v", err)
	}
	if info.filePosition != 0 {
		t.Fatalf("filePosition = %d, want 0 after replacement with smaller file", info.filePosition)
	}
}

func TestReopenIfNecessaryAfterTruncationParity(t *testing.T) {
	logsDir := t.TempDir()
	logPath := createLogFile(t, logsDir, "raylet.err", "line1\nline2\n")

	info := &logFileInfo{filename: logPath}
	if err := info.openAtCurrentPosition(); err != nil {
		t.Fatalf("openAtCurrentPosition() error = %v", err)
	}
	info.filePosition = int64(len("line1\nline2\n"))

	if err := os.WriteFile(logPath, []byte("short\n"), 0o644); err != nil {
		t.Fatalf("truncate write: %v", err)
	}

	if err := info.reopenIfNecessary(); err != nil {
		t.Fatalf("reopenIfNecessary() error = %v", err)
	}
	if info.filePosition != 0 {
		t.Fatalf("filePosition = %d, want 0 after truncation", info.filePosition)
	}
	line := make([]byte, len("short\n"))
	if _, err := info.fileHandle.Read(line); err != nil {
		t.Fatalf("Read() error = %v", err)
	}
	if got := string(line); got != "short\n" {
		t.Fatalf("first line after truncation = %q, want %q", got, "short\n")
	}
}

func TestReopenIfNecessaryKeepsPositionForLargerReplacementParity(t *testing.T) {
	logsDir := t.TempDir()
	logPath := createLogFile(t, logsDir, "raylet.err", "line1\nline2\n")

	info := &logFileInfo{filename: logPath}
	if err := info.openAtCurrentPosition(); err != nil {
		t.Fatalf("openAtCurrentPosition() error = %v", err)
	}
	info.filePosition = int64(len("line1\n"))

	replacementPath := filepath.Join(logsDir, "replacement.err")
	if err := os.WriteFile(replacementPath, []byte("line1\nline2\nline3\n"), 0o644); err != nil {
		t.Fatalf("write replacement: %v", err)
	}
	if err := os.Rename(replacementPath, logPath); err != nil {
		t.Fatalf("replace file: %v", err)
	}

	if err := info.reopenIfNecessary(); err != nil {
		t.Fatalf("reopenIfNecessary() error = %v", err)
	}
	if info.filePosition != int64(len("line1\n")) {
		t.Fatalf("filePosition = %d, want %d after larger replacement", info.filePosition, len("line1\n"))
	}
}

func TestInPlaceRewriteDetected(t *testing.T) {
	logsDir := t.TempDir()
	publisher := &recordingPublisher{}
	// ~2KB initial content so the rewrite below lands between the consumed
	// position and the last observed size.
	logPath := createLogFile(t, logsDir, "raylet.err", "AAA\nBBB\n"+strings.Repeat("x", 2000)+"\n")

	m, err := New("127.0.0.1", logsDir, publisher)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	m.maxLinesPerRead = 2

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
	if got := m.openFiles[0].filePosition; got != int64(len("AAA\nBBB\n")) {
		t.Fatalf("filePosition = %d, want %d", got, len("AAA\nBBB\n"))
	}
	size, err := m.openFiles[0].currentSize()
	if err != nil {
		t.Fatalf("currentSize() error = %v", err)
	}
	if m.openFiles[0].filePosition >= size {
		t.Fatalf("test setup broken: position %d must be below size %d", m.openFiles[0].filePosition, size)
	}

	// Truncate and rewrite in place (same inode) with content that is larger
	// than the consumed position but smaller than the last observed size.
	rewritten := "XXX\n" + strings.Repeat("y", 96)
	if err := os.WriteFile(logPath, []byte(rewritten), 0o644); err != nil {
		t.Fatalf("rewrite in place: %v", err)
	}

	published, err = m.checkLogFilesAndPublishUpdates()
	if err != nil {
		t.Fatalf("second checkLogFilesAndPublishUpdates() error = %v", err)
	}
	if !published {
		t.Fatal("expected rewritten content to be published")
	}
	last := publisher.batches[len(publisher.batches)-1]
	if len(last.Lines) == 0 || last.Lines[0] != "XXX" {
		t.Fatalf("lines = %#v, want first line \"XXX\" after in-place rewrite", last.Lines)
	}
	if got := m.openFiles[0].filePosition; got != int64(len(rewritten)) {
		t.Fatalf("filePosition = %d, want %d after rewrite", got, len(rewritten))
	}
}
