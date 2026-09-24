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
	"context"
	"errors"
	"fmt"
	"os"
	"reflect"
	"testing"
	"time"
)

func TestIntegrationWithMonitorRun(t *testing.T) {
	logsDir := t.TempDir()
	publisher := newAsyncRecordingPublisher()
	logPath := createLogFile(t, logsDir, "worker-6df6d5dd-01000000-47660.out", "first\n")

	m, err := New("127.0.0.1", logsDir, publisher)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	m.maxFilesOpen = 8

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- m.Run(ctx)
	}()

	first := publisher.WaitForBatchCount(t, 1, 2*time.Second)
	if got := first[0].Lines; !reflect.DeepEqual(got, []string{"first"}) {
		t.Fatalf("first lines = %#v, want %#v", got, []string{"first"})
	}

	appendLog(t, logPath, "second\n")
	second := publisher.WaitForBatchCount(t, 2, 2*time.Second)
	if got := second[1].Lines; !reflect.DeepEqual(got, []string{"second"}) {
		t.Fatalf("second lines = %#v, want %#v", got, []string{"second"})
	}

	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("Run() error = %v, want context.Canceled", err)
	}
}

func TestIntegrationActorTaskJobPrefixTransitions(t *testing.T) {
	logsDir := t.TempDir()
	publisher := newAsyncRecordingPublisher()
	logPath := createLogFile(t, logsDir, "worker-6df6d5dd-01000000-47660.out", "initial\n")

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

	// First drain to consume initial content
	published, err := m.checkLogFilesAndPublishUpdates()
	if err != nil {
		t.Fatalf("initial checkLogFilesAndPublishUpdates() error = %v", err)
	}
	if !published {
		t.Fatal("expected initial published = true")
	}

	// Now append prefixes and payload
	appendLog(t, logPath, logPrefixActorName+"ActorA\n")
	appendLog(t, logPath, logPrefixTaskName+"TaskA\n")
	appendLog(t, logPath, logPrefixJobID+"02000000\n")
	appendLog(t, logPath, "payload\n")

	published, err = m.checkLogFilesAndPublishUpdates()
	if err != nil {
		t.Fatalf("checkLogFilesAndPublishUpdates() error = %v", err)
	}
	if !published {
		t.Fatal("expected published = true")
	}

	batches := publisher.Snapshot()
	if len(batches) != 2 {
		t.Fatalf("len(batches) = %d, want 2", len(batches))
	}
	if batches[1].ActorName != "ActorA" || batches[1].TaskName != "TaskA" || batches[1].JobID != "02000000" {
		t.Fatalf("batch metadata = %#v, want actor/task/job transitions applied", batches[1])
	}
}

func TestIntegrationReopenAfterCloseAll(t *testing.T) {
	logsDir := t.TempDir()
	publisher := newAsyncRecordingPublisher()
	logPath := createLogFile(t, logsDir, "raylet.err", "one\n")

	m, err := New("127.0.0.1", logsDir, publisher)
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
	if _, err := m.checkLogFilesAndPublishUpdates(); err != nil {
		t.Fatalf("checkLogFilesAndPublishUpdates() error = %v", err)
	}
	if err := m.closeAllFiles(); err != nil {
		t.Fatalf("closeAllFiles() error = %v", err)
	}

	appendLog(t, logPath, "two\n")
	if err := m.openClosedFiles(); err != nil {
		t.Fatalf("second openClosedFiles() error = %v", err)
	}
	if _, err := m.checkLogFilesAndPublishUpdates(); err != nil {
		t.Fatalf("second checkLogFilesAndPublishUpdates() error = %v", err)
	}

	batches := publisher.Snapshot()
	if len(batches) != 2 {
		t.Fatalf("len(batches) = %d, want 2", len(batches))
	}
	if got := batches[1].Lines; !reflect.DeepEqual(got, []string{"two"}) {
		t.Fatalf("reopened lines = %#v, want %#v", got, []string{"two"})
	}
}

func TestIntegrationReplacementAndTruncation(t *testing.T) {
	logsDir := t.TempDir()
	publisher := newAsyncRecordingPublisher()
	logPath := createLogFile(t, logsDir, "worker-6df6d5dd-01000000-47660.err", "alpha\nbeta\n")

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
	if _, err := m.checkLogFilesAndPublishUpdates(); err != nil {
		t.Fatalf("initial checkLogFilesAndPublishUpdates() error = %v", err)
	}

	writeFileAtomically(t, logPath, "gamma\n")
	if _, err := m.checkLogFilesAndPublishUpdates(); err != nil {
		t.Fatalf("replacement checkLogFilesAndPublishUpdates() error = %v", err)
	}

	appendLog(t, logPath, "delta\n")
	if _, err := m.checkLogFilesAndPublishUpdates(); err != nil {
		t.Fatalf("append-after-replacement error = %v", err)
	}

	truncateFile(t, logPath, 0)
	appendLog(t, logPath, "epsilon\n")
	if _, err := m.checkLogFilesAndPublishUpdates(); err != nil {
		t.Fatalf("truncate checkLogFilesAndPublishUpdates() error = %v", err)
	}

	batches := publisher.Snapshot()
	if got := batches[len(batches)-1].Lines; !reflect.DeepEqual(got, []string{"epsilon"}) {
		t.Fatalf("last lines = %#v, want %#v", got, []string{"epsilon"})
	}
}

func TestIntegrationMissingFileDuringOpen(t *testing.T) {
	logsDir := t.TempDir()
	logPath := createLogFile(t, logsDir, "raylet.err", "line\n")

	m, err := New("127.0.0.1", logsDir, newAsyncRecordingPublisher())
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if err := m.updateLogFilenames(); err != nil {
		t.Fatalf("updateLogFilenames() error = %v", err)
	}
	if err := os.Remove(logPath); err != nil {
		t.Fatalf("remove log file: %v", err)
	}
	if err := m.openClosedFiles(); err != nil {
		t.Fatalf("openClosedFiles() error = %v", err)
	}
	if _, ok := m.tracked[logPath]; ok {
		t.Fatalf("tracked still contains %q", logPath)
	}
}

func TestIntegrationRunContinuesAfterPublishFailure(t *testing.T) {
	logsDir := t.TempDir()
	logPath := createLogFile(t, logsDir, "raylet.err", "one\n")

	publisher := newAsyncRecordingPublisher()
	publisher.err = os.ErrInvalid

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
		t.Fatal("expected published = true even when publisher returns error")
	}

	appendLog(t, logPath, "two\n")
	published, err = m.checkLogFilesAndPublishUpdates()
	if err != nil {
		t.Fatalf("second checkLogFilesAndPublishUpdates() error = %v", err)
	}
	if !published {
		t.Fatal("expected second published = true after publish failure")
	}
}

// TestIntegrationMultipleFilesGrowingConcurrently tests that the monitor
// correctly handles multiple log files growing simultaneously.
// This covers the "the concurrent multi-file growth scenario" scenario from the test plan.
func TestIntegrationMultipleFilesGrowingConcurrently(t *testing.T) {
	logsDir := t.TempDir()
	publisher := newAsyncRecordingPublisher()

	// Create 5 worker log files with initial content
	const numFiles = 5
	logPaths := make([]string, numFiles)
	for i := 0; i < numFiles; i++ {
		logPaths[i] = createLogFile(t, logsDir,
			fmt.Sprintf("worker-6df6d5dd-01000000-%d.out", 10000+i),
			fmt.Sprintf("initial-%d\n", i))
	}

	m, err := New("127.0.0.1", logsDir, publisher)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	m.maxFilesOpen = 8

	// Start the monitor
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- m.Run(ctx)
	}()

	// Wait for initial batches from all files
	initialBatches := publisher.WaitForBatchCount(t, numFiles, 2*time.Second)
	if len(initialBatches) != numFiles {
		t.Fatalf("expected %d initial batches, got %d", numFiles, len(initialBatches))
	}

	// Verify initial content
	for i := 0; i < numFiles; i++ {
		found := false
		for _, batch := range initialBatches {
			if len(batch.Lines) == 1 && batch.Lines[0] == fmt.Sprintf("initial-%d", i) {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("file %d initial content not found in batches", i)
		}
	}

	// Now append new content to all files simultaneously
	for i := 0; i < numFiles; i++ {
		appendLog(t, logPaths[i], fmt.Sprintf("appended-%d\n", i))
	}

	// Wait for appended batches (should be numFiles more)
	appendedBatches := publisher.WaitForBatchCount(t, numFiles*2, 2*time.Second)
	if len(appendedBatches) != numFiles*2 {
		t.Fatalf("expected %d total batches, got %d", numFiles*2, len(appendedBatches))
	}

	// Verify all appended content was received
	for i := 0; i < numFiles; i++ {
		found := false
		for _, batch := range appendedBatches {
			if len(batch.Lines) == 1 && batch.Lines[0] == fmt.Sprintf("appended-%d", i) {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("file %d appended content not found in batches", i)
		}
	}

	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("Run() error = %v, want context.Canceled", err)
	}
}

// TestIntegrationMaxFilesOpenWithMultipleGrowingFiles tests the file pool
// management under pressure when maxFilesOpen is set to a small value.
// This covers the "file pool pressure test: maxFilesOpen limit" scenario from the test plan.
func TestIntegrationMaxFilesOpenWithMultipleGrowingFiles(t *testing.T) {
	logsDir := t.TempDir()
	publisher := newAsyncRecordingPublisher()

	// Create 5 worker log files with initial content
	// Use current test process PID to ensure isProcAlive returns true
	testPID := os.Getpid()
	const numFiles = 5
	const maxFilesOpen = 2 // Very small limit to force close/reopen cycles
	logPaths := make([]string, numFiles)
	for i := 0; i < numFiles; i++ {
		logPaths[i] = createLogFile(t, logsDir,
			fmt.Sprintf("worker-6df6d5dd-01000000-%d.out", testPID+i),
			fmt.Sprintf("line1-%d\n", i))
	}

	m, err := New("127.0.0.1", logsDir, publisher)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	m.maxFilesOpen = maxFilesOpen
	m.isProcAlive = func(int) bool { return true }

	// Start the monitor
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- m.Run(ctx)
	}()

	// Wait for initial batches from all files
	initialBatches := publisher.WaitForBatchCount(t, numFiles, 3*time.Second)
	if len(initialBatches) != numFiles {
		t.Fatalf("expected %d initial batches, got %d", numFiles, len(initialBatches))
	}

	// Verify all initial content was received
	for i := 0; i < numFiles; i++ {
		found := false
		for _, batch := range initialBatches {
			if len(batch.Lines) == 1 && batch.Lines[0] == fmt.Sprintf("line1-%d", i) {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("file %d initial content not found in batches", i)
		}
	}

	// Now append second line to all files
	for i := 0; i < numFiles; i++ {
		appendLog(t, logPaths[i], fmt.Sprintf("line2-%d\n", i))
	}

	// Wait for second round of batches
	secondBatches := publisher.WaitForBatchCount(t, numFiles*2, 3*time.Second)
	if len(secondBatches) != numFiles*2 {
		t.Fatalf("expected %d total batches after second append, got %d", numFiles*2, len(secondBatches))
	}

	// Verify all second line content was received
	for i := 0; i < numFiles; i++ {
		found := false
		for _, batch := range secondBatches {
			if len(batch.Lines) == 1 && batch.Lines[0] == fmt.Sprintf("line2-%d", i) {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("file %d second line content not found in batches", i)
		}
	}

	// Append third line to verify continued operation
	for i := 0; i < numFiles; i++ {
		appendLog(t, logPaths[i], fmt.Sprintf("line3-%d\n", i))
	}

	// Wait for third round of batches
	thirdBatches := publisher.WaitForBatchCount(t, numFiles*3, 3*time.Second)
	if len(thirdBatches) != numFiles*3 {
		t.Fatalf("expected %d total batches after third append, got %d", numFiles*3, len(thirdBatches))
	}

	// Verify all third line content was received
	for i := 0; i < numFiles; i++ {
		found := false
		for _, batch := range thirdBatches {
			if len(batch.Lines) == 1 && batch.Lines[0] == fmt.Sprintf("line3-%d", i) {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("file %d third line content not found in batches", i)
		}
	}

	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("Run() error = %v, want context.Canceled", err)
	}

	// Verify no batches were duplicated
	seenLines := make(map[string]bool)
	for _, batch := range thirdBatches {
		for _, line := range batch.Lines {
			if seenLines[line] {
				t.Errorf("duplicate line found: %s", line)
			}
			seenLines[line] = true
		}
	}

	// Verify all expected lines were received exactly once
	for i := 0; i < numFiles; i++ {
		for j := 1; j <= 3; j++ {
			line := fmt.Sprintf("line%d-%d", j, i)
			if !seenLines[line] {
				t.Errorf("missing line: %s", line)
			}
		}
	}
}
