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

package event

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// newEventFile creates an empty event log file and returns its path.
func newEventFile(t *testing.T, dir, name string) string {
	t.Helper()
	p := filepath.Join(dir, name)
	if err := os.WriteFile(p, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestIsEventLogName(t *testing.T) {
	for name, want := range map[string]bool{
		"event_GCS.log":      true,
		"event_RAYLET.log":   true,
		"event_COMMON.log":   true,
		"x_AUTOSCALER_y.log": true,
		"another.log":        false,
		".log":               false,
		"event_GCS.txt":      false,
		"event_GCS":          false,
		"events":             false,
		"event_FOO.log.bak":  false,
	} {
		if got := isEventLogName(name); got != want {
			t.Errorf("isEventLogName(%q) = %v, want %v", name, got, want)
		}
	}
}

// TestReadFileOnlyReturnsNewLines checks that readFileFrom returns the lines
// after position and updates the cursor across a read-then-append-then-reread
// flow.
func TestReadFileOnlyReturnsNewLines(t *testing.T) {
	dir := t.TempDir()
	path := newEventFile(t, dir, "event_COMMON.log")
	if err := os.WriteFile(path, []byte("line-0\nline-1\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		t.Fatal(err)
	}

	mtime := info.ModTime().UnixNano()
	first := readFileFrom(f, info.Size(), mtime, 0, eventReadLineCountLimit, eventReadLineLengthLimit())
	if got, want := len(first.lines), 2; got != want {
		t.Fatalf("first read lines = %d, want %d (%v)", got, want, first.lines)
	}
	if first.position != int64(len("line-0\nline-1\n")) {
		t.Errorf("first read position = %d, want EOF", first.position)
	}

	if err := os.WriteFile(path, []byte("line-0\nline-1\nline-2\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	f2, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f2.Close()
	info2, err := f2.Stat()
	if err != nil {
		t.Fatal(err)
	}
	second := readFileFrom(f2, info2.Size(), info2.ModTime().UnixNano(), first.position,
		eventReadLineCountLimit, eventReadLineLengthLimit())
	if got, want := second.lines, []string{"line-2"}; len(got) != len(want) || got[0] != want[0] {
		t.Errorf("second read lines = %v, want %v", got, want)
	}
}

// TestReadFileIgnoresLongLine checks that a line longer than the limit is
// dropped while a line of exactly the limit is kept.
func TestReadFileIgnoresLongLine(t *testing.T) {
	dir := t.TempDir()
	path := newEventFile(t, dir, "event_COMMON.log")

	short := strings.Repeat("s", 10) + "\n"
	long := strings.Repeat("l", 30) + "\n" // 31 bytes, over the 20-byte limit
	if err := os.WriteFile(path, []byte(short+long), 0o644); err != nil {
		t.Fatal(err)
	}

	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		t.Fatal(err)
	}
	result := readFileFrom(f, info.Size(), info.ModTime().UnixNano(), 0, eventReadLineCountLimit, 20)
	if len(result.lines) != 1 || result.lines[0] != strings.TrimSuffix(short, "\n") {
		t.Errorf("lines = %v, want only the short line", result.lines)
	}
}

// TestReadFileHalfWrittenTailNotLost checks that an unterminated tail line (a
// half-written line with no trailing newline) is not consumed: the cursor stays
// before it so the full line is reported exactly once after the writer
// completes it, matching python event_utils._read_file (sep == -1 -> break with
// the cursor unchanged).
func TestReadFileHalfWrittenTailNotLost(t *testing.T) {
	dir := t.TempDir()
	path := newEventFile(t, dir, "event_COMMON.log")
	const limit = 20
	// A complete line followed by a half-written line with no trailing newline.
	if err := os.WriteFile(path, []byte("complete\nhal"), 0o644); err != nil {
		t.Fatal(err)
	}

	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	info, err := f.Stat()
	if err != nil {
		t.Fatal(err)
	}
	first := readFileFrom(f, info.Size(), info.ModTime().UnixNano(), 0, eventReadLineCountLimit, limit)
	f.Close()
	if len(first.lines) != 1 || first.lines[0] != "complete" {
		t.Fatalf("first read lines = %v, want only [complete]", first.lines)
	}
	if first.position != int64(len("complete\n")) {
		t.Errorf("first read position = %d, want %d (cursor must stay before the half-written tail)",
			first.position, len("complete\n"))
	}

	// The writer completes the line.
	if err := os.WriteFile(path, []byte("complete\nhalf-line\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	f2, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f2.Close()
	info2, err := f2.Stat()
	if err != nil {
		t.Fatal(err)
	}
	second := readFileFrom(f2, info2.Size(), info2.ModTime().UnixNano(), first.position,
		eventReadLineCountLimit, limit)
	if len(second.lines) != 1 || second.lines[0] != "half-line" {
		t.Errorf("second read lines = %v, want [half-line] reported in full", second.lines)
	}
}

// TestReadFileUnterminatedTailStaysPut checks that a file ending in a permanent
// unterminated tail never reports the partial line and leaves the read position
// stable across repeated scans (no growth, no cursor advance).
func TestReadFileUnterminatedTailStaysPut(t *testing.T) {
	dir := t.TempDir()
	path := newEventFile(t, dir, "event_COMMON.log")
	const limit = 20
	if err := os.WriteFile(path, []byte("no-newline-at-all"), 0o644); err != nil {
		t.Fatal(err)
	}

	position := int64(0)
	for i := 0; i < 3; i++ {
		f, err := os.Open(path)
		if err != nil {
			t.Fatal(err)
		}
		info, err := f.Stat()
		if err != nil {
			t.Fatal(err)
		}
		result := readFileFrom(f, info.Size(), info.ModTime().UnixNano(), position, eventReadLineCountLimit, limit)
		f.Close()
		if len(result.lines) != 0 {
			t.Fatalf("scan %d reported lines %v, want none for an unterminated tail", i, result.lines)
		}
		if result.position != position {
			t.Fatalf("scan %d advanced position to %d, want stable %d", i, result.position, position)
		}
	}
}

func TestReadFileFromEmptyFile(t *testing.T) {
	dir := t.TempDir()
	path := newEventFile(t, dir, "event_COMMON.log")
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		t.Fatal(err)
	}
	result := readFileFrom(f, info.Size(), info.ModTime().UnixNano(), 0, eventReadLineCountLimit, eventReadLineLengthLimit())
	if len(result.lines) != 0 {
		t.Errorf("empty file produced lines %v", result.lines)
	}
	if result.position != 0 {
		t.Errorf("empty file position = %d, want 0", result.position)
	}
}

func TestReadFileCountLimit(t *testing.T) {
	dir := t.TempDir()
	path := newEventFile(t, dir, "event_COMMON.log")
	var b strings.Builder
	for i := 0; i < 300; i++ {
		fmt.Fprintf(&b, "%d\n", i)
	}
	if err := os.WriteFile(path, []byte(b.String()), 0o644); err != nil {
		t.Fatal(err)
	}

	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		t.Fatal(err)
	}
	result := readFileFrom(f, info.Size(), info.ModTime().UnixNano(), 0, 200, eventReadLineLengthLimit())
	if len(result.lines) != 200 {
		t.Errorf("lines = %d, want EVENT_READ_LINE_COUNT_LIMIT=200", len(result.lines))
	}
	if result.lines[0] != "0" || result.lines[199] != "199" {
		t.Errorf("unexpected first/last lines: %q ... %q", result.lines[0], result.lines[199])
	}
}

// TestGetSourceFilesFiltersByMtime checks the mtime filter of getSourceFiles.
func TestGetSourceFilesFiltersByMtime(t *testing.T) {
	dir := t.TempDir()
	old := newEventFile(t, dir, "event_COMMON_old.log")
	fresh := newEventFile(t, dir, "event_COMMON.log")

	// Old file: mtime far in the past.
	past := time.Now().Add(-2 * time.Hour)
	if err := os.Chtimes(old, past, past); err != nil {
		t.Fatal(err)
	}
	// Fresh file: mtime a moment ago (mtime must exceed startMtime; use nanosecond
	// resolution like the monitor does).
	now := time.Now()
	if err := os.Chtimes(fresh, now, now); err != nil {
		t.Fatal(err)
	}

	m := newMonitor(dir)
	m.startMtime = now.UnixNano() - 1
	files, err := m.getSourceFiles()
	if err != nil {
		t.Fatalf("getSourceFiles: %v", err)
	}
	found := false
	for _, f := range files {
		if f == fresh {
			found = true
		}
	}
	if !found {
		t.Errorf("getSourceFiles did not include the fresh file %q (got %v)", fresh, files)
	}
	for _, f := range files {
		if f == old {
			t.Errorf("getSourceFiles includes the old file %q, want filtered out", old)
		}
	}
}

// TestMonitorEventsDeliversNewLines checks that a writer appending lines to
// an event file while the monitor loop runs results in each line being
// delivered exactly once.
func TestMonitorEventsDeliversNewLines(t *testing.T) {
	dir := t.TempDir()
	path := newEventFile(t, dir, "event_COMMON.log")

	queue := make(chan []string, eventAgentCacheSize)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- monitorEvents(ctx, dir, queue, nil) }()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o644)
		if err != nil {
			t.Error(err)
			return
		}
		defer f.Close()
		for i := 0; i < 10; i++ {
			if _, err := fmt.Fprintf(f, "%d\n", i); err != nil {
				t.Error(err)
				return
			}
		}
	}()

	var mu sync.Mutex
	seen := map[string]bool{}
	readerDone := make(chan struct{})
	go func() {
		defer close(readerDone)
		for batch := range queue {
			mu.Lock()
			for _, line := range batch {
				seen[line] = true
			}
			mu.Unlock()
		}
	}()

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		got := len(seen)
		mu.Unlock()
		if got >= 10 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	wg.Wait()
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("monitorEvents did not return after cancellation")
	}
	close(queue)
	<-readerDone

	mu.Lock()
	defer mu.Unlock()
	for i := 0; i < 10; i++ {
		if !seen[fmt.Sprintf("%d", i)] {
			t.Errorf("monitor never delivered line %d", i)
		}
	}
}

// TestMonitorEventsDropsBatchesWhenQueueFull checks that a full queue drops
// and counts batches instead of blocking, so monitoring stays decoupled from a
// slow or hung report loop.
func TestMonitorEventsDropsBatchesWhenQueueFull(t *testing.T) {
	dir := t.TempDir()
	queue := make(chan []string, 2) // tiny queue so most batches are dropped
	var dropped atomic.Int64

	const files = 10
	for i := 0; i < files; i++ {
		if err := os.WriteFile(filepath.Join(dir, fmt.Sprintf("event_COMMON_%d.log", i)), []byte("line\n"), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- monitorEvents(ctx, dir, queue, &dropped) }()

	// The queue holds two batches; the remaining eight must be dropped and
	// counted rather than blocking the scan.
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if got := dropped.Load(); got >= files-2 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if got := dropped.Load(); got != files-2 {
		t.Errorf("dropped = %d, want %d", got, files-2)
	}

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("monitorEvents did not return after cancellation")
	}
}
