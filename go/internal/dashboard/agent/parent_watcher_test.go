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

package agent

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// stubParentPid replaces the parent-pid source for the duration of a test.
func stubParentPid(t *testing.T, fn func() int) {
	t.Helper()
	old := parentPidFn
	parentPidFn = fn
	t.Cleanup(func() { parentPidFn = old })
}

func TestRayletPidRejectsMissingAndMalformedValues(t *testing.T) {
	for _, raw := range []string{"", "abc", "0", "-3"} {
		t.Run(raw, func(t *testing.T) {
			t.Setenv(rayletPIDEnvName, raw)
			if pid, ok := rayletPid(); ok {
				t.Fatalf("rayletPid(%q) = (%d, true), want ok=false", raw, pid)
			}
		})
	}
}

func TestRayletPidAcceptsAPositiveInteger(t *testing.T) {
	t.Setenv(rayletPIDEnvName, "4321")
	pid, ok := rayletPid()
	if !ok || pid != 4321 {
		t.Fatalf("rayletPid = (%d, %v), want (4321, true)", pid, ok)
	}
}

func TestParentCheckIntervalFallsBackWhenUnsetOrNonPositive(t *testing.T) {
	t.Setenv(parentCheckIntervalSEnvName, "")
	if got := parentCheckInterval(); got != defaultParentCheckInterval {
		t.Fatalf("interval = %v, want %v", got, defaultParentCheckInterval)
	}
	t.Setenv(parentCheckIntervalSEnvName, "0")
	if got := parentCheckInterval(); got != defaultParentCheckInterval {
		t.Fatalf("interval = %v, want the default for a non-positive override", got)
	}
	t.Setenv(parentCheckIntervalSEnvName, "0.01")
	if got := parentCheckInterval(); got != 10*time.Millisecond {
		t.Fatalf("interval = %v, want 10ms", got)
	}
	// A positive float can still convert to a non-positive duration. Both of
	// these fall back to the default rather than reaching time.NewTicker, which
	// panics on a non-positive interval.
	t.Setenv(parentCheckIntervalSEnvName, "1e-12")
	if got := parentCheckInterval(); got != defaultParentCheckInterval {
		t.Fatalf("interval = %v, want the default for a sub-nanosecond override", got)
	}
	t.Setenv(parentCheckIntervalSEnvName, "1e30")
	if got := parentCheckInterval(); got != defaultParentCheckInterval {
		t.Fatalf("interval = %v, want the default for an overflowing override", got)
	}
}

// TestParentWatcherCancelsOnlyAfterTheThreshold proves a single missed poll is
// tolerated and that cancellation happens exactly at the fifth.
func TestParentWatcherCancelsOnlyAfterTheThreshold(t *testing.T) {
	t.Setenv(rayletPIDEnvName, "4242")
	t.Setenv(parentCheckIntervalSEnvName, "0.005")

	var calls atomic.Int32
	// The first four polls still see the raylet, the rest do not.
	stubParentPid(t, func() int {
		if calls.Add(1) <= 4 {
			return 4242
		}
		return 1
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	StartParentWatcher(ctx, cancel, t.TempDir())

	select {
	case <-ctx.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("watcher never cancelled the context")
	}
	// Polls 1-4 matched, polls 5-9 are the five consecutive misses; the
	// cancel happens on poll 9.
	if got := calls.Load(); got < 9 {
		t.Fatalf("cancelled after %d polls, want at least 9 (4 matches + %d misses)", got, parentDeathThreshold)
	}
}

// TestParentWatcherRecoversFromATransientMiss proves the counter resets, so a
// flapping parent cannot accumulate its way to a false positive.
func TestParentWatcherRecoversFromATransientMiss(t *testing.T) {
	t.Setenv(rayletPIDEnvName, "4242")
	t.Setenv(parentCheckIntervalSEnvName, "0.005")

	// miss, miss, miss, match, then stay matched forever.
	var calls atomic.Int32
	stubParentPid(t, func() int {
		if n := calls.Add(1); n <= 3 {
			return 1
		}
		return 4242
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	StartParentWatcher(ctx, cancel, t.TempDir())

	select {
	case <-ctx.Done():
		t.Fatal("three transient misses cancelled the context; the threshold is five consecutive misses")
	case <-time.After(200 * time.Millisecond):
	}
}

// TestParentWatcherResetsTheDeathCountOnAMatch pins the reset: a flapping
// parent must not accumulate transient misses into a false positive. Polls
// alternate between a miss and a match, so a counter that is not cleared on a
// matched poll reaches the threshold while a cleared one stays at one.
func TestParentWatcherResetsTheDeathCountOnAMatch(t *testing.T) {
	t.Setenv(rayletPIDEnvName, "4242")
	t.Setenv(parentCheckIntervalSEnvName, "0.005")

	var calls atomic.Int32
	stubParentPid(t, func() int {
		if calls.Add(1)%2 == 1 {
			return 1
		}
		return 4242
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	StartParentWatcher(ctx, cancel, t.TempDir())

	// Long enough for several times the threshold's worth of missed polls.
	select {
	case <-ctx.Done():
		t.Fatalf("flapping parent cancelled the context after %d polls; a matched poll must reset the death count", calls.Load())
	case <-time.After(300 * time.Millisecond):
	}
	if got := calls.Load(); got < 2*parentDeathThreshold {
		t.Fatalf("observed only %d polls, want at least %d to cover the threshold", got, 2*parentDeathThreshold)
	}
}

func TestParentWatcherIsANoOpWithoutARayletPid(t *testing.T) {
	t.Setenv(rayletPIDEnvName, "")
	// The interval must be short, or the 400ms default would hide a watcher
	// that starts up anyway: its fifth miss would land long after this test
	// ends, and the test would pass no matter what the guard does.
	t.Setenv(parentCheckIntervalSEnvName, "0.005")
	// Every poll sees a parent that is not the raylet, which is exactly the
	// condition that kills the agent when a raylet pid is configured.
	stubParentPid(t, func() int { return 1 })

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	StartParentWatcher(ctx, cancel, t.TempDir())

	// 100ms spans 20 polls at the 5ms interval, four times the five-miss
	// threshold a configured raylet pid would trip at ~25ms.
	select {
	case <-ctx.Done():
		t.Fatal("watcher cancelled the context with no raylet pid; a missing or malformed value must never shut down a healthy agent")
	case <-time.After(100 * time.Millisecond):
	}
}

func TestClassifyRayletDeathDetectsGracefulExit(t *testing.T) {
	graceful, msg := classifyRayletDeath([]string{"starting", "Raylet received SIGTERM, shutting down"})
	if !graceful {
		t.Fatalf("classifyRayletDeath = (graceful=false, %q), want graceful=true for a SIGTERM log", msg)
	}
	if msg != "" {
		t.Errorf("classifyRayletDeath message = %q, want empty for a graceful exit", msg)
	}
	// A tail window can start on the marker itself, so a marker-only input must
	// classify as graceful too. This also pins that the marker decides the
	// outcome, rather than the presence of any non-marker line.
	graceful, msg = classifyRayletDeath([]string{"Raylet received SIGTERM, shutting down"})
	if !graceful {
		t.Fatalf("classifyRayletDeath(marker only) = (graceful=false, %q), want graceful=true", msg)
	}
}

// TestClassifyRayletDeathHandlesUnexpectedExit asserts the marker's absence
// classifies as unexpected and that the surfaced tail is the last 20 lines, not
// the whole log.
func TestClassifyRayletDeathHandlesUnexpectedExit(t *testing.T) {
	const firstLine = "first line that must be truncated away"
	lines := []string{firstLine}
	for len(lines) < rayletLogMaxPublishLines+10 {
		lines = append(lines, "filler line")
	}
	lines = append(lines, "last line")

	graceful, msg := classifyRayletDeath(lines)
	if graceful {
		t.Fatal("classifyRayletDeath = graceful=true for a log without the SIGTERM marker, want false")
	}
	if !strings.HasPrefix(msg, "Raylet is terminated. ") {
		t.Errorf("message %q does not start with the Python prefix %q", msg, "Raylet is terminated. ")
	}
	if !strings.Contains(msg, "Termination is unexpected") {
		t.Errorf("message %q does not report an unexpected termination", msg)
	}
	if strings.Contains(msg, firstLine) {
		t.Errorf("message %q contains the first line, want only the last %d lines", msg, rayletLogMaxPublishLines)
	}
	if !strings.Contains(msg, "    last line") {
		t.Errorf("message %q does not contain the last line prefixed with four spaces", msg)
	}
	// Python joins the tail as `"    " + "    ".join(lines)`, so every surfaced
	// line is prefixed with four spaces and nothing else is added.
	const tailHeader = "lines of the Raylet logs:\n"
	idx := strings.Index(msg, tailHeader)
	if idx < 0 {
		t.Fatalf("message %q does not contain the tail header %q", msg, tailHeader)
	}
	for _, line := range strings.Split(strings.TrimSuffix(msg[idx+len(tailHeader):], "\n"), "\n") {
		if !strings.HasPrefix(line, "    ") {
			t.Errorf("tail line %q is not prefixed with four spaces", line)
		}
	}
}

func TestReportRayletDeathToleratesAMissingLog(t *testing.T) {
	dir := t.TempDir()
	if _, err := tailFileLines(filepath.Join(dir, "raylet.out"), rayletLogTailSize); err == nil {
		t.Fatal("tailFileLines on a missing raylet.out = nil error, want an error")
	}
	// The read-failure and empty-log-dir paths must be silent no-ops rather
	// than panics: there is nothing to classify without a log.
	reportRayletDeath(dir)
	reportRayletDeath("")
}

// TestReportRayletDeathReadsOnlyTheTailWindow proves a log larger than the
// 1 MiB window is still tailed successfully: the read starts at
// size-rayletLogTailSize, so the marker at the very end is found without
// loading the whole file.
func TestReportRayletDeathReadsOnlyTheTailWindow(t *testing.T) {
	dir := t.TempDir()
	pad := strings.Repeat("x", 2*int(rayletLogTailSize))
	writeRayletLog(t, dir, pad+"\nRaylet received SIGTERM, shutting down\n")

	lines, err := tailFileLines(filepath.Join(dir, "raylet.out"), rayletLogTailSize)
	if err != nil {
		t.Fatalf("tailFileLines: %v", err)
	}
	if len(lines) > 3 {
		t.Fatalf("read %d lines, want only the tail window", len(lines))
	}
	// The pad holds no newlines, so a whole-file read also yields three
	// elements and the count above alone proves nothing. The truncated first
	// element is what distinguishes a windowed read: it cannot be longer than
	// the window, while a whole-file read would return the full 2 MiB pad.
	if got := len(lines[0]); got >= int(rayletLogTailSize) {
		t.Fatalf("first tailed line is %d bytes, want fewer than the %d-byte window: the whole file was read", got, rayletLogTailSize)
	}
	if !strings.Contains(lines[len(lines)-2], rayletGracefulExitMarker) {
		t.Fatalf("tail = %q, want the graceful-exit marker near the end", lines[len(lines)-2])
	}
	// The marker lives in the tail window, so the tailed lines classify as a
	// graceful exit.
	if graceful, msg := classifyRayletDeath(lines); !graceful {
		t.Fatalf("classifyRayletDeath(tail) = graceful=false, %q, want a graceful exit", msg)
	}
}

// writeRayletLog writes content to <dir>/raylet.out.
func writeRayletLog(t *testing.T, dir, content string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, "raylet.out"), []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}
