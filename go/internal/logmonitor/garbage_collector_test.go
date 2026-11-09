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
	"testing"
	"time"
)

// createFileOfSize creates a file of exactly size bytes under root/rel and
// returns its full path. Mirrors the C++ GcTestBase::CreateFileOfSize helper.
func createFileOfSize(t *testing.T, root, rel string, size int) string {
	t.Helper()
	path := filepath.Join(root, rel)
	if err := os.WriteFile(path, make([]byte, size), 0o644); err != nil {
		t.Fatalf("create %s: %v", path, err)
	}
	return path
}

// touchFileAge rewinds the file mtime to age in the past. Mirrors the C++
// GcTestBase::TouchFile helper.
func touchFileAge(t *testing.T, path string, age time.Duration) {
	t.Helper()
	old := time.Now().Add(-age)
	if err := os.Chtimes(path, old, old); err != nil {
		t.Fatalf("touch %s: %v", path, err)
	}
}

// newGcFixture builds a GarbageCollector for tests with an explicit retention
// (7 days, matching the C++ default), a large default total-size cap so Tier 2
// never interferes with Tier-1-focused tests, GC enabled, and a recorder for
// every onFileDeleted callback.
func newGcFixture(t *testing.T, dir string, alive func(int) bool, deleted *[]string) *GarbageCollector {
	t.Helper()
	return &GarbageCollector{
		logsDir:     dir,
		isProcAlive: alive,
		onFileDeleted: func(p string) {
			if deleted != nil {
				*deleted = append(*deleted, p)
			}
		},
		gcInterval:    300 * time.Second,
		retention:     604800 * time.Second,
		maxTotalBytes: 10737418240,
		enabled:       true,
	}
}

func expectDeleted(t *testing.T, path string) {
	t.Helper()
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("expected file %s to be deleted, stat err = %v", path, err)
	}
}

func expectKept(t *testing.T, path string) {
	t.Helper()
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("expected file %s to be kept, stat err = %v", path, err)
	}
}

// --- PID extraction ---

func TestExtractCandidatePID(t *testing.T) {
	cases := []struct {
		name string
		file string
		want int
	}{
		{"controller", "controller_12345.log", 12345},
		{"controller_build", "controller_build_downstream_6789.log", 6789},
		{"replica", "replica_svc_abc_5678.log", 5678},
		{"proxy", "proxy_10.0.0.1_9012.log", 9012},
		{"rotated_serve_log", "replica_svc_abc_5678.log.1", 5678},
		{"event_core_worker", "event_CORE_WORKER_12345.log", 12345},
		{"event_export_task", "event_EXPORT_TASK_12345.log", 12345},
		{"worker_out", "worker-abc123-45678.out", 45678},
		{"worker_err", "worker-abc123-45678.err", 45678},
		{"java_worker", "java-worker-abc123-90123.log", 90123},
		{"python_core_driver", "python-core-driver-abc123_def_34567.log", 34567},
		{"python_core_worker", "python-core-worker-abc123_def_56789.log", 56789},
		{"cpp_core_driver", "cpp-core-driver-abc123_def_11111.log", 11111},
		{"cpp_core_worker_real", "cpp-core-worker-2c6d3e9e94a53b99049354f1525e11dbced71114c84a545c0070c5da_129.log", 129},
		{"mm_multi_segment", "mm_core_mm_22222.log", 22222},
		{"mm_upstream", "mm_upstream_upstream_49nghfme_33333.log", 33333},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pid := extractCandidatePID(tc.file)
			if pid == nil {
				t.Fatalf("extractCandidatePID(%q) = nil, want %d", tc.file, tc.want)
			}
			if *pid != tc.want {
				t.Fatalf("extractCandidatePID(%q) = %d, want %d", tc.file, *pid, tc.want)
			}
		})
	}
}

func TestExtractCandidatePIDNoPid(t *testing.T) {
	files := []string{
		"event_GCS.log",
		"event_EXPORT_ACTOR.log",
		"job-driver-submission_42.log",
		"runtime_env_setup-12345.log",
		"mm_core_mm_49nghfme.log",
		"cpp-core-worker-2c6d5da.log",
	}
	for _, file := range files {
		if pid := extractCandidatePID(file); pid != nil {
			t.Fatalf("extractCandidatePID(%q) = %d, want nil", file, *pid)
		}
	}
}

func TestExtractCandidatePIDEdgeCases(t *testing.T) {
	cases := []struct {
		file string
		want int
		ok   bool
	}{
		{"", 0, false},
		{"worker-abc-xyz.out", 0, false},
		{"worker-abc-99999999.out", 0, false},
		{"/tmp/ray/logs/worker-abc-12345.out", 12345, true},
	}
	for _, tc := range cases {
		pid := extractCandidatePID(tc.file)
		if !tc.ok {
			if pid != nil {
				t.Fatalf("extractCandidatePID(%q) = %d, want nil", tc.file, *pid)
			}
			continue
		}
		if pid == nil || *pid != tc.want {
			t.Fatalf("extractCandidatePID(%q) = %v, want %d", tc.file, pid, tc.want)
		}
	}
}

// --- Tier 1 ---

func TestTier1DeadProcessExpiredMtimeDeletes(t *testing.T) {
	dir := t.TempDir()
	path := createFileOfSize(t, dir, "worker-abc-99999.out", 100)
	touchFileAge(t, path, 999999*time.Second)

	var deleted []string
	gc := newGcFixture(t, dir, func(int) bool { return false }, &deleted)
	gc.CollectNow()

	expectDeleted(t, path)
	if len(deleted) != 1 {
		t.Fatalf("deleted count = %d, want 1 (%v)", len(deleted), deleted)
	}
	if deleted[0] != path {
		t.Fatalf("deleted path = %q, want %q", deleted[0], path)
	}
}

func TestTier1ActiveProcessExpiredMtimeKeeps(t *testing.T) {
	dir := t.TempDir()
	path := createFileOfSize(t, dir, "worker-abc-99998.out", 100)
	touchFileAge(t, path, 999999*time.Second)

	var deleted []string
	gc := newGcFixture(t, dir, func(int) bool { return true }, &deleted)
	gc.CollectNow()

	expectKept(t, path)
	if len(deleted) != 0 {
		t.Fatalf("deleted count = %d, want 0", len(deleted))
	}
}

func TestTier1NoPidExpiredMtimeDeletes(t *testing.T) {
	dir := t.TempDir()
	path := createFileOfSize(t, dir, "job-driver-sub_42.log", 100)
	touchFileAge(t, path, 999999*time.Second)

	var deleted []string
	// isProcAlive is not consulted for no-PID files.
	gc := newGcFixture(t, dir, func(int) bool { return true }, &deleted)
	gc.CollectNow()

	expectDeleted(t, path)
	if len(deleted) != 1 {
		t.Fatalf("deleted count = %d, want 1", len(deleted))
	}
}

func TestTier1DeadProcessRecentMtimeKeeps(t *testing.T) {
	dir := t.TempDir()
	path := createFileOfSize(t, dir, "worker-abc-99997.out", 100)

	var deleted []string
	gc := newGcFixture(t, dir, func(int) bool { return false }, &deleted)
	gc.CollectNow()

	expectKept(t, path)
	if len(deleted) != 0 {
		t.Fatalf("deleted count = %d, want 0", len(deleted))
	}
}

func TestTier1MtimeJustUnderRetentionKeeps(t *testing.T) {
	dir := t.TempDir()
	path := createFileOfSize(t, dir, "worker-abc-99996.out", 100)
	// Strict greater-than: 604700s < 604800s retention must keep the file.
	touchFileAge(t, path, 604700*time.Second)

	var deleted []string
	gc := newGcFixture(t, dir, func(int) bool { return false }, &deleted)
	gc.CollectNow()

	expectKept(t, path)
	if len(deleted) != 0 {
		t.Fatalf("deleted count = %d, want 0", len(deleted))
	}
}

// --- Tier 2 ---

func TestTier2EvictsOldestFirst(t *testing.T) {
	dir := t.TempDir()
	oldest := createFileOfSize(t, dir, "worker-aaa-11111.out", 100)
	touchFileAge(t, oldest, 100*time.Second)
	middle := createFileOfSize(t, dir, "worker-bbb-22222.out", 100)
	touchFileAge(t, middle, 50*time.Second)
	newest := createFileOfSize(t, dir, "worker-ccc-33333.out", 100)

	var deleted []string
	gc := newGcFixture(t, dir, func(int) bool { return false }, &deleted)
	gc.maxTotalBytes = 100
	gc.CollectNow()

	expectDeleted(t, oldest)
	expectDeleted(t, middle)
	expectKept(t, newest)
	if len(deleted) != 2 {
		t.Fatalf("deleted count = %d, want 2 (%v)", len(deleted), deleted)
	}
}

func TestTier2SkipsAliveProcess(t *testing.T) {
	dir := t.TempDir()
	aaa := createFileOfSize(t, dir, "worker-aaa-55555.out", 100)
	touchFileAge(t, aaa, 100*time.Second)
	bbb := createFileOfSize(t, dir, "worker-bbb-44444.out", 100)
	touchFileAge(t, bbb, 80*time.Second)
	ccc := createFileOfSize(t, dir, "worker-ccc-66666.out", 100)
	touchFileAge(t, ccc, 50*time.Second)
	ddd := createFileOfSize(t, dir, "worker-ddd-77777.out", 100)

	var deleted []string
	gc := newGcFixture(t, dir, func(pid int) bool { return pid == 44444 }, &deleted)
	gc.maxTotalBytes = 100
	gc.CollectNow()

	expectKept(t, bbb)
	expectDeleted(t, aaa)
	expectDeleted(t, ccc)
	expectDeleted(t, ddd)
	if len(deleted) != 3 {
		t.Fatalf("deleted count = %d, want 3 (%v)", len(deleted), deleted)
	}
}

func TestTier2UnderCapNoOp(t *testing.T) {
	dir := t.TempDir()
	e := createFileOfSize(t, dir, "worker-eee-88888.out", 50)
	f := createFileOfSize(t, dir, "worker-fff-99999.out", 50)

	var deleted []string
	gc := newGcFixture(t, dir, func(int) bool { return false }, &deleted)
	gc.maxTotalBytes = 100
	gc.CollectNow()

	expectKept(t, e)
	expectKept(t, f)
	if len(deleted) != 0 {
		t.Fatalf("deleted count = %d, want 0", len(deleted))
	}
}

// --- Subdirectory scanning ---

func TestScanDirectoryUsesCustomPatterns(t *testing.T) {
	dir := t.TempDir()
	createFileOfSize(t, dir, "controller_12345.log", 100)
	createFileOfSize(t, dir, "worker-abc-99999.out", 100)

	gc := &GarbageCollector{logsDir: dir}
	candidates := gc.scanDirectory(dir, serveFilePatterns)

	if len(candidates) != 1 {
		t.Fatalf("scanDirectory(serveFilePatterns) candidates = %d, want 1", len(candidates))
	}
	if filepath.Base(candidates[0].path) != "controller_12345.log" {
		t.Fatalf("candidate path = %q, want controller_12345.log", candidates[0].path)
	}
	if candidates[0].pid == nil || *candidates[0].pid != 12345 {
		t.Fatalf("candidate pid = %v, want 12345", candidates[0].pid)
	}
}

func TestCollectNowScansServeSubdir(t *testing.T) {
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "serve"), 0o755); err != nil {
		t.Fatalf("mkdir serve: %v", err)
	}
	path := createFileOfSize(t, dir, "serve/controller_12345.log", 100)
	touchFileAge(t, path, 999999*time.Second)

	var deleted []string
	gc := newGcFixture(t, dir, func(int) bool { return false }, &deleted)
	gc.CollectNow()

	expectDeleted(t, path)
	if len(deleted) != 1 {
		t.Fatalf("deleted count = %d, want 1", len(deleted))
	}
}

func TestCollectNowScansEventsSubdir(t *testing.T) {
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "events"), 0o755); err != nil {
		t.Fatalf("mkdir events: %v", err)
	}
	path := createFileOfSize(t, dir, "events/event_GCS.log", 100)
	touchFileAge(t, path, 999999*time.Second)

	var deleted []string
	gc := newGcFixture(t, dir, func(int) bool { return false }, &deleted)
	gc.CollectNow()

	expectDeleted(t, path)
	if len(deleted) != 1 {
		t.Fatalf("deleted count = %d, want 1", len(deleted))
	}
}

func TestCollectNowScansExportEventsSubdir(t *testing.T) {
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "export_events"), 0o755); err != nil {
		t.Fatalf("mkdir export_events: %v", err)
	}
	path := createFileOfSize(t, dir, "export_events/event_EXPORT_ACTOR.log", 100)
	touchFileAge(t, path, 999999*time.Second)

	var deleted []string
	gc := newGcFixture(t, dir, func(int) bool { return false }, &deleted)
	gc.CollectNow()

	expectDeleted(t, path)
	if len(deleted) != 1 {
		t.Fatalf("deleted count = %d, want 1", len(deleted))
	}
}

func TestCollectNowCleansOldDirectory(t *testing.T) {
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "old"), 0o755); err != nil {
		t.Fatalf("mkdir old: %v", err)
	}
	path := createFileOfSize(t, dir, "old/worker-old-33333.out", 100)
	touchFileAge(t, path, 999999*time.Second)

	var deleted []string
	gc := newGcFixture(t, dir, func(int) bool { return false }, &deleted)
	gc.CollectNow()

	expectDeleted(t, path)
	if len(deleted) != 1 {
		t.Fatalf("deleted count = %d, want 1", len(deleted))
	}
}

func TestCollectNowMissingSubdirNoCrash(t *testing.T) {
	dir := t.TempDir()
	path := createFileOfSize(t, dir, "worker-keep-55555.out", 100)

	var deleted []string
	gc := newGcFixture(t, dir, func(int) bool { return true }, &deleted)
	gc.CollectNow()

	expectKept(t, path)
}

// --- Interval guard ---

func TestMaybeCollectIntervalGuard(t *testing.T) {
	dir := t.TempDir()
	path := createFileOfSize(t, dir, "worker-abc-88888.out", 100)
	touchFileAge(t, path, 999999*time.Second)

	var deleted []string
	gc := newGcFixture(t, dir, func(int) bool { return false }, &deleted)
	gc.gcInterval = 300 * time.Second
	gc.lastRun = time.Now().Add(-(301 * time.Second))

	gc.MaybeCollect()
	if len(deleted) != 1 {
		t.Fatalf("deleted count after first MaybeCollect = %d, want 1", len(deleted))
	}

	gc.MaybeCollect()
	if len(deleted) != 1 {
		t.Fatalf("deleted count after immediate second MaybeCollect = %d, want 1 (interval guard)", len(deleted))
	}

	gc.enabled = false
	gc.lastRun = time.Time{}
	gc.MaybeCollect()
	if len(deleted) != 1 {
		t.Fatalf("deleted count after disabled MaybeCollect = %d, want 1", len(deleted))
	}
}

// TestMaybeCollectDisabledIsNoOp verifies that GC is always created by New()
// regardless of RAY_log_gc_enabled, and that the enabled flag read at
// construction gates the Run-loop cycle: with RAY_log_gc_enabled=false an
// expired file survives MaybeCollect.
func TestMaybeCollectDisabledIsNoOp(t *testing.T) {
	t.Setenv("RAY_log_gc_enabled", "false")
	dir := t.TempDir()
	path := createFileOfSize(t, dir, "worker-abc-99994.out", 100)
	touchFileAge(t, path, 999999*time.Second)

	m, err := New("127.0.0.1", dir, &recordingPublisher{})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if m.gc == nil {
		t.Fatal("expected gc to be created regardless of RAY_log_gc_enabled")
	}
	if m.gc.enabled {
		t.Fatal("expected gc.enabled = false when RAY_log_gc_enabled=false")
	}
	m.gc.isProcAlive = func(int) bool { return false }
	m.gc.gcInterval = 300 * time.Second
	m.gc.lastRun = time.Now().Add(-(301 * time.Second))

	m.gc.MaybeCollect()
	expectKept(t, path)
}

// --- Monitor wiring ---

// TestRemoveTrackedFileCleansAllStructures verifies that removeTrackedFile
// drops a path from tracked, openFiles (closing the handle) and closedFiles at
// once, mirroring C++ OnGcFileDeleted.
func TestRemoveTrackedFileCleansAllStructures(t *testing.T) {
	logsDir := t.TempDir()
	p := createLogFile(t, logsDir, "worker-tracked-11111.out", "line\n")

	m, err := New("127.0.0.1", logsDir, &recordingPublisher{})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	handle, err := os.Open(p)
	if err != nil {
		t.Fatalf("open %s: %v", p, err)
	}
	m.tracked[p] = struct{}{}
	m.openFiles = []*logFileInfo{{filename: p, fileHandle: handle}}
	m.closedFiles = []*logFileInfo{{filename: p}}

	m.removeTrackedFile(p)

	if len(m.tracked) != 0 {
		t.Fatalf("len(tracked) = %d, want 0", len(m.tracked))
	}
	if len(m.openFiles) != 0 {
		t.Fatalf("len(openFiles) = %d, want 0", len(m.openFiles))
	}
	if len(m.closedFiles) != 0 {
		t.Fatalf("len(closedFiles) = %d, want 0", len(m.closedFiles))
	}
	if err := handle.Close(); err == nil {
		t.Fatal("expected file handle to be closed by removeTrackedFile")
	}
}

// TestRemoveTrackedFileNoOpForUntracked verifies that an untracked path leaves
// all three structures untouched.
func TestRemoveTrackedFileNoOpForUntracked(t *testing.T) {
	logsDir := t.TempDir()
	trackedA := createLogFile(t, logsDir, "worker-tracked-a.out", "line\n")
	trackedB := createLogFile(t, logsDir, "worker-tracked-b.out", "line\n")

	m, err := New("127.0.0.1", logsDir, &recordingPublisher{})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	handleA, err := os.Open(trackedA)
	if err != nil {
		t.Fatalf("open %s: %v", trackedA, err)
	}
	m.tracked[trackedA] = struct{}{}
	m.tracked[trackedB] = struct{}{}
	m.openFiles = []*logFileInfo{{filename: trackedA, fileHandle: handleA}}
	m.closedFiles = []*logFileInfo{{filename: trackedB}}

	untracked := filepath.Join(logsDir, "worker-untracked-00000.out")
	m.removeTrackedFile(untracked)

	if len(m.tracked) != 2 {
		t.Fatalf("len(tracked) = %d, want 2", len(m.tracked))
	}
	if len(m.openFiles) != 1 || m.openFiles[0].filename != trackedA {
		t.Fatalf("openFiles = %v, want [%s]", m.openFiles, trackedA)
	}
	if m.openFiles[0].fileHandle == nil {
		t.Fatal("openFiles[0].fileHandle = nil, want open handle preserved")
	}
	if len(m.closedFiles) != 1 || m.closedFiles[0].filename != trackedB {
		t.Fatalf("closedFiles = %v, want [%s]", m.closedFiles, trackedB)
	}
	_ = handleA.Close()
}

// TestGcEndToEndDeletesTrackedFile mirrors the C++ GcCollectNow end-to-end
// test: GC deletes an expired dead-process file and the tracking structures
// are cleaned up through the onFileDeleted callback.
func TestGcEndToEndDeletesTrackedFile(t *testing.T) {
	logsDir := t.TempDir()
	p := createLogFile(t, logsDir, "worker-e2e-55555.out", "line\n")
	touchFileAge(t, p, 999999*time.Second)

	m, err := New("127.0.0.1", logsDir, &recordingPublisher{})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	// Deterministic dead check regardless of what PID 55555 refers to on the
	// host (mirrors the C++ is_proc_alive=[](int){return false;} unit tests).
	m.gc.isProcAlive = func(int) bool { return false }

	handle, err := os.Open(p)
	if err != nil {
		t.Fatalf("open %s: %v", p, err)
	}
	m.tracked[p] = struct{}{}
	m.openFiles = []*logFileInfo{{filename: p, fileHandle: handle}}
	m.closedFiles = []*logFileInfo{{filename: p}}

	m.gc.CollectNow()

	expectDeleted(t, p)
	if len(m.tracked) != 0 {
		t.Fatalf("len(tracked) = %d, want 0", len(m.tracked))
	}
	if len(m.openFiles) != 0 {
		t.Fatalf("len(openFiles) = %d, want 0", len(m.openFiles))
	}
	if len(m.closedFiles) != 0 {
		t.Fatalf("len(closedFiles) = %d, want 0", len(m.closedFiles))
	}
}

// TestGcEndToEndKeepsLiveProcessFile mirrors the C++ GcCollectNow keeps-live
// test: a file whose process is alive is kept and its tracking structures
// stay intact. The PID is fixed at a value below the 1000000 valid-PID bound
// (the test process PID can exceed it on hosts with large PID counters).
func TestGcEndToEndKeepsLiveProcessFile(t *testing.T) {
	logsDir := t.TempDir()
	p := createLogFile(t, logsDir, "worker-e2e-12345.out", "line\n")
	touchFileAge(t, p, 999999*time.Second)

	m, err := New("127.0.0.1", logsDir, &recordingPublisher{})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	m.gc.isProcAlive = func(pid int) bool { return pid == 12345 }

	handle, err := os.Open(p)
	if err != nil {
		t.Fatalf("open %s: %v", p, err)
	}
	m.tracked[p] = struct{}{}
	m.openFiles = []*logFileInfo{{filename: p, fileHandle: handle}}

	m.gc.CollectNow()

	expectKept(t, p)
	if len(m.tracked) != 1 {
		t.Fatalf("len(tracked) = %d, want 1", len(m.tracked))
	}
	if len(m.openFiles) != 1 || m.openFiles[0].filename != p {
		t.Fatalf("openFiles = %v, want [%s]", m.openFiles, p)
	}
	if m.openFiles[0].fileHandle == nil {
		t.Fatal("openFiles[0].fileHandle = nil, want open handle preserved")
	}
	_ = handle.Close()
}
