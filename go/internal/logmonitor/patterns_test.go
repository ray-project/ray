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
	"path"
	"path/filepath"
	"reflect"
	"sort"
	"testing"
)

func TestParseWorkerFilenameParity(t *testing.T) {
	jobID, pid, ok := parseWorkerFilename("/tmp/session/logs/worker-6df6d5dd-01000000-47660.out")
	if !ok {
		t.Fatal("expected worker filename to match Python worker regex")
	}
	if jobID != "01000000" {
		t.Fatalf("jobID = %q, want %q", jobID, "01000000")
	}
	if pid == nil || *pid != 47660 {
		t.Fatalf("pid = %v, want 47660", pid)
	}
}

func TestParseRuntimeEnvFilenameParity(t *testing.T) {
	jobID, ok := parseRuntimeEnvFilename("/tmp/session/logs/runtime_env_setup-12345.log")
	if !ok {
		t.Fatal("expected runtime_env filename to match Python runtime_env regex")
	}
	if jobID != "12345" {
		t.Fatalf("jobID = %q, want %q", jobID, "12345")
	}
}

func TestClassifyComponentPIDParity(t *testing.T) {
	tests := []struct {
		path string
		want string
	}{
		{path: "/tmp/logs/raylet.err", want: "raylet"},
		{path: "/tmp/logs/gcs_server.1.err", want: "gcs_server"},
		{path: "/tmp/logs/monitor.log", want: "autoscaler"},
		{path: filepath.Join("/tmp/logs/events", "event_AUTOSCALER.log"), want: "autoscaler"},
		{path: "/tmp/logs/runtime_env_setup-12345.log", want: "runtime_env"},
	}

	for _, tt := range tests {
		if got := classifyComponentPID(tt.path); got != tt.want {
			t.Fatalf("classifyComponentPID(%q) = %q, want %q", tt.path, got, tt.want)
		}
	}
}

func TestLogPatternsParity(t *testing.T) {
	patterns := logPatterns(true, true)
	want := map[string]bool{
		workerOutLogPattern: true,
		workerErrLogPattern: true,
		"java-worker*.log":  true,
		"raylet*.err":       true,
		"gcs_server*.err":   true,
		filepath.Join("events", "event_AUTOSCALER.log"): true,
		"runtime_env*.log": true,
	}

	for _, pattern := range patterns {
		delete(want, pattern)
	}
	if len(want) != 0 {
		t.Fatalf("missing patterns: %v", want)
	}
}

func TestParseWorkerFilenamePIDRangeValidation(t *testing.T) {
	tests := []struct {
		name string
		path string
		ok   bool
		pid  int
	}{
		{name: "pid zero", path: "worker-abc123-0", ok: false},
		{name: "pid negative", path: "worker-abc123--5", ok: false},
		{name: "pid equal upper bound", path: "worker-abc123-1000000", ok: false},
		{name: "pid above upper bound", path: "worker-abc123-1000001", ok: false},
		{name: "pid just below upper bound", path: "worker-abc123-999999", ok: true, pid: 999999},
		{name: "pid in range", path: "worker-abc123-12345", ok: true, pid: 12345},
		{name: "pid not numeric", path: "worker-abc123-abc", ok: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, pid, ok := parseWorkerFilename(tt.path)
			if ok != tt.ok {
				t.Fatalf("parseWorkerFilename(%q) ok = %v, want %v", tt.path, ok, tt.ok)
			}
			if !ok {
				return
			}
			if pid == nil || *pid != tt.pid {
				t.Fatalf("parseWorkerFilename(%q) pid = %v, want %d", tt.path, pid, tt.pid)
			}
		})
	}
}

func TestIsErrFile(t *testing.T) {
	tests := []struct {
		path string
		want bool
	}{
		{path: "worker.err", want: true},
		{path: "foo.xerr", want: false},
		{path: "worker.out", want: false},
		{path: "a/b/c.ERR", want: false},
	}

	for _, tt := range tests {
		if got := isErrFile(tt.path); got != tt.want {
			t.Fatalf("isErrFile(%q) = %v, want %v", tt.path, got, tt.want)
		}
	}
}

func TestReaddirPathsRecursive(t *testing.T) {
	tests := []struct {
		name     string
		maxDepth int
		setup    func(t *testing.T, dir string)
		wantRel  []string
		wantErr  bool
	}{
		{
			name:     "nested directories",
			maxDepth: 32,
			setup: func(t *testing.T, dir string) {
				if err := os.MkdirAll(filepath.Join(dir, "a", "b"), 0o755); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(dir, "a", "b", "c.log"), []byte("x"), 0o644); err != nil {
					t.Fatal(err)
				}
			},
			wantRel: []string{filepath.Join("a", "b", "c.log")},
		},
		{
			name:     "symlink to directory is descended",
			maxDepth: 32,
			setup: func(t *testing.T, dir string) {
				target := filepath.Join(t.TempDir(), "target")
				if err := os.MkdirAll(target, 0o755); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(target, "t.log"), []byte("x"), 0o644); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(target, filepath.Join(dir, "link")); err != nil {
					t.Fatal(err)
				}
			},
			wantRel: []string{filepath.Join("link", "t.log")},
		},
		{
			name:     "broken symlink is skipped",
			maxDepth: 32,
			setup: func(t *testing.T, dir string) {
				if err := os.Symlink(filepath.Join(dir, "missing"), filepath.Join(dir, "dangling")); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(dir, "f.log"), []byte("x"), 0o644); err != nil {
					t.Fatal(err)
				}
			},
			wantRel: []string{"f.log"},
		},
		{
			name:     "symlink loop does not hang",
			maxDepth: 32,
			setup: func(t *testing.T, dir string) {
				if err := os.Symlink("ln2", filepath.Join(dir, "ln1")); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink("ln1", filepath.Join(dir, "ln2")); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(dir, "f.log"), []byte("x"), 0o644); err != nil {
					t.Fatal(err)
				}
			},
			wantRel: []string{"f.log"},
		},
		{
			name:     "regular files at one level",
			maxDepth: 32,
			setup: func(t *testing.T, dir string) {
				for _, name := range []string{"a.log", "b.err", "c.out"} {
					if err := os.WriteFile(filepath.Join(dir, name), []byte("x"), 0o644); err != nil {
						t.Fatal(err)
					}
				}
			},
			wantRel: []string{"a.log", "b.err", "c.out"},
		},
		{
			name:     "max depth one collects one level only",
			maxDepth: 1,
			setup: func(t *testing.T, dir string) {
				for _, name := range []string{"a.log", "b.err"} {
					if err := os.WriteFile(filepath.Join(dir, name), []byte("x"), 0o644); err != nil {
						t.Fatal(err)
					}
				}
				if err := os.MkdirAll(filepath.Join(dir, "sub", "sub2"), 0o755); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(dir, "sub", "deep.log"), []byte("x"), 0o644); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(dir, "sub", "sub2", "deeper.log"), []byte("x"), 0o644); err != nil {
					t.Fatal(err)
				}
			},
			wantRel: []string{"a.log", "b.err"},
		},
		{
			name:     "max depth zero collects nothing",
			maxDepth: 0,
			setup: func(t *testing.T, dir string) {
				if err := os.WriteFile(filepath.Join(dir, "a.log"), []byte("x"), 0o644); err != nil {
					t.Fatal(err)
				}
			},
			wantRel: []string{},
		},
		{
			name:     "nonexistent directory returns error",
			maxDepth: 32,
			setup:    func(t *testing.T, dir string) {},
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			if tt.setup != nil {
				tt.setup(t, dir)
			}
			if tt.wantErr {
				_, err := readdirPathsRecursive(filepath.Join(dir, "missing"), tt.maxDepth)
				if err == nil {
					t.Fatal("readdirPathsRecursive() error = nil, want error")
				}
				return
			}
			got, err := readdirPathsRecursive(dir, tt.maxDepth)
			if err != nil {
				t.Fatalf("readdirPathsRecursive() error = %v", err)
			}
			want := make([]string, 0, len(tt.wantRel))
			for _, rel := range tt.wantRel {
				want = append(want, filepath.Join(dir, rel))
			}
			sort.Strings(got)
			sort.Strings(want)
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("readdirPathsRecursive(%q) = %v, want %v", dir, got, want)
			}
		})
	}
}

func TestWorkerLogGlobPatternsMatchOnlyOutAndErr(t *testing.T) {
	tests := []struct {
		pattern  string
		filename string
		want     bool
	}{
		{pattern: workerOutLogPattern, filename: "worker-abc-01000000-12345.out", want: true},
		{pattern: workerErrLogPattern, filename: "worker-abc-01000000-12345.err", want: true},
		{pattern: workerOutLogPattern, filename: "worker-abc-01000000-12345.err", want: false},
		{pattern: workerErrLogPattern, filename: "worker-abc-01000000-12345.out", want: false},
		{pattern: workerOutLogPattern, filename: "worker-abc-01000000-12345.log", want: false},
		{pattern: workerErrLogPattern, filename: "worker-abc-01000000-12345.log", want: false},
	}

	for _, tt := range tests {
		got, err := path.Match(tt.pattern, tt.filename)
		if err != nil {
			t.Fatalf("path.Match(%q, %q) returned error: %v", tt.pattern, tt.filename, err)
		}
		if got != tt.want {
			t.Fatalf("path.Match(%q, %q) = %v, want %v", tt.pattern, tt.filename, got, tt.want)
		}
	}
}
