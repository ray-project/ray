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
	"path"
	"path/filepath"
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
