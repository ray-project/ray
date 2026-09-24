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

type parityFile struct {
	Path     string `json:"path"`
	Contents string `json:"contents"`
}

type parityOperation struct {
	Kind     string `json:"kind"`
	Path     string `json:"path,omitempty"`
	Contents string `json:"contents,omitempty"`
	Size     int64  `json:"size,omitempty"`
}

type parityScenario struct {
	Name       string            `json:"name"`
	Files      []parityFile      `json:"files"`
	Operations []parityOperation `json:"operations"`
}

func TestPythonParityScenarios(t *testing.T) {
	scenarioPath := runfilePath(t, "go/internal/logmonitor/testdata/parity_scenarios.json")
	runnerPath := runfilePath(t, "go/internal/logmonitor/testdata/python_parity_runner.py")
	referencePath := runfilePath(t, "python/ray/_private/log_monitor.py")

	scenarios := loadParityScenarios(t, scenarioPath)
	goBatches := executeScenariosInGo(t, scenarios)
	pythonBatches := executeScenariosInPython(t, runnerPath, referencePath, scenarioPath)

	if !reflect.DeepEqual(goBatches, pythonBatches) {
		t.Fatalf("Go batches != Python batches\nGo: %#v\nPython: %#v", goBatches, pythonBatches)
	}
}

func loadParityScenarios(t *testing.T, path string) []parityScenario {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read scenarios: %v", err)
	}
	var scenarios []parityScenario
	if err := json.Unmarshal(data, &scenarios); err != nil {
		t.Fatalf("unmarshal scenarios: %v", err)
	}
	return scenarios
}

func executeScenariosInGo(t *testing.T, scenarios []parityScenario) map[string][]LogBatch {
	t.Helper()
	results := make(map[string][]LogBatch, len(scenarios))
	for _, scenario := range scenarios {
		logsDir := filepath.Join(t.TempDir(), scenario.Name)
		if err := os.MkdirAll(logsDir, 0o755); err != nil {
			t.Fatalf("mkdir logs dir: %v", err)
		}
		publisher := newAsyncRecordingPublisher()
		monitor, err := New("127.0.0.1", logsDir, publisher)
		if err != nil {
			t.Fatalf("New() error: %v", err)
		}
		monitor.maxFilesOpen = 8

		for _, file := range scenario.Files {
			path := filepath.Join(logsDir, file.Path)
			if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
				t.Fatalf("mkdir parent: %v", err)
			}
			if err := os.WriteFile(path, []byte(file.Contents), 0o644); err != nil {
				t.Fatalf("write initial file: %v", err)
			}
		}

		for _, op := range scenario.Operations {
			switch op.Kind {
			case "scan":
				if err := monitor.updateLogFilenames(); err != nil {
					t.Fatalf("updateLogFilenames() error: %v", err)
				}
			case "open":
				if err := monitor.openClosedFiles(); err != nil {
					t.Fatalf("openClosedFiles() error: %v", err)
				}
			case "drain":
				if _, err := monitor.checkLogFilesAndPublishUpdates(); err != nil {
					t.Fatalf("checkLogFilesAndPublishUpdates() error: %v", err)
				}
			case "append":
				appendLog(t, filepath.Join(logsDir, op.Path), op.Contents)
			case "replace":
				writeFileAtomically(t, filepath.Join(logsDir, op.Path), op.Contents)
			case "truncate":
				truncateFile(t, filepath.Join(logsDir, op.Path), op.Size)
			default:
				t.Fatalf("unknown operation kind %q", op.Kind)
			}
		}

		results[scenario.Name] = publisher.Snapshot()
	}
	return results
}

func executeScenariosInPython(t *testing.T, runnerPath, referencePath, scenarioPath string) map[string][]LogBatch {
	t.Helper()
	logsRoot := t.TempDir()
	cmd := exec.Command("python3", runnerPath, referencePath, scenarioPath, logsRoot)
	cmd.Env = os.Environ()
	var stdout bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("python parity runner failed: %v", err)
	}
	var out map[string][]LogBatch
	if err := json.Unmarshal(stdout.Bytes(), &out); err != nil {
		t.Fatalf("unmarshal python output: %v\nstdout=%s", err, stdout.String())
	}
	return out
}

func normalizeBatches(in []LogBatch) []LogBatch {
	out := make([]LogBatch, len(in))
	for i, batch := range in {
		out[i] = LogBatch{
			IP:        batch.IP,
			PID:       batch.PID,
			JobID:     batch.JobID,
			IsErr:     batch.IsErr,
			Lines:     append([]string(nil), batch.Lines...),
			ActorName: batch.ActorName,
			TaskName:  batch.TaskName,
		}
	}
	return out
}
