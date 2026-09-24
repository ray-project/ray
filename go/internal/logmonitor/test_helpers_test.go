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
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

type asyncRecordingPublisher struct {
	mu      sync.Mutex
	batches []LogBatch
	err     error
	notify  chan struct{}
}

func newAsyncRecordingPublisher() *asyncRecordingPublisher {
	return &asyncRecordingPublisher{
		notify: make(chan struct{}, 32),
	}
}

func (p *asyncRecordingPublisher) Publish(batch LogBatch) error {
	p.mu.Lock()
	p.batches = append(p.batches, batch)
	p.mu.Unlock()

	select {
	case p.notify <- struct{}{}:
	default:
	}
	return p.err
}

func (p *asyncRecordingPublisher) Snapshot() []LogBatch {
	p.mu.Lock()
	defer p.mu.Unlock()

	out := make([]LogBatch, len(p.batches))
	copy(out, p.batches)
	return out
}

func (p *asyncRecordingPublisher) WaitForBatchCount(t *testing.T, want int, timeout time.Duration) []LogBatch {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		snapshot := p.Snapshot()
		if len(snapshot) >= want {
			return snapshot
		}
		select {
		case <-p.notify:
		case <-time.After(10 * time.Millisecond):
		}
	}

	t.Fatalf("timed out waiting for %d batches; got %d", want, len(p.Snapshot()))
	return nil
}

func writeFileAtomically(t *testing.T, path, contents string) {
	t.Helper()
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, []byte(contents), 0o644); err != nil {
		t.Fatalf("write temp file %s: %v", tmp, err)
	}
	if err := os.Rename(tmp, path); err != nil {
		t.Fatalf("rename %s -> %s: %v", tmp, path, err)
	}
}

func truncateFile(t *testing.T, path string, size int64) {
	t.Helper()
	if err := os.Truncate(path, size); err != nil {
		t.Fatalf("truncate %s: %v", path, err)
	}
}

func runfilePath(t *testing.T, short string) string {
	t.Helper()
	root := os.Getenv("TEST_SRCDIR")
	workspace := os.Getenv("TEST_WORKSPACE")
	if root == "" || workspace == "" {
		t.Fatal("TEST_SRCDIR or TEST_WORKSPACE is empty")
	}
	return filepath.Join(root, workspace, short)
}

func writeJSONFile(t *testing.T, path string, value any) {
	t.Helper()
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		t.Fatalf("marshal json: %v", err)
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatalf("write json file %s: %v", path, err)
	}
}
