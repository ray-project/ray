// Copyright 2025 The Ray Authors.
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

package worker

import (
	"testing"

	"github.com/ray-project/ray/go/pkg/options"
)

func TestNewOptions_Defaults(t *testing.T) {
	o := NewOptions(
		options.WorkerTypeWorker,
		options.NetworkOptions{},
		options.JobOptions{},
		options.RuntimeOptions{},
	)
	if o == nil {
		t.Fatal("expected non-nil Options")
	}
	if o.WorkerType != options.WorkerTypeWorker {
		t.Errorf("WorkerType = %v, want %v", o.WorkerType, options.WorkerTypeWorker)
	}
	if o.CodeSearchPath != nil {
		t.Errorf("expected nil CodeSearchPath by default, got %v", o.CodeSearchPath)
	}
}

func TestWithCodeSearchPath(t *testing.T) {
	paths := []string{"/lib1", "/lib2"}
	o := NewOptions(
		options.WorkerTypeDriver,
		options.NetworkOptions{},
		options.JobOptions{},
		options.RuntimeOptions{},
		WithCodeSearchPath(paths),
	)
	if o.WorkerType != options.WorkerTypeDriver {
		t.Errorf("WorkerType = %v, want %v", o.WorkerType, options.WorkerTypeDriver)
	}
	if len(o.CodeSearchPath) != 2 {
		t.Fatalf("CodeSearchPath length = %d, want %d", len(o.CodeSearchPath), 2)
	}
	for i, want := range paths {
		if o.CodeSearchPath[i] != want {
			t.Errorf("CodeSearchPath[%d] = %q, want %q", i, o.CodeSearchPath[i], want)
		}
	}
}

func TestWorker_NewAndInitialState(t *testing.T) {
	o := NewOptions(options.WorkerTypeWorker, options.NetworkOptions{}, options.JobOptions{}, options.RuntimeOptions{})
	w := New(o)
	if w == nil {
		t.Fatal("expected non-nil Worker")
	}
	if w.IsRunning() {
		t.Error("expected worker not running immediately after New")
	}
	if h := w.GetHandle(); h != nil {
		t.Errorf("expected nil handle before Run, got %v", h)
	}
	if w.opts != o {
		t.Error("expected New to retain the provided Options")
	}
}

func TestWorker_ShutdownBeforeRun(t *testing.T) {
	o := NewOptions(options.WorkerTypeWorker, options.NetworkOptions{}, options.JobOptions{}, options.RuntimeOptions{})
	w := New(o)
	// Shutdown before Run must not panic and must leave the worker idle.
	w.Shutdown()
	if w.IsRunning() {
		t.Error("expected worker not running after Shutdown")
	}
}
