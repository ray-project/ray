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

package worker

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/ray-project/ray/go/pkg/options"
	"github.com/ray-project/ray/go/pkg/runtime/plugin"
)

// newTestWorker builds a Worker whose seams are injected fakes.
func newTestWorker(t *testing.T, workerType options.WorkerType, jobID string) *Worker {
	t.Helper()
	sock := filepath.Join(t.TempDir(), "store.sock")
	raylet := filepath.Join(t.TempDir(), "raylet.sock")
	if err := os.WriteFile(sock, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(raylet, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	return New(NewOptions(
		workerType,
		options.NetworkOptions{
			NodeIPAddress:   "127.0.0.1",
			NodeManagerPort: 12345,
			GcsAddress:      "127.0.0.1:6379",
		},
		options.JobOptions{JobID: jobID, ClusterID: "00"},
		options.RuntimeOptions{
			StoreSocket:  sock,
			RayletSocket: raylet,
			LogDir:       t.TempDir(),
		},
	))
}

func TestRunValidatesPluginBeforeSockets(t *testing.T) {
	w := newTestWorker(t, options.WorkerTypeWorker, "")

	origValidate := validatePluginLoadableFn
	validatePluginLoadableFn = func() error {
		return plugin.ErrPluginNotLoadable(errors.New("plugin missing"))
	}
	defer func() { validatePluginLoadableFn = origValidate }()

	origInit := initRuntimeWithOptionsFn
	initRuntimeWithOptionsFn = func(*options.InitializeOptions) error {
		t.Fatal("initRuntimeWithOptionsFn must not be called when the plugin is unloadable")
		return nil
	}
	defer func() { initRuntimeWithOptionsFn = origInit }()

	err := w.Run()
	if err == nil {
		t.Fatal("Run() = nil error, want plugin error")
	}
	if got := err.Error(); got != "go_runtime.so plugin not loadable: plugin missing" {
		t.Fatalf("Run() error = %q, want plugin-not-loadable message", got)
	}
}

func TestRunValidatesSocketsBeforeInit(t *testing.T) {
	w := New(NewOptions(
		options.WorkerTypeWorker,
		options.NetworkOptions{NodeIPAddress: "127.0.0.1", NodeManagerPort: 12345, GcsAddress: "127.0.0.1:6379"},
		options.JobOptions{},
		options.RuntimeOptions{StoreSocket: "/nonexistent/store.sock", RayletSocket: "/nonexistent/raylet.sock", LogDir: t.TempDir()},
	))
	origInit := initRuntimeWithOptionsFn
	called := false
	initRuntimeWithOptionsFn = func(*options.InitializeOptions) error {
		called = true
		return nil
	}
	defer func() { initRuntimeWithOptionsFn = origInit }()

	_ = w.Run()
	if called {
		t.Fatal("initRuntimeWithOptionsFn called despite missing sockets")
	}
}

func TestRunPassesConfiguredJobIDToInit(t *testing.T) {
	// Regression for the env-refill bug: the worker must hand the configured
	// JobID to api.InitWithOptions; the nil-forcing for Worker mode happens in
	// base.InitializeOptionsFromAPI, never at the worker layer.
	w := newTestWorker(t, options.WorkerTypeWorker, "01020304")

	origValidate := validatePluginLoadableFn
	validatePluginLoadableFn = func() error { return nil }
	defer func() { validatePluginLoadableFn = origValidate }()

	origEnv, hadEnv := os.LookupEnv("RAY_JOB_ID")
	_ = os.Setenv("RAY_JOB_ID", "aabbccdd")
	defer func() {
		if hadEnv {
			_ = os.Setenv("RAY_JOB_ID", origEnv)
		} else {
			_ = os.Unsetenv("RAY_JOB_ID")
		}
	}()

	var captured *options.InitializeOptions
	origInit := initRuntimeWithOptionsFn
	initRuntimeWithOptionsFn = func(opts *options.InitializeOptions) error {
		captured = opts
		return errors.New("stop after init") // Run returns after api.Internal() fails
	}
	defer func() { initRuntimeWithOptionsFn = origInit }()

	_ = w.Run()
	if captured == nil {
		t.Fatal("initRuntimeWithOptionsFn was not called")
	}
	if captured.Job.JobID != "01020304" {
		t.Fatalf("captured Job.JobID = %q, want 01020304 (worker must pass configured JobID)", captured.Job.JobID)
	}
}

func TestRunPropagatesInitError(t *testing.T) {
	w := newTestWorker(t, options.WorkerTypeWorker, "")
	origValidate := validatePluginLoadableFn
	validatePluginLoadableFn = func() error { return nil }
	defer func() { validatePluginLoadableFn = origValidate }()

	sentinel := errors.New("gcs not ready")
	origInit := initRuntimeWithOptionsFn
	initRuntimeWithOptionsFn = func(*options.InitializeOptions) error { return sentinel }
	defer func() { initRuntimeWithOptionsFn = origInit }()

	err := w.Run()
	if !errors.Is(err, sentinel) {
		t.Fatalf("Run() error = %v, want sentinel", err)
	}
}
