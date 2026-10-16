// Copyright 2026 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package api

import (
	"testing"

	"github.com/ray-project/ray/go/pkg/options"
)

// TestInitWithOptionsSkipDriverLogSetup verifies the inline driver-logger block
// (runtime.go) runs when SkipDriverLogSetup is unset and is skipped when set.
// The default seam is swapped for a recorder; InitWithOptions returns before
// plugin load because no GCS address is provided (Driver mode validation).
func TestInitWithOptionsSkipDriverLogSetup(t *testing.T) {
	orig := setupDriverLoggerFn
	calls := 0
	setupDriverLoggerFn = func(logDir string) error {
		calls++
		return nil
	}
	defer func() { setupDriverLoggerFn = orig }()

	t.Run("runs when unset", func(t *testing.T) {
		calls = 0
		_ = InitWithOptions(&options.InitializeOptions{
			WorkerType: options.WorkerTypeDriver,
			Runtime:    options.RuntimeOptions{LogDir: t.TempDir()},
		})
		if calls != 1 {
			t.Fatalf("setupDriverLoggerFn called %d times, want 1", calls)
		}
	})

	t.Run("skipped when set", func(t *testing.T) {
		calls = 0
		_ = InitWithOptions(&options.InitializeOptions{
			WorkerType:         options.WorkerTypeDriver,
			Runtime:            options.RuntimeOptions{LogDir: t.TempDir()},
			SkipDriverLogSetup: true,
		})
		if calls != 0 {
			t.Fatalf("setupDriverLoggerFn called %d times, want 0", calls)
		}
	})

	t.Run("no-op for non-driver worker type", func(t *testing.T) {
		calls = 0
		_ = InitWithOptions(&options.InitializeOptions{
			WorkerType: options.WorkerTypeLocal,
			Runtime:    options.RuntimeOptions{LogDir: t.TempDir()},
		})
		if calls != 0 {
			t.Fatalf("setupDriverLoggerFn called %d times, want 0", calls)
		}
	})

	t.Run("preserves explicit WorkerTypeWorker", func(t *testing.T) {
		// Regression for the thin-layer worker path (worker.Run ->
		// api.InitWithOptions): WorkerTypeWorker == 0 is the zero value, so
		// the old "coerce zero WorkerType to Driver" default silently turned
		// workers into drivers. The driver-logger block (Driver-only) must be
		// skipped, proving the worker type survives to base.Initialize, where
		// the Worker-mode JobID nil-forcing and C++ worker registration
		// depend on it.
		calls = 0
		_ = InitWithOptions(&options.InitializeOptions{
			WorkerType: options.WorkerTypeWorker,
			Runtime:    options.RuntimeOptions{LogDir: t.TempDir()},
		})
		if calls != 0 {
			t.Fatalf("setupDriverLoggerFn called %d times for WorkerTypeWorker, want 0", calls)
		}
	})
}
