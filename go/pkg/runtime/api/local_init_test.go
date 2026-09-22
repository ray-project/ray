//go:build apilocalinit

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

// Verifies that InitWithOptions routes WorkerTypeLocal through the registered
// initializer instead of loading the go_runtime.so plugin. It is gated behind
// the "apilocalinit" build tag because both RegisterInitializer and setHandle
// mutate package-global state that the default api tests assume is unset; run
// it in isolation with:
//
//	go test -tags apilocalinit ./pkg/runtime/api/ -run Local
package api

import (
	"testing"

	"github.com/ray-project/ray/go/pkg/options"
	"github.com/ray-project/ray/go/pkg/runtime/contract"
)

// localInitTestHandle is a minimal contract.RuntimeHandle sentinel. The local
// short-circuit only calls fn(&initOpts) and setHandle(handle); it never
// dereferences the handle, so a stub suffices.
type localInitTestHandle struct{}

func (localInitTestHandle) IsRuntimeHandle()          {}
func (localInitTestHandle) Runtime() contract.Runtime { return nil }

// TestLocalInitRoutesThroughInitializer verifies that with WorkerTypeLocal,
// InitWithOptions invokes the initializer registered for that worker type
// (dependency inversion) and returns before plugin.Open, which would otherwise
// require the plugin to be built with exactly the same package versions as this
// test binary.
func TestLocalInitRoutesThroughInitializer(t *testing.T) {
	// Save and restore the registered initializer so this test cannot leak
	// into sibling api tests.
	prevInit := getInitFunc(options.WorkerTypeLocal)
	defer func() {
		RegisterInitializer(options.WorkerTypeLocal, prevInit)
		clearHandle()
	}()

	called := false
	RegisterInitializer(options.WorkerTypeLocal, func(opts *options.InitializeOptions) (contract.RuntimeHandle, error) {
		called = true
		if opts.WorkerType != options.WorkerTypeLocal {
			t.Fatalf("initializer received WorkerType %v, want WorkerTypeLocal", opts.WorkerType)
		}
		return localInitTestHandle{}, nil
	})

	if err := InitWithOptions(&options.InitializeOptions{
		Runtime:    options.RuntimeOptions{StartupToken: 1},
		WorkerType: options.WorkerTypeLocal,
	}); err != nil {
		t.Fatalf("InitWithOptions(local) failed: %v", err)
	}

	if !called {
		t.Fatal("registered initializer was not invoked for WorkerTypeLocal")
	}

	h, ok := tryGetHandle()
	if !ok {
		t.Fatal("local init did not store a runtime handle")
	}
	if _, isSentinel := h.(localInitTestHandle); !isSentinel {
		t.Fatalf("stored handle is %T, want the initializer's sentinel handle", h)
	}
}

// TestLocalInitWithoutInitializerFallsThrough verifies the backward-compatible
// fall-through: when no initializer is registered for WorkerTypeLocal,
// InitWithOptions is not short-circuited (it proceeds toward the plugin path
// instead of silently succeeding with a nil handle).
func TestLocalInitWithoutInitializerFallsThrough(t *testing.T) {
	prevInit := getInitFunc(options.WorkerTypeLocal)
	defer func() {
		RegisterInitializer(options.WorkerTypeLocal, prevInit)
		clearHandle()
	}()

	RegisterInitializer(options.WorkerTypeLocal, nil)

	// With no initializer, the local short-circuit is skipped and the plugin
	// path is reached; with no plugin path configured the init must fail
	// rather than report success.
	err := InitWithOptions(&options.InitializeOptions{
		Runtime:    options.RuntimeOptions{StartupToken: 1},
		WorkerType: options.WorkerTypeLocal,
	})
	if err == nil {
		t.Fatal("InitWithOptions(local) without an initializer must not report success")
	}
}
