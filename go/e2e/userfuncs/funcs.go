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

// Package userfuncs provides user-defined functions for Ray Go applications.
// This package is designed to be compiled as a plugin (.so file) that can be
// loaded by worker processes to access user-defined functions.
//
// Usage:
//
//	// Compile as plugin: bazel build //go/e2e/userfuncs/plugin:userfuncs_plugin
//	// (artifact: bazel-bin/go/e2e/userfuncs/plugin/userfuncs.so)
//	// Load in worker: plugin.Open("userfuncs.so")
//	// Call RegisterFunctions() to register all functions with the global registry
package userfuncs

import (
	"github.com/ray-project/ray/go/pkg/runtime/api"
)

// ============================================================================
// User-defined functions
// These are the Ray tasks that will be executed remotely on workers
// ============================================================================

//go:ray:task
// goAdd is a simple addition function that will be executed remotely.
// This function is registered with the Ray runtime and can be called
// from the driver code using api.Remote[int](goAdd).Call(10, 20).
func goAdd(x, y int) int {
	return x + y
}

//go:ray:task
// goMultiply multiplies two integers.
func goMultiply(x, y int) int {
	return x * y
}

//go:ray:task
// goConcat concatenates two strings.
func goConcat(a, b string) string {
	return a + b
}

//go:ray:task
// goCompute performs a more complex computation.
func goCompute(x, y, z int) int {
	return x*y + z
}

// ============================================================================
// Plugin registration
// This is the entry point for worker processes to load and register functions
// ============================================================================

// RegisterFunctions registers all user-defined functions with the Ray runtime.
// This function is exported for worker processes to call when loading the plugin.
//
// Worker loading example:
//
//	p, err := plugin.Open("userfuncs.so")
//	if err != nil {
//	    // handle error
//	}
//	sym, err := p.Lookup("RegisterFunctions")
//	if err == nil {
//	    if registerFunc, ok := sym.(func() error); ok {
//	        registerFunc()
//	    }
//	}
//
// Returns:
//   - error: any error encountered during registration
func RegisterFunctions() error {
	if err := api.RegisterFunction(goAdd); err != nil {
		return err
	}
	if err := api.RegisterFunction(goMultiply); err != nil {
		return err
	}
	if err := api.RegisterFunction(goConcat); err != nil {
		return err
	}
	if err := api.RegisterFunction(goCompute); err != nil {
		return err
	}
	// echoBytes is the zero-copy benchmark's large-object pass-through task.
	// Registered manually (no //go:ray:task) because raygen generation is broken.
	if err := api.RegisterFunction(echoBytes); err != nil {
		return err
	}
	// runtimeEnvOfTask is the task-side receiver for the task-level runtime
	// env submission path. Registered manually (no //go:ray:task) so it can be
	// invoked with zero arguments.
	if err := api.RegisterFunction(runtimeEnvOfTask); err != nil {
		return err
	}
	// reportNodeID reports the worker node id so the placement group e2e
	// drivers can verify PG-bound tasks actually run on a cluster node.
	// Registered manually (no //go:ray:task) so it can be invoked with zero
	// arguments.
	if err := api.RegisterFunction(reportNodeID); err != nil {
		return err
	}
	// Register actor constructors too (see actors.go).
	if err := registerActors(); err != nil {
		return err
	}
	return nil
}

// init is automatically called when the package is imported.
// This registers all functions with the global registry, making them
// available for both driver and worker processes.
//
// Import side-effect:
//
//	import _ "github.com/ray-project/ray/go/userfuncs"
//
// This will trigger init() and register all functions automatically.
func init() {
	// Ignore errors - functions may already be registered
	_ = RegisterFunctions()
}

// ============================================================================
// Helper functions for direct access (optional)
// These allow driver code to call functions without using the registry
// ============================================================================

// GetGoAdd returns the goAdd function for direct use.
// This is useful when you want to call the function directly without
// going through the Ray remote mechanism.
func GetGoAdd() func(int, int) int {
	return goAdd
}

// GetGoMultiply returns the goMultiply function for direct use.
func GetGoMultiply() func(int, int) int {
	return goMultiply
}

// GetGoConcat returns the goConcat function for direct use.
func GetGoConcat() func(string, string) string {
	return goConcat
}

// GetGoCompute returns the goCompute function for direct use.
func GetGoCompute() func(int, int, int) int {
	return goCompute
}
