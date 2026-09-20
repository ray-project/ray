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

// Package base provides base types and interfaces for Ray Go Runtime.
package base

import (
	"sync/atomic"

	rayerrors "github.com/ray-project/ray/go/internal/errors"
)

// RuntimeFactory defines the runtime factory interface.
//
// Design notes:
// 1. Factory pattern facilitates switching between different implementations (Native/Local/Mock).
// 2. Concrete implementations (e.g., NativeRuntimeFactory) are defined in the native package.
// 3. Corresponds to Java's RayNativeRuntime constructor.
type RuntimeFactory interface {
	// CreateRuntime creates a runtime instance.
	CreateRuntime(opts InitializeOptions) (Runtime, error)
}

// globalRuntimeFactory is the global runtime factory instance.
// Uses atomic.Pointer for lock-free thread-safe access.
//
// Design notes:
// 1. Dependency injection is implemented through package-level variables.
// 2. Concrete implementations are set during initialization (e.g., NativeRuntimeFactory in the native package).
// 3. Upper-layer code (e.g., plugin_exports.go) only depends on the base package.
// 4. Thread-safe using atomic.Pointer, no explicit locking required.
var globalRuntimeFactory atomic.Pointer[RuntimeFactory]

// SetRuntimeFactory sets the global runtime factory.
//
// Note: This function should be called at program startup, typically initialized by the native package.
// Thread-safe: uses atomic store operation.
func SetRuntimeFactory(factory RuntimeFactory) {
	globalRuntimeFactory.Store(&factory)
}

// CreateRuntime creates a runtime instance using the global factory.
//
// This is the entry point used by upper-layer code (e.g., plugin_exports.go).
// Thread-safe: uses atomic load operation.
//
// Note: Validation of initialization options is performed in options.go
// during the API-to-internal options conversion.
func CreateRuntime(opts InitializeOptions) (Runtime, error) {
	factory := globalRuntimeFactory.Load()
	if factory == nil {
		return nil, rayerrors.ErrRuntimeFactoryNotSet
	}

	return (*factory).CreateRuntime(opts)
}
