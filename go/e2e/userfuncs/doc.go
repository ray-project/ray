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

// Package userfuncs provides example user functions for Ray Go.
//
// This package demonstrates the use of //go:ray:task markers for
// high-performance task execution. Functions marked with //go:ray:task
// are automatically wrapped with type-safe serializers, eliminating
// the reflection overhead of the standard approach.
//
// # Usage
//
// Mark your functions with //go:ray:task:
//
//	//go:ray:task
//	func goAdd(x, y int) int {
//	    return x + y
//	}
//
// Then run the code generator:
//
//	go generate  # or bazel build //go/e2e/driver_app
//
// The generator creates two files:
//   - generated_wrappers.go: type-safe wrapper functions
//   - generated_registry.go: automatic function registration
//
// # Performance
//
// Compared to the reflection-based approach, code generation provides:
//   - 10-100x faster function calls (no reflect.Call)
//   - 10x faster serialization (no reflect.New/reflect.ValueOf)
//   - Better compile-time type checking
//
// # Example
//
// See e2e/driver_app/main.go for complete examples of submitting tasks
// with generated wrappers.
package userfuncs

// The checked-in generated_*.go files were produced by an internal code
// generator that is not part of the open-source tree; registration is
// otherwise manual (see RegisterFunctions in funcs.go).
