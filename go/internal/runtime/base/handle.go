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
	"github.com/ray-project/ray/go/pkg/runtime/contract"
	"github.com/ray-project/ray/go/pkg/runtime/object"
)

// Compile-time type check to ensure RuntimeHandleImpl implements contract.RuntimeHandle.
// This variable is never used at runtime; it exists solely to trigger a compile-time
// error if RuntimeHandleImpl doesn't implement the interface correctly.
var _ contract.RuntimeHandle = (*RuntimeHandleImpl[contract.Runtime])(nil)

// RuntimeHandle is a type-safe handle for Ray Runtime.
// This interface provides type-safe handling of Runtime and ObjectStore instances.
//
// Note: This is an internal type used to pass Runtime and ObjectStore
// instances between plugin and worker code. It is not part of the
// public API.
type RuntimeHandle interface {
	// IsRuntimeHandle is a marker method to prevent external implementations.
	IsRuntimeHandle()
	// Runtime returns the Runtime instance for accessing runtime methods directly.
	Runtime() Runtime
	// ObjectStore returns the ObjectStore instance for accessing object storage.
	// The ObjectStore is managed internally by the Runtime and should not be
	// accessed directly by external code.
	ObjectStore() object.ObjectStore
}

// RuntimeHandleImpl is the default implementation of RuntimeHandle.
// Uses Go 1.18+ generics for type-safe storage of Runtime instance.
// ObjectStore is accessed through Runtime.GetObjectStore() to maintain encapsulation.
//
// Note: Fields are intentionally unexported to hide implementation details.
// Use Runtime() and ObjectStore() accessor methods to access the underlying instances.
type RuntimeHandleImpl[R Runtime] struct {
	runtime R // Runtime instance (type-safe with generics)
	// ObjectStore is not stored here; it's accessed through Runtime.GetObjectStore()
}

func (r *RuntimeHandleImpl[R]) IsRuntimeHandle() {}

// Runtime returns the Runtime instance for accessing runtime methods directly.
func (r *RuntimeHandleImpl[R]) Runtime() Runtime {
	return r.runtime
}

// ObjectStore returns the ObjectStore instance.
// The ObjectStore is managed internally by the Runtime and accessed
// through Runtime.GetObjectStore(). This maintains encapsulation and
// ensures ObjectStore lifecycle is tied to Runtime lifecycle.
//
// Note: After Runtime.Shutdown() is called, the ObjectStore should not
// be used as it may have been closed along with the Runtime.
func (r *RuntimeHandleImpl[R]) ObjectStore() object.ObjectStore {
	return r.runtime.GetObjectStore()
}

// NewRuntimeHandle creates a new RuntimeHandle instance with type-safe generics.
// The ObjectStore is managed internally by the Runtime and accessed through
// Runtime.GetObjectStore(). This maintains encapsulation and ensures ObjectStore
// lifecycle is tied to Runtime lifecycle.
func NewRuntimeHandle[R Runtime](runtime R) RuntimeHandle {
	return &RuntimeHandleImpl[R]{
		runtime: runtime,
	}
}
