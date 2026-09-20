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

package function

import (
	"errors"

	"github.com/ray-project/ray/go/pkg/ids"
)

// Common errors used by function execution
var (
	// ErrTaskExecutionFailed is returned when task execution fails
	ErrTaskExecutionFailed = errors.New("task execution failed")
)

// Function represents an executable function that can be invoked by Ray.
//
// Design notes:
// 1. This is a simple function type that takes FunctionArgs and returns serialized results.
// 2. Go uses explicit registration, so we don't need complex reflection like Java.
// 3. The function is responsible for serializing/deserializing its own arguments and results.
//
// Parameters:
//   - args: Function arguments (already deserialized by the runtime)
//
// Returns:
//   - []SerializedObject: Serialized return values
//   - error: Any error during execution
type Function func(args []FunctionArg) ([]SerializedObject, error)

// SerializedObject represents a serialized return value from a function.
type SerializedObject struct {
	// Data is the serialized byte array.
	Data []byte
	// Metadata is optional metadata (e.g., type information).
	Metadata []byte
}

// FunctionArg represents a function argument in a task spec.
//
// Design notes:
// 1. Either ObjectRef (pass by reference) or Data (pass by value) should be set.
// 2. Corresponds to Java's io.ray.runtime.task.FunctionArg.
// 3. Used by TaskSubmitter to build task specs.
type FunctionArg struct {
	// ObjectRef is the object ID for pass-by-reference arguments.
	// If set, Data should be nil.
	ObjectRef *ObjectRefData

	// Data is the serialized data for pass-by-value arguments.
	// If set, ObjectRef should be nil.
	Data *SerializedData

	// OwnerAddress is the owner address for pass-by-reference arguments.
	// This is used to locate the object owner.
	OwnerAddress []byte
}

// ObjectRefData contains information about an object reference.
type ObjectRefData struct {
	// ObjectID is the unique identifier of the object.
	ObjectID ids.ObjectID

	// ReleaseAfterSubmit indicates the local reference backing this argument is
	// held only for task submission (added by PutWithID) and must be released
	// once the task has been submitted. It is set only for the internal
	// pass-by-reference arguments created by convertArgToFunctionArg; user
	// supplied ObjectRefs manage their own reference lifecycle.
	ReleaseAfterSubmit bool
}

// SerializedData contains serialized object data.
type SerializedData struct {
	// Data is the serialized byte array.
	Data []byte

	// Metadata is optional metadata (e.g., type information).
	Metadata []byte

	// OwnerAddress is the owner address for nested references.
	OwnerAddress []byte
}

// NewFunctionArgByValue creates a FunctionArg that will be passed by value.
func NewFunctionArgByValue(data []byte, metadata []byte) FunctionArg {
	return FunctionArg{
		Data: &SerializedData{
			Data:     data,
			Metadata: metadata,
		},
		ObjectRef:    nil,
		OwnerAddress: nil,
	}
}

// NewFunctionArgByRef creates a FunctionArg that will be passed by reference.
func NewFunctionArgByRef(objectID ids.ObjectID, ownerAddress []byte) FunctionArg {
	return FunctionArg{
		ObjectRef: &ObjectRefData{
			ObjectID: objectID,
		},
		Data:         nil,
		OwnerAddress: ownerAddress,
	}
}

// IsPassByRef returns true if this argument is passed by reference.
func (f FunctionArg) IsPassByRef() bool {
	return f.ObjectRef != nil
}

// IsPassByValue returns true if this argument is passed by value.
func (f FunctionArg) IsPassByValue() bool {
	return f.Data != nil
}

// Manager defines the interface for function management.
// This interface abstracts function registration and lookup operations.
//
// Design notes:
//  1. The interface is kept minimal, containing only essential methods.
//  2. It allows the Runtime interface to depend on this abstraction rather than
//     concrete implementations, following the Dependency Inversion Principle.
type Manager interface {
	// RegisterFunction registers a function with the given descriptor.
	// Returns an error if the descriptor is invalid or registration fails.
	RegisterFunction(descriptor *GoFunctionDescriptor, fn Function) error

	// GetFunction retrieves a registered function by its descriptor.
	// Returns the function or nil if not found, plus any error.
	GetFunction(descriptor *GoFunctionDescriptor) (Function, error)

	// GetFunctionByBaseDescriptor retrieves a function using FunctionDescriptor.
	// This is used when receiving task requests from the C++ core worker.
	GetFunctionByBaseDescriptor(descriptor FunctionDescriptor) (Function, error)

	// IsRegistered checks if a function with the given descriptor is registered.
	// Returns true if registered, false otherwise.
	IsRegistered(descriptor *GoFunctionDescriptor) bool

	// ListRegisteredFunctions returns a list of all registered function descriptors.
	ListRegisteredFunctions() []string
}
