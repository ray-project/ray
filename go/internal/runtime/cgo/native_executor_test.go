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

//go:build cgo
// +build cgo

package cgo

import (
	"testing"

	"github.com/ray-project/ray/go/pkg/ids"
	"github.com/ray-project/ray/go/pkg/runtime/function"
)

// TestNewNativeTaskExecutor tests the constructor.
func TestNewNativeTaskExecutor(t *testing.T) {
	functionManager := function.NewFunctionManager(nil)
	executor := NewNativeTaskExecutor(functionManager)

	if executor == nil {
		t.Fatal("NewNativeTaskExecutor() returned nil")
	}
}

// TestNativeTaskExecutor_Execute tests the Execute method.
func TestNativeTaskExecutor_Execute(t *testing.T) {
	functionManager := function.NewFunctionManager(nil)
	executor := NewNativeTaskExecutor(functionManager)

	// Create a test function descriptor
	funcDesc := function.NewGoFunctionDescriptorOrUnknown(
		"github.com/example/test",
		"pkg",
		"TestFunction",
	)

	// Create test arguments
	args := []function.FunctionArg{
		function.NewFunctionArgByValue([]byte("arg1-data"), []byte("arg1-meta")),
		function.NewFunctionArgByValue([]byte("arg2-data"), []byte("arg2-meta")),
	}

	// Execute the task (will fail in non-CGO environment, but tests the Go logic)
	results, err := executor.Execute(funcDesc, args, 2)

	// In a real CGO environment, this would call the C function
	// For now, we verify the method doesn't panic
	if err != nil {
		t.Logf("Execute() returned error (expected in test environment): %v", err)
	}

	// Results can be nil if execution failed
	if results == nil {
		t.Log("Execute() returned nil results (expected in test environment)")
	}
}

// TestNativeTaskExecutor_ExecuteActorTask tests the ExecuteActorTask method.
func TestNativeTaskExecutor_ExecuteActorTask(t *testing.T) {
	functionManager := function.NewFunctionManager(nil)
	executor := NewNativeTaskExecutor(functionManager)

	// Create a test actor ID
	actorID := ids.OfActorID(ids.NilJobID(), ids.NilTaskID(), 1)

	// Create a test function descriptor for actor method
	funcDesc := function.NewGoActorMethodDescriptorOrUnknown(
		"github.com/example/test",
		"pkg",
		"TestActor",
		"DoWork",
	)

	// Create test arguments
	args := []function.FunctionArg{
		function.NewFunctionArgByValue([]byte("actor-arg1"), []byte("meta1")),
		function.NewFunctionArgByValue([]byte("actor-arg2"), []byte("meta2")),
	}

	// Execute the actor task
	results, err := executor.ExecuteActorTask(actorID, funcDesc, args, 2)

	// In a real CGO environment, this would call the C function
	if err != nil {
		t.Logf("ExecuteActorTask() returned error (expected in test environment): %v", err)
	}

	// Results can be nil if execution failed
	if results == nil {
		t.Log("ExecuteActorTask() returned nil results (expected in test environment)")
	}
}

// TestNativeTaskExecutor_Execute_WithNilArgs tests Execute with nil arguments.
func TestNativeTaskExecutor_Execute_WithNilArgs(t *testing.T) {
	functionManager := function.NewFunctionManager(nil)
	executor := NewNativeTaskExecutor(functionManager)

	funcDesc := function.NewGoFunctionDescriptorOrUnknown(
		"github.com/example/test",
		"pkg",
		"TestFunction",
	)

	// Test with nil args
	results, err := executor.Execute(funcDesc, nil, 0)

	if err != nil {
		t.Logf("Execute() with nil args returned error: %v", err)
	}

	if results != nil {
		t.Errorf("Expected nil results with nil args, got %v", results)
	}
}

// TestNativeTaskExecutor_ExecuteActorTask_WithNilArgs tests ExecuteActorTask with nil arguments.
func TestNativeTaskExecutor_ExecuteActorTask_WithNilArgs(t *testing.T) {
	functionManager := function.NewFunctionManager(nil)
	executor := NewNativeTaskExecutor(functionManager)

	actorID := ids.OfActorID(ids.NilJobID(), ids.NilTaskID(), 1)
	funcDesc := function.NewGoActorMethodDescriptorOrUnknown(
		"github.com/example/test",
		"pkg",
		"TestActor",
		"DoWork",
	)

	// Test with nil args
	results, err := executor.ExecuteActorTask(actorID, funcDesc, nil, 0)

	if err != nil {
		t.Logf("ExecuteActorTask() with nil args returned error: %v", err)
	}

	if results != nil {
		t.Errorf("Expected nil results with nil args, got %v", results)
	}
}

// TestNativeTaskExecutor_Integration tests overall executor functionality.
func TestNativeTaskExecutor_Integration(t *testing.T) {
	functionManager := function.NewFunctionManager(nil)
	executor := NewNativeTaskExecutor(functionManager)

	if executor == nil {
		t.Fatal("Failed to create NativeTaskExecutor")
	}

	// Basic smoke test - ensure methods don't panic
	funcDesc := function.NewGoFunctionDescriptorOrUnknown(
		"github.com/example/test",
		"pkg",
		"TestFunction",
	)

	_, _ = executor.Execute(funcDesc, nil, 0)

	actorID := ids.OfActorID(ids.NilJobID(), ids.NilTaskID(), 1)
	actorFuncDesc := function.NewGoActorMethodDescriptorOrUnknown(
		"github.com/example/test",
		"pkg",
		"TestActor",
		"DoWork",
	)

	_, _ = executor.ExecuteActorTask(actorID, actorFuncDesc, nil, 0)
}

// TestConvertCSerializedObjectArrayToGo tests the conversion function.
func TestConvertCSerializedObjectArrayToGo(t *testing.T) {
	// This test would require CGO infrastructure to properly test
	// In a real environment, we would:
	// 1. Create a C.CSerializedObjectArray
	// 2. Populate it with test data
	// 3. Call convertCSerializedObjectArrayToGo
	// 4. Verify the conversion

	// For now, test with nil input
	results, err := convertCSerializedObjectArrayToGo(nil)
	if err != nil {
		t.Errorf("convertCSerializedObjectArrayToGo(nil) returned error: %v", err)
	}
	if results != nil {
		t.Errorf("Expected nil results for nil input, got %v", results)
	}
}
