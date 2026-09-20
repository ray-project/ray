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

package api

import (
	"testing"

	"github.com/ray-project/ray/go/pkg/ids"
)

// TestObjectRefCreation tests creating an ObjectRef.
func TestObjectRefCreation(t *testing.T) {
	objectID := ids.NilObjectID()
	ref := NewObjectRef[string](objectID, "test-type", false)

	if ref == nil {
		t.Fatal("NewObjectRef should return a non-nil reference")
	}

	if ref.ObjectID() != objectID {
		t.Errorf("ObjectID mismatch: expected %v, got %v", objectID, ref.ObjectID())
	}
}

// TestObjectRefGet tests that Get returns an error when runtime is not initialized.
// This is a unit test for the API layer, verifying proper error handling.
func TestObjectRefGet(t *testing.T) {
	// Create an ObjectRef (this should work without runtime)
	ref := NewObjectRef[string](ids.NilObjectID(), "test-type", false)
	if ref == nil {
		t.Fatal("NewObjectRef should return a non-nil reference")
	}

	// Get should return an error when runtime is not initialized
	_, err := ref.Get()
	if err == nil {
		t.Error("Get should return an error when runtime is not initialized")
	} else {
		t.Logf("Get correctly returned error: %v", err)
	}
}

// TestObjectRefGetWithTimeout tests that GetWithTimeout returns an error when runtime is not initialized.
func TestObjectRefGetWithTimeout(t *testing.T) {
	// Create an ObjectRef
	ref := NewObjectRef[int](ids.NilObjectID(), "test-type", false)
	if ref == nil {
		t.Fatal("NewObjectRef should return a non-nil reference")
	}

	// GetWithTimeout should return an error when runtime is not initialized
	_, err := ref.GetWithTimeout(1000)
	if err == nil {
		t.Error("GetWithTimeout should return an error when runtime is not initialized")
	} else {
		t.Logf("GetWithTimeout correctly returned error: %v", err)
	}
}

// TestWaitResult tests the WaitResult type.
func TestWaitResult(t *testing.T) {
	ready := []*ObjectRef[string]{
		NewObjectRef[string](ids.NilObjectID(), "test-type", false),
	}
	unready := []*ObjectRef[string]{
		NewObjectRef[string](ids.NilObjectID(), "test-type", false),
	}

	result := NewWaitResult(ready, unready)

	if result == nil {
		t.Fatal("NewWaitResult should return a non-nil result")
	}

	if len(result.Ready()) != 1 {
		t.Errorf("Ready list length mismatch: expected 1, got %d", len(result.Ready()))
	}

	if len(result.Unready()) != 1 {
		t.Errorf("Unready list length mismatch: expected 1, got %d", len(result.Unready()))
	}
}

// TestRuntimeContext tests the RuntimeContext type.
func TestRuntimeContext(t *testing.T) {
	jobID := ids.NilJobID()
	taskID := ids.NilTaskID()
	actorID := ids.NilActorID()
	nodeID := ids.NilNodeID()

	ctx := NewRuntimeContext(jobID, taskID, actorID, "test-namespace", "test-runtime-env", nodeID, false)

	if ctx == nil {
		t.Fatal("NewRuntimeContext should return a non-nil context")
	}

	if ctx.JobID() != jobID {
		t.Errorf("JobID mismatch: expected %v, got %v", jobID, ctx.JobID())
	}

	if ctx.TaskID() != taskID {
		t.Errorf("TaskID mismatch: expected %v, got %v", taskID, ctx.TaskID())
	}

	if ctx.ActorID() != actorID {
		t.Errorf("ActorID mismatch: expected %v, got %v", actorID, ctx.ActorID())
	}

	if ctx.Namespace() != "test-namespace" {
		t.Errorf("Namespace mismatch: expected 'test-namespace', got '%s'", ctx.Namespace())
	}

	if ctx.RuntimeEnv() != "test-runtime-env" {
		t.Errorf("RuntimeEnv mismatch: expected 'test-runtime-env', got '%s'", ctx.RuntimeEnv())
	}

	if ctx.NodeID() != nodeID {
		t.Errorf("NodeID mismatch: expected %v, got %v", nodeID, ctx.NodeID())
	}

	if ctx.IsLocalMode() != false {
		t.Errorf("IsLocalMode mismatch: expected false, got %v", ctx.IsLocalMode())
	}
}

// TestAPIFunctionsPlaceholder tests that API functions exist (placeholder for phase 2).
func TestAPIFunctionsPlaceholder(t *testing.T) {
	// Test that Init function exists
	err := Init()
	if err != nil {
		t.Logf("Init returned error (expected in phase 1): %v", err)
	}

	// Test that IsInitialized function exists
	initialized := IsInitialized()
	if initialized {
		t.Logf("IsInitialized returned true (unexpected in phase 1)")
		// Only call Shutdown if runtime was initialized
		Shutdown()
	}
}

// TestPutAndGet tests that Put and Get return errors when runtime is not initialized.
func TestPutAndGet(t *testing.T) {
	// Put should return an error when runtime is not initialized
	_, err := Put("test-value", nil)
	if err == nil {
		t.Error("Put should return an error when runtime is not initialized")
	} else {
		t.Logf("Put correctly returned error: %v", err)
	}

	// Get should also return an error when runtime is not initialized
	ref := NewObjectRef[string](ids.NilObjectID(), "test-type", false)
	_, err = Get(ref)
	if err == nil {
		t.Error("Get should return an error when runtime is not initialized")
	} else {
		t.Logf("Get correctly returned error: %v", err)
	}
}

// TestWait tests that Wait returns an error when runtime is not initialized.
func TestWait(t *testing.T) {
	ref := NewObjectRef[string](ids.NilObjectID(), "test-type", false)
	waitList := []*ObjectRef[string]{ref}

	// Wait should return an error when runtime is not initialized
	_, err := Wait(waitList, 1, 1000, false)
	if err == nil {
		t.Error("Wait should return an error when runtime is not initialized")
	} else {
		t.Logf("Wait correctly returned error: %v", err)
	}
}

// TestGetActorPlaceholder tests the GetActor function (placeholder for phase 2).
func TestGetActorPlaceholder(t *testing.T) {
	// Phase 1: Just verify the function exists
	handle, err := GetActor[string]("test-actor")
	if handle != nil {
		t.Logf("GetActor returned non-nil handle (unexpected in phase 1)")
	}
	if err != nil {
		t.Logf("GetActor returned error (expected in phase 1): %v", err)
	}
}

// TestGetRuntimeContextPlaceholder tests the GetRuntimeContext function (placeholder for phase 2).
func TestGetRuntimeContextPlaceholder(t *testing.T) {
	// Phase 1: Just verify the function exists
	ctx, err := GetRuntimeContext()
	if ctx != nil {
		t.Logf("GetRuntimeContext returned non-nil context (unexpected in phase 1)")
	}
	if err != nil {
		t.Logf("GetRuntimeContext returned error (expected in phase 1): %v", err)
	}
}

// TestExitActorPlaceholder tests the ExitActor function (placeholder for phase 2).
func TestExitActorPlaceholder(t *testing.T) {
	// Phase 1: Just verify the function exists
	err := ExitActor()
	if err != nil {
		t.Logf("ExitActor returned error (expected in phase 1): %v", err)
	}
}
