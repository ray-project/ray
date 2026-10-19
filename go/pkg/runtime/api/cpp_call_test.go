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

	"github.com/ray-project/ray/go/pkg/errors"
	"github.com/ray-project/ray/go/pkg/ids"
	"github.com/ray-project/ray/go/pkg/runtime/function"
	"github.com/ray-project/ray/go/pkg/runtime/object"
)

// assertRuntimeErrorState asserts err is a *errors.RuntimeError with the given
// operation and state. It mirrors the helper of the same name in
// placement_group_test.go, which is gated behind the "apipg" build tag (a
// different test target) and so is not visible from this untagged target.
func assertRuntimeErrorState(t *testing.T, err error, operation, state string) {
	t.Helper()
	re, ok := err.(*errors.RuntimeError)
	if !ok {
		t.Fatalf("expected *errors.RuntimeError, got %T: %v", err, err)
	}
	if re.Operation != operation || re.State != state {
		t.Fatalf("RuntimeError{operation: %q, state: %q}, want {%q, %q}", re.Operation, re.State, operation, state)
	}
}

func TestRemoteCpp(t *testing.T) {
	caller := RemoteCpp[int]("add", "")
	if caller == nil {
		t.Fatal("RemoteCpp should return non-nil caller")
	}
	if caller.functionDescriptor == nil {
		t.Fatal("functionDescriptor should be set")
	}
	if caller.functionDescriptor.CreateFunctionName != "add" {
		t.Errorf("CreateFunctionName mismatch: got %q", caller.functionDescriptor.CreateFunctionName)
	}
	if caller.functionDescriptor.Caller != "GO" {
		t.Errorf("Caller should be GO, got %q", caller.functionDescriptor.Caller)
	}
	if caller.functionDescriptor.ClassName != "" {
		t.Errorf("ClassName should be empty, got %q", caller.functionDescriptor.ClassName)
	}
	if caller.numReturns != 1 {
		t.Errorf("numReturns should be 1, got %d", caller.numReturns)
	}
	if caller.err != nil {
		t.Errorf("unexpected err: %v", caller.err)
	}
}

func TestRemoteCpp_WithClassName(t *testing.T) {
	caller := RemoteCpp[int]("create", "Calculator")
	if caller.functionDescriptor.ClassName != "Calculator" {
		t.Errorf("ClassName mismatch: got %q", caller.functionDescriptor.ClassName)
	}
}

func TestRemoteCpp_InvalidFunctionName(t *testing.T) {
	caller := RemoteCpp[int]("", "")
	if caller.err == nil {
		t.Fatal("expected deferred error for empty function name")
	}
	_, err := caller.Call(1, 2)
	if err == nil {
		t.Fatal("Call should return the deferred error")
	}
}

func TestRemoteCppVoid(t *testing.T) {
	caller := RemoteCppVoid("log_event", "")
	if caller.numReturns != 0 {
		t.Errorf("numReturns should be 0, got %d", caller.numReturns)
	}
	if caller.functionDescriptor.CreateFunctionName != "log_event" {
		t.Errorf("CreateFunctionName mismatch")
	}
}

func TestCppTaskCaller_BuilderChain(t *testing.T) {
	caller := RemoteCpp[int]("add", "")
	ret := caller.WithNumReturns(2).WithResources(map[string]float64{"CPU": 2.0})
	if ret != caller {
		t.Error("builder methods should return the same caller")
	}
	if caller.numReturns != 2 {
		t.Errorf("numReturns should be 2, got %d", caller.numReturns)
	}
	if caller.options.Resources["CPU"] != 2.0 {
		t.Errorf("resources should contain CPU:2.0")
	}
}

func TestCppTaskCaller_DescriptorIsFunctionDescriptor(t *testing.T) {
	caller := RemoteCpp[int]("add", "")
	var _ function.FunctionDescriptor = caller.functionDescriptor
}

func TestRemoteCppActor(t *testing.T) {
	creator := RemoteCppActor("create_actor", "Calculator")
	if creator == nil {
		t.Fatal("RemoteCppActor should return non-nil creator")
	}
	if creator.functionDescriptor == nil {
		t.Fatal("functionDescriptor should be set")
	}
	if creator.functionDescriptor.CreateFunctionName != "create_actor" {
		t.Errorf("CreateFunctionName mismatch: got %q", creator.functionDescriptor.CreateFunctionName)
	}
	if creator.functionDescriptor.Caller != "GO" {
		t.Errorf("Caller mismatch: got %q", creator.functionDescriptor.Caller)
	}
	if creator.functionDescriptor.ClassName != "Calculator" {
		t.Errorf("ClassName mismatch: got %q", creator.functionDescriptor.ClassName)
	}
	if creator.err != nil {
		t.Errorf("unexpected err: %v", creator.err)
	}
}

func TestRemoteCppActor_InvalidFunctionName(t *testing.T) {
	creator := RemoteCppActor("", "Calculator")
	if creator.err == nil {
		t.Fatal("expected deferred error for empty create function name")
	}
	_, err := creator.Create()
	if err == nil {
		t.Fatal("Create should return the deferred error")
	}
}

func TestRemoteCppActor_WithPlacementGroupCarriesBinding(t *testing.T) {
	// Actor-level placement-group binding is recorded on the creation options
	// by the embedded actorOptions.WithPlacementGroup, which the C++ actor
	// creator shares with the Go actor creator.
	pg := &PlacementGroup{id: ids.OfPlacementGroupID(ids.NewJobID())}
	creator := RemoteCppActor("create_actor", "Calculator").WithPlacementGroup(pg, 1)
	if creator.options.PlacementGroup == nil {
		t.Fatal("expected placement group binding to be recorded on the actor creation options")
	}
	if creator.options.PlacementGroup.ID != pg.ID() {
		t.Fatalf("PlacementGroup.ID = %v, want %v", creator.options.PlacementGroup.ID, pg.ID())
	}
	if creator.options.PlacementGroup.BundleIndex != 1 {
		t.Fatalf("PlacementGroup.BundleIndex = %d, want 1", creator.options.PlacementGroup.BundleIndex)
	}
}

func TestCppActorHandle_FromCreator(t *testing.T) {
	actorID, err := ids.ActorIDFromHex("0102030405060708090a0b0c0d0e0f10")
	if err != nil {
		t.Fatalf("failed to build actor ID: %v", err)
	}
	handle := &CppActorHandle{
		baseActorHandle: baseActorHandle{
			nativeHandle: &object.NativeActorHandle{
				ActorID:  actorID,
				Language: object.LanguageCpp,
			},
		},
		className: "Calculator",
	}

	caller := CppActorTask[int](handle, "add")
	if caller == nil {
		t.Fatal("CppActorTask should return non-nil caller")
	}
	if caller.err != nil {
		t.Fatalf("unexpected err: %v", caller.err)
	}
	if caller.methodDescriptor == nil {
		t.Fatal("methodDescriptor should be set")
	}
	if caller.methodDescriptor.CreateFunctionName != "add" {
		t.Errorf("CreateFunctionName mismatch: got %q", caller.methodDescriptor.CreateFunctionName)
	}
	if caller.methodDescriptor.Caller != "GO" {
		t.Errorf("Caller mismatch: got %q", caller.methodDescriptor.Caller)
	}
	if caller.methodDescriptor.ClassName != "Calculator" {
		t.Errorf("ClassName mismatch: got %q", caller.methodDescriptor.ClassName)
	}
	if caller.actorID != handle.ID() {
		t.Errorf("actorID mismatch")
	}
	if len(caller.args) != 0 {
		t.Errorf("expected 0 args, got %d", len(caller.args))
	}
}

func TestCppActorHandle_ID(t *testing.T) {
	actorID, err := ids.ActorIDFromHex("0102030405060708090a0b0c0d0e0f10")
	if err != nil {
		t.Fatalf("failed to build actor ID: %v", err)
	}
	handle := &CppActorHandle{
		baseActorHandle: baseActorHandle{
			nativeHandle: &object.NativeActorHandle{ActorID: actorID, Language: object.LanguageCpp},
		},
		className: "Calculator",
	}
	if handle.ID() != actorID {
		t.Errorf("ID mismatch")
	}
}

func TestCppActorTaskCaller_WithConcurrencyGroup(t *testing.T) {
	actorID, err := ids.ActorIDFromHex("0102030405060708090a0b0c0d0e0f10")
	if err != nil {
		t.Fatalf("failed to build actor ID: %v", err)
	}
	handle := &CppActorHandle{
		baseActorHandle: baseActorHandle{
			nativeHandle: &object.NativeActorHandle{ActorID: actorID, Language: object.LanguageCpp},
		},
		className: "Calculator",
	}

	caller := CppActorTask[int](handle, "add")
	if caller == nil {
		t.Fatal("CppActorTask should return a non-nil caller")
	}
	if caller.err != nil {
		t.Fatalf("unexpected err: %v", caller.err)
	}

	ret := caller.WithConcurrencyGroup("cg1")
	if ret != caller {
		t.Error("WithConcurrencyGroup should return the same caller for chaining")
	}
	if caller.options.ConcurrencyGroupName != "cg1" {
		t.Errorf("ConcurrencyGroupName mismatch: got %q, want %q", caller.options.ConcurrencyGroupName, "cg1")
	}

	// Chaining with other taskOptions builders keeps the concrete type and
	// preserves the group name (mirroring ActorTaskCaller.WithConcurrencyGroup).
	chained := caller.WithNumReturns(2).WithConcurrencyGroup("cg2")
	if chained != caller {
		t.Error("chained builders should return the same caller")
	}
	if caller.options.ConcurrencyGroupName != "cg2" {
		t.Errorf("ConcurrencyGroupName after chaining: got %q, want %q", caller.options.ConcurrencyGroupName, "cg2")
	}
	if caller.numReturns != 2 {
		t.Errorf("numReturns should be 2 after chaining, got %d", caller.numReturns)
	}
}

// TestCppActorTaskNilHandle verifies that a nil handle — reachable when actor
// creation failed and its error was ignored — produces a caller carrying the
// deferred error instead of panicking on the handle dereference.
func TestCppActorTaskNilHandle(t *testing.T) {
	caller := CppActorTask[int](nil, "add")
	if caller == nil {
		t.Fatal("CppActorTask(nil, ...) returned nil caller")
	}
	if _, err := caller.Remote(); err == nil {
		t.Fatal("Remote on nil-handle caller should carry the deferred error")
	}
}
