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

	"github.com/ray-project/ray/go/pkg/ids"
	"github.com/ray-project/ray/go/pkg/runtime/object"
)

// TestGetPythonActorWithNamespace_HandleFeedsActorTask verifies that a
// PythonActorHandle carries the module/class names and actor ID needed by
// ActorTask: the constructed method descriptor must use the actor's module,
// class and the requested method name. It mirrors the construction in
// PythonActorCreator.Create (call.go) and the assertion pattern in
// python_call_test.go.
func TestGetPythonActorWithNamespace_HandleFeedsActorTask(t *testing.T) {
	actorID, err := ids.ActorIDFromHex("0102030405060708090a0b0c0d0e0f10")
	if err != nil {
		t.Fatalf("failed to build actor ID: %v", err)
	}

	// Build the handle the same way GetPythonActorWithNamespace wraps the
	// submitter-retrieved actor ID: a NativeActorHandle tagged LanguagePython
	// plus the caller-supplied module/class names.
	handle := &PythonActorHandle{
		baseActorHandle: baseActorHandle{
			nativeHandle: object.NewNativeActorHandle(actorID, object.LanguagePython),
		},
		moduleName: "ray.serve._private.controller",
		className:  "ServeController",
	}
	if handle.ID() != actorID {
		t.Errorf("actorID mismatch")
	}

	caller := ActorTask[int](handle, "check_alive")
	if caller == nil {
		t.Fatal("ActorTask should return non-nil caller")
	}
	if caller.err != nil {
		t.Fatalf("unexpected err: %v", caller.err)
	}
	if caller.methodDescriptor == nil {
		t.Fatal("methodDescriptor should be set")
	}
	if caller.methodDescriptor.ModuleName != "ray.serve._private.controller" {
		t.Errorf("ModuleName mismatch: got %q", caller.methodDescriptor.ModuleName)
	}
	if caller.methodDescriptor.ClassName != "ServeController" {
		t.Errorf("ClassName mismatch: got %q", caller.methodDescriptor.ClassName)
	}
	if caller.methodDescriptor.FunctionName != "check_alive" {
		t.Errorf("FunctionName mismatch: got %q", caller.methodDescriptor.FunctionName)
	}
	if caller.actorID != handle.ID() {
		t.Errorf("actorID mismatch")
	}
	if len(caller.args) != 0 {
		t.Errorf("expected 0 args, got %d", len(caller.args))
	}
}
