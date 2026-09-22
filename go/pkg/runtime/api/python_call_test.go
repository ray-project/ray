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
	"github.com/ray-project/ray/go/pkg/runtime/object"
)

func TestRemotePython(t *testing.T) {
	caller := RemotePython[int]("my_module", "add", "")
	if caller == nil {
		t.Fatal("RemotePython should return non-nil caller")
	}
	if caller.functionDescriptor == nil {
		t.Fatal("functionDescriptor should be set")
	}
	if caller.functionDescriptor.ModuleName != "my_module" {
		t.Errorf("ModuleName mismatch: got %q", caller.functionDescriptor.ModuleName)
	}
	if caller.functionDescriptor.FunctionName != "add" {
		t.Errorf("FunctionName mismatch: got %q", caller.functionDescriptor.FunctionName)
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

func TestRemotePython_WithClassName(t *testing.T) {
	caller := RemotePython[int]("calculator", "add", "Calculator")
	if caller.functionDescriptor.ClassName != "Calculator" {
		t.Errorf("ClassName mismatch: got %q", caller.functionDescriptor.ClassName)
	}
}

func TestRemotePython_InvalidModuleName(t *testing.T) {
	caller := RemotePython[int]("", "add", "")
	if caller.err == nil {
		t.Fatal("expected deferred error for empty module name")
	}
	_, err := caller.Call(1, 2)
	if err == nil {
		t.Fatal("Call should return the deferred error")
	}
}

func TestRemotePythonVoid(t *testing.T) {
	caller := RemotePythonVoid("my_module", "log_event", "")
	if caller.numReturns != 0 {
		t.Errorf("numReturns should be 0, got %d", caller.numReturns)
	}
	if caller.functionDescriptor.ModuleName != "my_module" {
		t.Errorf("ModuleName mismatch")
	}
}

func TestPythonTaskCaller_BuilderChain(t *testing.T) {
	caller := RemotePython[int]("my_module", "add", "")
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

func TestRemotePythonActor(t *testing.T) {
	creator := RemotePythonActor("calculator", "Calculator")
	if creator == nil {
		t.Fatal("RemotePythonActor should return non-nil creator")
	}
	if creator.functionDescriptor == nil {
		t.Fatal("functionDescriptor should be set")
	}
	if creator.functionDescriptor.ModuleName != "calculator" {
		t.Errorf("ModuleName mismatch: got %q", creator.functionDescriptor.ModuleName)
	}
	if creator.functionDescriptor.ClassName != "Calculator" {
		t.Errorf("ClassName mismatch: got %q", creator.functionDescriptor.ClassName)
	}
	if creator.functionDescriptor.FunctionName != "__init__" {
		t.Errorf("FunctionName should be __init__, got %q", creator.functionDescriptor.FunctionName)
	}
	if creator.err != nil {
		t.Errorf("unexpected err: %v", creator.err)
	}
}

func TestRemotePythonActor_InvalidModuleName(t *testing.T) {
	creator := RemotePythonActor("", "Calculator")
	if creator.err == nil {
		t.Fatal("expected deferred error for empty module name")
	}
	_, err := creator.Create()
	if err == nil {
		t.Fatal("Create should return the deferred error")
	}
}

func TestPythonActorHandle_ActorTask(t *testing.T) {
	actorID, err := ids.ActorIDFromHex("0102030405060708090a0b0c0d0e0f10")
	if err != nil {
		t.Fatalf("failed to build actor ID: %v", err)
	}
	handle := &PythonActorHandle{
		nativeHandle: &object.NativeActorHandle{
			ActorID:  actorID,
			Language: object.LanguagePython,
		},
		moduleName: "calculator",
		className:  "Calculator",
	}

	caller := ActorTask[int](handle, "add")
	if caller == nil {
		t.Fatal("Task should return non-nil caller")
	}
	if caller.err != nil {
		t.Fatalf("unexpected err: %v", caller.err)
	}
	if caller.methodDescriptor == nil {
		t.Fatal("methodDescriptor should be set")
	}
	if caller.methodDescriptor.FunctionName != "add" {
		t.Errorf("FunctionName mismatch: got %q", caller.methodDescriptor.FunctionName)
	}
	if caller.methodDescriptor.ClassName != "Calculator" {
		t.Errorf("ClassName mismatch: got %q", caller.methodDescriptor.ClassName)
	}
	if len(caller.args) != 0 {
		t.Errorf("expected 0 args, got %d", len(caller.args))
	}
	if caller.actorID != handle.ID() {
		t.Errorf("actorID mismatch")
	}
}
