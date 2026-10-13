//go:build cgo
// +build cgo

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

package cgo

import (
	"testing"

	"github.com/ray-project/ray/go/pkg/runtime/contract"
	"github.com/ray-project/ray/go/pkg/runtime/function"
)

// TestProctitleTitles_GoNormalTask verifies a normal Go task uses the
// function name as the title and restores to "ray::IDLE".
func TestProctitleTitles_GoNormalTask(t *testing.T) {
	desc := function.NewGoFunctionDescriptorOrUnknown("github.com/example/app", "pkg/tasks", "MyTask")
	title, next := proctitleTitles(contract.TaskTypeNormal, desc)
	if title != "ray::MyTask" {
		t.Fatalf("title = %q, want %q", title, "ray::MyTask")
	}
	if next != "ray::IDLE" {
		t.Fatalf("next = %q, want %q", next, "ray::IDLE")
	}
}

// TestProctitleTitles_GoActorTask verifies an actor method task titles the
// method but restores to the actor class, mirroring Python "ray::<class_name>".
func TestProctitleTitles_GoActorTask(t *testing.T) {
	desc := function.NewGoActorMethodDescriptorOrUnknown("github.com/example/app", "pkg/actors", "MyActor", "DoWork")
	title, next := proctitleTitles(contract.TaskTypeActorTask, desc)
	if title != "ray::MyActor.DoWork" {
		t.Fatalf("title = %q, want %q", title, "ray::MyActor.DoWork")
	}
	if next != "ray::MyActor" {
		t.Fatalf("next = %q, want %q", next, "ray::MyActor")
	}
}

// TestProctitleTitles_GoActorCreation verifies the actor creation task restores
// to the actor class being created.
func TestProctitleTitles_GoActorCreation(t *testing.T) {
	desc := function.NewGoActorMethodDescriptorOrUnknown("github.com/example/app", "pkg/actors", "MyActor", function.ConstructorName)
	_, next := proctitleTitles(contract.TaskTypeActorCreation, desc)
	if next != "ray::MyActor" {
		t.Fatalf("next = %q, want %q", next, "ray::MyActor")
	}
}

// TestProctitleTitles_PythonCrossLanguage verifies a cross-language Python
// actor task (executed on the Go worker) derives the class name the same way
// Python does.
func TestProctitleTitles_PythonCrossLanguage(t *testing.T) {
	desc, err := function.NewPythonFunctionDescriptor("my_module", "Calculator", "add", "hash")
	if err != nil {
		t.Fatal(err)
	}
	title, next := proctitleTitles(contract.TaskTypeActorTask, desc)
	if title != "ray::Calculator.add" {
		t.Fatalf("title = %q, want %q", title, "ray::Calculator.add")
	}
	if next != "ray::Calculator" {
		t.Fatalf("next = %q, want %q", next, "ray::Calculator")
	}

	// Module-level Python normal task has no class name.
	desc2, err := function.NewPythonFunctionDescriptor("my_module", "", "top_level", "hash")
	if err != nil {
		t.Fatal(err)
	}
	title2, next2 := proctitleTitles(contract.TaskTypeNormal, desc2)
	if title2 != "ray::top_level" {
		t.Fatalf("title = %q, want %q", title2, "ray::top_level")
	}
	if next2 != "ray::IDLE" {
		t.Fatalf("next = %q, want %q", next2, "ray::IDLE")
	}
}

// TestProctitleTitles_PrefixNotDuplicated verifies an actor class that already
// carries the "ray::" prefix (e.g. a cross-language C++ actor with class name
// "ray::serve::ReplicaActorBase") is not double-prefixed, mirroring C++'s
// StartsWith check.
func TestProctitleTitles_PrefixNotDuplicated(t *testing.T) {
	desc, err := function.NewCppFunctionDescriptor(
		"ray::serve::ReplicaActorBase.HandleRequest",
		"GO",
		"ray::serve::ReplicaActorBase",
	)
	if err != nil {
		t.Fatal(err)
	}
	_, next := proctitleTitles(contract.TaskTypeActorTask, desc)
	if next != "ray::serve::ReplicaActorBase" {
		t.Fatalf("next = %q, want %q", next, "ray::serve::ReplicaActorBase")
	}
}
