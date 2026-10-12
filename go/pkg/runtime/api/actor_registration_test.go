// Copyright 2025 The Ray Authors.
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
	"reflect"
	"testing"

	"github.com/ray-project/ray/go/pkg/runtime/function"
)

// registerTestActor is a stateful actor type used by the actor registration tests.
type registerTestActor struct {
	value int
}

// Increment is an actor method.
func (a *registerTestActor) Increment(delta int) int {
	a.value += delta
	return a.value
}

// newRegisterTestActor is the constructor factory.
func newRegisterTestActor(initial int) *registerTestActor {
	return &registerTestActor{value: initial}
}

// TestRegisterActorClassRegistersInit verifies that RegisterActorClass stores the
// constructor under a "<init>" descriptor that matches what extractActorTypeDescriptor
// produces, so the worker can find it from the actor creation task's descriptor.
func TestRegisterActorClassRegistersInit(t *testing.T) {
	if err := RegisterActorClass((*registerTestActor)(nil), newRegisterTestActor); err != nil {
		t.Fatalf("RegisterActorClass failed: %v", err)
	}

	entries, ok := GetRegisteredFunctions()
	if !ok {
		t.Fatal("expected registered functions, got none")
	}

	var found *function.GoFunctionDescriptor
	for _, e := range entries {
		if e.Descriptor().MethodName() == function.ConstructorName &&
			e.Descriptor().FunctionName == "registerTestActor" {
			found = e.Descriptor()
			break
		}
	}
	if found == nil {
		t.Fatal("expected a '<init>' descriptor for registerTestActor, not found")
	}
}

// TestActorDescriptorMatchesRegistration verifies that the descriptor produced by
// extractActorTypeDescriptor for an actor class matches the descriptor key under
// which RegisterActorClass stored the constructor. This is the contract that lets the
// worker resolve the constructor from the task spec.
func TestActorDescriptorMatchesRegistration(t *testing.T) {
	_ = RegisterActorClass((*registerTestActor)(nil), newRegisterTestActor)

	creator := Actor[*registerTestActor]((*registerTestActor)(nil))
	if creator == nil {
		t.Fatal("Actor returned nil creator")
	}
	desc := creator.functionDescriptor
	if desc == nil {
		t.Fatal("creator.functionDescriptor is nil")
	}
	if desc.MethodName() != function.ConstructorName {
		t.Errorf("expected methodName '<init>', got %q", desc.MethodName())
	}
	if desc.FunctionName != "registerTestActor" {
		t.Errorf("expected FunctionName registerTestActor, got %q", desc.FunctionName)
	}
	if desc.PackagePath == "unknown" || desc.PackagePath == "" {
		t.Errorf("expected a real package path, got %q", desc.PackagePath)
	}

	// The key must match the registered constructor key.
	entry, ok := GetRegisteredFunctions()
	if !ok {
		t.Fatal("expected registered functions")
	}
	for _, e := range entry {
		if e.Descriptor().MethodName() == function.ConstructorName &&
			e.Descriptor().FunctionName == "registerTestActor" {
			if e.Descriptor().String() != desc.String() {
				t.Errorf("descriptor key mismatch: registered=%q actor=%q",
					e.Descriptor().String(), desc.String())
			}
			return
		}
	}
	t.Fatal("expected '<init>' entry for registerTestActor, not found")
}

// TestActorZeroValueConstructorRegisters verifies that calling Actor with a type (rather
// than a factory function) still registers a zero-value "<init>" constructor.
func TestActorZeroValueConstructorRegisters(t *testing.T) {
	creator := Actor[*registerTestActor]((*registerTestActor)(nil))
	if creator == nil {
		t.Fatal("Actor returned nil creator")
	}
	if creator.functionDescriptor.MethodName() != function.ConstructorName {
		t.Errorf("expected '<init>' method name, got %q", creator.functionDescriptor.MethodName())
	}
}

// TestExplicitConstructorNotOverwritten verifies that the type-pointer form of
// Actor (which would otherwise register a zero-value constructor) does NOT
// overwrite an explicitly-registered constructor for the same actor class.
// This is the regression test for the local-mode defect where
// api.Actor[*Counter]((*Counter)(nil)) clobbered the 1-arg newCounter(initial)
// registered via api.RegisterActorClass, so Create(42) failed with "argument
// count mismatch: expected 0, got 1".
func TestExplicitConstructorNotOverwritten(t *testing.T) {
	if err := RegisterActorClass((*registerTestActor)(nil), newRegisterTestActor); err != nil {
		t.Fatalf("RegisterActorClass failed: %v", err)
	}

	// Using the type-pointer form must NOT replace the explicit 1-arg
	// constructor with the zero-value fallback.
	creator := Actor[*registerTestActor]((*registerTestActor)(nil))
	if creator == nil {
		t.Fatal("Actor returned nil creator")
	}

	// The stored constructor must still be newRegisterTestActor (1 param), not
	// the zero-value constructor (0 params).
	entry, ok := GetRegisteredFunctions()
	if !ok {
		t.Fatal("expected registered functions")
	}
	var fn interface{}
	for _, e := range entry {
		if e.Descriptor().MethodName() == function.ConstructorName &&
			e.Descriptor().FunctionName == "registerTestActor" {
			fn = e.Function()
			break
		}
	}
	if fn == nil {
		t.Fatal("expected '<init>' entry for registerTestActor, not found")
	}
	if got := reflect.TypeOf(fn).NumIn(); got != 1 {
		t.Errorf("explicit constructor overwritten by type form: expected 1 arg, got %d", got)
	}
}
