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

package native

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ray-project/ray/go/internal/runtime/serializer"
	rayerrors "github.com/ray-project/ray/go/pkg/errors"
	"github.com/ray-project/ray/go/pkg/ids"
	"github.com/ray-project/ray/go/pkg/runtime/function"
	"github.com/ray-project/ray/go/pkg/runtime/object"
)

// testActor is a simple stateful actor used by the unit tests below.
type testActor struct {
	value int
}

// valueTypedErr is a value-type error (a struct implementing error), used to
// verify the constructor's trailing-error handling is safe for non-reference
// error types.
type valueTypedErr struct {
	msg string
}

func (e valueTypedErr) Error() string { return e.msg }

// Add increments the internal counter by the argument and returns the new value.
func (a *testActor) Add(delta int) int {
	a.value += delta
	return a.value
}

// Get returns the current counter value.
func (a *testActor) Get() int {
	return a.value
}

// AddErr returns (new value, error); a negative delta is rejected.
func (a *testActor) AddErr(delta int) (int, error) {
	if delta < 0 {
		return 0, fmt.Errorf("negative delta %d", delta)
	}
	a.value += delta
	return a.value, nil
}

// NilErr returns the current value with a nil error.
func (a *testActor) NilErr() (int, error) {
	return a.value, nil
}

// Exit returns an intentional actor exit error.
func (a *testActor) Exit() error {
	return rayerrors.NewActorExitError("test actor exiting")
}

// newTestActor is the constructor factory used by the tests.
func newTestActor(initial int) *testActor {
	return &testActor{value: initial}
}

func newActorDesc(t *testing.T, methodName string) *function.GoFunctionDescriptor {
	t.Helper()
	desc, err := function.NewGoActorMethodDescriptor(
		"github.com/ray-project/ray/go/internal/runtime/native",
		"internal/runtime/native",
		"testActor",
		methodName,
	)
	if err != nil {
		t.Fatalf("failed to build actor descriptor: %v", err)
	}
	return desc
}

// valueArg serializes a value into a pass-by-value FunctionArg.
func valueArg(t *testing.T, v interface{}) function.FunctionArg {
	t.Helper()
	ser := object.GetSerializer()
	nativeObj, err := ser.Serialize(v)
	if err != nil {
		t.Fatalf("failed to serialize argument: %v", err)
	}
	return function.NewFunctionArgByValue(nativeObj.Data, nativeObj.Metadata)
}

func TestActorManager_ConstructActor(t *testing.T) {
	manager := NewActorManager()

	initDesc := newActorDesc(t, function.ConstructorName)
	manager.RegisterConstructor(initDesc.String(), WrapActorConstructor(newTestActor))

	// Construct with an initial value of 10.
	instance, err := manager.ConstructActor(initDesc, []function.FunctionArg{valueArg(t, 10)})
	if err != nil {
		t.Fatalf("ConstructActor failed: %v", err)
	}
	actor, ok := instance.(*testActor)
	if !ok {
		t.Fatalf("expected *testActor, got %T", instance)
	}
	if actor.value != 10 {
		t.Errorf("expected actor.value=10, got %d", actor.value)
	}
}

func TestActorManager_ConstructActor_Unregistered(t *testing.T) {
	manager := NewActorManager()
	_, err := manager.ConstructActor(newActorDesc(t, function.ConstructorName), nil)
	if err == nil {
		t.Fatal("expected error for unregistered constructor, got nil")
	}
}

func TestActorManager_InstanceLifecycle(t *testing.T) {
	manager := NewActorManager()
	actorID := ids.OfActorID(ids.NilJobID(), ids.NilTaskID(), 42)
	actor := &testActor{value: 1}

	manager.SetInstance(actorID, actor)
	got, ok := manager.GetInstance(actorID)
	if !ok || got != actor {
		t.Fatalf("GetInstance: expected actor, got %v (ok=%v)", got, ok)
	}

	manager.RemoveInstance(actorID)
	if _, ok := manager.GetInstance(actorID); ok {
		t.Fatal("RemoveInstance: instance should be gone")
	}
}

func TestCallActorMethod_WithReceiver(t *testing.T) {
	manager := NewActorManager()
	actorID := ids.OfActorID(ids.NilJobID(), ids.NilTaskID(), 43)

	initDesc := newActorDesc(t, function.ConstructorName)
	manager.RegisterConstructor(initDesc.String(), WrapActorConstructor(newTestActor))
	instance, err := manager.ConstructActor(initDesc, []function.FunctionArg{valueArg(t, 5)})
	if err != nil {
		t.Fatalf("ConstructActor failed: %v", err)
	}
	manager.SetInstance(actorID, instance)

	// Call Add(7) on the instance. The result (12) must be serialized back.
	addDesc := newActorDesc(t, "Add")
	results, err := CallActorMethod(instance, addDesc, []function.FunctionArg{valueArg(t, 7)})
	if err != nil {
		t.Fatalf("CallActorMethod failed: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}

	ser := object.GetSerializer()
	var result int
	if err := ser.DeserializeTo(&object.NativeRayObject{Data: results[0].Data, Metadata: results[0].Metadata}, &result); err != nil {
		t.Fatalf("failed to deserialize result: %v", err)
	}
	if result != 12 {
		t.Errorf("expected 12, got %d", result)
	}

	// The actor state must have been mutated on the stored instance.
	got, _ := manager.GetInstance(actorID)
	if actor, ok := got.(*testActor); !ok || actor.value != 12 {
		t.Errorf("expected actor.value=12 after Add, got %+v", got)
	}
}

func TestCallActorMethod_VoidAndGetter(t *testing.T) {
	manager := NewActorManager()
	actorID := ids.OfActorID(ids.NilJobID(), ids.NilTaskID(), 44)

	initDesc := newActorDesc(t, function.ConstructorName)
	manager.RegisterConstructor(initDesc.String(), WrapActorConstructor(newTestActor))
	instance, _ := manager.ConstructActor(initDesc, []function.FunctionArg{valueArg(t, 3)})
	manager.SetInstance(actorID, instance)

	// Get() returns the current value (3).
	getDesc := newActorDesc(t, "Get")
	results, err := CallActorMethod(instance, getDesc, nil)
	if err != nil {
		t.Fatalf("CallActorMethod(Get) failed: %v", err)
	}
	ser := object.GetSerializer()
	var got int
	if err := ser.DeserializeTo(&object.NativeRayObject{Data: results[0].Data, Metadata: results[0].Metadata}, &got); err != nil {
		t.Fatalf("failed to deserialize Get result: %v", err)
	}
	if got != 3 {
		t.Errorf("expected 3, got %d", got)
	}
}

func TestCallActorMethod_MissingMethod(t *testing.T) {
	desc, err := function.NewGoActorMethodDescriptor(
		"github.com/ray-project/ray/go/internal/runtime/native",
		"internal/runtime/native",
		"testActor",
		"DoesNotExist",
	)
	if err != nil {
		t.Fatalf("failed to build descriptor: %v", err)
	}
	_, err = CallActorMethod(&testActor{}, desc, nil)
	if err == nil {
		t.Fatal("expected error for missing method, got nil")
	}
}

func TestWrapActorConstructor_WithError(t *testing.T) {
	boom := errors.New("construction failed")
	failingCtor := func(initial int) (*testActor, error) {
		return nil, boom
	}
	ctor := WrapActorConstructor(failingCtor)
	desc := newActorDesc(t, function.ConstructorName)
	manager := NewActorManager()
	manager.RegisterConstructor(desc.String(), ctor)
	_, err := manager.ConstructActor(desc, []function.FunctionArg{valueArg(t, 1)})
	if err == nil {
		t.Fatal("expected error from constructor, got nil")
	}
}

func TestWrapActorConstructor_ValueTypedError(t *testing.T) {
	failingCtor := func(initial int) (*testActor, valueTypedErr) {
		return nil, valueTypedErr{msg: "construction failed"}
	}
	ctor := WrapActorConstructor(failingCtor)
	desc := newActorDesc(t, function.ConstructorName)
	manager := NewActorManager()
	manager.RegisterConstructor(desc.String(), ctor)
	_, err := manager.ConstructActor(desc, []function.FunctionArg{valueArg(t, 1)})
	if err == nil {
		t.Fatal("expected error from value-typed error constructor, got nil")
	}
	if !strings.Contains(err.Error(), "construction failed") {
		t.Fatalf("unexpected error: got %q, want it to contain %q", err.Error(), "construction failed")
	}
}

func TestCallActorMethod_TooManyArgs(t *testing.T) {
	instance := &testActor{value: 1}
	desc := newActorDesc(t, "Add")
	_, err := CallActorMethod(instance, desc, []function.FunctionArg{valueArg(t, 1), valueArg(t, 2)})
	if err == nil {
		t.Fatal("expected argument count error, got nil")
	}
	if got, want := err.Error(), "actor method Add on *native.testActor expects 1 argument(s), got 2"; got != want {
		t.Fatalf("unexpected error: got %q, want %q", got, want)
	}
}

func TestCallActorMethod_TooFewArgs(t *testing.T) {
	instance := &testActor{value: 1}
	desc := newActorDesc(t, "Add")
	_, err := CallActorMethod(instance, desc, nil)
	if err == nil {
		t.Fatal("expected argument count error, got nil")
	}
	if got, want := err.Error(), "actor method Add on *native.testActor expects 1 argument(s), got 0"; got != want {
		t.Fatalf("unexpected error: got %q, want %q", got, want)
	}
}

func TestCallActorMethod_ErrorReturn(t *testing.T) {
	instance := &testActor{value: 5}
	desc := newActorDesc(t, "AddErr")
	results, err := CallActorMethod(instance, desc, []function.FunctionArg{valueArg(t, -1)})
	if err == nil {
		t.Fatal("expected non-nil error, got nil")
	}
	if len(results) != 0 {
		t.Fatalf("expected 0 results on error return, got %d", len(results))
	}
	if got := err.Error(); got != "negative delta -1" {
		t.Fatalf("unexpected error: %q", got)
	}
}

func TestCallActorMethod_NilErrorReturn(t *testing.T) {
	instance := &testActor{value: 7}
	desc := newActorDesc(t, "NilErr")
	results, err := CallActorMethod(instance, desc, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 result (nil error dropped), got %d", len(results))
	}
	ser := object.GetSerializer()
	var got int
	if err := ser.DeserializeTo(&object.NativeRayObject{Data: results[0].Data, Metadata: results[0].Metadata}, &got); err != nil {
		t.Fatalf("failed to deserialize result: %v", err)
	}
	if got != 7 {
		t.Fatalf("expected 7, got %d", got)
	}
}

func TestCallActorMethod_ActorExitReturn(t *testing.T) {
	instance := &testActor{value: 1}
	desc := newActorDesc(t, "Exit")
	results, err := CallActorMethod(instance, desc, nil)
	if err == nil {
		t.Fatal("expected ActorExitError, got nil")
	}
	if !rayerrors.IsActorExitError(err) {
		t.Fatalf("expected ActorExitError, got %T", err)
	}
	if len(results) != 0 {
		t.Fatalf("expected 0 results on exit, got %d", len(results))
	}
}

// Ensure serializer is initialized via its package init.
var _ = serializer.BufferPoolWrapper{}

// concurrentActor tracks the maximum number of methods running at once.
type concurrentActor struct {
	mu            sync.Mutex
	current       int
	maxConcurrent int
}

func (a *concurrentActor) Enter() {
	a.mu.Lock()
	a.current++
	if a.current > a.maxConcurrent {
		a.maxConcurrent = a.current
	}
	a.mu.Unlock()
	time.Sleep(50 * time.Millisecond)
	a.mu.Lock()
	a.current--
	a.mu.Unlock()
}

func TestActorManager_ConcurrentMethodExecution(t *testing.T) {
	// With no per-actor lock, two methods of the same actor must be able to run
	// concurrently (the C++ side controls actual serialization).
	manager := NewActorManager()
	actorID := ids.OfActorID(ids.NilJobID(), ids.NilTaskID(), 50)
	actor := &concurrentActor{}
	manager.SetInstance(actorID, actor)

	desc := newActorDesc(t, "Enter")
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := manager.ExecuteActorMethod(actorID, desc, nil); err != nil {
				t.Errorf("ExecuteActorMethod failed: %v", err)
			}
		}()
	}
	wg.Wait()
	if actor.maxConcurrent < 2 {
		t.Errorf("expected concurrent execution without per-actor lock, max concurrent = %d", actor.maxConcurrent)
	}
}

func TestActorManager_ExecuteActorMethod_ExitRemovesInstance(t *testing.T) {
	manager := NewActorManager()
	actorID := ids.OfActorID(ids.NilJobID(), ids.NilTaskID(), 51)
	manager.SetInstance(actorID, &testActor{value: 1})

	desc := newActorDesc(t, "Exit")
	results, err := manager.ExecuteActorMethod(actorID, desc, nil)
	if err == nil || !rayerrors.IsActorExitError(err) {
		t.Fatalf("expected ActorExitError, got %v", err)
	}
	if len(results) != 0 {
		t.Fatalf("expected 0 results, got %d", len(results))
	}
	if _, ok := manager.GetInstance(actorID); ok {
		t.Fatal("actor instance must be removed after intentional exit")
	}
}

func TestActorManager_ExecuteActorMethod_ErrorKeepsInstance(t *testing.T) {
	manager := NewActorManager()
	actorID := ids.OfActorID(ids.NilJobID(), ids.NilTaskID(), 52)
	manager.SetInstance(actorID, &testActor{value: 5})

	desc := newActorDesc(t, "AddErr")
	_, err := manager.ExecuteActorMethod(actorID, desc, []function.FunctionArg{valueArg(t, -1)})
	if err == nil {
		t.Fatal("expected error from method, got nil")
	}
	if _, ok := manager.GetInstance(actorID); !ok {
		t.Fatal("actor instance must remain after a normal error")
	}
}

func TestActorManager_ExecuteActorMethod_MissingInstance(t *testing.T) {
	manager := NewActorManager()
	actorID := ids.OfActorID(ids.NilJobID(), ids.NilTaskID(), 53)
	desc := newActorDesc(t, "Add")
	_, err := manager.ExecuteActorMethod(actorID, desc, nil)
	if err == nil {
		t.Fatal("expected instance-not-found error, got nil")
	}
	if !strings.Contains(err.Error(), "actor instance not found") {
		t.Fatalf("unexpected error: %q", err)
	}
}

// typedNilCtor returns a typed nil instance (an interface holding a nil pointer),
// which must be treated as a construction failure rather than a valid actor.
func typedNilCtor(int) *testActor {
	return nil
}

func TestActorManager_ConstructActor_TypedNil(t *testing.T) {
	manager := NewActorManager()
	initDesc := newActorDesc(t, function.ConstructorName)
	manager.RegisterConstructor(initDesc.String(), WrapActorConstructor(typedNilCtor))

	_, err := manager.ConstructActor(initDesc, []function.FunctionArg{valueArg(t, 1)})
	if err == nil {
		t.Fatal("expected nil-instance error for typed nil constructor, got nil")
	}
	if !strings.Contains(err.Error(), "nil instance") {
		t.Fatalf("unexpected error: %q", err)
	}
}

func TestActorManager_ExecuteActorMethod_TypedNilInstance(t *testing.T) {
	manager := NewActorManager()
	actorID := ids.OfActorID(ids.NilJobID(), ids.NilTaskID(), 54)
	desc := newActorDesc(t, "Add")

	// A typed nil pointer stored as the instance must be rejected before any
	// reflective method call (previously this panicked in MethodByName).
	var typedNil *testActor
	manager.SetInstance(actorID, typedNil)

	_, err := manager.ExecuteActorMethod(actorID, desc, []function.FunctionArg{valueArg(t, 1)})
	if err == nil {
		t.Fatal("expected nil-instance error for typed nil actor, got nil")
	}
	if !strings.Contains(err.Error(), "not found or nil") {
		t.Fatalf("unexpected error: %q", err)
	}
}
