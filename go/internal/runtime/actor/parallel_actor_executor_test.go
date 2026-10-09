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

package actor

import (
	"fmt"
	"testing"

	"github.com/ray-project/ray/go/pkg/runtime/function"
	"github.com/ray-project/ray/go/pkg/runtime/object"
)

type counter struct{}

func (c *counter) Add(a, b int) int { return a + b }

// errorCounter's Div returns a trailing error following the (T, error)
// convention so Execute (via CallActorMethod) must surface it as a task failure.
type errorCounter struct{}

func (c *errorCounter) Div(a, b int) (int, error) {
	if b == 0 {
		return 0, fmt.Errorf("division by zero")
	}
	return a / b, nil
}

func newCounterDesc(method string) *function.GoFunctionDescriptor {
	desc, err := function.NewGoActorMethodDescriptor("github.com/ray-project/ray", "pkg", "counter", method)
	if err != nil {
		panic(err)
	}
	return desc
}

// counterCtor is a wrapped user constructor that always returns a fresh counter.
func counterCtor(args []function.FunctionArg) (interface{}, error) {
	return &counter{}, nil
}

func TestParallelActorExecutorExecute(t *testing.T) {
	ex := NewParallelActorExecutor()
	if err := ex.ConstructInstances(2, counterCtor, nil); err != nil {
		t.Fatalf("ConstructInstances: %v", err)
	}

	desc := newCounterDesc("Add")
	out, err := ex.Execute(1, desc, []function.FunctionArg{valueArg(t, 2), valueArg(t, 3)})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if len(out) != 1 {
		t.Fatalf("len(out) = %d, want 1", len(out))
	}
	var got int
	ser := object.GetSerializer()
	if err := ser.DeserializeTo(&object.NativeRayObject{Data: out[0].Data, Metadata: out[0].Metadata}, &got); err != nil {
		t.Fatalf("DeserializeTo: %v", err)
	}
	if got != 5 {
		t.Fatalf("out = %v, want 5", got)
	}
}

func TestParallelActorExecutorErrorReturn(t *testing.T) {
	ctor := func(args []function.FunctionArg) (interface{}, error) { return &errorCounter{}, nil }
	ex := NewParallelActorExecutor()
	if err := ex.ConstructInstances(1, ctor, nil); err != nil {
		t.Fatalf("ConstructInstances: %v", err)
	}
	desc, err := function.NewGoActorMethodDescriptor("github.com/ray-project/ray", "pkg", "counter", "Div")
	if err != nil {
		t.Fatalf("NewGoActorMethodDescriptor: %v", err)
	}

	// Non-nil trailing error: Execute must surface it as an error, not a value.
	out, err := ex.Execute(0, desc, []function.FunctionArg{valueArg(t, 1), valueArg(t, 0)})
	if err == nil {
		t.Fatal("Execute: expected error for division by zero")
	}
	if out != nil {
		t.Fatalf("Execute: out = %v, want nil on error", out)
	}

	// Nil trailing error: the value must be returned normally.
	out, err = ex.Execute(0, desc, []function.FunctionArg{valueArg(t, 6), valueArg(t, 3)})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if len(out) != 1 {
		t.Fatalf("len(out) = %d, want 1", len(out))
	}
	var got int
	ser := object.GetSerializer()
	if err := ser.DeserializeTo(&object.NativeRayObject{Data: out[0].Data, Metadata: out[0].Metadata}, &got); err != nil {
		t.Fatalf("DeserializeTo: %v", err)
	}
	if got != 2 {
		t.Fatalf("out = %v, want 2", got)
	}
}

func TestParallelActorExecutorInstanceOutOfRange(t *testing.T) {
	ex := NewParallelActorExecutor()
	if err := ex.ConstructInstances(2, counterCtor, nil); err != nil {
		t.Fatalf("ConstructInstances: %v", err)
	}

	desc := newCounterDesc("Add")
	if _, err := ex.Execute(2, desc, []function.FunctionArg{valueArg(t, 1), valueArg(t, 2)}); err == nil {
		t.Fatal("Execute: expected error for out-of-range instanceId")
	}
	if _, err := ex.Execute(-1, desc, []function.FunctionArg{valueArg(t, 1), valueArg(t, 2)}); err == nil {
		t.Fatal("Execute: expected error for negative instanceId")
	}
}

func TestParallelActorExecutorCtorError(t *testing.T) {
	ctor := func(args []function.FunctionArg) (interface{}, error) { return nil, fmt.Errorf("ctor failed") }
	ex := NewParallelActorExecutor()
	if err := ex.ConstructInstances(1, ctor, nil); err == nil {
		t.Fatal("ConstructInstances: expected error from failing ctor")
	}
}

func TestParallelActorExecutorNilInstance(t *testing.T) {
	ctor := func(args []function.FunctionArg) (interface{}, error) { return nil, nil }
	ex := NewParallelActorExecutor()
	// A constructor that returns a nil instance must be rejected at construction.
	if err := ex.ConstructInstances(1, ctor, nil); err == nil {
		t.Fatal("ConstructInstances: expected error for nil instance")
	}
}

func TestParallelActorExecutorConstructValidations(t *testing.T) {
	ex := NewParallelActorExecutor()
	if err := ex.ConstructInstances(0, counterCtor, nil); err == nil {
		t.Fatal("ConstructInstances: expected error for n < 1")
	}
	if err := ex.ConstructInstances(2, nil, nil); err == nil {
		t.Fatal("ConstructInstances: expected error for nil userCtor")
	}
}
