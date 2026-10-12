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
	"testing"

	"github.com/ray-project/ray/go/pkg/ids"
	"github.com/ray-project/ray/go/pkg/runtime/submitter"
)

// numReturnsActor exercises the actor method signatures used by the caller
// tests.
type numReturnsActor struct{}

func (a *numReturnsActor) Void()                {}
func (a *numReturnsActor) One() int             { return 0 }
func (a *numReturnsActor) OneErr() (int, error) { return 0, nil }
func (a *numReturnsActor) ErrOnly() error       { return nil }
func (a *numReturnsActor) Two() (int, int)      { return 0, 0 }

func TestActorTaskCaller_WithNameAndConcurrencyGroup(t *testing.T) {
	actorID := ids.OfActorID(ids.NilJobID(), ids.NilTaskID(), 3)
	handle := NewActorHandleImpl[numReturnsActor](actorID)

	caller := handle.Task((*numReturnsActor).One).
		WithName("my-task").
		WithConcurrencyGroup("cg1")
	if caller.err != nil {
		t.Fatalf("Task returned error: %v", caller.err)
	}
	if caller.options == nil {
		t.Fatal("options is nil")
	}
	if caller.options.Name != "my-task" {
		t.Fatalf("options.Name = %q, want %q", caller.options.Name, "my-task")
	}
	if caller.options.ConcurrencyGroupName != "cg1" {
		t.Fatalf("options.ConcurrencyGroupName = %q, want %q", caller.options.ConcurrencyGroupName, "cg1")
	}
}

func TestTaskCaller_WithPlacementGroup(t *testing.T) {
	pg := &PlacementGroup{id: ids.OfPlacementGroupID(ids.NewJobID())}
	caller := Remote[int](func() int { return 0 })

	if got := caller.WithPlacementGroup(nil, 3); got != caller {
		t.Fatal("WithPlacementGroup(nil) did not return the same caller")
	}
	if caller.options.PlacementGroup != nil {
		t.Fatalf("options.PlacementGroup = %+v, want nil for a nil group", caller.options.PlacementGroup)
	}

	got := caller.WithPlacementGroup(pg, 2)
	if got != caller {
		t.Fatal("WithPlacementGroup did not return the same caller for chaining")
	}
	if caller.options.PlacementGroup == nil {
		t.Fatal("options.PlacementGroup is nil after WithPlacementGroup")
	}
	if caller.options.PlacementGroup.ID != pg.ID() {
		t.Fatalf("options.PlacementGroup.ID = %v, want %v", caller.options.PlacementGroup.ID, pg.ID())
	}
	if caller.options.PlacementGroup.BundleIndex != 2 {
		t.Fatalf("options.PlacementGroup.BundleIndex = %d, want 2", caller.options.PlacementGroup.BundleIndex)
	}
}

func TestActorOptionsNewBuilders(t *testing.T) {
	type actor struct{}
	creator := Actor[*actor](nil)
	creator.WithLifetime(submitter.ActorLifetimeDetached)
	creator.WithAsync(true)
	creator.WithMaxPendingCalls(5)
	if creator.options.Lifetime != submitter.ActorLifetimeDetached {
		t.Errorf("Lifetime = %v, want Detached", creator.options.Lifetime)
	}
	if !creator.options.IsAsync {
		t.Errorf("IsAsync = false, want true")
	}
	if creator.options.MaxPendingCalls != 5 {
		t.Errorf("MaxPendingCalls = %d, want 5", creator.options.MaxPendingCalls)
	}
}
