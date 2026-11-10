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
	"github.com/ray-project/ray/go/pkg/runtime/object"
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

// TestWrapPythonArgsStampsCrossLanguageMetadata verifies that a value argument
// routed through the Python wrapping path carries cross-language metadata so
// the Python worker knows it must deserialize it as a cross-language value,
// while the Go-native path keeps the Go metadata untouched.
func TestWrapPythonArgsStampsCrossLanguageMetadata(t *testing.T) {
	// The api test binary has no real serializer registered; the minimal test
	// serializer mimics the Go metadata type for plain values. Restore the nil
	// default afterwards so sibling tests are unaffected.
	object.SetSerializer(testSerializer{})
	defer object.SetSerializer(nil)

	args := wrapPythonArgs([]interface{}{42})
	if len(args) != 2 {
		t.Fatalf("wrapPythonArgs returned %d args, want 2 (dummy marker + value)", len(args))
	}

	marker := args[0]
	if !marker.IsPassByValue() || marker.Data == nil {
		t.Fatal("dummy marker should be passed by value")
	}
	if got := string(marker.Data.Metadata); got != object.MetadataTypeRaw {
		t.Errorf("dummy marker metadata = %q, want %q", got, object.MetadataTypeRaw)
	}

	value := args[1]
	if !value.IsPassByValue() || value.Data == nil {
		t.Fatal("wrapped python arg should be passed by value")
	}
	if got := string(value.Data.Metadata); got != object.MetadataTypeCrossLanguage {
		t.Errorf("wrapped python arg metadata = %q, want %q", got, object.MetadataTypeCrossLanguage)
	}

	// The typed Go-native path must keep its own metadata type.
	goArgs := convertArgs(42)
	if len(goArgs) != 1 {
		t.Fatalf("convertArgs returned %d args, want 1", len(goArgs))
	}
	if got := string(goArgs[0].Data.Metadata); got != object.MetadataTypeGo {
		t.Errorf("go-native arg metadata = %q, want %q", got, object.MetadataTypeGo)
	}

	// Pass-by-reference arguments carry no serialized data, so they must keep
	// their non-XLANG shape: by-ref routing only, no metadata stamp.
	ref := &ObjectRef[[]byte]{} // zero ObjectID is fine: only by-ref routing is asserted
	refArgs := wrapPythonArgs([]interface{}{ref})
	if len(refArgs) != 2 {
		t.Fatalf("wrapPythonArgs returned %d args, want 2 (dummy marker + ref)", len(refArgs))
	}
	refArg := refArgs[1]
	if refArg.IsPassByValue() {
		t.Error("wrapped python object-ref arg routed by value, want by-ref")
	}
	if refArg.ObjectRef == nil {
		t.Fatal("wrapped python object-ref arg is missing its object reference")
	}
	if refArg.Data != nil {
		t.Errorf("wrapped python object-ref arg carries serialized data %q, want none", refArg.Data.Data)
	}
}

// TestPythonActorCreatorBuilders pins the options recorded by the cross-language
// actor builder methods: detached lifetime, async mode, and head-node affinity
// are all required by the dashboard agent job module's supervisor actors.
func TestPythonActorCreatorBuilders(t *testing.T) {
	creator := RemotePythonActor("m", "C").
		WithLifetime(submitter.ActorLifetimeDetached).
		WithAsync(true).
		WithNodeAffinity("abc", false)
	if creator.err != nil {
		t.Fatalf("RemotePythonActor returned error: %v", creator.err)
	}
	if creator.options.Lifetime != submitter.ActorLifetimeDetached {
		t.Errorf("Lifetime = %v, want Detached", creator.options.Lifetime)
	}
	if !creator.options.IsAsync {
		t.Error("IsAsync = false, want true")
	}
	if creator.options.NodeAffinity == nil {
		t.Fatal("NodeAffinity = nil after WithNodeAffinity")
	}
	if creator.options.NodeAffinity.NodeID != "abc" {
		t.Errorf("NodeAffinity.NodeID = %q, want %q", creator.options.NodeAffinity.NodeID, "abc")
	}
	if creator.options.NodeAffinity.Soft {
		t.Error("NodeAffinity.Soft = true, want false")
	}
}

// TestPythonActorTaskCallerWithMaxRetries pins the retry option used by the
// supervisor ping, which must retry until the actor is reachable.
func TestPythonActorTaskCallerWithMaxRetries(t *testing.T) {
	handle := &PythonActorHandle{
		baseActorHandle: baseActorHandle{nativeHandle: object.NewNativeActorHandle(ids.NilActorID(), object.LanguagePython)},
		moduleName:      "m",
		className:       "C",
	}
	caller := ActorTask[struct{}](handle, "ping").WithMaxRetries(-1)
	if caller.err != nil {
		t.Fatalf("ActorTask returned error: %v", caller.err)
	}
	if caller.options.RetryPolicy == nil {
		t.Fatal("RetryPolicy = nil after WithMaxRetries")
	}
	if caller.options.RetryPolicy.MaxRetries != -1 {
		t.Errorf("RetryPolicy.MaxRetries = %d, want -1", caller.options.RetryPolicy.MaxRetries)
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
