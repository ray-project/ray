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
	"strconv"
	"testing"

	"github.com/ray-project/ray/go/pkg/ids"
	"github.com/ray-project/ray/go/pkg/runtime/function"
	"github.com/ray-project/ray/go/pkg/runtime/object"
	"github.com/ray-project/ray/go/pkg/runtime/submitter"
	"github.com/vmihailenco/msgpack/v5"
)

// testSerializer is a minimal object.Serializer used to register a working
// serializer for the mock runtime, since convertArgs requires one.
type testSerializer struct{}

func (testSerializer) Serialize(obj interface{}) (*object.NativeRayObject, error) {
	data, err := msgpack.Marshal(obj)
	if err != nil {
		return nil, err
	}
	return object.NewNativeRayObject(data, []byte(object.MetadataTypeGo)), nil
}
func (testSerializer) Deserialize(nativeObj *object.NativeRayObject, objectID *ids.ObjectID, objectType string) (interface{}, error) {
	return nil, nil
}
func (testSerializer) DeserializeTo(nativeObj *object.NativeRayObject, target interface{}) error { return nil }
func (testSerializer) AddContainedObjectID(objectID ids.ObjectID)                              {}
func (testSerializer) GetAndClearContainedObjectIDs() []ids.ObjectID                          { return nil }
func (testSerializer) SetOuterObjectID(objectID ids.ObjectID)                                 {}
func (testSerializer) GetOuterObjectID() ids.ObjectID                                          { return ids.NilObjectID() }
func (testSerializer) ResetOuterObjectID()                                                    {}
func (testSerializer) EstimateBufferSize(obj interface{}) int                                  { return 0 }
func (testSerializer) GetBuffer(size int) []byte                                               { return make([]byte, size) }
func (testSerializer) PutBuffer(buf []byte)                                                   {}
func (testSerializer) IsCrossLanguageType(obj interface{}) bool                               { return false }

type Counter struct {
	N int
}

func (c *Counter) Add(n int) int { c.N += n; return c.N }

func TestActorCreatorWithConcurrencyGroups(t *testing.T) {
	creator := Actor[*Counter](&Counter{})
	creator.WithConcurrencyGroups(ConcurrencyGroup{Name: "cg1", MaxCalls: 2, Methods: []string{"Add"}})
	creator.WithDefaultConcurrencyGroup(3)

	groups := creator.options.ConcurrencyGroups
	if len(groups) != 2 {
		t.Fatalf("len(groups) = %d, want 2", len(groups))
	}
	if groups[0].Name != "cg1" || groups[0].MaxCalls != 2 || len(groups[0].Methods) != 1 || groups[0].Methods[0] != "Add" {
		t.Fatalf("groups[0] = %+v, want {cg1,2,[Add]}", groups[0])
	}
	if groups[1].Name != "default" || groups[1].MaxCalls != 3 || len(groups[1].Methods) != 0 {
		t.Fatalf("groups[1] = %+v, want {default,3,[]}", groups[1])
	}
}

// itoa is a small helper aliasing strconv.Itoa used by the parallel actor tests.
func itoa(i int) string { return strconv.Itoa(i) }

// mockRuntime wraps recordingRuntime (which already implements every
// contract.Runtime method) to supply the injected task submitter.
type mockRuntime struct {
	recordingRuntime
	sub submitter.TaskSubmitter
}

func (r mockRuntime) GetTaskSubmitter() submitter.TaskSubmitter { return r.sub }

// initMockRuntime injects a mock runtime whose GetTaskSubmitter returns the
// given submitter, so getTaskSubmitter()/createActorWithSubmitter reach it. It
// also registers a minimal serializer so convertArgs can serialize arguments.
//
// It reuses recordingHandle (rather than a new handle type) because the global
// currentHandle is an atomic.Value that only accepts one concrete type per
// test binary; mixing handle types panics.
func initMockRuntime(t *testing.T, sub submitter.TaskSubmitter) {
	t.Helper()
	object.SetSerializer(testSerializer{})
	SetRuntimeHandleForWorker(recordingHandle{rt: mockRuntime{sub: sub}})
	t.Cleanup(clearHandle)
}

// recordingSubmitter records the last actor-creation options and the last task
// concurrency group name for assertion.
type recordingSubmitter struct {
	submitter.TaskSubmitter
	lastGroups []submitter.ConcurrencyGroup
	lastCGName string
	createArgs []function.FunctionArg
}

func (r *recordingSubmitter) CreateActor(functionDescriptor function.FunctionDescriptor, args []function.FunctionArg, options *submitter.ActorCreationOptions) (ids.ActorID, error) {
	r.lastGroups = options.ConcurrencyGroups
	r.createArgs = args
	return ids.NilActorID(), nil
}

func (r *recordingSubmitter) SubmitActorTask(actorID ids.ActorID, functionDescriptor function.FunctionDescriptor, args []function.FunctionArg, numReturns int, options *submitter.TaskOptions) ([]ids.ObjectID, error) {
	r.lastCGName = options.ConcurrencyGroupName
	return []ids.ObjectID{ids.NilObjectID()}, nil
}

func TestParallelActorCreateRegistersNInstanceGroups(t *testing.T) {
	initMockRuntime(t, &recordingSubmitter{})

	handle, err := NewParallelActor[*Counter](&Counter{}).SetParallelism(4).Create()
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if handle.GetParallelism() != 4 {
		t.Fatalf("GetParallelism() = %d, want 4", handle.GetParallelism())
	}
	if handle.GetHandle() == nil {
		t.Fatalf("GetHandle() = nil, want non-nil wrapper actor handle")
	}

	rs := getTaskSubmitter().(*recordingSubmitter)
	if len(rs.lastGroups) != 4 {
		t.Fatalf("len(groups) = %d, want 4", len(rs.lastGroups))
	}
	for i := 0; i < 4; i++ {
		wantName := "PARALLEL_INSTANCE_" + itoa(i)
		if rs.lastGroups[i].Name != wantName {
			t.Fatalf("groups[%d].Name = %q, want %q", i, rs.lastGroups[i].Name, wantName)
		}
		if rs.lastGroups[i].MaxCalls != 1 {
			t.Fatalf("groups[%d].MaxCalls = %d, want 1", i, rs.lastGroups[i].MaxCalls)
		}
		if len(rs.lastGroups[i].Methods) != 1 || rs.lastGroups[i].Methods[0] != "Execute" {
			t.Fatalf("groups[%d].Methods = %v, want [Execute]", i, rs.lastGroups[i].Methods)
		}
	}

	// The wrapper actor creation args must carry parallelism and the user actor
	// constructor descriptor (order: [parallelism, ctorDesc]).
	if len(rs.createArgs) < 2 {
		t.Fatalf("len(createArgs) = %d, want >= 2", len(rs.createArgs))
	}
	var parallelism int
	if err := msgpack.Unmarshal(rs.createArgs[0].Data.Data, &parallelism); err != nil {
		t.Fatalf("unmarshal parallelism: %v", err)
	}
	if parallelism != 4 {
		t.Fatalf("createArgs[0] parallelism = %d, want 4", parallelism)
	}
	var ctorDesc []string
	if err := msgpack.Unmarshal(rs.createArgs[1].Data.Data, &ctorDesc); err != nil {
		t.Fatalf("unmarshal ctorDesc: %v", err)
	}
	if len(ctorDesc) != 4 || ctorDesc[3] != function.ConstructorName {
		t.Fatalf("createArgs[1] ctorDesc = %v, want 4-element descriptor ending in %q", ctorDesc, function.ConstructorName)
	}
}

func TestParallelActorInstanceRoutesToGroup(t *testing.T) {
	initMockRuntime(t, &recordingSubmitter{})
	handle, _ := NewParallelActor[*Counter](&Counter{}).SetParallelism(4).Create()

	caller := handle.GetInstance(1).Task((*Counter).Add, 10)
	_, err := caller.Remote()
	if err != nil {
		t.Fatalf("Remote: %v", err)
	}
	rs := getTaskSubmitter().(*recordingSubmitter)
	if rs.lastCGName != "PARALLEL_INSTANCE_1" {
		t.Fatalf("ConcurrencyGroupName = %q, want PARALLEL_INSTANCE_1", rs.lastCGName)
	}
}

func TestActorCreatorWithConcurrencyGroupsEmptyNamePanics(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic for concurrency group with empty name")
		}
	}()
	creator := Actor[*Counter](&Counter{})
	creator.WithConcurrencyGroups(ConcurrencyGroup{Name: "", MaxCalls: 2, Methods: []string{"Add"}})
}

func TestActorCreatorWithConcurrencyGroupsInvalidMaxCallsPanics(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic for MaxCalls < -1")
		}
	}()
	creator := Actor[*Counter](&Counter{})
	creator.WithConcurrencyGroups(ConcurrencyGroup{Name: "cg1", MaxCalls: -2, Methods: []string{"Add"}})
}

func TestParallelActorHandleGetInstanceOutOfRangePanics(t *testing.T) {
	initMockRuntime(t, &recordingSubmitter{})
	handle, err := NewParallelActor[*Counter](&Counter{}).SetParallelism(4).Create()
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	// Valid indices must not panic.
	for _, i := range []int{0, 3} {
		func() {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("GetInstance(%d) should not panic, got %v", i, r)
				}
			}()
			_ = handle.GetInstance(i)
		}()
	}

	for _, i := range []int{-1, 4, 100} {
		i := i
		func() {
			defer func() {
				if r := recover(); r == nil {
					t.Fatalf("GetInstance(%d) should panic", i)
				}
			}()
			_ = handle.GetInstance(i)
		}()
	}
}
