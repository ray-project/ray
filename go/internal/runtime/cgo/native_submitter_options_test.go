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

//go:build cgo
// +build cgo

package cgo

/*
#include <stdlib.h>
*/
import "C"
import (
	"testing"
	"unsafe"

	"github.com/ray-project/ray/go/pkg/runtime/submitter"
	"github.com/stretchr/testify/assert"
)

// TestConvertTaskOptionsToC_NameAndConcurrencyGroup verifies the name and
// concurrency_group_name fields are converted and freed correctly.
func TestConvertTaskOptionsToC_NameAndConcurrencyGroup(t *testing.T) {
	opts := &submitter.TaskOptions{
		Name:                 "task-1",
		ConcurrencyGroupName: "cg1",
	}
	result := convertTaskOptionsToC(opts)
	assert.NotNil(t, result)
	assert.NotNil(t, result.name)
	assert.Equal(t, "task-1", C.GoString(result.name))
	assert.NotNil(t, result.concurrency_group_name)
	assert.Equal(t, "cg1", C.GoString(result.concurrency_group_name))
	freeTaskOptions(result)
}

// TestConvertTaskOptionsToC_EmptyStringsNotAllocated verifies empty name and
// group name produce NULL C pointers, so an empty name/group does not allocate
// a C string.
func TestConvertTaskOptionsToC_EmptyStringsNotAllocated(t *testing.T) {
	opts := &submitter.TaskOptions{
		Name:                 "",
		ConcurrencyGroupName: "",
	}
	result := convertTaskOptionsToC(opts)
	assert.NotNil(t, result)
	assert.Nil(t, result.name, "Empty name must not allocate a C string")
	assert.Nil(t, result.concurrency_group_name, "Empty group name must not allocate a C string")
	freeTaskOptions(result)
}

// TestConvertTaskOptionsToC_AllFieldsCombined verifies the new fields coexist
// with the pre-existing conversion fields.
func TestConvertTaskOptionsToC_AllFieldsCombined(t *testing.T) {
	opts := &submitter.TaskOptions{
		Name:                 "my-task",
		ConcurrencyGroupName: "cg2",
		RuntimeEnv:           `{"pip": ["numpy"]}`,
	}
	result := convertTaskOptionsToC(opts)
	assert.NotNil(t, result)
	assert.Equal(t, "my-task", C.GoString(result.name))
	assert.Equal(t, "cg2", C.GoString(result.concurrency_group_name))
	assert.Equal(t, `{"pip": ["numpy"]}`, C.GoString(result.runtime_env))
	freeTaskOptions(result)
}

// TestConvertActorCreationOptionsToC_LifetimeAsyncMaxPendingCalls verifies the
// lifetime/isAsync/maxPendingCalls fields are forwarded into the C struct: a
// detached actor sets is_detached=1, an async actor sets is_asyncio=1, and
// max_pending_calls carries the exact value (defaulting to -1 unlimited).
func TestConvertActorCreationOptionsToC_LifetimeAsyncMaxPendingCalls(t *testing.T) {
	opts := &submitter.ActorCreationOptions{
		Lifetime:        submitter.ActorLifetimeDetached,
		IsAsync:         true,
		MaxPendingCalls: 42,
	}
	result := convertActorCreationOptionsToC(opts, nil)
	assert.NotNil(t, result)
	assert.Equal(t, C.int(1), result.is_detached)
	assert.Equal(t, C.int(1), result.is_asyncio)
	assert.Equal(t, C.int(42), result.max_pending_calls)
	freeActorCreationOptions(result)

	// A non-detached, synchronous actor keeps the zero defaults.
	opts = &submitter.ActorCreationOptions{}
	result = convertActorCreationOptionsToC(opts, nil)
	assert.NotNil(t, result)
	assert.Equal(t, C.int(0), result.is_detached)
	assert.Equal(t, C.int(0), result.is_asyncio)
	// The caller (CreateActor) normalizes a zero MaxPendingCalls to -1 before
	// conversion; a raw zero struct therefore maps to 0 here, matching the
	// normalization boundary.
	assert.Equal(t, C.int(0), result.max_pending_calls)
	freeActorCreationOptions(result)
}

// TestConvertActorCreationOptionsToC_ConcurrencyGroups verifies the
// concurrency-group flat arrays are allocated with the expected layout and that
// freeActorCreationOptions releases them without error. The module/package/
// actorType prefix of each method descriptor comes from actorDesc[0..2], the
// method name from each group's Methods entry.
func TestConvertActorCreationOptionsToC_ConcurrencyGroups(t *testing.T) {
	opts := &submitter.ActorCreationOptions{
		MaxConcurrency: 4,
		ConcurrencyGroups: []submitter.ConcurrencyGroup{
			{
				Name:     "cg1",
				MaxCalls: 2,
				Methods:  []string{"Add", "Sub"},
			},
			{
				Name:     "cg2",
				MaxCalls: 0, // resolves to the actor's max concurrency (4)
				Methods:  nil,
			},
		},
	}
	actorDesc := []string{"module", "pkg", "actorType"}
	result := convertActorCreationOptionsToC(opts, actorDesc)
	assert.NotNil(t, result)
	assert.Equal(t, C.int(2), result.cg_count)

	names := (*[2]*C.char)(unsafe.Pointer(result.cg_names))[:2:2]
	maxConc := (*[2]C.int)(unsafe.Pointer(result.cg_max_concurrency))[:2:2]
	fdCounts := (*[2]C.int)(unsafe.Pointer(result.cg_fd_counts))[:2:2]

	// Group 0: explicit MaxCalls=2 and two method descriptors.
	assert.Equal(t, "cg1", C.GoString(names[0]))
	assert.Equal(t, C.int(2), maxConc[0])
	assert.Equal(t, C.int(2), fdCounts[0])

	// Group 1: MaxCalls=0 resolves to the actor's max concurrency, no methods.
	assert.Equal(t, "cg2", C.GoString(names[1]))
	assert.Equal(t, C.int(4), maxConc[1])
	assert.Equal(t, C.int(0), fdCounts[1])

	fdArrays := (*[2]**C.char)(unsafe.Pointer(result.cg_fds))[:2:2]
	// A group with no methods has no descriptor array allocated.
	assert.Nil(t, fdArrays[1])

	// Verify group 0's first method descriptor: a 4-element array
	// [module, package, actorType, methodName].
	assert.NotNil(t, fdArrays[0])
	methodPtrs := (*[2]*C.char)(unsafe.Pointer(fdArrays[0]))[:2:2]
	firstFD := (*[4]*C.char)(unsafe.Pointer(methodPtrs[0]))[:4:4]
	assert.Equal(t, "module", C.GoString(firstFD[0]))
	assert.Equal(t, "pkg", C.GoString(firstFD[1]))
	assert.Equal(t, "actorType", C.GoString(firstFD[2]))
	assert.Equal(t, "Add", C.GoString(firstFD[3]))

	secondFD := (*[4]*C.char)(unsafe.Pointer(methodPtrs[1]))[:4:4]
	assert.Equal(t, "Sub", C.GoString(secondFD[3]))

	freeActorCreationOptions(result)
}
