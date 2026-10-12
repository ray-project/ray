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

// Package cgo provides CGO bindings for Ray runtime.
// This package is organized into subdirectories by function:
//   - boundary/: CGO boundary handling (CoreWorker lifecycle)
//   - interfaces/: Interface implementations (WorkerContext, TaskExecutor, TaskSubmitter)
//   - memory/: Memory management (object allocation)
//   - callback/: Callback functions (called from C++)
//   - utils/: Shared utilities (type conversion)
package cgo

/*
#include <stdlib.h>
#include <stdint.h>
#include <stdbool.h>
#include "src/ray/core_worker/lib/go/native_runtime.h"
#include "src/ray/core_worker/lib/go/native_worker_context.h"
*/
import "C"

import (
	"unsafe"

	"github.com/ray-project/ray/go/internal/runtime/base"
	"github.com/ray-project/ray/go/pkg/ids"
)

// NativeWorkerContext implements WorkerContext for cluster mode.
type NativeWorkerContext struct{}

// NewNativeWorkerContext creates a new NativeWorkerContext instance.
func NewNativeWorkerContext() *NativeWorkerContext {
	return &NativeWorkerContext{}
}

// GetCurrentWorkerId returns the ID of the current worker.
func (c *NativeWorkerContext) GetCurrentWorkerId() ids.UniqueID {
	cData := C.CNativeWorkerContext_GetCurrentWorkerId()
	if cData == nil {
		return ids.NilUniqueID()
	}
	defer C.CNativeCommon_FreeCByteArray(cData)

	data := C.GoBytes(unsafe.Pointer(cData.data), C.int(cData.size))
	id, err := ids.UniqueIDFromBinary(data)
	if err != nil {
		return ids.NilUniqueID()
	}
	return id
}

// GetCurrentJobID returns the ID of the current job.
func (c *NativeWorkerContext) GetCurrentJobID() ids.JobID {
	cData := C.CNativeWorkerContext_GetCurrentJobID()
	if cData == nil {
		return ids.NilJobID()
	}
	defer C.CNativeCommon_FreeCByteArray(cData)

	data := C.GoBytes(unsafe.Pointer(cData.data), C.int(cData.size))
	id, err := ids.JobIDFromBinary(data)
	if err != nil {
		return ids.NilJobID()
	}
	return id
}

// GetCurrentActorID returns the ID of the current actor.
func (c *NativeWorkerContext) GetCurrentActorID() ids.ActorID {
	cData := C.CNativeWorkerContext_GetCurrentActorID()
	if cData == nil {
		return ids.NilActorID()
	}
	defer C.CNativeCommon_FreeCByteArray(cData)

	data := C.GoBytes(unsafe.Pointer(cData.data), C.int(cData.size))
	id, err := ids.ActorIDFromBinary(data)
	if err != nil {
		return ids.NilActorID()
	}
	return id
}

// GetCurrentTaskType returns the type of the current task.
func (c *NativeWorkerContext) GetCurrentTaskType() base.TaskType {
	cType := C.CNativeWorkerContext_GetCurrentTaskType()
	return base.TaskType(cType)
}

// GetCurrentTaskID returns the ID of the current task.
func (c *NativeWorkerContext) GetCurrentTaskID() ids.TaskID {
	cData := C.CNativeWorkerContext_GetCurrentTaskID()
	if cData == nil {
		return ids.NilTaskID()
	}
	defer C.CNativeCommon_FreeCByteArray(cData)

	data := C.GoBytes(unsafe.Pointer(cData.data), C.int(cData.size))
	id, err := ids.TaskIDFromBinary(data)
	if err != nil {
		return ids.NilTaskID()
	}
	return id
}

// GetRpcAddress returns the RPC address bytes of the current worker.
func (c *NativeWorkerContext) GetRpcAddress() []byte {
	cData := C.CNativeWorkerContext_GetRpcAddress()
	if cData == nil {
		return nil
	}
	defer C.CNativeCommon_FreeCByteArray(cData)

	return C.GoBytes(unsafe.Pointer(cData.data), C.int(cData.size))
}

// GetSerializedRuntimeEnv returns the serialized runtime environment.
//
// The C++ side returns an owned CByteArray (see native_worker_context.cc), so
// the buffer is read here and released with CNativeCommon_FreeCByteArray. The
// value is copied out as a Go string before the C memory is freed.
func (c *NativeWorkerContext) GetSerializedRuntimeEnv() string {
	cData := C.CNativeWorkerContext_GetSerializedRuntimeEnv()
	if cData == nil {
		return ""
	}
	defer C.CNativeCommon_FreeCByteArray(cData)

	return string(C.GoBytes(unsafe.Pointer(cData.data), C.int(cData.size)))
}

// GetNamespace returns the current namespace.
//
// The C++ side returns an owned CByteArray (see native_worker_context.cc), so
// the buffer is read here and released with CNativeCommon_FreeCByteArray.
func (c *NativeWorkerContext) GetNamespace() string {
	cData := C.CNativeWorkerContext_GetNamespace()
	if cData == nil {
		return ""
	}
	defer C.CNativeCommon_FreeCByteArray(cData)

	return string(C.GoBytes(unsafe.Pointer(cData.data), C.int(cData.size)))
}

// GetCurrentNodeID returns the ID of the current node.
func (c *NativeWorkerContext) GetCurrentNodeID() ids.NodeID {
	cData := C.CNativeWorkerContext_GetCurrentNodeID()
	if cData == nil {
		return ids.NilNodeID()
	}
	defer C.CNativeCommon_FreeCByteArray(cData)

	data := C.GoBytes(unsafe.Pointer(cData.data), C.int(cData.size))
	id, err := ids.NodeIDFromBinary(data)
	if err != nil {
		return ids.NilNodeID()
	}
	return id
}
