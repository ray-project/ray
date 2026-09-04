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
// This file is part of the cgo package's utils subdirectory.
package cgo

/*
#include <stdlib.h>
#include <stdint.h>
#include <stdbool.h>
#include "src/ray/core_worker/lib/go/native_task_submitter.h"
#include "src/ray/core_worker/lib/go/native_task_executor.h"
#include "src/ray/core_worker/lib/go/native_runtime.h"
*/
import "C"
import (
	"fmt"
	"sort"
	"strings"
	"unsafe"

	"github.com/ray-project/ray/go/pkg/runtime/function"
)

// ConvertFunctionArgToC converts a Go FunctionArg to C.CFunctionArg.
// This is a shared utility function used by both executor and submitter.
func ConvertFunctionArgToC(arg function.FunctionArg) C.CFunctionArg {
	var cArg C.CFunctionArg

	if arg.IsPassByValue() && arg.Data != nil {
		var dataPtr *C.char
		var dataLen C.int
		var metadataPtr *C.char
		var metadataLen C.int

		if len(arg.Data.Data) > 0 {
			dataPtr = (*C.char)(unsafe.Pointer(&arg.Data.Data[0]))
			dataLen = C.int(len(arg.Data.Data))
		}
		if len(arg.Data.Metadata) > 0 {
			metadataPtr = (*C.char)(unsafe.Pointer(&arg.Data.Metadata[0]))
			metadataLen = C.int(len(arg.Data.Metadata))
		}
		C.CFunctionArg_SetValue(&cArg, dataPtr, dataLen, metadataPtr, metadataLen)
	} else if arg.IsPassByRef() && arg.ObjectRef != nil {
		objectIDBinary := arg.ObjectRef.ObjectID.Binary()
		var objectIDPtr *C.char
		var objectIDLen C.int
		var ownerAddrPtr *C.char
		var ownerAddrLen C.int

		if len(objectIDBinary) > 0 {
			objectIDPtr = (*C.char)(unsafe.Pointer(&objectIDBinary[0]))
			objectIDLen = C.int(len(objectIDBinary))
		}
		if len(arg.OwnerAddress) > 0 {
			ownerAddrPtr = (*C.char)(unsafe.Pointer(&arg.OwnerAddress[0]))
			ownerAddrLen = C.int(len(arg.OwnerAddress))
		}
		C.CFunctionArg_SetReference(&cArg, objectIDPtr, objectIDLen, ownerAddrPtr, ownerAddrLen)
	}

	return cArg
}

// ResourcesToString converts resources map to string format "CPU:2.0,GPU:1.0".
// Keys are sorted for deterministic output.
// This is a shared utility function used by multiple packages.
func ResourcesToString(resources map[string]float64) string {
	if len(resources) == 0 {
		return ""
	}

	// Sort keys for deterministic output
	keys := make([]string, 0, len(resources))
	for k := range resources {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var builder strings.Builder
	for i, key := range keys {
		if i > 0 {
			builder.WriteString(",")
		}
		// Format with one decimal place for consistency
		builder.WriteString(fmt.Sprintf("%s:%.1f", key, resources[key]))
	}
	return builder.String()
}

// CStringSlice converts a Go string slice to C char** array.
// Returns the array and a cleanup function to free all strings.
// Usage: cArray, free := CStringSlice(strs); defer free()
// This is a shared utility function used by both executor and submitter.
func CStringSlice(strs []string) ([]*C.char, func()) {
	cArray := make([]*C.char, len(strs))
	for i, s := range strs {
		cArray[i] = C.CString(s)
	}
	return cArray, func() {
		for _, s := range cArray {
			C.free(unsafe.Pointer(s))
		}
	}
}

// ToCString converts a Go string to a C string and returns a cleanup function.
// This is a shared utility function used throughout the cgo package.
// Usage: cStr, free := ToCString(goStr); defer free()
func ToCString(s string) (*C.char, func()) {
	c := C.CString(s)
	return c, func() { C.free(unsafe.Pointer(c)) }
}
