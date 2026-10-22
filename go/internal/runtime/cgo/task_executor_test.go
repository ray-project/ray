//go:build cgo
// +build cgo

// Copyright 2025 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under
// the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS
// OF ANY KIND, either express or implied. See the License for the specific language
// governing permissions and limitations under the License.

package cgo

/*
#include <stdlib.h>
#include "src/ray/core_worker/lib/go/native_task_executor.h"
*/
import "C"
import (
	"bytes"
	"testing"
	"unsafe"

	"github.com/ray-project/ray/go/internal/runtime/serializer"
	"github.com/ray-project/ray/go/pkg/ids"
	"github.com/ray-project/ray/go/pkg/runtime/function"
	"github.com/ray-project/ray/go/pkg/runtime/object"
)

// TestConvertCFunctionArgToBase_ValueType tests conversion of value-type arguments
func TestConvertCFunctionArgToBase_ValueType(t *testing.T) {
	// Create a C.CFunctionArg with value data
	cArg := C.CFunctionArg{}
	valueData := []byte("test value data")
	valueMetadata := []byte("test metadata")

	// Set value data using CGO
	C.CFunctionArg_SetValue(
		&cArg,
		(*C.char)(unsafe.Pointer(&valueData[0])),
		C.int(len(valueData)),
		(*C.char)(unsafe.Pointer(&valueMetadata[0])),
		C.int(len(valueMetadata)),
	)

	// Convert to Go
	goArg := convertCFunctionArgToBase(cArg)

	// Verify conversion
	if !goArg.IsPassByValue() {
		t.Errorf("Expected pass-by-value, got pass-by-reference")
	}
	if goArg.Data == nil {
		t.Fatal("Expected data to be non-nil")
	}
	if string(goArg.Data.Data) != string(valueData) {
		t.Errorf("Expected data %q, got %q", string(valueData), string(goArg.Data.Data))
	}
	if string(goArg.Data.Metadata) != string(valueMetadata) {
		t.Errorf("Expected metadata %q, got %q", string(valueMetadata), string(goArg.Data.Metadata))
	}
	if goArg.ObjectRef != nil {
		t.Errorf("Expected ObjectRef to be nil, got %v", goArg.ObjectRef)
	}
	if goArg.OwnerAddress != nil {
		t.Errorf("Expected OwnerAddress to be nil, got %v", goArg.OwnerAddress)
	}
}

// TestConvertCFunctionArgToBase_ReferenceType tests conversion of reference-type arguments
func TestConvertCFunctionArgToBase_ReferenceType(t *testing.T) {
	// Create a test ObjectID (28 bytes as defined in ids.ObjectIDSize)
	objectID, err := ids.ObjectIDFromBinary(make([]byte, ids.ObjectIDSize))
	if err != nil {
		t.Fatalf("Failed to create ObjectID: %v", err)
	}

	ownerAddress := []byte("tcp://127.0.0.1:12345")

	// Create a C.CFunctionArg with reference data
	cArg := C.CFunctionArg{}
	objectIDBinary := objectID.Binary()

	C.CFunctionArg_SetReference(
		&cArg,
		(*C.char)(unsafe.Pointer(&objectIDBinary[0])),
		C.int(len(objectIDBinary)),
		(*C.char)(unsafe.Pointer(&ownerAddress[0])),
		C.int(len(ownerAddress)),
	)

	// Convert to Go
	goArg := convertCFunctionArgToBase(cArg)

	// Verify conversion
	if !goArg.IsPassByRef() {
		t.Errorf("Expected pass-by-reference, got pass-by-value")
	}
	if goArg.ObjectRef == nil {
		t.Fatal("Expected ObjectRef to be non-nil")
	}
	if goArg.ObjectRef.ObjectID != objectID {
		t.Errorf("Expected ObjectID %v, got %v", objectID, goArg.ObjectRef.ObjectID)
	}
	if string(goArg.OwnerAddress) != string(ownerAddress) {
		t.Errorf("Expected OwnerAddress %q, got %q", string(ownerAddress), string(goArg.OwnerAddress))
	}
	if goArg.Data != nil {
		t.Errorf("Expected Data to be nil, got %v", goArg.Data)
	}
}

// TestConvertCFunctionArgToBase_EmptyValue tests conversion of empty value arguments
func TestConvertCFunctionArgToBase_EmptyValue(t *testing.T) {
	cArg := C.CFunctionArg{}
	C.CFunctionArg_SetValue(&cArg, nil, 0, nil, 0)

	goArg := convertCFunctionArgToBase(cArg)

	if !goArg.IsPassByValue() {
		t.Errorf("Expected pass-by-value, got pass-by-reference")
	}
	if goArg.Data == nil {
		t.Error("Expected Data to be non-nil (empty but not nil)")
	}
	if len(goArg.Data.Data) != 0 {
		t.Errorf("Expected empty data, got %d bytes", len(goArg.Data.Data))
	}
}

// TestConvertCFunctionArgToBase_UnknownType tests conversion with unknown type (defaults to value)
func TestConvertCFunctionArgToBase_UnknownType(t *testing.T) {
	cArg := C.CFunctionArg{}
	// Don't set any type - should default to value with empty data

	goArg := convertCFunctionArgToBase(cArg)

	if !goArg.IsPassByValue() {
		t.Errorf("Expected pass-by-value for unknown type, got pass-by-reference")
	}
	if goArg.Data == nil {
		t.Error("Expected Data to be non-nil")
	}
	if len(goArg.Data.Data) != 0 {
		t.Errorf("Expected empty data, got %d bytes", len(goArg.Data.Data))
	}
}

// TestConvertGoResultToC_Success tests successful result conversion
func TestConvertGoResultToC_Success(t *testing.T) {
	results := []function.SerializedObject{
		{
			Data:     []byte("result1 data"),
			Metadata: []byte("result1 metadata"),
		},
		{
			Data:     []byte("result2 data"),
			Metadata: []byte("result2 metadata"),
		},
	}

	cArray := convertGoResultToC(results, nil, len(results))
	if cArray == nil {
		t.Fatal("Expected non-nil C array")
	}
	defer C.free(unsafe.Pointer(cArray.objects))
	defer C.free(unsafe.Pointer(cArray))

	if cArray.count != C.int(len(results)) {
		t.Errorf("Expected count %d, got %d", len(results), cArray.count)
	}

	// Verify each result
	for i, result := range results {
		obj := (*C.CSerializedObject)(unsafe.Pointer(uintptr(unsafe.Pointer(cArray.objects)) + uintptr(i)*unsafe.Sizeof(C.CSerializedObject{})))

		if obj.data_size != C.int(len(result.Data)) {
			t.Errorf("Result %d: expected data size %d, got %d", i, len(result.Data), obj.data_size)
		}
		if obj.data != nil && len(result.Data) > 0 {
			goData := C.GoBytes(unsafe.Pointer(obj.data), obj.data_size)
			if string(goData) != string(result.Data) {
				t.Errorf("Result %d: expected data %q, got %q", i, string(result.Data), string(goData))
			}
		}

		if obj.metadata_size != C.int(len(result.Metadata)) {
			t.Errorf("Result %d: expected metadata size %d, got %d", i, len(result.Metadata), obj.metadata_size)
		}
		if obj.metadata != nil && len(result.Metadata) > 0 {
			goMetadata := C.GoBytes(unsafe.Pointer(obj.metadata), obj.metadata_size)
			if string(goMetadata) != string(result.Metadata) {
				t.Errorf("Result %d: expected metadata %q, got %q", i, string(result.Metadata), string(goMetadata))
			}
		}
	}
}

// TestConvertGoResultToC_Error tests error result conversion: a non-nil error
// causes numReturns serialized error objects to be returned (not nil).
func TestConvertGoResultToC_Error(t *testing.T) {
	results := []function.SerializedObject{
		{Data: []byte("should not be returned")},
	}

	cArray := convertGoResultToC(results, function.ErrTaskExecutionFailed, len(results))
	if cArray == nil {
		t.Fatal("Expected non-nil C array with serialized error objects, got nil")
	}
	defer C.free(unsafe.Pointer(cArray.objects))
	defer C.free(unsafe.Pointer(cArray))

	if cArray.count != C.int(len(results)) {
		t.Errorf("Expected count %d, got %d", len(results), cArray.count)
	}
}

// TestConvertGoResultToC_EmptyResults tests empty result conversion: a
// successfully-executed task with zero return values must produce a non-nil
// array with count == 0 (not nil), so the C++ callback returns Status::OK
// instead of treating the nil pointer as an infrastructure failure.
func TestConvertGoResultToC_EmptyResults(t *testing.T) {
	cArray := convertGoResultToC([]function.SerializedObject{}, nil, 0)
	if cArray == nil {
		t.Fatal("Expected non-nil empty C array, got nil")
	}
	defer C.free(unsafe.Pointer(cArray))

	if cArray.count != 0 {
		t.Errorf("Expected count 0, got %d", cArray.count)
	}
	if cArray.objects != nil {
		t.Errorf("Expected nil objects pointer for empty results, got non-nil")
	}
}

// TestConvertGoResultToC_PadsVoidResultToNumReturns is the regression test for
// the cross-language void-actor-method crash: a logically-void Go method
// (e.g. CheckHealth() error) produces zero SerializedObjects while the
// cross-language caller expects numReturns == 1 (DEFAULT_ACTOR_METHOD_NUM_RETURN_VALS).
// Without padding, C++ leaves the return slot nullptr and CoreWorker::ExecuteTask
// CHECK-fails in task_receiver.cc. The padding slot must be a valid
// cross-language nil object (9-byte length header + msgpack nil) with
// MetadataTypeCrossLanguage (XLANG) metadata, which Python's _deserialize_object
// requires (a Go metadata payload would be rejected).
func TestConvertGoResultToC_PadsVoidResultToNumReturns(t *testing.T) {
	cArray := convertGoResultToC([]function.SerializedObject{}, nil, 1)
	if cArray == nil {
		t.Fatal("Expected non-nil C array, got nil")
	}
	defer C.CNativeCommon_FreeCSerializedObjectArray(cArray)

	if cArray.count != 1 {
		t.Fatalf("Expected count 1 (padded), got %d", cArray.count)
	}
	if cArray.objects == nil {
		t.Fatal("Expected non-nil objects pointer after padding, got nil")
	}
	obj := (*C.CSerializedObject)(unsafe.Pointer(cArray.objects))
	if obj.data == nil || obj.data_size <= 0 {
		t.Fatal("Expected padding slot to carry non-empty data")
	}
	// Data layout: 9-byte cross-language length header + msgpack nil (0xc0).
	paddingData := C.GoBytes(unsafe.Pointer(obj.data), obj.data_size)
	if len(paddingData) != 10 {
		t.Fatalf("Expected exactly 9-byte header + msgpack nil payload (10 bytes), got %d bytes", len(paddingData))
	}
	if paddingData[len(paddingData)-1] != 0xc0 {
		t.Errorf("Expected trailing msgpack nil byte 0xc0, got %#x", paddingData[len(paddingData)-1])
	}
	if obj.metadata == nil {
		t.Fatal("Expected padding slot to carry metadata")
	}
	metadata := C.GoBytes(unsafe.Pointer(obj.metadata), obj.metadata_size)
	if string(metadata) != string(object.MetadataTypeCrossLanguage) {
		t.Errorf("Expected padding metadata %q, got %q", object.MetadataTypeCrossLanguage, metadata)
	}
}

// TestConvertGoResultToC_PadsPartialResults tests that a short result array is
// padded up to numReturns while the real results keep their original data and
// metadata.
func TestConvertGoResultToC_PadsPartialResults(t *testing.T) {
	results := []function.SerializedObject{
		{Data: []byte("real result"), Metadata: []byte("REAL")},
	}
	cArray := convertGoResultToC(results, nil, 3)
	if cArray == nil {
		t.Fatal("Expected non-nil C array, got nil")
	}
	defer C.CNativeCommon_FreeCSerializedObjectArray(cArray)

	if cArray.count != 3 {
		t.Fatalf("Expected count 3, got %d", cArray.count)
	}
	objs := unsafe.Slice((*C.CSerializedObject)(cArray.objects), int(cArray.count))

	first := C.GoBytes(unsafe.Pointer(objs[0].data), objs[0].data_size)
	if string(first) != "real result" {
		t.Errorf("Expected first slot to keep original data %q, got %q", "real result", first)
	}
	firstMeta := C.GoBytes(unsafe.Pointer(objs[0].metadata), objs[0].metadata_size)
	if string(firstMeta) != "REAL" {
		t.Errorf("Expected first slot to keep original metadata %q, got %q", "REAL", firstMeta)
	}

	for i := 1; i < 3; i++ {
		if objs[i].data == nil || objs[i].data_size <= 0 {
			t.Fatalf("Expected padded slot %d to carry non-empty data", i)
		}
		meta := C.GoBytes(unsafe.Pointer(objs[i].metadata), objs[i].metadata_size)
		if string(meta) != string(object.MetadataTypeCrossLanguage) {
			t.Errorf("Expected padded slot %d metadata %q, got %q", i, object.MetadataTypeCrossLanguage, meta)
		}
	}
}

// TestConvertGoResultToC_NoPadWhenResultsMatchOrExceedNumReturns verifies that
// padding only kicks in when len(results) < numReturns: an exact match keeps
// count == len(results), and a numReturns of 0 (actor creation task) never pads.
func TestConvertGoResultToC_NoPadWhenResultsMatchOrExceedNumReturns(t *testing.T) {
	results := []function.SerializedObject{
		{Data: []byte("a"), Metadata: []byte("A")},
		{Data: []byte("b"), Metadata: []byte("B")},
	}

	cArray := convertGoResultToC(results, nil, 2)
	if cArray == nil {
		t.Fatal("Expected non-nil C array, got nil")
	}
	defer C.CNativeCommon_FreeCSerializedObjectArray(cArray)
	if cArray.count != 2 {
		t.Errorf("Exact match: expected count 2, got %d", cArray.count)
	}

	cArray0 := convertGoResultToC(results, nil, 0)
	if cArray0 == nil {
		t.Fatal("Expected non-nil C array, got nil")
	}
	defer C.CNativeCommon_FreeCSerializedObjectArray(cArray0)
	if cArray0.count != 2 {
		t.Errorf("numReturns=0: expected count to stay 2 (no padding), got %d", cArray0.count)
	}
}

// TestCrossLanguageNilPayloadMatchesSerializerWireFormat is the drift guard for
// crossLanguageNilPayload: the hand-built padding bytes must stay byte-identical
// to what the serializer's EncodeToBuffer produces for a nil value (compact-int
// length header + msgpack nil). If the wire format (MessagePackOffset, compact
// ints, msgpack nil byte) ever changes, this test fails and forces the padding
// payload to be updated in lockstep instead of silently drifting.
func TestCrossLanguageNilPayloadMatchesSerializerWireFormat(t *testing.T) {
	buf := make([]byte, serializer.MessagePackOffset, serializer.MessagePackOffset+1)
	msgpackSerializer := serializer.NewMsgpackSerializer()
	if err := msgpackSerializer.EncodeToBuffer(nil, &buf, false); err != nil {
		t.Fatalf("EncodeToBuffer(nil) failed: %v", err)
	}
	payload := crossLanguageNilPayload()
	if !bytes.Equal(payload, buf) {
		t.Errorf("crossLanguageNilPayload() = %x, want serializer nil wire format %x", payload, buf)
	}
}

// TestGoExecuteTask_Success tests successful task execution via GoExecuteTask export
func TestGoExecuteTask_Success(t *testing.T) {
	// Set up a mock task executor
	executorCalled := false
	SetTaskExecutor(func(
		taskType int,
		functionDescriptor function.FunctionDescriptor,
		args []function.FunctionArg,
		numReturns int,
		actorID ids.ActorID,
	) ([]function.SerializedObject, error) {
		executorCalled = true

		if taskType != 1 {
			t.Errorf("Expected taskType 1, got %d", taskType)
		}
		if functionDescriptor == nil {
			t.Error("Expected non-nil function descriptor")
		}
		if len(args) != 2 {
			t.Errorf("Expected 2 args, got %d", len(args))
		}
		if numReturns != 1 {
			t.Errorf("Expected numReturns 1, got %d", numReturns)
		}

		return []function.SerializedObject{
			{Data: []byte("test result")},
		}, nil
	})

	// Prepare function descriptor (4-element Go descriptor list)
	funcDescList := []string{"github.com/example/test", "pkg", "TestFunction", ""}
	cFuncDesc := make([]*C.char, len(funcDescList))
	for i, s := range funcDescList {
		cFuncDesc[i] = C.CString(s)
		defer C.free(unsafe.Pointer(cFuncDesc[i]))
	}

	// Prepare arguments
	cArgs := make([]C.CFunctionArg, 2)
	valueData := []byte("arg value")
	C.CFunctionArg_SetValue(&cArgs[0], (*C.char)(unsafe.Pointer(&valueData[0])), C.int(len(valueData)), nil, 0)
	C.CFunctionArg_SetValue(&cArgs[1], nil, 0, nil, 0)

	// Call GoExecuteTask
	result := GoExecuteTask(
		C.int(function.LanguageGo), // language
		1,                          // taskType
		&cFuncDesc[0],
		C.int(len(funcDescList)),
		&cArgs[0],
		C.int(len(cArgs)),
		1,   // numReturns
		nil, // actorIDData
		0,   // actorIDSize
	)

	if result == nil {
		t.Fatal("Expected non-nil result")
	}
	defer C.free(unsafe.Pointer(result.objects))
	defer C.free(unsafe.Pointer(result))

	if !executorCalled {
		t.Error("Expected task executor to be called")
	}

	if result.count != 1 {
		t.Errorf("Expected 1 result, got %d", result.count)
	}
}

// TestGoExecuteTask_ExecutorNotRegistered tests execution without registered executor
func TestGoExecuteTask_ExecutorNotRegistered(t *testing.T) {
	// Clear task executor
	SetTaskExecutor(nil)

	// Prepare minimal function descriptor (4-element Go descriptor list)
	funcDescList := []string{"github.com/example/test", "pkg", "TestFunction", ""}
	cFuncDesc := make([]*C.char, len(funcDescList))
	for i, s := range funcDescList {
		cFuncDesc[i] = C.CString(s)
		defer C.free(unsafe.Pointer(cFuncDesc[i]))
	}

	result := GoExecuteTask(
		C.int(function.LanguageGo), // language
		1,                          // taskType
		&cFuncDesc[0],
		C.int(len(funcDescList)),
		nil, // args
		0,   // argsCount
		0,   // numReturns
		nil, // actorIDData
		0,   // actorIDSize
	)

	if result != nil {
		t.Error("Expected nil result when executor not registered, got non-nil")
	}
}

// TestRegisterTaskExecutorCallback tests callback registration
func TestRegisterTaskExecutorCallback(t *testing.T) {
	// This should not panic
	RegisterTaskExecutorCallback()
	// Note: We can't easily verify the C++ side registration, but the call should succeed
}

// TestSetTaskExecutor tests setting the task executor
func TestSetTaskExecutor(t *testing.T) {
	SetTaskExecutor(func(
		taskType int,
		functionDescriptor function.FunctionDescriptor,
		args []function.FunctionArg,
		numReturns int,
		actorID ids.ActorID,
	) ([]function.SerializedObject, error) {
		return nil, nil
	})

	if taskExecutor == nil {
		t.Error("Expected taskExecutor to be set")
	}

	// Verify it's the same function by checking if it can be called
	executorCalled := false
	SetTaskExecutor(func(
		taskType int,
		functionDescriptor function.FunctionDescriptor,
		args []function.FunctionArg,
		numReturns int,
		actorID ids.ActorID,
	) ([]function.SerializedObject, error) {
		executorCalled = true
		return nil, nil
	})

	// Call the executor indirectly through GoExecuteTask setup
	// We can't directly call taskExecutor as it's not exported, but we can verify
	// that SetTaskExecutor sets it by checking the behavior in GoExecuteTask tests
	_ = executorCalled
}
