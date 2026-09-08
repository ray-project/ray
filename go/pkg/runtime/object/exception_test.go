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

package object

import (
	"bytes"
	"fmt"
	"strings"
	"testing"

	"github.com/vmihailenco/msgpack/v5"
)

// TestParseErrorType tests metadata-based error object detection per the C++ RayObject::IsException
// convention.
func TestParseErrorType(t *testing.T) {
	tests := []struct {
		metadata    []byte
		wantType    int
		wantIsError bool
	}{
		{[]byte("0"), RpcErrorTypeWorkerDied, true},
		{[]byte("1"), RpcErrorTypeActorDied, true},
		{[]byte("3"), RpcErrorTypeTaskExecutionException, true},
		{[]byte("33"), RpcErrorTypeWorkerStartupFailed, true},
		{[]byte("34"), 34, true},
		// Forward-compatible: any canonical 1-2 digit decimal is treated as an error type
		// even when this tree's common.proto does not define it (e.g. value 2 here); the
		// readable name falls back to UNKNOWN_%d in errorTypeName.
		{[]byte("2"), 2, true},
		{[]byte("35"), 35, true},
		{[]byte("99"), 99, true},
		// Non-canonical decimal representations do not match the C++ string comparison.
		{[]byte("03"), 0, false},
		// Normal object metadata is never an error.
		{[]byte("GO"), 0, false},
		{[]byte("RAW"), 0, false},
		{[]byte("XLANG"), 0, false},
		{[]byte("PYTHON"), 0, false},
		{[]byte("ACTOR_HANDLE"), 0, false},
		{[]byte("TASK_EXECUTION_EXCEPTION"), 0, false},
		// Empty or missing metadata is not an error.
		{[]byte{}, 0, false},
		{nil, 0, false},
	}

	for _, tc := range tests {
		gotType, gotIsError := ParseErrorType(tc.metadata)
		if gotIsError != tc.wantIsError {
			t.Errorf("ParseErrorType(%q) isError = %v, want %v", tc.metadata, gotIsError, tc.wantIsError)
		}
		if gotIsError && gotType != tc.wantType {
			t.Errorf("ParseErrorType(%q) type = %d, want %d", tc.metadata, gotType, tc.wantType)
		}
	}
}

// TestErrorObjectFromNative_NumericMetadata tests that numeric error-type metadata produces a
// readable exception.
func TestErrorObjectFromNative_NumericMetadata(t *testing.T) {
	// WORKER_STARTUP_FAILED (33): no dedicated class, message must include the error type name.
	nativeObj := &NativeRayObject{Data: nil, Metadata: []byte("33")}
	exc, ok := ErrorObjectFromNative(nativeObj)
	if !ok {
		t.Fatal("expected WORKER_STARTUP_FAILED to be detected as an error object")
	}
	if exc.ErrorCode() != RpcErrorTypeWorkerStartupFailed {
		t.Errorf("error code = %d, want %d", exc.ErrorCode(), RpcErrorTypeWorkerStartupFailed)
	}
	if !strings.Contains(exc.Error(), "WORKER_STARTUP_FAILED") {
		t.Errorf("error message should mention WORKER_STARTUP_FAILED, got: %s", exc.Error())
	}

	// WORKER_DIED (0): maps to RayWorkerException.
	nativeObj = &NativeRayObject{Data: nil, Metadata: []byte("0")}
	exc, ok = ErrorObjectFromNative(nativeObj)
	if !ok {
		t.Fatal("expected WORKER_DIED to be detected as an error object")
	}
	if _, isWorker := exc.(*RayWorkerException); !isWorker {
		t.Errorf("expected *RayWorkerException, got %T", exc)
	}
	if exc.ErrorCode() != ErrorCodeWorkerDied {
		t.Errorf("error code = %d, want %d", exc.ErrorCode(), ErrorCodeWorkerDied)
	}

	// ACTOR_DIED (1): maps to RayIDException for actors.
	nativeObj = &NativeRayObject{Data: nil, Metadata: []byte("1")}
	exc, ok = ErrorObjectFromNative(nativeObj)
	if !ok {
		t.Fatal("expected ACTOR_DIED to be detected as an error object")
	}
	if exc.ErrorCode() != ErrorCodeActorDied {
		t.Errorf("error code = %d, want %d", exc.ErrorCode(), ErrorCodeActorDied)
	}

	// TASK_EXECUTION_EXCEPTION (3) with a serialized RayErrorInfo message: the underlying cause
	// must be surfaced.
	data := buildErrorInfoData("worker failed to start: plugin version mismatch")
	nativeObj = &NativeRayObject{Data: data, Metadata: []byte("3")}
	exc, ok = ErrorObjectFromNative(nativeObj)
	if !ok {
		t.Fatal("expected TASK_EXECUTION_EXCEPTION to be detected as an error object")
	}
	if exc.ErrorCode() != ErrorCodeTaskExecutionException {
		t.Errorf("error code = %d, want %d", exc.ErrorCode(), ErrorCodeTaskExecutionException)
	}
	if !strings.Contains(exc.Error(), "plugin version mismatch") {
		t.Errorf("error message should carry the RayErrorInfo message, got: %s", exc.Error())
	}

	// ACTOR_DIED (1) with a serialized RayErrorInfo message: the message is surfaced while
	// ErrorCode() stays the Go ErrorCodeActorDied constant, consistent with the no-message path.
	data = buildErrorInfoData("actor died: oom")
	nativeObj = &NativeRayObject{Data: data, Metadata: []byte("1")}
	exc, ok = ErrorObjectFromNative(nativeObj)
	if !ok {
		t.Fatal("expected ACTOR_DIED with message to be detected as an error object")
	}
	if exc.ErrorCode() != ErrorCodeActorDied {
		t.Errorf("error code = %d, want %d", exc.ErrorCode(), ErrorCodeActorDied)
	}
	if !strings.Contains(exc.Error(), "actor died: oom") {
		t.Errorf("error message should carry the RayErrorInfo message, got: %s", exc.Error())
	}
}

// TestErrorObjectFromNative_GoWorkerError tests the Go worker's task-execution error object
// convention: metadata {"type":"error"} with a JSON error payload produced by
// convertGoResultToC in go/internal/runtime/cgo/task_executor.go.
func TestErrorObjectFromNative_GoWorkerError(t *testing.T) {
	data := []byte(`{"error_type":"RayTaskException","error_message":"Task execution panicked: PanicDiv: divide by zero","cause_message":"panic: PanicDiv: divide by zero"}`)
	nativeObj := &NativeRayObject{Data: data, Metadata: []byte(`{"type":"error"}`)}
	exc, ok := ErrorObjectFromNative(nativeObj)
	if !ok {
		t.Fatal("expected a Go worker error object to be detected as an error object")
	}
	if exc.ErrorCode() != ErrorCodeTaskExecutionException {
		t.Errorf("error code = %d, want %d", exc.ErrorCode(), ErrorCodeTaskExecutionException)
	}
	if !strings.Contains(exc.Error(), "PanicDiv") {
		t.Errorf("error message should carry the panic message, got: %s", exc.Error())
	}

	// An unparseable payload still yields a readable exception carrying the raw data, so the
	// driver never sees a generic deserialization failure for a task-execution error.
	nativeObj = &NativeRayObject{Data: []byte("not-json"), Metadata: []byte(`{"type":"error"}`)}
	exc, ok = ErrorObjectFromNative(nativeObj)
	if !ok {
		t.Fatal("expected a Go worker error object with unparseable data to be detected")
	}
	if !strings.Contains(exc.Error(), "not-json") {
		t.Errorf("error message should carry the raw payload, got: %s", exc.Error())
	}
}

// TestErrorObjectFromNative_GoLocalMode tests the Go local-mode exception object convention:
// MetadataTypeTaskExecutionException metadata with msgpack-encoded ExceptionData.
func TestErrorObjectFromNative_GoLocalMode(t *testing.T) {
	serializer := &RayExceptionSerializer{}
	data, err := serializer.ToBytes(NewRayTaskExecutionException("task-1", fmt.Errorf("boom"), ""))
	if err != nil {
		t.Fatalf("ToBytes failed: %v", err)
	}
	nativeObj := &NativeRayObject{Data: data, Metadata: []byte(MetadataTypeTaskExecutionException)}
	exc, ok := ErrorObjectFromNative(nativeObj)
	if !ok {
		t.Fatal("expected TASK_EXECUTION_EXCEPTION metadata to be detected as an error object")
	}
	if exc.ErrorCode() != ErrorCodeTaskExecutionException {
		t.Errorf("error code = %d, want %d", exc.ErrorCode(), ErrorCodeTaskExecutionException)
	}
	if !strings.Contains(exc.Error(), "boom") {
		t.Errorf("error message should carry the exception cause, got: %s", exc.Error())
	}
}

// TestErrorObjectFromNative_NotError tests that normal objects are not treated as error objects.
func TestErrorObjectFromNative_NotError(t *testing.T) {
	for _, metadata := range [][]byte{
		[]byte("GO"),
		[]byte("RAW"),
		[]byte("XLANG"),
		[]byte("PYTHON"),
		[]byte("ACTOR_HANDLE"),
		nil,
		{},
	} {
		nativeObj := &NativeRayObject{Data: []byte{0x01}, Metadata: metadata}
		if exc, ok := ErrorObjectFromNative(nativeObj); ok {
			t.Errorf("metadata %q should not be an error object, got exception: %v", metadata, exc)
		}
	}

	// A nil object is never an error object.
	if _, ok := ErrorObjectFromNative(nil); ok {
		t.Error("nil NativeRayObject should not be an error object")
	}
}

// TestExtractErrorInfoMessage tests parsing the msgpack-wrapped rpc::RayErrorInfo protobuf used
// by the C++ side to carry error messages.
func TestExtractErrorInfoMessage(t *testing.T) {
	if got := extractErrorInfoMessage(nil); got != "" {
		t.Errorf("extractErrorInfoMessage(nil) = %q, want empty", got)
	}

	message := "worker failed to start: plugin version mismatch"
	got := extractErrorInfoMessage(buildErrorInfoData(message))
	if got != message {
		t.Errorf("extractErrorInfoMessage = %q, want %q", got, message)
	}

	// Garbage data must not panic and must yield an empty message.
	got = extractErrorInfoMessage([]byte{0x01, 0x02, 0x03})
	if got != "" {
		t.Errorf("extractErrorInfoMessage(garbage) = %q, want empty", got)
	}
}

// buildErrorInfoData builds the data payload of a C++ error object: a minimal rpc::RayErrorInfo
// protobuf with only error_message (field 5) set, wrapped as [msgpack int][msgpack bin] exactly
// like C++ MakeSerializedErrorBuffer in src/ray/common/ray_object.cc.
func buildErrorInfoData(message string) []byte {
	var pb []byte
	// Field 5 (error_message), wire type 2 (length-delimited).
	pb = append(pb, 0x2a)
	pb = append(pb, byte(len(message)))
	pb = append(pb, message...)

	bin, err := msgpack.Marshal(pb)
	if err != nil {
		return nil
	}

	buf := new(bytes.Buffer)
	enc := msgpack.NewEncoder(buf)
	if err := enc.EncodeInt64(int64(len(bin))); err != nil {
		return nil
	}
	buf.Write(bin)
	return buf.Bytes()
}
