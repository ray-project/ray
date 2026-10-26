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
	"encoding/json"
	"strconv"
	"testing"

	protogo "github.com/ray-project/ray/go/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"
	goproto "google.golang.org/protobuf/proto"
)

// TestMarshalTaskExecutionErrorData_WireForm pins the cross-language task-error
// object wire form produced by MarshalTaskExecutionErrorData:
//   - Metadata: the decimal rpc::ErrorType for TASK_EXECUTION_EXCEPTION ("3").
//   - Data: a 9-byte msgpack compact-int length header, then a msgpack bin
//     wrapping a serialized rpc::RayException whose language is GO and whose
//     formatted_exception_string carries the human-readable message.
//
// The Go driver reads this object through ErrorObjectFromNative with the
// dual-dialect reader: a Go-written payload (RayException PB, Language: GO)
// is accepted first, and a C++-written payload (RayErrorInfo) falls through
// to extractErrorInfoMessage as the fallback dialect.
func TestMarshalTaskExecutionErrorData_WireForm(t *testing.T) {
	data, err := MarshalTaskExecutionErrorData("boom: the real cause")
	require.NoError(t, err)
	// Metadata is the decimal rpc::ErrorType for TASK_EXECUTION_EXCEPTION.
	assert.Equal(t, []byte(strconv.Itoa(RpcErrorTypeTaskExecutionException)), TaskExecutionErrorMetadata())
	// Strip the 9-byte length header, decode the msgpack bin, parse the PB.
	require.Greater(t, len(data), 9)
	var pbBytes []byte
	require.NoError(t, msgpack.Unmarshal(data[9:], &pbBytes))
	var exc protogo.RayException
	require.NoError(t, goproto.Unmarshal(pbBytes, &exc))
	assert.Equal(t, protogo.Language_GO, exc.GetLanguage())
	assert.Equal(t, "boom: the real cause", exc.GetFormattedExceptionString())
}

// TestMarshalTaskExecutionErrorData_HeaderFormat pins the exact byte layout of
// the data payload's 9-byte header: a msgpack compact int holding the wrapped
// section length, zero-padded to 9 bytes - the format encodeLengthHeader
// produces in go/internal/runtime/serializer/msgpack_serializer.go. This is a
// self-pin (it re-encodes with its own encoder); the cross-package drift guard
// against the canonical helper lives in that package's test
// (TestErrorLengthHeaderMatchesCanonical in
// go/internal/runtime/serializer/msgpack_serializer_test.go).
func TestMarshalTaskExecutionErrorData_HeaderFormat(t *testing.T) {
	data, err := MarshalTaskExecutionErrorData("drift probe")
	require.NoError(t, err)
	section, err := msgpack.Marshal(func() []byte {
		pb, _ := goproto.Marshal(&protogo.RayException{
			Language:                 protogo.Language_GO,
			FormattedExceptionString: "drift probe",
		})
		return pb
	}())
	require.NoError(t, err)
	// header(len(section)) + section == data  =>  reconstruct and compare
	var headerBuf bytes.Buffer
	enc := msgpack.NewEncoder(&headerBuf)
	enc.UseCompactInts(true)
	require.NoError(t, enc.EncodeInt(int64(len(section))))
	header := make([]byte, 9)
	copy(header, headerBuf.Bytes())
	assert.Equal(t, append(header, section...), data)
}

// TestErrorObjectFromNative_GoWireDialect verifies ErrorObjectFromNative reads
// a task-execution error object produced by MarshalTaskExecutionErrorData - the
// Go wire dialect (numeric metadata + 9-byte-header msgpack bin wrapping a
// RayException PB).
func TestErrorObjectFromNative_GoWireDialect(t *testing.T) {
	data, err := MarshalTaskExecutionErrorData("go wire: real cause")
	require.NoError(t, err)
	exc, ok := ErrorObjectFromNative(&NativeRayObject{Data: data, Metadata: TaskExecutionErrorMetadata()})
	require.True(t, ok)
	assert.Contains(t, exc.Error(), "go wire: real cause")
}

// TestErrorObjectFromNative_CppDialectStillReadable verifies ErrorObjectFromNative
// still reads the C++ error-object dialect: metadata is the decimal rpc::ErrorType
// and data is a msgpack-wrapped rpc::RayErrorInfo. C++ CreateErrorObject emits
// [msgpack int][msgpack bin][RayErrorInfo PB]; the layout matches what
// extractErrorInfoMessage parses (PB field 5 is error_message; see
// exception.go's extractErrorInfoMessage comment).
func TestErrorObjectFromNative_CppDialectStillReadable(t *testing.T) {
	info := &protogo.RayErrorInfo{ErrorMessage: "cpp dialect message"}
	pbBytes, err := goproto.Marshal(info)
	require.NoError(t, err)
	var section bytes.Buffer
	secEnc := msgpack.NewEncoder(&section)
	require.NoError(t, secEnc.EncodeInt(int64(len(pbBytes))))
	require.NoError(t, secEnc.EncodeBytes(pbBytes))
	exc, ok := ErrorObjectFromNative(&NativeRayObject{Data: section.Bytes(), Metadata: []byte(strconv.Itoa(RpcErrorTypeTaskExecutionException))})
	require.True(t, ok)
	assert.Contains(t, exc.Error(), "cpp dialect message")
}

// TestErrorObjectFromNative_NonGoPayloadNotReadAsGoDialect pins the language
// gate: a payload that structurally matches the Go dialect (9-byte length
// header + msgpack bin) but wraps a C++ rpc::RayErrorInfo must not be parsed as
// a Go rpc::RayException. RayErrorInfo has no field 1, so without the gate the
// mis-parse exposes the field-3 runtime-env sub-message bytes as the
// formatted_exception_string; the reader must reject it and fall back to the
// RayErrorInfo path instead of returning raw protobuf garbage as the message.
func TestErrorObjectFromNative_NonGoPayloadNotReadAsGoDialect(t *testing.T) {
	info := &protogo.RayErrorInfo{
		Error: &protogo.RayErrorInfo_RuntimeEnvSetupFailedError{
			RuntimeEnvSetupFailedError: &protogo.RuntimeEnvFailedContext{
				ErrorMessage: "runtime-env-submessage-bytes",
			},
		},
	}
	pbBytes, err := goproto.Marshal(info)
	require.NoError(t, err)
	section, err := msgpack.Marshal(pbBytes)
	require.NoError(t, err)
	header := make([]byte, messagePackOffset)
	var headerBuf bytes.Buffer
	enc := msgpack.NewEncoder(&headerBuf)
	enc.UseCompactInts(true)
	require.NoError(t, enc.EncodeInt(int64(len(section))))
	copy(header, headerBuf.Bytes())
	data := append(header, section...)

	exc, ok := ErrorObjectFromNative(&NativeRayObject{Data: data, Metadata: TaskExecutionErrorMetadata()})
	require.True(t, ok)
	// The gate rejects the Go dialect; the field-3 bytes never surface as the
	// message and the RayErrorInfo fallback is used instead.
	assert.NotContains(t, exc.Error(), "runtime-env-submessage-bytes")
	assert.Contains(t, exc.Error(), "task execution failed")
}

// TestErrorObjectFromNative_OversizedLengthHeaderNoPanic pins the int64-domain
// bounds check: a header encoding 1<<33 followed by a short payload must be
// rejected before any int narrowing, so the reader falls back to the
// RayErrorInfo path instead of panicking on platforms where int is 32-bit.
func TestErrorObjectFromNative_OversizedLengthHeaderNoPanic(t *testing.T) {
	header := make([]byte, messagePackOffset)
	var headerBuf bytes.Buffer
	enc := msgpack.NewEncoder(&headerBuf)
	enc.UseCompactInts(true)
	require.NoError(t, enc.EncodeUint(uint64(1)<<33))
	copy(header, headerBuf.Bytes())
	data := append(header, []byte("tiny payload")...)

	exc, ok := ErrorObjectFromNative(&NativeRayObject{Data: data, Metadata: TaskExecutionErrorMetadata()})
	require.True(t, ok)
	assert.Contains(t, exc.Error(), "task execution failed")
}

// TestErrorObjectFromNative_JsonCompatBranchRetained verifies the legacy Go
// worker JSON convention ({error_type, error_message} with {"type":"error"}
// metadata) keeps working alongside the new numeric wire form.
func TestErrorObjectFromNative_JsonCompatBranchRetained(t *testing.T) {
	payload, err := json.Marshal(map[string]string{"error_type": "RayTaskException", "error_message": "legacy json"})
	require.NoError(t, err)
	exc, ok := ErrorObjectFromNative(&NativeRayObject{Data: payload, Metadata: []byte(`{"type":"error"}`)})
	require.True(t, ok)
	assert.Contains(t, exc.Error(), "legacy json")
}
