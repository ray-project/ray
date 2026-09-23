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
	"bytes"
	"testing"

	"github.com/ray-project/ray/go/pkg/runtime/object"
)

// TestFunctionArgByValue_DeepCopy verifies that functionArgByValue deep-copies
// the NativeRayObject payload so the returned FunctionArg does not share
// backing arrays with the source (which may be recycled by the buffer pool).
func TestFunctionArgByValue_DeepCopy(t *testing.T) {
	nativeObj := object.NewNativeRayObject([]byte("payload-data"), []byte("RAW"))

	arg := functionArgByValue(nativeObj)

	if !arg.IsPassByValue() {
		t.Fatal("functionArgByValue should produce a pass-by-value arg")
	}
	if arg.Data == nil {
		t.Fatal("functionArgByValue should carry data")
	}
	if !bytes.Equal(arg.Data.Data, nativeObj.Data) {
		t.Errorf("data mismatch: got %q, want %q", arg.Data.Data, nativeObj.Data)
	}
	if !bytes.Equal(arg.Data.Metadata, nativeObj.Metadata) {
		t.Errorf("metadata mismatch: got %q, want %q", arg.Data.Metadata, nativeObj.Metadata)
	}

	// Mutating the source must not affect the copied arg.
	nativeObj.Data[0] = 'X'
	nativeObj.Metadata[0] = 'Y'
	if bytes.Equal(arg.Data.Data, nativeObj.Data) {
		t.Error("arg data shares backing array with source; expected a deep copy")
	}
	if bytes.Equal(arg.Data.Metadata, nativeObj.Metadata) {
		t.Error("arg metadata shares backing array with source; expected a deep copy")
	}
}
