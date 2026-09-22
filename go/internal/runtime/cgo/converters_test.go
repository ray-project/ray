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
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cgo

import (
	"testing"

	"github.com/ray-project/ray/go/pkg/ids"
	"github.com/ray-project/ray/go/pkg/runtime/function"
)

// TestConvertFunctionArgToC_ValueType tests conversion of value-type arguments to C
func TestConvertFunctionArgToC_ValueType(t *testing.T) {
	arg := function.FunctionArg{
		Data: &function.SerializedData{
			Data:     []byte("test value data"),
			Metadata: []byte("test metadata"),
		},
		ObjectRef:    nil,
		OwnerAddress: nil,
	}

	cArg := ConvertFunctionArgToC(arg)

	// We can't directly inspect C.CFunctionArg fields, but we can verify
	// the conversion doesn't panic and produces a valid C struct
	// The actual verification happens in the round-trip tests
	_ = cArg
}

// TestConvertFunctionArgToC_ReferenceType tests conversion of reference-type arguments to C
func TestConvertFunctionArgToC_ReferenceType(t *testing.T) {
	// Create a test ObjectID (ObjectIDSize is 28 bytes)
	objectID, err := ids.ObjectIDFromBinary(make([]byte, 28))
	if err != nil {
		t.Fatalf("Failed to create ObjectID: %v", err)
	}

	arg := function.FunctionArg{
		Data:         nil,
		ObjectRef:    &function.ObjectRefData{ObjectID: objectID},
		OwnerAddress: []byte("tcp://127.0.0.1:12345"),
	}

	cArg := ConvertFunctionArgToC(arg)
	_ = cArg
}

// TestConvertFunctionArgToC_EmptyValue tests conversion of empty value arguments
func TestConvertFunctionArgToC_EmptyValue(t *testing.T) {
	arg := function.FunctionArg{
		Data: &function.SerializedData{
			Data:     []byte{},
			Metadata: []byte{},
		},
		ObjectRef:    nil,
		OwnerAddress: nil,
	}

	cArg := ConvertFunctionArgToC(arg)
	_ = cArg
}

// TestConvertFunctionArgToC_NilData tests conversion with nil data
func TestConvertFunctionArgToC_NilData(t *testing.T) {
	arg := function.FunctionArg{
		Data:      nil,
		ObjectRef: nil,
	}

	cArg := ConvertFunctionArgToC(arg)
	_ = cArg
}

// TestGoTriggerGC tests that GoTriggerGC can be called without panic
func TestGoTriggerGC(t *testing.T) {
	// This should not panic
	GoTriggerGC()
	// Note: We can't verify that GC actually ran, but the call should succeed
}

// TestConvertFunctionArgToC_LargeData tests conversion with large data payloads
func TestConvertFunctionArgToC_LargeData(t *testing.T) {
	largeData := make([]byte, 1024*1024) // 1MB
	for i := range largeData {
		largeData[i] = byte(i % 256)
	}

	arg := function.FunctionArg{
		Data: &function.SerializedData{
			Data:     largeData,
			Metadata: []byte("metadata"),
		},
		ObjectRef:    nil,
		OwnerAddress: nil,
	}

	cArg := ConvertFunctionArgToC(arg)
	_ = cArg
}

// TestConvertFunctionArgToC_LongOwnerAddress tests conversion with long owner address
func TestConvertFunctionArgToC_LongOwnerAddress(t *testing.T) {
	objectID, err := ids.ObjectIDFromBinary(make([]byte, 28))
	if err != nil {
		t.Fatalf("Failed to create ObjectID: %v", err)
	}

	// Create a long owner address
	longAddress := make([]byte, 256)
	for i := range longAddress {
		longAddress[i] = byte('a' + (i % 26))
	}

	arg := function.FunctionArg{
		Data:         nil,
		ObjectRef:    &function.ObjectRefData{ObjectID: objectID},
		OwnerAddress: longAddress,
	}

	cArg := ConvertFunctionArgToC(arg)
	_ = cArg
}

// TestConvertFunctionArgToC_RoundTrip_Value tests round-trip conversion (Go -> C -> Go)
func TestConvertFunctionArgToC_RoundTrip_Value(t *testing.T) {
	originalData := []byte("round trip value data")
	originalMetadata := []byte("round trip metadata")

	arg := function.FunctionArg{
		Data: &function.SerializedData{
			Data:     originalData,
			Metadata: originalMetadata,
		},
		ObjectRef:    nil,
		OwnerAddress: nil,
	}

	// Convert to C
	cArg := ConvertFunctionArgToC(arg)

	// Convert back to Go (using the callback conversion function)
	roundTripArg := convertCFunctionArgToBase(cArg)

	// Verify round-trip
	if !roundTripArg.IsPassByValue() {
		t.Errorf("Expected pass-by-value after round-trip, got pass-by-reference")
	}
	if roundTripArg.Data == nil {
		t.Fatal("Expected Data to be non-nil after round-trip")
	}
	if string(roundTripArg.Data.Data) != string(originalData) {
		t.Errorf("Expected data %q, got %q", string(originalData), string(roundTripArg.Data.Data))
	}
	if string(roundTripArg.Data.Metadata) != string(originalMetadata) {
		t.Errorf("Expected metadata %q, got %q", string(originalMetadata), string(roundTripArg.Data.Metadata))
	}
}

// TestConvertFunctionArgToC_RoundTrip_Reference tests round-trip conversion for reference
func TestConvertFunctionArgToC_RoundTrip_Reference(t *testing.T) {
	objectID, err := ids.ObjectIDFromBinary(make([]byte, 28))
	if err != nil {
		t.Fatalf("Failed to create ObjectID: %v", err)
	}

	ownerAddress := []byte("tcp://127.0.0.1:12345")

	arg := function.FunctionArg{
		Data:         nil,
		ObjectRef:    &function.ObjectRefData{ObjectID: objectID},
		OwnerAddress: ownerAddress,
	}

	// Convert to C
	cArg := ConvertFunctionArgToC(arg)

	// Convert back to Go
	roundTripArg := convertCFunctionArgToBase(cArg)

	// Verify round-trip
	if !roundTripArg.IsPassByRef() {
		t.Errorf("Expected pass-by-reference after round-trip, got pass-by-value")
	}
	if roundTripArg.ObjectRef == nil {
		t.Fatal("Expected ObjectRef to be non-nil after round-trip")
	}
	if roundTripArg.ObjectRef.ObjectID != objectID {
		t.Errorf("Expected ObjectID %v, got %v", objectID, roundTripArg.ObjectRef.ObjectID)
	}
	if string(roundTripArg.OwnerAddress) != string(ownerAddress) {
		t.Errorf("Expected OwnerAddress %q, got %q", string(ownerAddress), string(roundTripArg.OwnerAddress))
	}
}
