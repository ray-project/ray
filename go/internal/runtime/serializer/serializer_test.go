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

package serializer

import (
	"bytes"
	"encoding/json"
	"testing"
)

// compareJSON compares two values by marshaling them to JSON and comparing the strings.
// This is useful for comparing interface{} values that may have different concrete types
// but represent the same data (e.g., int vs int64, struct vs map).
func compareJSON(t *testing.T, actual, expected interface{}) bool {
	t.Helper()
	actualJSON, err := json.Marshal(actual)
	if err != nil {
		t.Errorf("Failed to marshal actual value: %v", err)
		return false
	}
	expectedJSON, err := json.Marshal(expected)
	if err != nil {
		t.Errorf("Failed to marshal expected value: %v", err)
		return false
	}
	if string(actualJSON) != string(expectedJSON) {
		t.Errorf("JSON mismatch:\nactual  = %s\nexpected = %s", string(actualJSON), string(expectedJSON))
		return false
	}
	return true
}

// TestMsgpackSerializer_SerializePrimitive tests serializing primitive types.
func TestMsgpackSerializer_SerializePrimitive(t *testing.T) {
	s := NewMsgpackSerializer()

	tests := []struct {
		name  string
		value interface{}
	}{
		{"int", int(42)},
		{"int64", int64(42)},
		{"string", "hello"},
		{"float64", 3.14},
		{"bool", true},
		{"[]byte", []byte("hello")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data, err := s.Encode(tt.value)
			if err != nil {
				t.Fatalf("Encode() error = %v", err)
			}

			var result interface{}
			err = s.Decode(data, &result)
			if err != nil {
				t.Fatalf("Decode() error = %v", err)
			}

			compareJSON(t, result, tt.value)
		})
	}
}

// TestMsgpackSerializer_SerializeComplex tests serializing complex types.
func TestMsgpackSerializer_SerializeComplex(t *testing.T) {
	s := NewMsgpackSerializer()

	tests := []struct {
		name  string
		value interface{}
	}{
		{
			"map",
			map[string]interface{}{
				"a": 1,
				"b": "string",
				"c": []int{1, 2, 3},
			},
		},
		{
			"slice",
			[]interface{}{1, "two", 3.0, true},
		},
		{
			"struct",
			struct {
				A int
				B string
			}{42, "test"},
		},
		{
			"nested",
			map[string]map[string]int{
				"outer": {"inner": 42},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data, err := s.Encode(tt.value)
			if err != nil {
				t.Fatalf("Encode() error = %v", err)
			}

			var result interface{}
			err = s.Decode(data, &result)
			if err != nil {
				t.Fatalf("Decode() error = %v", err)
			}

			compareJSON(t, result, tt.value)
		})
	}
}

// TestMsgpackSerializer_SerializeNil tests serializing nil values.
func TestMsgpackSerializer_SerializeNil(t *testing.T) {
	s := NewMsgpackSerializer()

	data, err := s.Encode(nil)
	if err != nil {
		t.Fatalf("Encode(nil) error = %v", err)
	}

	var result interface{}
	err = s.Decode(data, &result)
	if err != nil {
		t.Fatalf("Decode() error = %v", err)
	}

	if result != nil {
		t.Errorf("Decode() = %v, want nil", result)
	}
}

// TestMsgpackSerializer_EmptyBuffer tests deserializing empty buffer.
func TestMsgpackSerializer_EmptyBuffer(t *testing.T) {
	s := NewMsgpackSerializer()

	data := []byte{}
	var result interface{}
	err := s.Decode(data, &result)
	if err == nil {
		t.Error("Decode(empty buffer) should return error")
	}
}

// TestMsgpackSerializer_InvalidData tests deserializing invalid data.
func TestMsgpackSerializer_InvalidData(t *testing.T) {
	s := NewMsgpackSerializer()

	// Test with empty data - should return error
	var result interface{}
	err := s.Decode([]byte{}, &result)
	if err == nil {
		t.Error("Decode(empty data) should return error")
	}

	// Test with truncated msgpack data (map header says 3 elements but only 1 provided)
	truncatedData := []byte{0x83, 0xA1, 0x61, 0x01} // map with 3 elements, but only 1 key-value pair
	err = s.Decode(truncatedData, &result)
	if err == nil {
		t.Error("Decode(truncated data) should return error")
	}
}

// TestMsgpackSerializer_LargeObject tests serializing high-size objects.
func TestMsgpackSerializer_LargeObject(t *testing.T) {
	s := NewMsgpackSerializer()

	largeData := make([]byte, 100*1024)
	for i := range largeData {
		largeData[i] = byte(i % 256)
	}

	data, err := s.Encode(largeData)
	if err != nil {
		t.Fatalf("Encode(large) error = %v", err)
	}

	var result interface{}
	err = s.Decode(data, &result)
	if err != nil {
		t.Fatalf("Decode() error = %v", err)
	}

	resultBytes, ok := result.([]byte)
	if !ok {
		t.Fatalf("Decode() returned non-bytes type: %T", result)
	}

	if !bytes.Equal(resultBytes, largeData) {
		t.Error("Decode(large) returned different data")
	}
}

// TestSerializer_Composition tests Serializer composition.
func TestSerializer_Composition(t *testing.T) {
	s := NewSerializer()

	// Test MsgpackSerializer delegation
	value := map[string]int{"a": 1, "b": 2}

	data, err := s.msgpack.Encode(value)
	if err != nil {
		t.Fatalf("msgpack.Encode() error = %v", err)
	}

	var result interface{}
	err = s.msgpack.Decode(data, &result)
	if err != nil {
		t.Fatalf("msgpack.Decode() error = %v", err)
	}

	compareJSON(t, result, value)
}

// TestSerializer_ContextMethods tests context methods delegation.
func TestSerializer_ContextMethods(t *testing.T) {
	s := NewSerializer()

	// Get context
	ctx := s.GetContext()
	if ctx == nil {
		t.Fatal("GetContext() returned nil")
	}

	// Put context
	s.PutContext()
	// Should not panic
}

// BenchmarkMsgpackSerializer_Primitive benchmarks primitive serialization.
func BenchmarkMsgpackSerializer_Primitive(b *testing.B) {
	s := NewMsgpackSerializer()
	value := map[string]interface{}{
		"a": 1,
		"b": "string",
		"c": 3.14,
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Encode(value)
	}
}

// BenchmarkMsgpackSerializer_Complex benchmarks complex serialization.
func BenchmarkMsgpackSerializer_Complex(b *testing.B) {
	s := NewMsgpackSerializer()
	value := map[string]interface{}{
		"int":    42,
		"string": "hello world",
		"float":  3.14159,
		"bool":   true,
		"slice":  []int{1, 2, 3, 4, 5},
		"map":    map[string]int{"a": 1, "b": 2},
		"nested": map[string]map[string]int{"outer": {"inner": 42}},
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Encode(value)
	}
}

func TestMsgpackSerializer_ExtensionRegistry(t *testing.T) {
	registry := NewExtensionRegistry()
	registry.RegisterPacker(&GoTypePacker{})
	registry.SetUnpacker(&GoTypeUnpacker{})

	s := NewMsgpackSerializerWithRegistry(registry)

	// Use a struct with unexported fields - this is NOT cross-language
	type localStruct struct {
		Key          string
		Num          int
		privateField string
	}
	value := localStruct{
		Key:          "value",
		Num:          42,
		privateField: "private",
	}

	packer := registry.FindPacker(value)
	t.Logf("Found packer: %v", packer != nil)
	if packer != nil {
		t.Logf("Packer type: %T", packer)
		t.Logf("CanPack result: %v", packer.CanPack(value))
	}

	data, err := s.Encode(value)
	if err != nil {
		t.Fatalf("Encode() error = %v", err)
	}

	t.Logf("Encoded data length: %d", len(data))
	if len(data) > 0 {
		t.Logf("First 20 bytes: %02x", data[:min(20, len(data))])
	}

	result, err := s.DecodeExtension(data)
	if err != nil {
		t.Fatalf("DecodeExtension() error = %v", err)
	}

	compareJSON(t, result, value)
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func TestMsgpackSerializer_ExtensionID101(t *testing.T) {
	if LanguageSpecificTypeExtensionID != 101 {
		t.Errorf("LanguageSpecificTypeExtensionID = %d, want 101", LanguageSpecificTypeExtensionID)
	}
}

func TestMsgpackSerializer_DecodeExtension_InvalidExtension(t *testing.T) {
	registry := NewExtensionRegistry()
	registry.SetUnpacker(&GoTypeUnpacker{})
	s := NewMsgpackSerializerWithRegistry(registry)

	value := map[string]int{"a": 1}
	data, err := s.Encode(value)
	if err != nil {
		t.Fatalf("Encode() error = %v", err)
	}

	_, err = s.DecodeExtension(data)
}
