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

	"github.com/ray-project/ray/go/pkg/runtime/object"
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
		name string
		// value is the value to encode.
		value interface{}
		// decoded is the JSON-comparable form a round trip through
		// interface{} must produce; it defaults to value when nil. A
		// fully-exported struct is a cross-language type and is encoded as a
		// positional msgpack array (matching the C++ MSGPACK_DEFINE
		// layout), so its round trip yields a positional slice rather than a
		// map.
		decoded interface{}
	}{
		{
			"map",
			map[string]interface{}{
				"a": 1,
				"b": "string",
				"c": []int{1, 2, 3},
			},
			nil,
		},
		{
			"slice",
			[]interface{}{1, "two", 3.0, true},
			nil,
		},
		{
			"struct",
			struct {
				A int
				B string
			}{42, "test"},
			[]interface{}{int8(42), "test"},
		},
		{
			"nested",
			map[string]map[string]int{
				"outer": {"inner": 42},
			},
			nil,
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

			want := tt.value
			if tt.decoded != nil {
				want = tt.decoded
			}
			compareJSON(t, result, want)
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

// TestSerializeTwoTierStrategy verifies the two-tier encoding strategy:
// small objects use the pool (dataFromPool=true), large objects are
// freshly allocated (dataFromPool=false).
func TestSerializeTwoTierStrategy(t *testing.T) {
	s := NewSerializer()

	// Small object: estimate <= MaxBufferSize -> pooled buffer.
	small := []byte("hello world")
	smallNative, err := s.Serialize(small)
	if err != nil {
		t.Fatalf("Serialize(small) failed: %v", err)
	}
	if !smallNative.DataFromPool() {
		t.Errorf("small object Data should be pool-backed (dataFromPool=true)")
	}
	if len(smallNative.Data) <= MessagePackOffset {
		t.Errorf("small object Data too short: %d", len(smallNative.Data))
	}

	// Large object: estimate > MaxBufferSize -> fresh allocation.
	// []byte of 2MB estimates to ~2.2MB, well over the 1MB pool ceiling.
	large := make([]byte, 2*1024*1024)
	for i := range large {
		large[i] = byte(i % 251)
	}
	largeNative, err := s.Serialize(large)
	if err != nil {
		t.Fatalf("Serialize(large) failed: %v", err)
	}
	if largeNative.DataFromPool() {
		t.Errorf("large object Data should NOT be pool-backed (fresh allocation)")
	}
	if len(largeNative.Data) <= MessagePackOffset+len(large) {
		t.Errorf("large object Data length mismatch: got %d", len(largeNative.Data))
	}

	// Round-trip: both must decode back to identical bytes.
	var smallBack, largeBack []byte
	if err := s.DeserializeTo(smallNative, &smallBack); err != nil {
		t.Fatalf("DeserializeTo(small) failed: %v", err)
	}
	if string(smallBack) != string(small) {
		t.Errorf("small round-trip mismatch: got %q want %q", smallBack, small)
	}
	if err := s.DeserializeTo(largeNative, &largeBack); err != nil {
		t.Fatalf("DeserializeTo(large) failed: %v", err)
	}
	if len(largeBack) != len(large) || largeBack[0] != large[0] || largeBack[len(large)-1] != large[len(large)-1] {
		t.Errorf("large round-trip mismatch: got %d bytes", len(largeBack))
	}

	// Return the pooled buffer to the pool so the test does not leak it.
	_ = smallNative.Close()
}

// TestSerializeUnderEstimateGrowsBuffer verifies the under-estimate path: a
// struct wrapping a large []byte is under-estimated by EstimateBufferSize
// (structs default to 4KB), so EncodeToBuffer grows the borrowed pooled buffer
// via append. The grown payload must NOT be marked dataFromPool (it is no
// longer pool-backed), must round-trip correctly, and the pool slot borrowed by
// GetBuffer must be returned so pool accounting does not leak.
func TestSerializeUnderEstimateGrowsBuffer(t *testing.T) {
	s := NewSerializer()

	type payload struct {
		Data []byte
	}
	// 256KB of payload in a struct: EstimateBufferSize sees a struct (4KB
	// default) and routes it to the pooled path, but encoding needs ~256KB,
	// forcing EncodeToBuffer to grow the borrowed buffer well past 4KB.
	blob := make([]byte, 256*1024)
	for i := range blob {
		blob[i] = byte(i % 251)
	}
	p := payload{Data: blob}

	beforeCount, beforeBytes, _, _ := defaultPool.GetMemoryUsage()

	nativeObj, err := s.Serialize(p)
	if err != nil {
		t.Fatalf("Serialize(under-estimated struct) failed: %v", err)
	}
	if nativeObj.DataFromPool() {
		t.Errorf("grown buffer should NOT be marked pool-backed")
	}

	var back payload
	if err := s.DeserializeTo(nativeObj, &back); err != nil {
		t.Fatalf("DeserializeTo failed: %v", err)
	}
	if len(back.Data) != len(blob) || back.Data[0] != blob[0] || back.Data[len(blob)-1] != blob[len(blob)-1] {
		t.Errorf("round-trip mismatch: got %d bytes", len(back.Data))
	}

	// The grown payload is non-pooled: Close() must not touch the pool.
	_ = nativeObj.Close()

	// The pooled slot borrowed by GetBuffer must have been returned (orig
	// path), so accounting must be back to baseline.
	afterCount, afterBytes, _, _ := defaultPool.GetMemoryUsage()
	if afterCount != beforeCount {
		t.Errorf("pool buffer count leaked: before=%d after=%d", beforeCount, afterCount)
	}
	if afterBytes != beforeBytes {
		t.Errorf("pool memory leaked: before=%d after=%d", beforeBytes, afterBytes)
	}
}

// xlangErrStruct is a fully-exported struct that also implements error. It is
// the shape that made the metadata tag and the payload layout disagree:
// CategorizeType classifies anything implementing error as an exception, so
// IsCrossLanguageType answers false for it, while the recursive field check
// used for the metadata tag answers true. The struct is therefore a
// cross-language value whose payload was written as a msgpack map.
type xlangErrStruct struct {
	Message string `msgpack:"message"`
	Code    int    `msgpack:"code"`
}

func (e xlangErrStruct) Error() string { return e.Message }

// TestSerializeErrorImplementingStructIsPositionalArray proves that the metadata
// tag and the payload layout of a serialized value come from one
// cross-language decision: a value tagged XLANG must be encoded as a positional
// array, whatever interfaces its type implements. When they disagree the C++
// worker is handed a msgpack map under an XLANG tag and fails with
// "msgpack::v1::type_error: std::bad_cast" while converting the argument to its
// MSGPACK_DEFINE type.
func TestSerializeErrorImplementingStructIsPositionalArray(t *testing.T) {
	s := NewSerializer()

	nativeObj, err := s.Serialize(xlangErrStruct{Message: "boom", Code: 7})
	if err != nil {
		t.Fatalf("Serialize failed: %v", err)
	}
	if got := string(nativeObj.Metadata); got != object.MetadataTypeCrossLanguage {
		t.Fatalf("metadata = %q, want %q", got, object.MetadataTypeCrossLanguage)
	}
	payload := nativeObj.Data[MessagePackOffset:]
	if len(payload) == 0 {
		t.Fatal("empty payload")
	}
	// 0x92 is the msgpack fixarray header for a 2-element array; a map would be
	// 0x82 (fixmap of 2) or another fixmap/map16/map32 header.
	if payload[0] != 0x92 {
		t.Fatalf("value tagged %q encoded with header %#x, want 0x92 (2-element array)",
			nativeObj.Metadata, payload[0])
	}

	// The array form must still decode back into the Go struct.
	var got xlangErrStruct
	if err := s.DeserializeTo(nativeObj, &got); err != nil {
		t.Fatalf("DeserializeTo failed: %v", err)
	}
	if got != (xlangErrStruct{Message: "boom", Code: 7}) {
		t.Fatalf("round trip = %+v, want {boom 7}", got)
	}
}
