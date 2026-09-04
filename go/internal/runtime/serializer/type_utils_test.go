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
	"testing"
)

// TestCategorizeType tests type categorization.
func TestCategorizeType(t *testing.T) {
	tests := []struct {
		name     string
		value    interface{}
		expected TypeCategory
	}{
		{"int", int(42), TypeCategoryPrimitive},
		{"int8", int8(42), TypeCategoryPrimitive},
		{"int16", int16(42), TypeCategoryPrimitive},
		{"int32", int32(42), TypeCategoryPrimitive},
		{"int64", int64(42), TypeCategoryPrimitive},
		{"uint", uint(42), TypeCategoryPrimitive},
		{"uint8", uint8(42), TypeCategoryPrimitive},
		{"uint16", uint16(42), TypeCategoryPrimitive},
		{"uint32", uint32(42), TypeCategoryPrimitive},
		{"uint64", uint64(42), TypeCategoryPrimitive},
		{"float32", float32(3.14), TypeCategoryPrimitive},
		{"float64", float64(3.14), TypeCategoryPrimitive},
		{"bool", true, TypeCategoryPrimitive},
		{"string", "hello", TypeCategoryPrimitive},
		{"[]byte", []byte("hello"), TypeCategoryArray},
		{"[]int", []int{1, 2, 3}, TypeCategoryArray},
		{"map", map[string]int{"a": 1}, TypeCategoryMap},
		{"struct", struct{ A int }{1}, TypeCategoryStruct},
		{"nil", nil, TypeCategoryUnknown},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := CategorizeType(tt.value)
			if result != tt.expected {
				t.Errorf("CategorizeType(%v) = %v, want %v", tt.value, result, tt.expected)
			}
		})
	}
}

// TestIsCrossLanguageType tests cross-language type checking.
func TestIsCrossLanguageType(t *testing.T) {
	tests := []struct {
		name     string
		value    interface{}
		expected bool
	}{
		{"int", int(42), true},
		{"string", "hello", true},
		{"float64", 3.14, true},
		{"bool", true, true},
		{"[]int", []int{1, 2, 3}, true},
		{"map[string]int", map[string]int{"a": 1}, true},
		{"struct", struct{ A int }{1}, true},
		{"complex64", complex64(1 + 2i), false},
		{"uintptr", uintptr(0x1234), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := IsCrossLanguageType(tt.value)
			if result != tt.expected {
				t.Errorf("IsCrossLanguageType(%v) = %v, want %v", tt.value, result, tt.expected)
			}
		})
	}
}

// TestIsLargeObject tests large object detection.
func TestIsLargeObject(t *testing.T) {
	// Large object threshold is 512KB for byte slices
	// A slice needs to be large enough to exceed the threshold
	tests := []struct {
		name     string
		value    interface{}
		expected bool
	}{
		{"small string", "hello", false},
		{"small slice", []int{1, 2, 3}, false},
		{"int", int(42), false},
		// 20000 ints = ~160KB, still below 512KB threshold
		// Use a larger size to ensure we exceed the threshold
		{"large slice", make([]byte, 600*1024), true},
		{"large map", make(map[string]int), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := IsLargeObject(tt.value)
			if result != tt.expected {
				t.Errorf("IsLargeObject(%v) = %v, want %v", tt.value, result, tt.expected)
			}
		})
	}
}

// BenchmarkCategorizeType benchmarks type categorization.
func BenchmarkCategorizeType(b *testing.B) {
	value := map[string]interface{}{
		"a": 1,
		"b": "string",
		"c": []int{1, 2, 3},
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		CategorizeType(value)
	}
}

// BenchmarkIsCrossLanguageType benchmarks cross-language type check.
func BenchmarkIsCrossLanguageType(b *testing.B) {
	value := map[string]interface{}{
		"a": 1,
		"b": "string",
		"c": []int{1, 2, 3},
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		IsCrossLanguageType(value)
	}
}
