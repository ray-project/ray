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

import "reflect"

// TypeCategory represents the category of a type for selecting serialization strategy.
type TypeCategory int

const (
	// TypeCategoryUnknown represents unknown type.
	TypeCategoryUnknown TypeCategory = iota

	// TypeCategoryPrimitive represents primitive types (int, float, string, bool).
	TypeCategoryPrimitive

	// TypeCategoryArray represents array types (slice, array).
	TypeCategoryArray

	// TypeCategoryMap represents map types.
	TypeCategoryMap

	// TypeCategoryStruct represents struct types.
	TypeCategoryStruct

	// TypeCategoryPointer represents pointer types.
	TypeCategoryPointer

	// TypeCategoryInterface represents interface types.
	TypeCategoryInterface

	// TypeCategoryActorHandle represents Actor handle types.
	TypeCategoryActorHandle

	// TypeCategoryException represents exception types.
	TypeCategoryException
)

// String returns the string representation of the type category.
func (c TypeCategory) String() string {
	switch c {
	case TypeCategoryUnknown:
		return "unknown"
	case TypeCategoryPrimitive:
		return "primitive"
	case TypeCategoryArray:
		return "array"
	case TypeCategoryMap:
		return "map"
	case TypeCategoryStruct:
		return "struct"
	case TypeCategoryPointer:
		return "pointer"
	case TypeCategoryInterface:
		return "interface"
	case TypeCategoryActorHandle:
		return "actor_handle"
	case TypeCategoryException:
		return "exception"
	default:
		return "unknown"
	}
}

// CategorizeType returns the type category for the given value.
// Uses reflect for comprehensive type coverage.
//
// Note: For nil values, this function returns TypeCategoryUnknown because
// Go's type system cannot determine the underlying type of a nil interface{}.
// Callers should handle nil cases explicitly if needed.
func CategorizeType(value interface{}) TypeCategory {
	if value == nil {
		return TypeCategoryUnknown
	}

	// Check for error type first (exception category)
	if _, ok := value.(error); ok {
		return TypeCategoryException
	}

	t := reflect.TypeOf(value)
	if t == nil {
		return TypeCategoryUnknown
	}

	switch t.Kind() {
	// Primitive types
	case reflect.Bool, reflect.String,
		reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
		reflect.Float32, reflect.Float64:
		return TypeCategoryPrimitive

	// Array/slice types
	case reflect.Slice, reflect.Array:
		return TypeCategoryArray

	// Map types
	case reflect.Map:
		return TypeCategoryMap

	// Struct types
	case reflect.Struct:
		return TypeCategoryStruct

	// Pointer types
	case reflect.Ptr:
		return TypeCategoryPointer

	// Interface types
	case reflect.Interface:
		return TypeCategoryInterface

	default:
		return TypeCategoryUnknown
	}
}

// isStruct checks if the value is a struct type.
func isStruct(v interface{}) bool {
	t := reflect.TypeOf(v)
	if t == nil {
		return false
	}
	// Dereference pointer
	for t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	return t.Kind() == reflect.Struct
}

// IsCrossLanguageType checks if a type can be transmitted across languages.
// Returns true if XLANG metadata can be used, false if GO metadata is needed.
// Optimized with two-phase checking:
//   - Phase 1: Quick check using CategorizeType for simple types (primitive, exception)
//   - Phase 2: Recursive check for complex types (slice, map, struct, pointer)
func IsCrossLanguageType(obj interface{}) bool {
	if obj == nil {
		return false
	}

	// Phase 1: Quick check for simple types using type category
	category := CategorizeType(obj)
	if isCross, isFinal := isCrossLanguageTypeByCategory(category); isFinal {
		return isCross
	}

	// Phase 2: Recursive check for complex types
	t := reflect.TypeOf(obj)
	return isCrossLanguageTypeRecursive(t)
}

// isCrossLanguageTypeByCategory checks if a type category can be transmitted across languages.
// This function uses the type category to make a quick decision for primitive types.
func isCrossLanguageTypeByCategory(category TypeCategory) (bool, bool) {
	switch category {
	case TypeCategoryPrimitive:
		return true, true // true = is cross-language, true = decision is final
	case TypeCategoryException:
		return false, true // exceptions are not cross-language
	default:
		return false, false // false = decision not final, need recursive check
	}
}

// isCrossLanguageTypeRecursive recursively checks the type (handles nested structures).
// Note: This function is kept for complex types (slice, map, struct, pointer) that need
// recursive checking. For simple types, use isCrossLanguageTypeByCategory for better performance.
func isCrossLanguageTypeRecursive(t reflect.Type) bool {
	switch t.Kind() {
	// Basic types.
	case reflect.Bool, reflect.String:
		return true

	// Integer types.
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return true

	// Floating point types.
	case reflect.Float32, reflect.Float64:
		return true

	// Slice/Array.
	case reflect.Slice, reflect.Array:
		return isCrossLanguageTypeRecursive(t.Elem())

	// Map.
	case reflect.Map:
		// Key must be string or basic type.
		if !isCrossLanguageTypeRecursive(t.Key()) {
			return false
		}
		// Value must be cross-language type.
		return isCrossLanguageTypeRecursive(t.Elem())

	// Struct.
	case reflect.Struct:
		// Check if all exported fields are cross-language types.
		for i := 0; i < t.NumField(); i++ {
			field := t.Field(i)
			if !field.IsExported() {
				// Has non-exported fields, can only be used as Go internal type.
				return false
			}
			if !isCrossLanguageTypeRecursive(field.Type) {
				return false
			}
		}
		return t.NumField() > 0

	// Interface type (interface{}).
	case reflect.Interface:
		// Can only be determined at runtime.
		return true

	// Pointer type - recursively check the pointed type.
	case reflect.Ptr:
		return isCrossLanguageTypeRecursive(t.Elem())

	default:
		// Other types (such as channel, function, unsafe.Pointer) do not support cross-language.
		return false
	}
}

// isLargeObject checks if an object is potentially large.
// Used for buffer size estimation.
// Threshold is set to 512KB based on typical large object detection needs.
func IsLargeObject(obj interface{}) bool {
	switch v := obj.(type) {
	case []byte:
		return len(v) > 512*1024 // 512KB
	case []interface{}:
		return len(v) > 100
	case map[string]interface{}:
		return len(v) > 50
	case map[interface{}]interface{}:
		return len(v) > 50
	default:
		return false
	}
}