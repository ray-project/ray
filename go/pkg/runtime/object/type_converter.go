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
	"fmt"
	"math"
)

// TypeConverter provides cross-language type compatibility conversion.
// Similar to Java's TypeConverter for cross-language serialization.
//
// This handles:
// - Integer range checking and conversion (int8/int16/int32/int64)
// - Float conversion (float32 ↔ float64)
// - Type compatibility validation
type TypeConverter struct{}

// NewTypeConverter creates a new TypeConverter.
func NewTypeConverter() *TypeConverter {
	return &TypeConverter{}
}

// ============================================================================
// Integer Type Conversion
// ============================================================================

// ConvertToInt8 converts an integer to int8 with range checking.
// Returns error if the value is out of int8 range.
func (c *TypeConverter) ConvertToInt8(value interface{}) (int8, error) {
	switch v := value.(type) {
	case int:
		if v < math.MinInt8 || v > math.MaxInt8 {
			return 0, fmt.Errorf("value %d out of int8 range [%d, %d]", v, math.MinInt8, math.MaxInt8)
		}
		return int8(v), nil
	case int8:
		return v, nil
	case int16:
		if v < math.MinInt8 || v > math.MaxInt8 {
			return 0, fmt.Errorf("value %d out of int8 range [%d, %d]", v, math.MinInt8, math.MaxInt8)
		}
		return int8(v), nil
	case int32:
		if v < math.MinInt8 || v > math.MaxInt8 {
			return 0, fmt.Errorf("value %d out of int8 range [%d, %d]", v, math.MinInt8, math.MaxInt8)
		}
		return int8(v), nil
	case int64:
		if v < math.MinInt8 || v > math.MaxInt8 {
			return 0, fmt.Errorf("value %d out of int8 range [%d, %d]", v, math.MinInt8, math.MaxInt8)
		}
		return int8(v), nil
	case uint:
		if v > math.MaxInt8 {
			return 0, fmt.Errorf("value %d out of int8 range [%d, %d]", v, math.MinInt8, math.MaxInt8)
		}
		return int8(v), nil
	case uint8:
		if v > math.MaxInt8 {
			return 0, fmt.Errorf("value %d out of int8 range [%d, %d]", v, math.MinInt8, math.MaxInt8)
		}
		return int8(v), nil
	case uint16:
		if v > math.MaxInt8 {
			return 0, fmt.Errorf("value %d out of int8 range [%d, %d]", v, math.MinInt8, math.MaxInt8)
		}
		return int8(v), nil
	case uint32:
		if v > math.MaxInt8 {
			return 0, fmt.Errorf("value %d out of int8 range [%d, %d]", v, math.MinInt8, math.MaxInt8)
		}
		return int8(v), nil
	case uint64:
		if v > math.MaxInt8 {
			return 0, fmt.Errorf("value %d out of int8 range [%d, %d]", v, math.MinInt8, math.MaxInt8)
		}
		return int8(v), nil
	default:
		return 0, fmt.Errorf("cannot convert %T to int8", value)
	}
}

// ConvertToInt16 converts an integer to int16 with range checking.
// Returns error if the value is out of int16 range.
func (c *TypeConverter) ConvertToInt16(value interface{}) (int16, error) {
	switch v := value.(type) {
	case int:
		if v < math.MinInt16 || v > math.MaxInt16 {
			return 0, fmt.Errorf("value %d out of int16 range [%d, %d]", v, math.MinInt16, math.MaxInt16)
		}
		return int16(v), nil
	case int8:
		return int16(v), nil
	case int16:
		return v, nil
	case int32:
		if v < math.MinInt16 || v > math.MaxInt16 {
			return 0, fmt.Errorf("value %d out of int16 range [%d, %d]", v, math.MinInt16, math.MaxInt16)
		}
		return int16(v), nil
	case int64:
		if v < math.MinInt16 || v > math.MaxInt16 {
			return 0, fmt.Errorf("value %d out of int16 range [%d, %d]", v, math.MinInt16, math.MaxInt16)
		}
		return int16(v), nil
	case uint:
		if v > math.MaxInt16 {
			return 0, fmt.Errorf("value %d out of int16 range [%d, %d]", v, math.MinInt16, math.MaxInt16)
		}
		return int16(v), nil
	case uint8:
		return int16(v), nil
	case uint16:
		if v > math.MaxInt16 {
			return 0, fmt.Errorf("value %d out of int16 range [%d, %d]", v, math.MinInt16, math.MaxInt16)
		}
		return int16(v), nil
	case uint32:
		if v > math.MaxInt16 {
			return 0, fmt.Errorf("value %d out of int16 range [%d, %d]", v, math.MinInt16, math.MaxInt16)
		}
		return int16(v), nil
	case uint64:
		if v > math.MaxInt16 {
			return 0, fmt.Errorf("value %d out of int16 range [%d, %d]", v, math.MinInt16, math.MaxInt16)
		}
		return int16(v), nil
	default:
		return 0, fmt.Errorf("cannot convert %T to int16", value)
	}
}

// ConvertToInt32 converts an integer to int32 with range checking.
// Returns error if the value is out of int32 range.
func (c *TypeConverter) ConvertToInt32(value interface{}) (int32, error) {
	switch v := value.(type) {
	case int:
		if int64(v) < math.MinInt32 || int64(v) > math.MaxInt32 {
			return 0, fmt.Errorf("value %d out of int32 range [%d, %d]", v, math.MinInt32, math.MaxInt32)
		}
		return int32(v), nil
	case int8:
		return int32(v), nil
	case int16:
		return int32(v), nil
	case int32:
		return v, nil
	case int64:
		if v < math.MinInt32 || v > math.MaxInt32 {
			return 0, fmt.Errorf("value %d out of int32 range [%d, %d]", v, math.MinInt32, math.MaxInt32)
		}
		return int32(v), nil
	case uint:
		if v > math.MaxInt32 {
			return 0, fmt.Errorf("value %d out of int32 range [%d, %d]", v, math.MinInt32, math.MaxInt32)
		}
		return int32(v), nil
	case uint8:
		return int32(v), nil
	case uint16:
		return int32(v), nil
	case uint32:
		if v > math.MaxInt32 {
			return 0, fmt.Errorf("value %d out of int32 range [%d, %d]", v, math.MinInt32, math.MaxInt32)
		}
		return int32(v), nil
	case uint64:
		if v > math.MaxInt32 {
			return 0, fmt.Errorf("value %d out of int32 range [%d, %d]", v, math.MinInt32, math.MaxInt32)
		}
		return int32(v), nil
	default:
		return 0, fmt.Errorf("cannot convert %T to int32", value)
	}
}

// ConvertToInt64 converts an integer to int64 with range checking.
// Returns error if the value is out of int64 range.
func (c *TypeConverter) ConvertToInt64(value interface{}) (int64, error) {
	switch v := value.(type) {
	case int:
		return int64(v), nil
	case int8:
		return int64(v), nil
	case int16:
		return int64(v), nil
	case int32:
		return int64(v), nil
	case int64:
		return v, nil
	case uint:
		return int64(v), nil
	case uint8:
		return int64(v), nil
	case uint16:
		return int64(v), nil
	case uint32:
		return int64(v), nil
	case uint64:
		if v > math.MaxInt64 {
			return 0, fmt.Errorf("value %d out of int64 range [%d, %d]", v, int64(0), math.MaxInt64)
		}
		return int64(v), nil
	default:
		return 0, fmt.Errorf("cannot convert %T to int64", value)
	}
}

// ============================================================================
// Float Type Conversion
// ============================================================================

// ConvertToFloat32 converts a float to float32 with precision checking.
// Returns error if the value is out of float32 range or loses significant precision.
func (c *TypeConverter) ConvertToFloat32(value interface{}) (float32, error) {
	switch v := value.(type) {
	case float32:
		return v, nil
	case float64:
		if v > math.MaxFloat32 || v < -math.MaxFloat32 {
			return 0, fmt.Errorf("value %v out of float32 range", v)
		}
		// Check for precision loss
		f32 := float32(v)
		if math.IsInf(float64(f32), 0) && !math.IsInf(v, 0) {
			return 0, fmt.Errorf("value %v causes overflow when converting to float32", v)
		}
		return f32, nil
	case int:
		return float32(v), nil
	case int8:
		return float32(v), nil
	case int16:
		return float32(v), nil
	case int32:
		return float32(v), nil
	case int64:
		return float32(v), nil
	case uint:
		return float32(v), nil
	case uint8:
		return float32(v), nil
	case uint16:
		return float32(v), nil
	case uint32:
		return float32(v), nil
	case uint64:
		return float32(v), nil
	default:
		return 0, fmt.Errorf("cannot convert %T to float32", value)
	}
}

// ConvertToFloat64 converts a value to float64.
func (c *TypeConverter) ConvertToFloat64(value interface{}) (float64, error) {
	switch v := value.(type) {
	case float32:
		return float64(v), nil
	case float64:
		return v, nil
	case int:
		return float64(v), nil
	case int8:
		return float64(v), nil
	case int16:
		return float64(v), nil
	case int32:
		return float64(v), nil
	case int64:
		return float64(v), nil
	case uint:
		return float64(v), nil
	case uint8:
		return float64(v), nil
	case uint16:
		return float64(v), nil
	case uint32:
		return float64(v), nil
	case uint64:
		return float64(v), nil
	default:
		return 0, fmt.Errorf("cannot convert %T to float64", value)
	}
}

// ============================================================================
// Type Compatibility Checking
// ============================================================================

// IsIntegerCompatible checks if two integer types are compatible for cross-language conversion.
// Returns true if the conversion is safe without data loss.
func (c *TypeConverter) IsIntegerCompatible(sourceType, targetType string) bool {
	// Same type is always compatible
	if sourceType == targetType {
		return true
	}

	// Safe widening conversions (no data loss)
	safeWidening := map[string][]string{
		"int8":   {"int16", "int32", "int64"},
		"int16":  {"int32", "int64"},
		"int32":  {"int64"},
		"uint8":  {"uint16", "uint32", "uint64", "int16", "int32", "int64"},
		"uint16": {"uint32", "uint64", "int32", "int64"},
		"uint32": {"uint64", "int64"},
	}

	if targets, ok := safeWidening[sourceType]; ok {
		for _, t := range targets {
			if t == targetType {
				return true
			}
		}
	}

	return false
}

// IsFloatCompatible checks if two float types are compatible for cross-language conversion.
// Returns true if the conversion is safe.
func (c *TypeConverter) IsFloatCompatible(sourceType, targetType string) bool {
	// Same type is always compatible
	if sourceType == targetType {
		return true
	}

	// float32 -> float64 is safe (widening)
	// float64 -> float32 may lose precision but is allowed with warning
	if (sourceType == "float32" && targetType == "float64") ||
		(sourceType == "float64" && targetType == "float32") {
		return true
	}

	return false
}

// ValidateIntegerRange checks if an integer value fits within the target type's range.
func (c *TypeConverter) ValidateIntegerRange(value int64, targetType string) bool {
	switch targetType {
	case "int8":
		return value >= math.MinInt8 && value <= math.MaxInt8
	case "int16":
		return value >= math.MinInt16 && value <= math.MaxInt16
	case "int32":
		return value >= math.MinInt32 && value <= math.MaxInt32
	case "int64":
		return true // int64 can hold any int64 value
	default:
		return false
	}
}
