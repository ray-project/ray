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
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTypeConverter_ConvertToInt8(t *testing.T) {
	converter := NewTypeConverter()

	tests := []struct {
		name      string
		input     interface{}
		want      int8
		wantError bool
	}{
		{"int in range", int(100), int8(100), false},
		{"int out of range (high)", int(200), 0, true},
		{"int out of range (low)", int(-200), 0, true},
		{"int8", int8(50), int8(50), false},
		{"int16 in range", int16(100), int8(100), false},
		{"int16 out of range", int16(200), 0, true},
		{"int32 in range", int32(100), int8(100), false},
		{"int64 in range", int64(100), int8(100), false},
		{"uint8 in range", uint8(100), int8(100), false},
		{"uint8 out of range", uint8(200), 0, true},
		{"boundary max", int(math.MaxInt8), math.MaxInt8, false},
		{"boundary min", int(math.MinInt8), math.MinInt8, false},
		{"invalid type", "string", 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := converter.ConvertToInt8(tt.input)
			if tt.wantError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.want, got)
			}
		})
	}
}

func TestTypeConverter_ConvertToInt16(t *testing.T) {
	converter := NewTypeConverter()

	tests := []struct {
		name      string
		input     interface{}
		want      int16
		wantError bool
	}{
		{"int in range", int(10000), int16(10000), false},
		{"int out of range (high)", int(40000), 0, true},
		{"int out of range (low)", int(-40000), 0, true},
		{"int8", int8(50), int16(50), false},
		{"int16", int16(10000), int16(10000), false},
		{"int32 in range", int32(10000), int16(10000), false},
		{"int32 out of range", int32(40000), 0, true},
		{"int64 in range", int64(10000), int16(10000), false},
		{"uint16 in range", uint16(10000), int16(10000), false},
		{"uint16 out of range", uint16(40000), 0, true},
		{"boundary max", int(math.MaxInt16), math.MaxInt16, false},
		{"boundary min", int(math.MinInt16), math.MinInt16, false},
		{"invalid type", "string", 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := converter.ConvertToInt16(tt.input)
			if tt.wantError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.want, got)
			}
		})
	}
}

func TestTypeConverter_ConvertToInt32(t *testing.T) {
	converter := NewTypeConverter()

	tests := []struct {
		name      string
		input     interface{}
		want      int32
		wantError bool
	}{
		{"int in range", int(1000000), int32(1000000), false},
		{"int32", int32(1000000), int32(1000000), false},
		{"int64 in range", int64(1000000), int32(1000000), false},
		{"int64 out of range", int64(math.MaxInt32 + 1), 0, true},
		{"uint32 in range", uint32(1000000), int32(1000000), false},
		{"uint32 out of range", uint32(math.MaxInt32 + 1), 0, true},
		{"boundary max", int(math.MaxInt32), math.MaxInt32, false},
		{"boundary min", int(math.MinInt32), math.MinInt32, false},
		{"invalid type", "string", 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := converter.ConvertToInt32(tt.input)
			if tt.wantError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.want, got)
			}
		})
	}
}

func TestTypeConverter_ConvertToInt64(t *testing.T) {
	converter := NewTypeConverter()

	tests := []struct {
		name      string
		input     interface{}
		want      int64
		wantError bool
	}{
		{"int", int(1000000), int64(1000000), false},
		{"int8", int8(50), int64(50), false},
		{"int16", int16(10000), int64(10000), false},
		{"int32", int32(1000000), int64(1000000), false},
		{"int64", int64(1000000), int64(1000000), false},
		{"uint64 in range", uint64(math.MaxInt64), int64(math.MaxInt64), false},
		{"uint64 out of range", uint64(math.MaxInt64) + 1, 0, true},
		{"invalid type", "string", 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := converter.ConvertToInt64(tt.input)
			if tt.wantError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.want, got)
			}
		})
	}
}

func TestTypeConverter_ConvertToFloat32(t *testing.T) {
	converter := NewTypeConverter()

	tests := []struct {
		name      string
		input     interface{}
		want      float32
		wantError bool
	}{
		{"float32", float32(3.14), float32(3.14), false},
		{"float64 in range", float64(3.14), float32(3.14), false},
		{"float64 overflow", float64(math.MaxFloat64), 0, true},
		{"int", int(100), float32(100), false},
		{"int64", int64(1000000), float32(1000000), false},
		{"uint", uint(100), float32(100), false},
		{"invalid type", "string", 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := converter.ConvertToFloat32(tt.input)
			if tt.wantError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.InDelta(t, tt.want, got, 0.001)
			}
		})
	}
}

func TestTypeConverter_ConvertToFloat64(t *testing.T) {
	converter := NewTypeConverter()

	tests := []struct {
		name      string
		input     interface{}
		want      float64
		wantError bool
	}{
		{"float32", float32(3.14), float64(3.14), false},
		{"float64", float64(3.14), float64(3.14), false},
		{"int", int(100), float64(100), false},
		{"int64", int64(1000000), float64(1000000), false},
		{"uint", uint(100), float64(100), false},
		{"invalid type", "string", 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := converter.ConvertToFloat64(tt.input)
			if tt.wantError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.InDelta(t, tt.want, got, 0.001)
			}
		})
	}
}

func TestTypeConverter_IsIntegerCompatible(t *testing.T) {
	converter := NewTypeConverter()

	tests := []struct {
		name       string
		sourceType string
		targetType string
		want       bool
	}{
		{"same type", "int32", "int32", true},
		{"safe widening int8->int16", "int8", "int16", true},
		{"safe widening int8->int32", "int8", "int32", true},
		{"safe widening int8->int64", "int8", "int64", true},
		{"safe widening int16->int32", "int16", "int32", true},
		{"safe widening int16->int64", "int16", "int64", true},
		{"safe widening int32->int64", "int32", "int64", true},
		{"unsafe narrowing int32->int8", "int32", "int8", false},
		{"unsafe narrowing int64->int16", "int64", "int16", false},
		{"uint8->int16", "uint8", "int16", true},
		{"uint16->int32", "uint16", "int32", true},
		{"uint32->int64", "uint32", "int64", true},
		{"invalid combination", "int8", "float32", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := converter.IsIntegerCompatible(tt.sourceType, tt.targetType)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestTypeConverter_IsFloatCompatible(t *testing.T) {
	converter := NewTypeConverter()

	tests := []struct {
		name       string
		sourceType string
		targetType string
		want       bool
	}{
		{"same type", "float32", "float32", true},
		{"float32->float64", "float32", "float64", true},
		{"float64->float32", "float64", "float32", true},
		{"invalid combination", "float32", "int32", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := converter.IsFloatCompatible(tt.sourceType, tt.targetType)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestTypeConverter_ValidateIntegerRange(t *testing.T) {
	converter := NewTypeConverter()

	tests := []struct {
		name       string
		value      int64
		targetType string
		want       bool
	}{
		{"int8 in range", 100, "int8", true},
		{"int8 max", math.MaxInt8, "int8", true},
		{"int8 min", math.MinInt8, "int8", true},
		{"int8 out of range (high)", 128, "int8", false},
		{"int8 out of range (low)", -129, "int8", false},
		{"int16 in range", 10000, "int16", true},
		{"int16 out of range", 40000, "int16", false},
		{"int32 in range", 1000000, "int32", true},
		{"int32 out of range", math.MaxInt32 + 1, "int32", false},
		{"int64 always true", math.MaxInt64, "int64", true},
		{"invalid type", 100, "invalid", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := converter.ValidateIntegerRange(tt.value, tt.targetType)
			assert.Equal(t, tt.want, got)
		})
	}
}
