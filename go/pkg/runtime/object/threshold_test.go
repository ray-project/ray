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
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDefaultLargestSizePassedByValue(t *testing.T) {
	// Verify the default threshold is 100KB (aligns with Java)
	assert.Equal(t, int64(100*1024), DefaultLargestSizePassedByValue, "Default threshold should be 100KB")
}

func TestShouldPassByValue(t *testing.T) {
	// Reset to default state
	SetLocalMode(true)

	tests := []struct {
		name        string
		dataSize    int
		isLocalMode bool
		threshold   int64
		expected    bool
	}{
		{
			name:        "small object in local mode",
			dataSize:    50 * 1024, // 50KB
			isLocalMode: true,
			threshold:   DefaultLargestSizePassedByValue,
			expected:    true, // Should pass by value (size < threshold)
		},
		{
			name:        "large object in local mode",
			dataSize:    150 * 1024, // 150KB
			isLocalMode: true,
			threshold:   DefaultLargestSizePassedByValue,
			expected:    false, // Should pass by reference (size > threshold)
		},
		{
			name:        "exactly threshold size",
			dataSize:    100 * 1024, // 100KB
			isLocalMode: true,
			threshold:   DefaultLargestSizePassedByValue,
			expected:    false, // Should pass by reference (size == threshold, not <)
		},
		{
			name:        "small object in cluster mode",
			dataSize:    10 * 1024, // 10KB
			isLocalMode: false,
			threshold:   DefaultLargestSizePassedByValue,
			expected:    true, // Should pass by value
		},
		{
			name:        "custom threshold in cluster mode",
			dataSize:    200 * 1024, // 200KB
			isLocalMode: false,
			threshold:   250 * 1024, // Custom 250KB threshold
			expected:    true,       // Should pass by value (size < custom threshold)
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Set up test state
			SetLocalMode(tt.isLocalMode)
			if tt.threshold != DefaultLargestSizePassedByValue {
				SetLargestSizePassedByValue(tt.threshold)
			}

			// Test ShouldPassByValue
			result := ShouldPassByValue(tt.dataSize, tt.isLocalMode)
			assert.Equal(t, tt.expected, result, "ShouldPassByValue(%d, %v) should return %v", tt.dataSize, tt.isLocalMode, tt.expected)

			// Reset threshold for next test
			if tt.threshold != DefaultLargestSizePassedByValue {
				SetLargestSizePassedByValue(DefaultLargestSizePassedByValue)
			}
		})
	}
}

func TestSetLocalMode(t *testing.T) {
	// Test local mode activation
	SetLocalMode(true)
	assert.True(t, GetIsLocalMode(), "IsLocalMode should be true")
	assert.Equal(t, int64(DefaultLargestSizePassedByValue), GetLargestSizePassedByValue(), "Local mode threshold should be 100KB")

	// Test cluster mode activation
	SetLocalMode(false)
	assert.False(t, GetIsLocalMode(), "IsLocalMode should be false")
}

func TestSetLargestSizePassedByValue(t *testing.T) {
	// Test setting custom threshold
	customThreshold := int64(200 * 1024)
	SetLargestSizePassedByValue(customThreshold)
	assert.Equal(t, customThreshold, GetLargestSizePassedByValue(), "Threshold should be set to custom value")

	// Reset to default - convert int constant to int64 for type compatibility
	SetLargestSizePassedByValue(int64(DefaultLargestSizePassedByValue))
	assert.Equal(t, int64(DefaultLargestSizePassedByValue), GetLargestSizePassedByValue(), "Threshold should be reset to default")
}
