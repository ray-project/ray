// Copyright 2026 The Ray Authors.
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

package autoscaler

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestDeleteMapEle tests the map element deletion.
func TestDeleteMapEle(t *testing.T) {
	t.Run("DeleteExistingKey", func(t *testing.T) {
		m := map[string]interface{}{
			"key1": "value1",
			"key2": "value2",
		}
		DeleteMapEle(m, "key1")
		assert.NotContains(t, m, "key1")
		assert.Contains(t, m, "key2")
		assert.Equal(t, 1, len(m))
	})

	t.Run("DeleteNonExistentKey", func(t *testing.T) {
		m := map[string]interface{}{
			"key1": "value1",
		}
		DeleteMapEle(m, "nonexistent")
		assert.Contains(t, m, "key1")
		assert.Equal(t, 1, len(m))
	})

	t.Run("DeleteFromNilMap", func(t *testing.T) {
		var nilMap map[string]interface{}
		// Must not panic.
		DeleteMapEle(nilMap, "key1")
		assert.Nil(t, nilMap)
	})

	t.Run("DeleteFromEmptyMap", func(t *testing.T) {
		m := make(map[string]interface{})
		DeleteMapEle(m, "key1")
		assert.Equal(t, 0, len(m))
	})

	t.Run("DeleteAllKeys", func(t *testing.T) {
		m := map[string]interface{}{
			"key1": "value1",
			"key2": "value2",
			"key3": "value3",
		}
		DeleteMapEle(m, "key1")
		DeleteMapEle(m, "key2")
		DeleteMapEle(m, "key3")
		assert.Equal(t, 0, len(m))
	})
}

// TestMergeMap tests the map merge.
func TestMergeMap(t *testing.T) {
	t.Run("MergeTwoMaps", func(t *testing.T) {
		target := map[string]interface{}{
			"key1": "value1",
		}
		source := map[string]interface{}{
			"key2": "value2",
			"key3": "value3",
		}
		result := MergeMap(target, source)
		assert.Equal(t, 3, len(result))
		assert.Equal(t, "value1", result["key1"])
		assert.Equal(t, "value2", result["key2"])
		assert.Equal(t, "value3", result["key3"])
	})

	t.Run("MergeWithOverlappingKeys", func(t *testing.T) {
		target := map[string]interface{}{
			"key1": "old_value1",
			"key2": "value2",
		}
		source := map[string]interface{}{
			"key1": "new_value1",
			"key3": "value3",
		}
		result := MergeMap(target, source)
		assert.Equal(t, 3, len(result))
		assert.Equal(t, "new_value1", result["key1"]) // Overwritten.
		assert.Equal(t, "value2", result["key2"])
		assert.Equal(t, "value3", result["key3"])
	})

	t.Run("MergeIntoEmptyMap", func(t *testing.T) {
		target := make(map[string]interface{})
		source := map[string]interface{}{
			"key1": "value1",
			"key2": "value2",
		}
		result := MergeMap(target, source)
		assert.Equal(t, 2, len(result))
		assert.Equal(t, "value1", result["key1"])
		assert.Equal(t, "value2", result["key2"])
	})

	t.Run("MergeEmptyMap", func(t *testing.T) {
		target := map[string]interface{}{
			"key1": "value1",
		}
		source := make(map[string]interface{})
		result := MergeMap(target, source)
		assert.Equal(t, 1, len(result))
		assert.Equal(t, "value1", result["key1"])
	})

	t.Run("MergeNestedMaps", func(t *testing.T) {
		target := map[string]interface{}{
			"outer": map[string]interface{}{
				"inner1": "value1",
			},
		}
		source := map[string]interface{}{
			"outer": map[string]interface{}{
				"inner2": "value2",
			},
			"key2": "value2",
		}
		result := MergeMap(target, source)
		assert.Equal(t, 2, len(result))
		// MergeMap is a shallow copy: the nested map is replaced entirely.
		assert.Equal(t, map[string]interface{}{"inner2": "value2"}, result["outer"])
		assert.Equal(t, "value2", result["key2"])
	})

	t.Run("MergeNilSource", func(t *testing.T) {
		target := map[string]interface{}{
			"key1": "value1",
		}
		var source map[string]interface{}
		result := MergeMap(target, source)
		assert.Equal(t, 1, len(result))
		assert.Equal(t, "value1", result["key1"])
	})
}

// TestGetValueWithDefault tests the lookup with a default value.
func TestGetValueWithDefault(t *testing.T) {
	t.Run("GetExistingKey", func(t *testing.T) {
		m := map[string]interface{}{
			"key1": "value1",
			"key2": 42,
		}
		result := GetValueWithDefault(m, "default", "key1")
		assert.Equal(t, "value1", result)

		resultInt := GetValueWithDefault(m, 0, "key2")
		assert.Equal(t, 42, resultInt)
	})

	t.Run("GetNonExistentKey", func(t *testing.T) {
		m := map[string]interface{}{
			"key1": "value1",
		}
		result := GetValueWithDefault(m, "default", "nonexistent")
		assert.Equal(t, "default", result)
	})

	t.Run("GetNestedKey", func(t *testing.T) {
		m := map[string]interface{}{
			"outer": map[string]interface{}{
				"inner": "nested_value",
			},
		}
		result := GetValueWithDefault(m, "default", "outer", "inner")
		assert.Equal(t, "nested_value", result)
	})

	t.Run("GetNonExistentNestedKey", func(t *testing.T) {
		m := map[string]interface{}{
			"outer": map[string]interface{}{
				"inner": "nested_value",
			},
		}
		result := GetValueWithDefault(m, "default", "outer", "nonexistent")
		assert.Equal(t, "default", result)
	})

	t.Run("GetTypeMismatch", func(t *testing.T) {
		m := map[string]interface{}{
			"key1": "string_value",
		}
		// Expect an int but the value is a string.
		result := GetValueWithDefault(m, 999, "key1")
		assert.Equal(t, 999, result) // The default value is returned.
	})

	t.Run("GetFromNilMap", func(t *testing.T) {
		var nilMap map[string]interface{}
		result := GetValueWithDefault(nilMap, "default", "key1")
		assert.Equal(t, "default", result)
	})

	t.Run("GetFromEmptyMap", func(t *testing.T) {
		m := make(map[string]interface{})
		result := GetValueWithDefault(m, "default", "key1")
		assert.Equal(t, "default", result)
	})

	t.Run("MultipleKeysWithIntermediateMissing", func(t *testing.T) {
		m := map[string]interface{}{
			"level1": map[string]interface{}{
				"level2": "deep_value",
			},
		}
		// An intermediate level is missing.
		result := GetValueWithDefault(m, "default", "level1", "missing", "level3")
		assert.Equal(t, "default", result)
	})

	t.Run("BooleanValue", func(t *testing.T) {
		m := map[string]interface{}{
			"bool_key": true,
		}
		result := GetValueWithDefault(m, false, "bool_key")
		assert.Equal(t, true, result)

		resultFalse := GetValueWithDefault(m, true, "nonexistent")
		assert.Equal(t, true, resultFalse) // The default value.
	})

	t.Run("FloatValue", func(t *testing.T) {
		m := map[string]interface{}{
			"float_key": 3.14,
		}
		result := GetValueWithDefault(m, 0.0, "float_key")
		assert.Equal(t, 3.14, result)
	})
}

// TestGetValue tests the lookup with an error return.
func TestGetValue(t *testing.T) {
	t.Run("GetExistingKey", func(t *testing.T) {
		m := map[string]interface{}{
			"key1": "value1",
			"key2": 42,
		}
		result, err := GetValue[string](m, "key1")
		assert.NoError(t, err)
		assert.Equal(t, "value1", result)

		resultInt, err := GetValue[int](m, "key2")
		assert.NoError(t, err)
		assert.Equal(t, 42, resultInt)
	})

	t.Run("GetNonExistentKey", func(t *testing.T) {
		m := map[string]interface{}{
			"key1": "value1",
		}
		result, err := GetValue[string](m, "nonexistent")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "not found in map")
		assert.Equal(t, "", result)
	})

	t.Run("GetNestedKey", func(t *testing.T) {
		m := map[string]interface{}{
			"outer": map[string]interface{}{
				"inner": "nested_value",
			},
		}
		result, err := GetValue[string](m, "outer", "inner")
		assert.NoError(t, err)
		assert.Equal(t, "nested_value", result)
	})

	t.Run("GetNonExistentNestedKey", func(t *testing.T) {
		m := map[string]interface{}{
			"outer": map[string]interface{}{
				"inner": "nested_value",
			},
		}
		result, err := GetValue[string](m, "outer", "nonexistent")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "not found in map")
		assert.Equal(t, "", result)
	})

	t.Run("GetTypeMismatch", func(t *testing.T) {
		m := map[string]interface{}{
			"key1": "string_value",
		}
		// Expect an int but the value is a string.
		result, err := GetValue[int](m, "key1")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "the data type of key1 is wrong")
		assert.Equal(t, 0, result)
	})
	t.Run("GetFromNilMap", func(t *testing.T) {
		var nilMap map[string]interface{}
		result, err := GetValue[string](nilMap, "key1")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "not found in map")
		assert.Equal(t, "", result)
	})

	t.Run("GetFromEmptyMap", func(t *testing.T) {
		m := make(map[string]interface{})
		result, err := GetValue[string](m, "key1")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "not found in map")
		assert.Equal(t, "", result)
	})

	t.Run("IntermediateNotMap", func(t *testing.T) {
		m := map[string]interface{}{
			"level1": "not_a_map",
		}
		result, err := GetValue[string](m, "level1", "level2")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "the data type of level2 is wrong")
		assert.Equal(t, "", result)
	})

	t.Run("BooleanValue", func(t *testing.T) {
		m := map[string]interface{}{
			"bool_key": true,
		}
		result, err := GetValue[bool](m, "bool_key")
		assert.NoError(t, err)
		assert.Equal(t, true, result)
	})

	t.Run("FloatValue", func(t *testing.T) {
		m := map[string]interface{}{
			"float_key": 3.14,
		}
		result, err := GetValue[float64](m, "float_key")
		assert.NoError(t, err)
		assert.Equal(t, 3.14, result)
	})
}

// TestContainKey tests the key existence check.
func TestContainKey(t *testing.T) {
	t.Run("KeyExists", func(t *testing.T) {
		m := map[string]interface{}{
			"key1": "value1",
			"key2": "value2",
		}
		assert.True(t, ContainKey(m, "key1"))
		assert.True(t, ContainKey(m, "key2"))
	})

	t.Run("KeyDoesNotExist", func(t *testing.T) {
		m := map[string]interface{}{
			"key1": "value1",
		}
		assert.False(t, ContainKey(m, "nonexistent"))
	})

	t.Run("EmptyMap", func(t *testing.T) {
		m := make(map[string]interface{})
		assert.False(t, ContainKey(m, "key1"))
	})

	t.Run("NilMap", func(t *testing.T) {
		var nilMap map[string]interface{}
		assert.False(t, ContainKey(nilMap, "key1"))
	})

	t.Run("KeyWithEmptyStringValue", func(t *testing.T) {
		m := map[string]interface{}{
			"": "empty_key_value",
		}
		assert.True(t, ContainKey(m, ""))
	})
}

// TestPutIfAbsent tests the conditional value set.
func TestPutIfAbsent(t *testing.T) {
	t.Run("PutWhenKeyAbsent", func(t *testing.T) {
		m := map[string]interface{}{
			"key1": "value1",
		}
		PutIfAbsent(m, "new_value", "key2")
		assert.Equal(t, "new_value", m["key2"])
		assert.Equal(t, 2, len(m))
	})

	t.Run("DoNotPutWhenKeyExists", func(t *testing.T) {
		m := map[string]interface{}{
			"key1": "old_value",
		}
		PutIfAbsent(m, "new_value", "key1")
		assert.Equal(t, "old_value", m["key1"]) // The original value is kept.
		assert.Equal(t, 1, len(m))
	})

	t.Run("PutDifferentTypes", func(t *testing.T) {
		m := map[string]interface{}{
			"string_key": "string_value",
		}
		PutIfAbsent(m, 42, "int_key")
		PutIfAbsent(m, true, "bool_key")
		PutIfAbsent(m, 3.14, "float_key")

		assert.Equal(t, 42, m["int_key"])
		assert.Equal(t, true, m["bool_key"])
		assert.Equal(t, 3.14, m["float_key"])
		assert.Equal(t, 4, len(m))
	})

	t.Run("PutInEmptyMap", func(t *testing.T) {
		m := make(map[string]interface{})
		PutIfAbsent(m, "value", "key1")
		assert.Equal(t, "value", m["key1"])
		assert.Equal(t, 1, len(m))
	})

	t.Run("PutInNilMap", func(t *testing.T) {
		var nilMap map[string]interface{}
		// Writing into a nil map panics; that is the expected Go behavior and
		// the test verifies it.
		assert.Panics(t, func() {
			PutIfAbsent(nilMap, "value", "key1")
		})
	})

	t.Run("PutMultipleTimes", func(t *testing.T) {
		m := make(map[string]interface{})
		PutIfAbsent(m, "first", "key1")
		PutIfAbsent(m, "second", "key1") // Not overwritten.
		PutIfAbsent(m, "third", "key2")

		assert.Equal(t, "first", m["key1"])
		assert.Equal(t, "third", m["key2"])
		assert.Equal(t, 2, len(m))
	})
}

// BenchmarkGetValueWithDefault is the benchmark.
func BenchmarkGetValueWithDefault(b *testing.B) {
	m := map[string]interface{}{
		"level1": map[string]interface{}{
			"level2": map[string]interface{}{
				"level3": "deep_value",
			},
		},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = GetValueWithDefault(m, "default", "level1", "level2", "level3")
	}
}

// BenchmarkGetValue is the benchmark.
func BenchmarkGetValue(b *testing.B) {
	m := map[string]interface{}{
		"level1": map[string]interface{}{
			"level2": map[string]interface{}{
				"level3": "deep_value",
			},
		},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = GetValue[string](m, "level1", "level2", "level3")
	}
}

// BenchmarkMergeMap is the benchmark.
func BenchmarkMergeMap(b *testing.B) {
	target := map[string]interface{}{
		"key1": "value1",
		"key2": "value2",
	}
	source := map[string]interface{}{
		"key3": "value3",
		"key4": "value4",
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = MergeMap(target, source)
	}
}
