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

func BenchmarkZeroCopy_Encode_SmallObject(b *testing.B) {
	s := NewZeroCopySerializer()
	obj := map[string]int{"key": 42}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := s.EncodeZeroCopy(obj)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkZeroCopy_Encode_LargeObject(b *testing.B) {
	s := NewZeroCopySerializer()

	// Create a large object (100KB+)
	largeMap := make(map[string]interface{})
	for i := 0; i < 10000; i++ {
		largeMap[formatKey(i)] = map[string]interface{}{
			"id":    i,
			"value": "some value data " + formatKey(i),
			"items": []int{i, i + 1, i + 2},
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := s.EncodeZeroCopy(largeMap)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkStandard_Encode_SmallObject(b *testing.B) {
	s := NewMsgpackSerializer()
	obj := map[string]int{"key": 42}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := s.Encode(obj)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkStandard_Encode_LargeObject(b *testing.B) {
	s := NewMsgpackSerializer()

	// Create a large object (100KB+)
	largeMap := make(map[string]interface{})
	for i := 0; i < 10000; i++ {
		largeMap[formatKey(i)] = map[string]interface{}{
			"id":    i,
			"value": "some value data " + formatKey(i),
			"items": []int{i, i + 1, i + 2},
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := s.Encode(largeMap)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkZeroCopy_Decode_SmallObject(b *testing.B) {
	s := NewZeroCopySerializer()
	obj := map[string]int{"key": 42}
	data, _ := s.EncodeZeroCopy(obj)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var result interface{}
		err := s.DecodeZeroCopy(data, &result)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkZeroCopy_Decode_LargeObject(b *testing.B) {
	s := NewZeroCopySerializer()

	// Create a large object
	largeMap := make(map[string]interface{})
	for i := 0; i < 10000; i++ {
		largeMap[formatKey(i)] = map[string]interface{}{
			"id":    i,
			"value": "some value data " + formatKey(i),
			"items": []int{i, i + 1, i + 2},
		}
	}

	data, _ := s.EncodeZeroCopy(largeMap)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var result interface{}
		err := s.DecodeZeroCopy(data, &result)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkStandard_Decode_SmallObject(b *testing.B) {
	s := NewMsgpackSerializer()
	obj := map[string]int{"key": 42}
	data, _ := s.Encode(obj)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var result interface{}
		err := s.Decode(data, &result)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkStandard_Decode_LargeObject(b *testing.B) {
	s := NewMsgpackSerializer()

	// Create a large object
	largeMap := make(map[string]interface{})
	for i := 0; i < 10000; i++ {
		largeMap[formatKey(i)] = map[string]interface{}{
			"id":    i,
			"value": "some value data " + formatKey(i),
			"items": []int{i, i + 1, i + 2},
		}
	}

	data, _ := s.Encode(largeMap)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var result interface{}
		err := s.Decode(data, &result)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkZeroCopy_EncodeToBuffer(b *testing.B) {
	s := NewZeroCopySerializer()
	obj := map[string]interface{}{
		"data": make([]byte, 10240), // 10KB
		"meta": map[string]int{"count": 100},
	}

	buf := make([]byte, 0, DefaultInitialBufferSize)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf = buf[:0] // Reset but retain capacity
		err := s.EncodeToBuffer(obj, &buf)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// formatKey formats an integer as a string key
func formatKey(i int) string {
	return "key_" + itoa(i)
}

// Simple integer to string conversion for benchmarking
func itoa(n int) string {
	if n == 0 {
		return "0"
	}

	var buf [20]byte
	i := len(buf)

	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}

	return string(buf[i:])
}
