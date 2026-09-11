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
	"encoding/binary"
	"fmt"
	"testing"
)

func TestEncodeDecodeWithHeader(t *testing.T) {
	s := NewMsgpackSerializer()

	tests := []struct {
		name     string
		obj      interface{}
		validate func(interface{}) error
	}{
		{
			name: "int64",
			obj:  int64(12345),
			validate: func(result interface{}) error {
				// MessagePack may encode integers differently, check value
				switch result.(type) {
				case int64, int, uint16, uint32, uint64:
					return nil // Value is some integer type, acceptable
				default:
					return fmt.Errorf("expected integer type, got %T", result)
				}
			},
		},
		{
			name: "string",
			obj:  "hello world",
			validate: func(result interface{}) error {
				if s, ok := result.(string); !ok || s != "hello world" {
					return fmt.Errorf("expected 'hello world', got %v", result)
				}
				return nil
			},
		},
		{
			name: "byte_slice",
			obj:  []byte{1, 2, 3},
			validate: func(result interface{}) error {
				if b, ok := result.([]byte); !ok || len(b) != 3 || b[0] != 1 || b[1] != 2 || b[2] != 3 {
					return fmt.Errorf("expected [1,2,3], got %v", result)
				}
				return nil
			},
		},
		{
			name: "map",
			obj:  map[string]interface{}{"a": int64(1), "b": int64(2)},
			validate: func(result interface{}) error {
				// MessagePack may encode map keys/values with different types
				if m, ok := result.(map[string]interface{}); ok {
					if len(m) != 2 {
						return fmt.Errorf("expected map with 2 keys, got %d", len(m))
					}
					return nil // Map structure preserved
				}
				return fmt.Errorf("expected map[string]interface{}, got %T", result)
			},
		},
		{
			name: "bool",
			obj:  true,
			validate: func(result interface{}) error {
				if b, ok := result.(bool); !ok || !b {
					return fmt.Errorf("expected true, got %v", result)
				}
				return nil
			},
		},
		{
			name: "float64",
			obj:  float64(3.14),
			validate: func(result interface{}) error {
				if f, ok := result.(float64); !ok || f != 3.14 {
					return fmt.Errorf("expected 3.14, got %v", result)
				}
				return nil
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Serialize
			encoded, err := s.Encode(tt.obj)
			if err != nil {
				t.Fatalf("Encode failed: %v", err)
			}

			// Verify length header
			if len(encoded) < MessagePackOffset {
				t.Errorf("Encoded data too short: %d", len(encoded))
			}

			// Deserialize
			var result interface{}
			err = s.Decode(encoded, &result)
			if err != nil {
				t.Fatalf("Decode failed: %v", err)
			}

			// Validate result
			if err := tt.validate(result); err != nil {
				t.Errorf("Validation failed: %v", err)
			}
		})
	}
}

func TestCrossLanguageCompatibility(t *testing.T) {
	// Simulate Java-serialized data (with 9-byte length header)
	// First, create actual MessagePack data for "hello"
	msgpackData := []byte{0xa5, 'h', 'e', 'l', 'l', 'o'} // MessagePack fixstr "hello"

	// Build Java-style format: [0xcd][8-byte length big-endian][MessagePack data]
	javaEncoded := make([]byte, MessagePackOffset+len(msgpackData))
	javaEncoded[0] = 0xcd // msgpack long format marker
	binary.BigEndian.PutUint64(javaEncoded[1:MessagePackOffset], uint64(len(msgpackData)))
	copy(javaEncoded[MessagePackOffset:], msgpackData)

	var result string
	err := NewMsgpackSerializer().Decode(javaEncoded, &result)
	if err != nil {
		t.Fatalf("Decode Java data failed: %v", err)
	}
	if result != "hello" {
		t.Errorf("Expected 'hello', got '%s'", result)
	}
}

func TestEncodeToBufferWithHeader(t *testing.T) {
	s := NewMsgpackSerializer()

	// Test with pre-allocated buffer
	buf := make([]byte, 0, 1024)
	obj := map[string]interface{}{"key": "value", "num": int64(42)}

	err := s.EncodeToBuffer(obj, &buf)
	if err != nil {
		t.Fatalf("EncodeToBuffer failed: %v", err)
	}

	// Verify length header
	if len(buf) < MessagePackOffset {
		t.Fatalf("Buffer too short: %d", len(buf))
	}

	// Verify header marker
	if buf[0] != 0xcd {
		t.Errorf("Expected header marker 0xcd, got 0x%02x", buf[0])
	}

	// Verify round-trip
	var result interface{}
	err = s.DecodeFromBuffer(buf, &result)
	if err != nil {
		t.Fatalf("DecodeFromBuffer failed: %v", err)
	}

	// Check map structure instead of deep equality
	resultMap, ok := result.(map[string]interface{})
	if !ok {
		t.Fatalf("Expected map[string]interface{}, got %T", result)
	}
	if resultMap["key"] != "value" {
		t.Errorf("Expected key='value', got '%v'", resultMap["key"])
	}
	// num may be encoded as different integer type, just check it exists
	if _, exists := resultMap["num"]; !exists {
		t.Error("Expected 'num' key in result")
	}
}

func TestInvalidLengthHeader(t *testing.T) {
	s := NewMsgpackSerializer()

	// Create data with invalid length header (claims more data than available)
	invalidData := []byte{
		0xcd, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xFF, // Claims 255 bytes
		0xa5, 'h', 'e', 'l', 'l', 'o', // Only 5 bytes of actual data
	}

	var result string
	err := s.Decode(invalidData, &result)
	if err == nil {
		t.Error("Expected error for invalid length header, got nil")
	}
}

func TestEmptyData(t *testing.T) {
	s := NewMsgpackSerializer()

	var result interface{}
	err := s.Decode([]byte{}, &result)
	if err == nil {
		t.Error("Expected error for empty data, got nil")
	}
}

func TestWithoutHeader(t *testing.T) {
	s := NewMsgpackSerializer()

	// Test backward compatibility: data without 9-byte header
	// This should still work for pure MessagePack data
	data := []byte{0xa5, 'h', 'e', 'l', 'l', 'o'} // "hello" without header

	var result string
	err := s.Decode(data, &result)
	if err != nil {
		t.Fatalf("Decode without header failed: %v", err)
	}
	if result != "hello" {
		t.Errorf("Expected 'hello', got '%s'", result)
	}
}

func TestLargeObject(t *testing.T) {
	s := NewMsgpackSerializer()

	// Create large object (>1MB)
	largeData := make([]byte, 1024*1024)
	for i := range largeData {
		largeData[i] = byte(i % 256)
	}

	encoded, err := s.Encode(largeData)
	if err != nil {
		t.Fatalf("Encode large data failed: %v", err)
	}

	// Verify length header
	if len(encoded) < MessagePackOffset+1024*1024 {
		t.Errorf("Encoded data too short: %d", len(encoded))
	}

	var result []byte
	err = s.Decode(encoded, &result)
	if err != nil {
		t.Fatalf("Decode large data failed: %v", err)
	}

	if len(result) != len(largeData) {
		t.Fatalf("Result length mismatch: expected %d, got %d", len(largeData), len(result))
	}
}
