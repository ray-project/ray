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
	"fmt"
	"strings"
	"testing"

	"github.com/ray-project/ray/go/pkg/runtime/object"
	"github.com/vmihailenco/msgpack/v5"
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
	// Simulate Java/Python-serialized data (with 9-byte length header).
	// The cross-language format stores the payload length as a msgpack-encoded
	// integer at offset 0, padded to MessagePackOffset bytes, followed by the
	// msgpack payload. Here the payload is the msgpack fixstr "hello" (6 bytes),
	// so the length header is the msgpack encoding of 6 (a single byte 0x06).
	msgpackData := []byte{0xa5, 'h', 'e', 'l', 'l', 'o'} // MessagePack fixstr "hello"

	crossLangEncoded := make([]byte, MessagePackOffset+len(msgpackData))
	crossLangEncoded[0] = 0x06 // msgpack positive fixint encoding of length 6
	copy(crossLangEncoded[MessagePackOffset:], msgpackData)

	var result string
	err := NewMsgpackSerializer().Decode(crossLangEncoded, &result)
	if err != nil {
		t.Fatalf("Decode cross-language data failed: %v", err)
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

	err := s.EncodeToBuffer(obj, &buf, isCrossLanguageValue(obj))
	if err != nil {
		t.Fatalf("EncodeToBuffer failed: %v", err)
	}

	// Verify length header
	if len(buf) < MessagePackOffset {
		t.Fatalf("Buffer too short: %d", len(buf))
	}

	// Verify the length header encodes the payload length as a msgpack integer
	// (the cross-language format), rather than a raw big-endian uint64.
	headerDec := msgpack.NewDecoder(bytes.NewReader(buf[:MessagePackOffset]))
	declaredLen, err := headerDec.DecodeInt()
	if err != nil {
		t.Fatalf("Decode length header failed: %v", err)
	}
	if declaredLen != len(buf)-MessagePackOffset {
		t.Errorf("Header length %d != payload length %d", declaredLen, len(buf)-MessagePackOffset)
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

// TestErrorLengthHeaderMatchesCanonical is the cross-package drift guard for
// the 9-byte cross-language length header. The header embedded in a Go
// task-error payload by object.MarshalTaskExecutionErrorData must byte-equal
// what this package's canonical encodeLengthHeader produces for the same
// payload-section length. The object package keeps its own copy of the format
// (this package's helper is unexported and the package is internal), so this
// test is what actually detects divergence - object's own header test only
// re-encodes with its own encoder and cannot see a change here.
func TestErrorLengthHeaderMatchesCanonical(t *testing.T) {
	for _, n := range []int{0, 9, 26, 300} {
		data, err := object.MarshalTaskExecutionErrorData(strings.Repeat("x", n))
		if err != nil {
			t.Fatalf("MarshalTaskExecutionErrorData(%d) failed: %v", n, err)
		}
		if len(data) < MessagePackOffset {
			t.Fatalf("payload for n=%d too short: %d", n, len(data))
		}
		embedded := data[:MessagePackOffset]
		dec := msgpack.NewDecoder(bytes.NewReader(embedded))
		sectionLen, err := dec.DecodeInt()
		if err != nil {
			t.Fatalf("decode embedded length header for n=%d failed: %v", n, err)
		}
		want, err := encodeLengthHeader(sectionLen)
		if err != nil {
			t.Fatalf("encodeLengthHeader(%d) failed: %v", sectionLen, err)
		}
		if !bytes.Equal(want, embedded) {
			t.Fatalf("length header drift for n=%d: object % x, serializer % x",
				n, embedded, want)
		}
	}
}

func TestInvalidLengthHeader(t *testing.T) {
	s := NewMsgpackSerializer()

	// Create data with invalid length header (claims more data than available)
	invalidData := []byte{
		0xcc, 0xff, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // msgpack uint8 0xcc 0xff claims 255 bytes
		0xa5, 'h', 'e', 'l', 'l', 'o', // Only 6 bytes of actual data
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

// xlangStudent mirrors the C++ MSGPACK_DEFINE(name, age) struct used by the
// Go -> C++ cross-language driver: a fully-exported struct is a cross-language
// type, so it must be encoded positionally.
type xlangStudent struct {
	Name string `msgpack:"name"`
	Age  int    `msgpack:"age"`
}

// TestEncodeCrossLanguageStructIsPositionalArray proves that a cross-language
// struct is encoded as a msgpack array (matching the C++ MSGPACK_DEFINE
// layout), not as a msgpack map. Before the fix the encoder used the default
// map encoding and the C++ worker failed with "std::bad_cast" while converting
// the argument to its MSGPACK_DEFINE type.
func TestEncodeCrossLanguageStructIsPositionalArray(t *testing.T) {
	s := NewMsgpackSerializer()

	buf := make([]byte, 0, 1024)
	if err := s.EncodeToBuffer(xlangStudent{Name: "tom", Age: 20}, &buf,
		isCrossLanguageValue(xlangStudent{Name: "tom", Age: 20})); err != nil {
		t.Fatalf("EncodeToBuffer failed: %v", err)
	}
	payload := buf[MessagePackOffset:]
	if len(payload) == 0 {
		t.Fatal("empty payload")
	}
	// 0x92 is the msgpack fixarray header for a 2-element array; a map would be
	// 0x82 (fixmap of 2) or another fixmap/ map16/ map32 header.
	if payload[0] != 0x92 {
		t.Fatalf("cross-language struct encoded with header %#x, want 0x92 (2-element array)", payload[0])
	}

	// The array form must still decode back into the Go struct.
	var got xlangStudent
	if err := s.DecodeFromBuffer(buf, &got); err != nil {
		t.Fatalf("DecodeFromBuffer failed: %v", err)
	}
	if got != (xlangStudent{Name: "tom", Age: 20}) {
		t.Fatalf("round trip = %+v, want {tom 20}", got)
	}

	// The Encode (bytes.Buffer) path must agree with EncodeToBuffer.
	encoded, err := s.Encode(xlangStudent{Name: "tom", Age: 20})
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}
	if encoded[MessagePackOffset] != 0x92 {
		t.Fatalf("Encode produced header %#x, want 0x92 (2-element array)", encoded[MessagePackOffset])
	}
}
