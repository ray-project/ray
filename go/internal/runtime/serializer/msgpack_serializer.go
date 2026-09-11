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
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/vmihailenco/msgpack/v5"
)

// MsgpackSerializer implements MessagePack serialization.
// Provides low-level MessagePack encoding and decoding capabilities.
type MsgpackSerializer struct {
	registry *ExtensionRegistry
}

// MessagePackOffset is the 9-byte length header offset for cross-language compatibility.
// This matches Java's MESSAGE_PACK_OFFSET and C++'s XLANG_HEADER_LEN.
const MessagePackOffset = 9

// parseLengthHeader parses the 9-byte length header if present.
// Returns the data slice starting after the header, or the original data if no header.
// Returns an error if the header is present but invalid.
// This function eliminates code duplication across Decode, DecodeExtension, and DecodeFromBuffer.
func parseLengthHeader(data []byte) ([]byte, error) {
	if len(data) >= MessagePackOffset {
		// Read length from header (bytes [1:9] in big-endian format)
		msgpackLen := int(binary.BigEndian.Uint64(data[1:MessagePackOffset]))

		// Validate length
		if MessagePackOffset+msgpackLen > len(data) {
			return nil, errors.New("msgpack: invalid length header")
		}

		// Skip header, parse from MessagePackOffset
		return data[MessagePackOffset : MessagePackOffset+msgpackLen], nil
	}
	return data, nil
}

// NewMsgpackSerializer creates a new MsgpackSerializer.
func NewMsgpackSerializer() *MsgpackSerializer {
	return &MsgpackSerializer{
		registry: nil,
	}
}

// NewMsgpackSerializerWithRegistry creates a new MsgpackSerializer with an ExtensionRegistry.
func NewMsgpackSerializerWithRegistry(registry *ExtensionRegistry) *MsgpackSerializer {
	return &MsgpackSerializer{
		registry: registry,
	}
}

// SetRegistry sets the ExtensionRegistry for the serializer.
func (s *MsgpackSerializer) SetRegistry(registry *ExtensionRegistry) {
	s.registry = registry
}

// Encode serializes an object to bytes using MessagePack.
// Uses compact integer encoding for efficiency.
// Adds 9-byte length header for cross-language compatibility (Java/C++).
// If ExtensionRegistry is set, uses extension mechanism for language-specific types.
func (s *MsgpackSerializer) Encode(obj interface{}) ([]byte, error) {
	var buf bytes.Buffer
	enc := msgpack.NewEncoder(&buf)
	enc.UseCompactInts(true)

	if s.registry != nil {
		if packer := s.registry.FindPacker(obj); packer != nil {
			extData, err := s.packExtension(obj, packer)
			if err != nil {
				return nil, err
			}
			if _, err := buf.Write(extData); err != nil {
				return nil, err
			}
		} else {
			if err := enc.Encode(obj); err != nil {
				return nil, err
			}
		}
	} else {
		if err := enc.Encode(obj); err != nil {
			return nil, err
		}
	}

	msgpackBytes := buf.Bytes()

	// Build result with 9-byte length header
	totalLen := MessagePackOffset + len(msgpackBytes)
	result := make([]byte, totalLen)

	// Write length header
	result[0] = 0xcd
	binary.BigEndian.PutUint64(result[1:MessagePackOffset], uint64(len(msgpackBytes)))

	// Copy MessagePack data
	copy(result[MessagePackOffset:], msgpackBytes)

	return result, nil
}

// packExtension packs an object using the extension mechanism.
func (s *MsgpackSerializer) packExtension(obj interface{}, packer TypePacker) ([]byte, error) {
	var payloadBuf bytes.Buffer
	payloadEnc := msgpack.NewEncoder(&payloadBuf)
	payloadEnc.UseCompactInts(true)

	if err := packer.Pack(obj, payloadEnc); err != nil {
		return nil, fmt.Errorf("failed to pack extension type: %w", err)
	}

	payloadBytes := payloadBuf.Bytes()

	var extBuf bytes.Buffer
	extEnc := msgpack.NewEncoder(&extBuf)
	extEnc.UseCompactInts(true)

	if err := extEnc.EncodeExtHeader(LanguageSpecificTypeExtensionID, len(payloadBytes)); err != nil {
		return nil, fmt.Errorf("failed to encode extension header: %w", err)
	}

	if _, err := extEnc.Writer().Write(payloadBytes); err != nil {
		return nil, fmt.Errorf("failed to write extension data: %w", err)
	}

	return extBuf.Bytes(), nil
}

// EncodeToBuffer serializes an object to a pre-allocated buffer.
// This is more efficient for large objects as it avoids buffer copying.
// Adds 9-byte length header at the beginning of the buffer.
func (s *MsgpackSerializer) EncodeToBuffer(obj interface{}, buf *[]byte) error {
	// Reserve 9 bytes for length header
	*buf = (*buf)[:MessagePackOffset]

	enc := msgpack.NewEncoder(&byteSliceWriter{buf: buf})
	enc.UseCompactInts(true)
	startPos := len(*buf)

	if err := enc.Encode(obj); err != nil {
		return err
	}

	// Calculate actual MessagePack data length
	msgpackLen := len(*buf) - startPos

	// Write length header at the beginning
	var headerBuf [MessagePackOffset]byte // Stack-allocated array to avoid heap allocation
	headerBuf[0] = 0xcd                   // msgpack long format marker
	binary.BigEndian.PutUint64(headerBuf[1:MessagePackOffset], uint64(msgpackLen))
	copy((*buf)[:MessagePackOffset], headerBuf[:])

	return nil
}

// Decode deserializes bytes to an object using MessagePack.
// Supports 9-byte length header format for cross-language compatibility.
// If ExtensionRegistry is set with an unpacker, supports extension type decoding.
func (s *MsgpackSerializer) Decode(data []byte, target interface{}) error {
	if len(data) == 0 {
		return errors.New("msgpack: empty data")
	}

	var err error
	data, err = parseLengthHeader(data)
	if err != nil {
		return err
	}

	dec := msgpack.NewDecoder(bytes.NewReader(data))

	if s.registry != nil && s.registry.GetUnpacker() != nil {
		_, err := s.DecodeExtension(data)
		if err == nil {
			return nil
		}
	}

	return dec.Decode(target)
}

// DecodeExtension decodes extension type data using the registered unpacker.
// This method should be called when the caller expects extension type data.
func (s *MsgpackSerializer) DecodeExtension(data []byte) (interface{}, error) {
	if len(data) == 0 {
		return nil, errors.New("msgpack: empty data")
	}

	var err error
	data, err = parseLengthHeader(data)
	if err != nil {
		return nil, err
	}

	dec := msgpack.NewDecoder(bytes.NewReader(data))

	extID, _, err := dec.DecodeExtHeader()
	if err != nil {
		return nil, err
	}

	if extID != LanguageSpecificTypeExtensionID {
		return nil, fmt.Errorf("msgpack: unknown ext id=%d, expected %d", extID, LanguageSpecificTypeExtensionID)
	}

	if s.registry == nil || s.registry.GetUnpacker() == nil {
		return nil, errors.New("msgpack: no unpacker registered for extension type")
	}

	return s.registry.unpackExtension(dec)
}

// DecodeFromBuffer deserializes from a pre-allocated buffer.
// Consistent with Decode(), this method returns an error for empty data.
// Supports 9-byte length header format for cross-language compatibility.
func (s *MsgpackSerializer) DecodeFromBuffer(data []byte, target interface{}) error {
	if len(data) == 0 {
		return errors.New("msgpack: empty data")
	}

	var err error
	data, err = parseLengthHeader(data)
	if err != nil {
		return err
	}

	dec := msgpack.NewDecoder(bytes.NewReader(data))
	return dec.Decode(target)
}

// byteSliceWriter implements io.Writer interface for direct byte slice writing.
type byteSliceWriter struct {
	buf *[]byte
}

// Write appends data to the byte slice.
func (w *byteSliceWriter) Write(p []byte) (int, error) {
	*w.buf = append(*w.buf, p...)
	return len(p), nil
}
