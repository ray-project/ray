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
	"sync"

	"github.com/vmihailenco/msgpack/v5"
)

// LanguageSpecificTypeExtensionID is the extension ID for language-specific types.
// This matches Java's LANGUAGE_SPECIFIC_TYPE_EXTENSION_ID (101).
// Extension ID 101 is reserved for language-specific serialization in Ray cross-language protocol.
const LanguageSpecificTypeExtensionID = 101

// TypePacker defines the interface for packing language-specific types.
// Implementations determine how to serialize Go-specific objects that cannot be
// directly represented in MessagePack's cross-language format.
type TypePacker interface {
	// CanPack returns true if this packer can handle the given object.
	CanPack(obj interface{}) bool
	// Pack serializes the object using the provided encoder.
	Pack(obj interface{}, enc *msgpack.Encoder) error
}

// TypeUnpacker defines the interface for unpacking language-specific types.
// Implementations determine how to deserialize objects that were serialized
// with language-specific extensions.
type TypeUnpacker interface {
	// Unpack deserializes the object from the provided decoder.
	Unpack(dec *msgpack.Decoder) (interface{}, error)
}

// ExtensionRegistry manages extension type packers and unpackers.
// Thread-safe for concurrent access.
type ExtensionRegistry struct {
	mu       sync.RWMutex
	packers  []TypePacker
	unpacker TypeUnpacker
}

// NewExtensionRegistry creates a new ExtensionRegistry.
func NewExtensionRegistry() *ExtensionRegistry {
	return &ExtensionRegistry{
		packers:  make([]TypePacker, 0),
		unpacker: nil,
	}
}

// RegisterPacker registers a type packer for extension serialization.
// Packers are checked in registration order during serialization.
func (r *ExtensionRegistry) RegisterPacker(packer TypePacker) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.packers = append(r.packers, packer)
}

// SetUnpacker sets the type unpacker for extension deserialization.
// Only one unpacker is supported at a time.
func (r *ExtensionRegistry) SetUnpacker(unpacker TypeUnpacker) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.unpacker = unpacker
}

// FindPacker finds the first packer that can handle the given object.
// Returns nil if no suitable packer is found.
func (r *ExtensionRegistry) FindPacker(obj interface{}) TypePacker {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, packer := range r.packers {
		if packer.CanPack(obj) {
			return packer
		}
	}
	return nil
}

// GetUnpacker returns the registered unpacker.
func (r *ExtensionRegistry) GetUnpacker() TypeUnpacker {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.unpacker
}

// GoTypePacker is the default packer for Go-specific types.
// It handles types that are not cross-language compatible.
type GoTypePacker struct{}

// CanPack returns true for non-cross-language types.
func (p *GoTypePacker) CanPack(obj interface{}) bool {
	// Don't pack cross-language types with extension
	return !IsCrossLanguageType(obj)
}

// Pack serializes Go-specific objects using standard MessagePack.
func (p *GoTypePacker) Pack(obj interface{}, enc *msgpack.Encoder) error {
	// For now, use standard MessagePack encoding
	// Future enhancement: could use gob or other Go-specific serialization
	return enc.Encode(obj)
}

// GoTypeUnpacker is the default unpacker for Go-specific types.
type GoTypeUnpacker struct{}

// Unpack deserializes Go-specific objects from MessagePack data.
func (u *GoTypeUnpacker) Unpack(dec *msgpack.Decoder) (interface{}, error) {
	var result interface{}
	err := dec.Decode(&result)
	return result, err
}

// unpackExtension unpacks an object using the extension mechanism.
// This is used internally by MsgpackSerializer during decoding.
func (r *ExtensionRegistry) unpackExtension(dec *msgpack.Decoder) (interface{}, error) {
	unpacker := r.GetUnpacker()
	if unpacker == nil {
		// No unpacker registered, use standard decoding
		var result interface{}
		err := dec.Decode(&result)
		return result, err
	}
	return unpacker.Unpack(dec)
}

// ExtensionData wraps extension data with metadata.
type ExtensionData struct {
	Language   string      `msgpack:"language"`
	DataType   string      `msgpack:"data_type"`
	Serialized interface{} `msgpack:"serialized"`
}

// NewExtensionData creates extension data for Go-specific types.
func NewExtensionData(data interface{}) *ExtensionData {
	return &ExtensionData{
		Language:   "GO",
		DataType:   fmt.Sprintf("%T", data),
		Serialized: data,
	}
}

// EncodeExtension encodes an object with extension header.
// This is used for explicit extension encoding when needed.
func (r *ExtensionRegistry) EncodeExtension(obj interface{}) ([]byte, error) {
	var buf bytes.Buffer
	enc := msgpack.NewEncoder(&buf)
	enc.UseCompactInts(true)

	// Write extension header
	extData := NewExtensionData(obj)
	if err := enc.Encode(extData); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

// DecodeExtension decodes an object from extension format.
func (r *ExtensionRegistry) DecodeExtension(data []byte) (interface{}, error) {
	dec := msgpack.NewDecoder(bytes.NewReader(data))
	var extData ExtensionData
	if err := dec.Decode(&extData); err != nil {
		return nil, err
	}

	// Verify language
	if extData.Language != "GO" {
		return nil, fmt.Errorf("unsupported extension language: %s", extData.Language)
	}

	return extData.Serialized, nil
}
