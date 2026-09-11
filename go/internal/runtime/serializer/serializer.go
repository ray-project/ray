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
	"fmt"
	"reflect"

	"github.com/ray-project/ray/go/pkg/ids"
	"github.com/ray-project/ray/go/pkg/runtime/object"
)

// Serializer provides a facade for serialization operations.
// Similar to Java's io.ray.runtime.serializer.Serializer
//
// This struct combines:
// - MsgpackSerializer for actual serialization
// - ContextManager for tracking nested object references
// - ExtensionRegistry for language-specific type handling
type Serializer struct {
	msgpack        *MsgpackSerializer
	contextManager *ContextManager
	registry       *ExtensionRegistry
}

// NewSerializer creates a new Serializer.
func NewSerializer() *Serializer {
	s := &Serializer{
		msgpack:        NewMsgpackSerializer(),
		contextManager: NewContextManager(),
		registry:       NewExtensionRegistry(),
	}
	// Register default Go type packer/unpacker
	s.registry.RegisterPacker(&GoTypePacker{})
	s.registry.SetUnpacker(&GoTypeUnpacker{})
	return s
}

// Encode serializes an object to bytes.
func (s *Serializer) Encode(obj interface{}) ([]byte, error) {
	return s.msgpack.Encode(obj)
}

// Decode deserializes bytes to object.
func (s *Serializer) Decode(data []byte, target interface{}) error {
	return s.msgpack.Decode(data, target)
}

// Serialize serializes an object to NativeRayObject.
// This method is the main entry point for serialization in the object package.
func (s *Serializer) Serialize(obj interface{}) (*NativeRayObject, error) {
	// Serialize to bytes using msgpack
	data, err := s.msgpack.Encode(obj)
	if err != nil {
		return nil, fmt.Errorf("failed to serialize object: %w", err)
	}

	// Determine metadata type based on object type
	metadata := s.determineMetadata(obj)

	// Get contained object IDs from context
	containedIDs := s.contextManager.getAndClearContainedObjectIDs()

	// Convert contained object IDs to binary format
	containedObjectIds := make([][]byte, len(containedIDs))
	for i, id := range containedIDs {
		containedObjectIds[i] = id.Binary()
	}

	// Convert to NativeRayObject
	return &NativeRayObject{
		Data:               data,
		Metadata:           metadata,
		ContainedObjectIds: containedObjectIds,
	}, nil
}

// determineMetadata determines the metadata type for the given object.
// Returns MetadataTypeCrossLanguage for cross-language types (e.g., actors, futures),
// or MetadataTypeGo for Go-specific types.
func (s *Serializer) determineMetadata(obj interface{}) []byte {
	if isCrossLanguageTypeRecursive(reflect.TypeOf(obj)) {
		return []byte(object.MetadataTypeCrossLanguage)
	}
	return []byte(object.MetadataTypeGo)
}

// Deserialize deserializes NativeRayObject to the original object with type information.
// The objectType parameter is used for type-safe deserialization, similar to Java's Serializer.decode(objectType).
// For now, this parameter is logged but not used in the actual deserialization logic.
// Future enhancements may use objectType for validation or specialized deserialization paths.
func (s *Serializer) Deserialize(nativeObj *NativeRayObject, objectID *ids.ObjectID, objectType string) (interface{}, error) {
	if nativeObj == nil {
		return nil, fmt.Errorf("native object is nil")
	}

	// Deserialize from bytes using msgpack
	// Decode to interface{} and let the caller handle type conversion
	var result interface{}
	if err := s.msgpack.Decode(nativeObj.Data, &result); err != nil {
		return nil, fmt.Errorf("failed to deserialize object: %w", err)
	}

	return result, nil
}

// DeserializeTo deserializes NativeRayObject directly to a target type.
// This avoids the issue of msgpack decoding small integers as int8/uint8.
//
// Parameters:
//   - nativeObj: The native ray object containing serialized data
//   - target: A pointer to the target type (e.g., &result where result is T)
//
// Returns:
//   - error: Any error during deserialization
func (s *Serializer) DeserializeTo(nativeObj *NativeRayObject, target interface{}) error {
	if nativeObj == nil {
		return fmt.Errorf("native object is nil")
	}

	if err := s.msgpack.Decode(nativeObj.Data, target); err != nil {
		return fmt.Errorf("failed to deserialize object: %w", err)
	}

	return nil
}

// Context management methods delegate to ContextManager

// AddContainedObjectID adds an object ID to current context.
func (s *Serializer) AddContainedObjectID(objectID ids.ObjectID) {
	s.contextManager.addContainedObjectID(objectID)
}

// GetAndClearContainedObjectIDs gets and clears contained object IDs from current context.
func (s *Serializer) GetAndClearContainedObjectIDs() []ids.ObjectID {
	return s.contextManager.getAndClearContainedObjectIDs()
}

// SetOuterObjectID sets the outer object ID in current context.
func (s *Serializer) SetOuterObjectID(objectID ids.ObjectID) {
	s.contextManager.setOuterObjectID(objectID)
}

// GetOuterObjectID gets the outer object ID from current context.
func (s *Serializer) GetOuterObjectID() ids.ObjectID {
	return s.contextManager.getOuterObjectID()
}

// ResetOuterObjectID resets the outer object ID in current context.
func (s *Serializer) ResetOuterObjectID() {
	s.contextManager.resetOuterObjectID()
}

// GetContext returns the current serialization context.
func (s *Serializer) GetContext() *SerializationContext {
	return s.contextManager.getOrCreateContext()
}

// PutContext returns the context to the pool.
func (s *Serializer) PutContext() {
	s.contextManager.putContext()
}

// ReturnContext returns the SerializationContext to the pool.
// It clears the provided context data and returns it to the pool for reuse.
//
// Note: The ctx parameter is used for explicit context clearing. If ctx is nil,
// the method will still clear the current goroutine's context.
// This method is kept for backward compatibility; prefer using PutContext() for new code.
func (s *Serializer) ReturnContext(ctx *SerializationContext) {
	s.contextManager.putContextWithClear(ctx)
}
