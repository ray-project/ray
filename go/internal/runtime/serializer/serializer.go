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
//
// Two-tier encoding strategy: when the estimated payload fits the tiered
// buffer pool (<= MaxBufferSize), the payload is encoded into a pooled buffer.
// Larger payloads are freshly allocated, matching Python msgpack.dumps / C++
// msgpack::sbuffer semantics.
//
// Note: EstimateBufferSize is heuristic (structs default to 4KB and nested
// []byte/string leaves are not counted recursively), so a struct/map wrapping
// large byte fields may be under-estimated and routed to the pooled path,
// where EncodeToBuffer grows the buffer via append. In that case the payload
// no longer lives in the pooled buffer and is treated as a fresh allocation
// instead (see below); the result is still correct, only slightly less optimal
// than a single fresh allocation.
func (s *Serializer) Serialize(obj interface{}) (*NativeRayObject, error) {
	// Determine metadata type based on object type
	metadata := s.determineMetadata(obj)

	// Get contained object IDs from context
	containedIDs := s.contextManager.getAndClearContainedObjectIDs()

	// Convert contained object IDs to binary format
	containedObjectIds := make([][]byte, len(containedIDs))
	for i, id := range containedIDs {
		containedObjectIds[i] = id.Binary()
	}

	// Two-tier encoding: pool-backed for small objects, fresh allocation for
	// large ones (aligned with Python/C++/Java which always allocate fresh).
	nativeObj := &NativeRayObject{
		Metadata:           metadata,
		ContainedObjectIds: containedObjectIds,
	}
	est := EstimateBufferSize(obj)
	if est <= MaxBufferSize {
		buf := GetBuffer(est)
		// Keep a reference to the pooled buffer. EncodeToBuffer appends into
		// *buf; if the payload outgrows the pooled capacity, append reallocates
		// and *buf points at a fresh (non-pool-aligned) array while the pooled
		// buffer is dropped. Holding orig lets us still return the pooled
		// buffer to the pool (slot + accounting) in that case.
		orig := buf
		if err := s.msgpack.EncodeToBuffer(obj, &buf); err != nil {
			PutBuffer(orig)
			return nil, fmt.Errorf("failed to serialize object: %w", err)
		}
		nativeObj.Data = buf
		if cap(buf) != cap(orig) {
			// Under-estimated: the payload no longer lives in the pooled
			// buffer. Return the original pooled buffer via orig so the pool
			// slot and its accounting are preserved, and treat the grown
			// payload as a fresh allocation: it is not marked pool-allocated,
			// so Close() leaves it to the GC.
			PutBuffer(orig)
		} else {
			// The payload lives in the pooled buffer (or a slice of it), so
			// mark it pool-allocated for Close() to return it to the pool.
			nativeObj.MarkDataFromPool()
		}
	} else {
		// Large objects: allocate a single fresh buffer and encode into it,
		// avoiding the bytes.Buffer growth + make/copy double buffer inside
		// Encode, so large-object Put is one encoding copy + one memcpy into the
		// object store (matching Python msgpack.dumps / C++ msgpack::sbuffer).
		// Kept out of the pool (fresh allocation per call, like the other
		// runtimes); the estimate already carries 10-20% msgpack overhead.
		buf := make([]byte, MessagePackOffset, est)
		if err := s.msgpack.EncodeToBuffer(obj, &buf); err != nil {
			return nil, fmt.Errorf("failed to serialize object: %w", err)
		}
		nativeObj.Data = buf
	}

	return nativeObj, nil
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
