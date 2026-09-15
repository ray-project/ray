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
	"github.com/ray-project/ray/go/pkg/ids"
)

// Serializer defines the interface for object serialization.
// This interface is implemented by concrete serializers in the serializer package.
// The interface allows different serialization implementations to be used
// interchangeably, following the Dependency Inversion Principle.
type Serializer interface {
	// Serialize serializes an object to NativeRayObject.
	// This method only performs serialization, it does NOT write to object store.
	//
	// Parameters:
	//   - obj: the object to serialize
	//
	// Returns:
	//   - *NativeRayObject: the serialized object
	//   - error: any error encountered during serialization
	//
	// Goroutine safety: Implementations should maintain goroutine-local context
	// for tracking nested object references, so this method should be safe to call concurrently.
	Serialize(obj interface{}) (*NativeRayObject, error)

	// Deserialize deserializes NativeRayObject to the original object with type information.
	// The objectType parameter is used for type-safe deserialization, similar to Java's Serializer.decode(objectType).
	//
	// Parameters:
	//   - nativeObj: the serialized object to deserialize
	//   - objectID: the object ID for error reporting
	//   - objectType: the expected object type for type checking
	//
	// Returns:
	//   - interface{}: the deserialized object
	//   - error: any error encountered during deserialization
	Deserialize(nativeObj *NativeRayObject, objectID *ids.ObjectID, objectType string) (interface{}, error)

	// DeserializeTo deserializes NativeRayObject directly to a target type.
	// This avoids the issue of msgpack decoding small integers as int8/uint8.
	//
	// Parameters:
	//   - nativeObj: the serialized object to deserialize
	//   - target: a pointer to the target type (e.g., &result where result is T)
	//
	// Returns:
	//   - error: any error encountered during deserialization
	DeserializeTo(nativeObj *NativeRayObject, target interface{}) error

	// Context management methods for tracking nested object references

	// AddContainedObjectID adds an object ID to the current serialization context.
	// This is used to track nested object references during serialization.
	AddContainedObjectID(objectID ids.ObjectID)

	// GetAndClearContainedObjectIDs gets and clears contained object IDs from the current context.
	// This should be called after serialization to retrieve nested object references.
	GetAndClearContainedObjectIDs() []ids.ObjectID

	// SetOuterObjectID sets the outer object ID in the current context.
	// This is used for nested object tracking.
	SetOuterObjectID(objectID ids.ObjectID)

	// GetOuterObjectID gets the outer object ID from the current context.
	GetOuterObjectID() ids.ObjectID

	// ResetOuterObjectID resets the outer object ID in the current context.
	ResetOuterObjectID()

	// Buffer management methods

	// EstimateBufferSize estimates the buffer size needed to serialize an object.
	// This is a heuristic-based estimation to optimize initial buffer allocation.
	EstimateBufferSize(obj interface{}) int

	// GetBuffer gets a buffer from the pool with at least the requested capacity.
	GetBuffer(size int) []byte

	// PutBuffer returns a buffer to the pool.
	PutBuffer(buf []byte)

	// IsCrossLanguageType checks if an object type requires cross-language serialization.
	IsCrossLanguageType(obj interface{}) bool
}
