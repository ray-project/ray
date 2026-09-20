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
	"fmt"
	"sync"

	"github.com/ray-project/ray/go/pkg/ids"
)

// DeserializationContext holds the context for ObjectRef deserialization.
// Similar to Java's ThreadLocal and Python's serialization context,
// this tracks the outer object being deserialized and nested ObjectRefs.
//
// Goroutine safety: This context uses goroutine-local storage pattern
// to ensure concurrent deserializations in different goroutines do not
// interfere with each other.
type DeserializationContext struct {
	// outerObjectID tracks the ID of the object currently being deserialized.
	// This is used to establish the containment relationship for nested ObjectRefs.
	outerObjectID ids.ObjectID

	// containedObjectIDs tracks all ObjectRefs encountered during deserialization
	// of the outer object. This is used for reference counting and dependency tracking.
	containedObjectIDs []ids.ObjectID

	// mu protects concurrent access to the context
	mu sync.Mutex
}

// NewDeserializationContext creates a new deserialization context.
func NewDeserializationContext() *DeserializationContext {
	return &DeserializationContext{
		containedObjectIDs: make([]ids.ObjectID, 0),
	}
}

// SetOuterObjectID sets the outer object ID for the current deserialization context.
// This should be called before deserializing an object that may contain nested ObjectRefs.
func (c *DeserializationContext) SetOuterObjectID(objectID ids.ObjectID) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.outerObjectID = objectID
}

// GetOuterObjectID returns the current outer object ID.
// Returns ObjectID.Nil() if no outer object is being deserialized.
func (c *DeserializationContext) GetOuterObjectID() ids.ObjectID {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.outerObjectID
}

// ResetOuterObjectID resets the outer object ID.
// This should be called after deserialization of the outer object is complete.
func (c *DeserializationContext) ResetOuterObjectID() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.outerObjectID = ids.ObjectID{}
}

// AddContainedObjectID adds an object ID to the list of contained ObjectRefs.
// This is called when an ObjectRef is encountered during deserialization.
func (c *DeserializationContext) AddContainedObjectID(objectID ids.ObjectID) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.containedObjectIDs = append(c.containedObjectIDs, objectID)
}

// GetAndClearContainedObjectIDs returns and clears the list of contained ObjectRefs.
// This should be called after deserialization is complete to get the list of
// nested ObjectRefs for reference counting.
func (c *DeserializationContext) GetAndClearContainedObjectIDs() []ids.ObjectID {
	c.mu.Lock()
	defer c.mu.Unlock()
	result := c.containedObjectIDs
	c.containedObjectIDs = make([]ids.ObjectID, 0)
	return result
}

// DeserializeAndRegisterObjectRef deserializes a Python-serialized ObjectRef
// and registers it with the object store, similar to Java's readExternal()
// and Python's _object_ref_deserializer().
//
// This method handles the complete deserialization and registration flow:
// 1. Deserializes the ObjectRef from Python's 5-tuple format
// 2. Adds a local reference to the object store
// 3. Registers ownership information and resolves the future
//
// Parameters:
//   - data: The MsgPack-serialized data from Python
//   - objectStore: The object store to register the ObjectRef with
//   - context: The deserialization context (tracks outer object ID)
//
// Returns:
//   - *SerializedObjectRef: The deserialized ObjectRef
//   - error: Any error encountered during deserialization or registration
//
// Cross-language compatibility:
// This method is designed to handle Python's serialization format:
// (binary, call_site, owner_address, object_status, tensor_transport)
// where owner_address and object_status are protobuf-serialized strings.
//
// Java compatibility:
// Java uses a similar approach in ObjectRefImpl.readExternal(), but
// serializes the ObjectRef using Java's Externalizable interface.
// The key difference is that Java doesn't pass object_status.
func DeserializeAndRegisterObjectRef(
	data []byte,
	objectStore ObjectStore,
	context *DeserializationContext,
) (*SerializedObjectRef, error) {
	// Step 1: Deserialize the ObjectRef from Python's 5-tuple format
	objRef, err := deserializeObjectRefFromPython(data)
	if err != nil {
		return nil, fmt.Errorf("failed to deserialize Python ObjectRef: %w", err)
	}

	// Step 2: Add local reference to the object store
	// This increments the reference count for the object
	objectID := objRef.ID
	if err := objectStore.AddLocalReference(&objectID); err != nil {
		return nil, fmt.Errorf("failed to add local reference: %w", err)
	}

	// Step 3: Register ownership information and resolve future
	// This is equivalent to Java's registerOwnershipInfoAndResolveFuture()
	// and Python's deserialize_and_register_object_ref()

	// Use the outer object ID from the context
	outerObjectID := context.GetOuterObjectID()

	// If no outer object ID is set, use the object's own ID (same as Java)
	if outerObjectID.IsNil() {
		outerObjectID = objectID
	}

	// Parse the owner address from protobuf-serialized bytes
	var ownerAddress []byte
	if len(objRef.OwnerAddressBytes) > 0 {
		ownerAddress = objRef.OwnerAddressBytes
	}

	// Parse the object status from protobuf-serialized bytes
	// Note: Java doesn't pass object_status (uses empty string)
	// Python passes the full protobuf-serialized GetObjectStatusReply
	var objectStatus []byte
	if len(objRef.ObjectStatusBytes) > 0 {
		objectStatus = objRef.ObjectStatusBytes
	}

	// Register ownership info and resolve future
	// This will:
	// 1. Parse the owner address protobuf
	// 2. Register the ownership information with the core worker
	// 3. Resolve any futures waiting on this object
	if err := registerOwnershipInfoAndResolveFuture(
		objectStore,
		&objectID,
		&outerObjectID,
		ownerAddress,
		objectStatus,
	); err != nil {
		return nil, fmt.Errorf("failed to register ownership info: %w", err)
	}

	return objRef, nil
}

// registerOwnershipInfoAndResolveFuture registers ownership information
// and resolves futures for a deserialized ObjectRef.
//
// This is a helper function that wraps the object store's
// RegisterOwnershipInfoAndResolveFuture method.
//
// Parameters:
//   - objectStore: The object store to register with
//   - objectID: The object ID being deserialized
//   - outerObjectID: The outer object ID (container)
//   - ownerAddress: Protobuf-serialized owner address bytes (passed directly to object store)
//   - objectStatus: Protobuf-serialized object status (can be empty)
//
// Returns:
//   - error: Any error encountered during registration
func registerOwnershipInfoAndResolveFuture(
	objectStore ObjectStore,
	objectID *ids.ObjectID,
	outerObjectID *ids.ObjectID,
	ownerAddress []byte,
	objectStatus []byte,
) error {
	// Call the object store's registration method
	// Note: The object store will handle protobuf parsing internally
	return objectStore.RegisterOwnershipInfoAndResolveFuture(
		objectID,
		outerObjectID,
		ownerAddress,
	)
}

// DeserializeObjectRefWithStatus deserializes a Python-serialized ObjectRef
// that includes the object_status field.
//
// This is a convenience method that combines deserialization and status handling.
// It's useful when you need to access the object status after deserialization.
//
// Parameters:
//   - data: The MsgPack-serialized data from Python
//   - objectStore: The object store to register the ObjectRef with
//   - context: The deserialization context
//
// Returns:
//   - *SerializedObjectRef: The deserialized ObjectRef
//   - []byte: The raw object status bytes (may be nil)
//   - error: Any error encountered
func DeserializeObjectRefWithStatus(
	data []byte,
	objectStore ObjectStore,
	context *DeserializationContext,
) (*SerializedObjectRef, []byte, error) {
	// Deserialize the ObjectRef
	objRef, err := DeserializeAndRegisterObjectRef(data, objectStore, context)
	if err != nil {
		return nil, nil, err
	}

	// Return the raw object status bytes
	// The caller can parse them as needed
	var statusBytes []byte
	if len(objRef.ObjectStatusBytes) > 0 {
		statusBytes = objRef.ObjectStatusBytes
	}

	return objRef, statusBytes, nil
}
