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

	"github.com/ray-project/ray/go/pkg/ids"
	"github.com/ray-project/ray/go/pkg/log"
)

// ============================================================================
// Core Data Structures - NativeRayObject
// ============================================================================

// NativeRayObject represents a Ray object in binary format.
// This structure holds the serialized data, metadata, and nested object references
// for Ray objects stored in the object store.
//
// Note: Defined in non-CGO file for easier testing and reuse.
type NativeRayObject struct {
	Data               []byte   // serialized object data
	Metadata           []byte   // object metadata
	ContainedObjectIds [][]byte // nested object IDs in binary format
}

// NewNativeRayObject creates a new NativeRayObject.
func NewNativeRayObject(data, metadata []byte) *NativeRayObject {
	return &NativeRayObject{
		Data:               data,
		Metadata:           metadata,
		ContainedObjectIds: [][]byte{},
	}
}

// SetContainedObjectIds sets the nested object IDs.
func (n *NativeRayObject) SetContainedObjectIds(objectIds []ids.ObjectID) {
	n.ContainedObjectIds = make([][]byte, len(objectIds))
	for i, oid := range objectIds {
		n.ContainedObjectIds[i] = oid.Binary()
	}
}

// GetContainedObjectIds returns the nested object IDs.
// Invalid binary data is silently skipped to avoid panics.
// In normal operation, all contained IDs should be valid.
func (n *NativeRayObject) GetContainedObjectIds() []ids.ObjectID {
	result := make([]ids.ObjectID, 0, len(n.ContainedObjectIds))
	for _, cid := range n.ContainedObjectIds {
		oid, err := ids.ObjectIDFromBinary(cid)
		if err == nil {
			result = append(result, oid)
		} else {
			// Log warning for invalid IDs to help diagnose data corruption issues
			log.Log.V(1).Error(err, "skipping invalid contained object ID", "binaryLength", len(cid))
		}
	}
	return result
}

// String returns the string representation for debugging.
func (n *NativeRayObject) String() string {
	if n == nil {
		return "NativeRayObject(nil)"
	}
	return fmt.Sprintf("NativeRayObject{Data: %d bytes, Metadata: %d bytes, ContainedObjectIds: %d}",
		len(n.Data), len(n.Metadata), len(n.ContainedObjectIds))
}

// IsEmpty checks if the object is empty.
func (n *NativeRayObject) IsEmpty() bool {
	return n == nil || (len(n.Data) == 0 && len(n.Metadata) == 0)
}

// Close releases the buffer associated with this NativeRayObject back to the pool.
// Callers should explicitly call Close() when done.
// Implements io.Closer interface.
func (n *NativeRayObject) Close() error {
	if n.Data != nil {
		PutBuffer(n.Data)
		n.Data = nil
	}
	return nil
}

// ============================================================================
// Serialization Result Types
// ============================================================================

// SerializationResult represents the result of serialization.
// Contains serialized data and metadata.
type SerializationResult struct {
	// Data is the serialized byte data.
	Data []byte

	// Metadata describes the serialization format and type.
	Metadata string

	// ContainedObjectIds is the list of nested object IDs
	// for tracking referenced objects during serialization.
	ContainedObjectIds []ids.ObjectID
}

// ============================================================================
// Metadata Type
// ============================================================================

// MetadataType represents the serialization metadata type.
// Used to identify the serialization format and object type.
type MetadataType string

// Metadata type constants.
// These constants must be consistent with Java/Python implementations.
const (
	// MetadataTypeCrossLanguage is the cross-language serialization format (formerly XLANG).
	// Used for object serialization between Go and Java/Python/C++.
	MetadataTypeCrossLanguage = "XLANG"

	// MetadataTypeXLang is an alias for cross-language serialization format.
	// Same as MetadataTypeCrossLanguage, for backward compatibility.
	MetadataTypeXLang = "XLANG"

	// MetadataTypeGo is the Go-specific serialization format.
	// Used for object serialization in pure Go environments using MessagePack.
	MetadataTypeGo = "GO"

	// MetadataTypeRaw is the raw byte format.
	// Stores byte data directly without any serialization.
	MetadataTypeRaw = "RAW"

	// MetadataTypeActorHandle is the Actor handle type.
	// Used for serializing ActorHandle objects.
	MetadataTypeActorHandle = "ACTOR_HANDLE"

	// MetadataTypeTaskExecutionException is the task execution exception type.
	// Used for serializing exceptions during task execution.
	MetadataTypeTaskExecutionException = "TASK_EXECUTION_EXCEPTION"

	// MetadataTypeJava is the Java serialization format.
	// Used for Java object serialization.
	MetadataTypeJava = "JAVA"

	// MetadataTypePython is the Python serialization format.
	// Used for Python object serialization.
	MetadataTypePython = "PYTHON"
)

// ============================================================================
// Language Type
// ============================================================================

// Language represents the programming language type.
// Used to identify the language of the Actor or object owner.
type Language string

// Language type constants.
// Used to identify the language of the Actor or object owner.
const (
	// LanguageJava represents Java language.
	LanguageJava = "JAVA"

	// LanguagePython represents Python language.
	LanguagePython = "PYTHON"

	// LanguageCpp represents C++ language.
	LanguageCpp = "CPP"

	// LanguageGo represents Go language.
	LanguageGo = "GO"
)

// ============================================================================
// Error Code Constants
// ============================================================================

// Error code constants.
// Consistent with definitions in Ray C++ Core Worker.
const (
	// ErrorCodeWorkerDied indicates worker process died.
	ErrorCodeWorkerDied = 1

	// ErrorCodeActorDied indicates Actor died.
	ErrorCodeActorDied = 2

	// ErrorCodeActorUnavailable indicates Actor is unavailable.
	ErrorCodeActorUnavailable = 3

	// ErrorCodeObjectUnreconstructable indicates object cannot be reconstructed.
	ErrorCodeObjectUnreconstructable = 4

	// ErrorCodeObjectLost indicates object is lost.
	ErrorCodeObjectLost = 5

	// ErrorCodeOwnerDied indicates owner process died.
	ErrorCodeOwnerDied = 6

	// ErrorCodeObjectDeleted indicates object has been deleted.
	ErrorCodeObjectDeleted = 7

	// ErrorCodeTaskExecutionException indicates task execution exception.
	ErrorCodeTaskExecutionException = 8
)

// RpcErrorType* are the rpc::ErrorType numbers used as error-object metadata, matching the C++
// enum rpc::ErrorType in src/ray/protobuf/common.proto (see C++ RayObject::IsException). They
// identify the error kind carried by an error object's metadata and differ from the ErrorCode*
// constants above: ErrorCode* identifies Go exceptions inside the msgpack ExceptionData payload,
// while RpcErrorType* is the metadata value written by the C++ core worker. Only the values with
// a dedicated Go exception class have a factory in errorTypeFactories (see exception.go); the
// rest (e.g. WORKER_STARTUP_FAILED) are surfaced through newRayErrorTypeException.
const (
	// RpcErrorTypeWorkerDied indicates the worker died while executing the task.
	RpcErrorTypeWorkerDied = 0
	// RpcErrorTypeActorDied indicates the actor died while executing the task.
	RpcErrorTypeActorDied = 1
	// RpcErrorTypeTaskExecutionException indicates the task failed due to user code failure.
	RpcErrorTypeTaskExecutionException = 3
	// RpcErrorTypeWorkerStartupFailed indicates the worker failed to start after multiple retries.
	RpcErrorTypeWorkerStartupFailed = 33
)
