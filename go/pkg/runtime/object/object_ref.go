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
	"github.com/vmihailenco/msgpack/v5"
)

// SerializedObjectRef represents a serialized ObjectRef for cross-language transmission.
// Compatible with Python, Java, and C++ ObjectRef serialization format using MessagePack.
//
// This structure is used for passing object references across language boundaries
// (Go <-> Java <-> Python <-> C++) during serialization. It contains the minimal
// information needed to reconstruct the ObjectRef in the target language runtime.
//
// Serialization format matches Python's object_ref_reducer output:
// (binary, call_site, owner_address, object_status, tensor_transport)
// where owner_address and object_status are protobuf-serialized strings.
//
// Note: This is different from api.ObjectRef[T] which is the public API for users.
// SerializedObjectRef is only used internally for serialization/deserialization.
type SerializedObjectRef struct {
	// ID is the unique object identifier (28-byte binary).
	ID ids.ObjectID

	// Type describes the object type (for type checking in target language).
	Type string

	// Owner contains the owner address information for cross-node access.
	// For Python compatibility, this can be stored as protobuf-serialized bytes.
	Owner *ActorOwnerAddress

	// OwnerAddressBytes contains the protobuf-serialized owner address.
	// This is used for cross-language compatibility with Python/Java.
	// When set, it takes precedence over the Owner field during serialization.
	OwnerAddressBytes []byte

	// ObjectStatusBytes contains the protobuf-serialized object status.
	// This is used for cross-language compatibility with Python.
	ObjectStatusBytes []byte

	// CallSite is the call site information for debugging.
	CallSite string

	// TensorTransport is the tensor transport type.
	TensorTransport int

	// Language is the source language that created this ObjectRef.
	Language Language
}

// NewSerializedObjectRef creates a new SerializedObjectRef from an object ID and optional metadata.
func NewSerializedObjectRef(id ids.ObjectID, objectType string, owner *ActorOwnerAddress, language Language) *SerializedObjectRef {
	return &SerializedObjectRef{
		ID:       id,
		Type:     objectType,
		Owner:    owner,
		Language: language,
	}
}

// Serialize serializes the SerializedObjectRef to NativeRayObject.
// Returns a NativeRayObject with XLANG metadata for cross-language compatibility.
//
// This method preserves all fields from the original Python/Java 5-tuple format:
// (binary, call_site, owner_address, object_status, tensor_transport)
// while also supporting the Go map format with id, type, language, owner fields.
//
// Cross-language compatibility:
// - Python: Uses the 6-tuple format (binary, call_site, owner_address, object_status, tensor_transport, type)
// - Java/C++/Go: Uses the map format with all fields preserved
//
// Note: This is different from api.ObjectRef[T] which is the public API for users.
// SerializedObjectRef is only used internally for serialization/deserialization.
func (o *SerializedObjectRef) Serialize() (*NativeRayObject, error) {
	// Check for nil ID - both nil pointer and zero value should fail
	if o == nil || o.ID.IsNil() {
		return nil, fmt.Errorf("SerializedObjectRef ID cannot be nil")
	}

	var refData interface{}

	// Choose format based on language for cross-language compatibility
	// Python expects tuple format, while Java/C++/Go expect map format
	switch o.Language {
	case LanguagePython:
		// Python format: 6-tuple (binary, call_site, owner_address, object_status, tensor_transport, type)
		// This matches Python's object_ref_reducer output format
		refData = []interface{}{
			o.ID.Binary(),
			o.CallSite,
			o.OwnerAddressBytes,
			o.ObjectStatusBytes,
			o.TensorTransport,
			o.Type,
		}
	default:
		// Java/C++/Go format: map with all fields
		// This matches Java's writeExternal() which preserves all fields
		refData = map[string]interface{}{
			"id":                  o.ID.Binary(),
			"type":                o.Type,
			"language":            o.Language,
			"call_site":           o.CallSite,
			"object_status_bytes": o.ObjectStatusBytes,
			"tensor_transport":    o.TensorTransport,
			"owner_address_bytes": o.OwnerAddressBytes,
		}
		if o.Owner != nil {
			refData.(map[string]interface{})["owner"] = map[string]interface{}{
				"ip":        o.Owner.IPAddress,
				"port":      int64(o.Owner.Port),
				"worker_id": o.Owner.WorkerID,
			}
		}
	}

	data, err := msgpack.Marshal(refData)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal SerializedObjectRef: %w", err)
	}

	nativeObj := &NativeRayObject{
		Data:     data,
		Metadata: []byte(MetadataTypeCrossLanguage),
	}

	// Set contained object ID for dependency tracking.
	// This ensures the referenced object is not GC'd while the SerializedObjectRef exists.
	nativeObj.SetContainedObjectIds([]ids.ObjectID{o.ID})

	return nativeObj, nil
}

// DeserializeObjectRef deserializes a SerializedObjectRef from NativeRayObject.
// Restores a SerializedObjectRef object from cross-language serialized data.
//
// This method supports Go and Python serialization formats:
// - Go/Java/C++ format: map with id, type, language, owner fields
// - Python format: 6-tuple (binary, call_site, owner_address, object_status, tensor_transport, type)
//
// Language-based routing with format fallback:
// The 'language' field tells us what CREATED the object.
// Serialize() uses language-specific formats:
// - LanguagePython: Uses 6-tuple format for Python compatibility
// - LanguageGo/Java/C++: Uses map format
//
// Deserialization strategy:
// 1. If language field exists:
//    a. LanguageGo: Use Go format (map)
//    b. LanguagePython: First try Go format (map), then Python format (tuple) for backward compatibility
//    c. Other languages: Try Go format first, then Python format
// 2. If no language field (Python tuple format), use format-based detection (Go first, then Python)
//
// This ensures backward compatibility while supporting language-specific serialization.
func DeserializeObjectRef(nativeObj *NativeRayObject) (*SerializedObjectRef, error) {
	if nativeObj == nil {
		return nil, fmt.Errorf("NativeRayObject is nil")
	}

	// Verify metadata type.
	metadata := string(nativeObj.Metadata)
	if metadata != MetadataTypeCrossLanguage && metadata != MetadataTypeXLang {
		return nil, fmt.Errorf(
			"invalid metadata type for SerializedObjectRef: expected %s or %s, got %s",
			MetadataTypeCrossLanguage, MetadataTypeXLang, metadata)
	}

	// Try to extract language field for routing with format fallback
	// First, try to unmarshal as Go format to check for language field
	var rawData map[string]interface{}
	if err := msgpack.Unmarshal(nativeObj.Data, &rawData); err == nil {
		// Check if language field exists
		if langStr, ok := rawData["language"].(string); ok {
			language := Language(langStr)
			// Language field detected - use format fallback strategy
			switch language {
			case LanguageGo:
				// Go format confirmed, use Go deserializer
				return deserializeObjectRefFromGo(nativeObj.Data)
			case LanguagePython:
				// Python language detected, but Serialize() always uses map format!
				// Strategy: First try Go format (map), only fall back to Python format (tuple) if it fails
				if objRef, err := deserializeObjectRefFromGo(nativeObj.Data); err == nil && objRef != nil {
					return objRef, nil
				}
				// Go format failed, try Python format (tuple) for backward compatibility
				return deserializeObjectRefFromPython(nativeObj.Data)
			default:
				// Unknown language (e.g., Java, C++), try Go format first
				if objRef, err := deserializeObjectRefFromGo(nativeObj.Data); err == nil && objRef != nil {
					return objRef, nil
				}
				// Fall back to Python format for backward compatibility
				return deserializeObjectRefFromPython(nativeObj.Data)
			}
		}
	}

	// No language field found, use format-based detection
	// Try Go format first (map) - this is the preferred format for Go objects
	if objRef, err := deserializeObjectRefFromGo(nativeObj.Data); err == nil && objRef != nil {
		return objRef, nil
	}

	// If Go format fails, try Python format (tuple) for backward compatibility
	if objRef, err := deserializeObjectRefFromPython(nativeObj.Data); err == nil && objRef != nil {
		return objRef, nil
	}

	// Both formats failed, return a generic error
	return nil, fmt.Errorf("failed to unmarshal SerializedObjectRef data")
}

// deserializeObjectRefFromPython deserializes a Python-serialized ObjectRef.
// Python format: (binary, call_site, owner_address, object_status, tensor_transport, type)
// where owner_address and object_status are protobuf-serialized strings.
// The 'type' field at index 5 is added for proper deserialization.
func deserializeObjectRefFromPython(data []byte) (*SerializedObjectRef, error) {
	var tuple []interface{}
	if err := msgpack.Unmarshal(data, &tuple); err != nil {
		return nil, fmt.Errorf("failed to unmarshal Python ObjectRef tuple: %w", err)
	}

	// Python format: (binary, call_site, owner_address, object_status, tensor_transport, type)
	// We support both 5-tuple (original) and 6-tuple (with type field)
	if len(tuple) < 5 {
		return nil, fmt.Errorf(
			"Python ObjectRef tuple must have at least 5 elements, got %d", len(tuple))
	}

	// Extract binary (ObjectID)
	idBytes, ok := tuple[0].([]byte)
	if !ok {
		return nil, fmt.Errorf("Python ObjectRef binary field is not []byte")
	}
	id, err := ids.ObjectIDFromBinary(idBytes)
	if err != nil {
		return nil, fmt.Errorf("failed to parse ObjectID from Python format: %w", err)
	}

	// Extract call_site
	callSite, _ := tuple[1].(string)

	// Extract owner_address (protobuf-serialized bytes)
	var ownerAddressBytes []byte
	if tuple[2] != nil {
		ownerAddressBytes, _ = tuple[2].([]byte)
	}

	// Extract object_status (protobuf-serialized bytes)
	var objectStatusBytes []byte
	if tuple[3] != nil {
		objectStatusBytes, _ = tuple[3].([]byte)
	}

	// Extract tensor_transport
	tensorTransport, _ := tuple[4].(int)

	// Extract type (optional, for backward compatibility with 5-tuple format)
	var objType string
	if len(tuple) >= 6 {
		objType, _ = tuple[5].(string)
	}
	if objType == "" {
		// Default to "ObjectRef" for backward compatibility
		objType = "ObjectRef"
	}

	return &SerializedObjectRef{
		ID:                id,
		Type:              objType,
		CallSite:          callSite,
		OwnerAddressBytes: ownerAddressBytes,
		ObjectStatusBytes: objectStatusBytes,
		TensorTransport:   tensorTransport,
		Language:          LanguagePython,
	}, nil
}

// convertToInt converts various numeric types to int.
// Returns (0, false) if the value is not a numeric type.
func convertToInt(v interface{}) (int, bool) {
	switch val := v.(type) {
	case int:
		return val, true
	case int64:
		return int(val), true
	case uint64:
		return int(val), true
	case float64:
		return int(val), true
	case int32:
		return int(val), true
	default:
		return 0, false
	}
}

// deserializeObjectRefFromGo deserializes a Go-serialized ObjectRef.
// Go format: map with id, type, language, owner fields.
func deserializeObjectRefFromGo(data []byte) (*SerializedObjectRef, error) {
	var rawData map[string]interface{}
	if err := msgpack.Unmarshal(data, &rawData); err != nil {
		return nil, fmt.Errorf("failed to unmarshal Go ObjectRef map: %w", err)
	}

	// Extract ID.
	var id ids.ObjectID
	if idBytes, ok := rawData["id"].([]byte); ok {
		var err error
		id, err = ids.ObjectIDFromBinary(idBytes)
		if err != nil {
			return nil, fmt.Errorf("failed to parse ObjectID from binary: %w", err)
		}
	} else {
		return nil, fmt.Errorf("Go ObjectRef missing 'id' field")
	}

	// Extract Type.
	objType, _ := rawData["type"].(string)

	// Extract Language.
	var language Language
	if langStr, ok := rawData["language"].(string); ok {
		language = Language(langStr)
	} else {
		// Default to GO if not specified.
		language = LanguageGo
	}

	// Extract Owner address (optional).
	var owner *ActorOwnerAddress
	if ownerRaw, ok := rawData["owner"]; ok {
		// Convert to map[string]interface{} if needed (msgpack may use map[interface{}]interface{})
		var ownerData map[string]interface{}
		switch v := ownerRaw.(type) {
		case map[string]interface{}:
			ownerData = v
		case map[interface{}]interface{}:
			ownerData = make(map[string]interface{})
			for k, val := range v {
				if keyStr, ok := k.(string); ok {
					ownerData[keyStr] = val
				}
			}
		}

		if ownerData != nil {
			owner = &ActorOwnerAddress{}
			if ip, ok := ownerData["ip"].(string); ok {
				owner.IPAddress = ip
			}
			// Handle port as different numeric types (msgpack may encode as int, uint64, float64, or int64)
			if portVal, ok := ownerData["port"]; ok {
				if port, ok := convertToInt(portVal); ok {
					owner.Port = port
				}
			}
			if workerID, ok := ownerData["worker_id"].(string); ok {
				owner.WorkerID = workerID
			}
		}
	}

	return &SerializedObjectRef{
		ID:       id,
		Type:     objType,
		Language: language,
		Owner:    owner,
	}, nil
}

// DeserializeObjectRefFromNative deserializes a SerializedObjectRef from NativeRayObject.
// Restores a SerializedObjectRef object from cross-language serialized data.
//
// This method supports both Python and Go serialization formats:
// - Python format: 5-tuple (binary, call_site, owner_address, object_status, tensor_transport)
// - Go format: map with id, type, language, owner fields
func DeserializeObjectRefFromNative(nativeObj *NativeRayObject) (*SerializedObjectRef, error) {
	if nativeObj == nil {
		return nil, fmt.Errorf("NativeRayObject is nil")
	}

	// Verify metadata type.
	metadata := string(nativeObj.Metadata)
	if metadata != MetadataTypeCrossLanguage && metadata != MetadataTypeXLang {
		return nil, fmt.Errorf(
			"invalid metadata type for SerializedObjectRef: expected %s or %s, got %s",
			MetadataTypeCrossLanguage, MetadataTypeXLang, metadata)
	}

	// Try Python format first (5-tuple)
	if objRef, err := deserializeObjectRefFromPython(nativeObj.Data); err == nil {
		return objRef, nil
	}

	// Fallback to Go format (map)
	return deserializeObjectRefFromGo(nativeObj.Data)
}

// MarshalMsgpack implements msgpack.Marshaler interface for direct SerializedObjectRef serialization.
func (o *SerializedObjectRef) MarshalMsgpack() ([]byte, error) {
	data := map[string]interface{}{
		"id":       o.ID.Binary(),
		"type":     o.Type,
		"language": o.Language,
	}
	if o.Owner != nil {
		// Manually expand owner fields to ensure correct msgpack encoding
		data["owner"] = map[string]interface{}{
			"ip":        o.Owner.IPAddress,
			"port":      int64(o.Owner.Port),
			"worker_id": o.Owner.WorkerID,
		}
	}
	return msgpack.Marshal(data)
}

// UnmarshalMsgpack implements msgpack.Unmarshaler interface for direct SerializedObjectRef deserialization.
func (o *SerializedObjectRef) UnmarshalMsgpack(data []byte) error {
	var raw map[string]interface{}
	if err := msgpack.Unmarshal(data, &raw); err != nil {
		return err
	}

	// Extract ID.
	if idBytes, ok := raw["id"].([]byte); ok {
		id, err := ids.ObjectIDFromBinary(idBytes)
		if err != nil {
			return fmt.Errorf("failed to parse ObjectID: %w", err)
		}
		o.ID = id
	}

	// Extract Type.
	if t, ok := raw["type"].(string); ok {
		o.Type = t
	}

	// Extract Owner.
	if ownerRaw, ok := raw["owner"]; ok {
		// Convert to map[string]interface{} if needed (msgpack may use map[interface{}]interface{})
		var ownerData map[string]interface{}
		switch v := ownerRaw.(type) {
		case map[string]interface{}:
			ownerData = v
		case map[interface{}]interface{}:
			ownerData = make(map[string]interface{})
			for k, val := range v {
				if keyStr, ok := k.(string); ok {
					ownerData[keyStr] = val
				}
			}
		}

		if ownerData != nil {
			o.Owner = &ActorOwnerAddress{}
			if ip, ok := ownerData["ip"].(string); ok {
				o.Owner.IPAddress = ip
			}
			// Handle port as different numeric types (msgpack may encode as int, uint64, float64, or int64)
			if portVal, ok := ownerData["port"]; ok {
				switch p := portVal.(type) {
				case int:
					o.Owner.Port = p
				case int64:
					o.Owner.Port = int(p)
				case uint64:
					o.Owner.Port = int(p)
				case float64:
					o.Owner.Port = int(p)
				case int32:
					o.Owner.Port = int(p)
				}
			}
			if workerID, ok := ownerData["worker_id"].(string); ok {
				o.Owner.WorkerID = workerID
			}
		}
	}

	// Extract Language.
	if lang, ok := raw["language"].(string); ok {
		o.Language = Language(lang)
	}

	return nil
}
