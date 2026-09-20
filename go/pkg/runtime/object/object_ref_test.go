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
	"testing"

	"github.com/ray-project/ray/go/pkg/ids"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSerializedObjectRef_Serialize_Deserialize_RoundTrip(t *testing.T) {
	// Create a test ObjectID
	objectID := ids.NewObjectID()

	// Create SerializedObjectRef with all fields
	owner := &ActorOwnerAddress{
		IPAddress: "192.168.1.100",
		Port:      8080,
		WorkerID:  "worker-123",
	}

	objRef := NewSerializedObjectRef(objectID, "MyType", owner, LanguageGo)

	// Serialize
	nativeObj, err := objRef.Serialize()
	require.NoError(t, err)
	require.NotNil(t, nativeObj)

	// Verify metadata
	assert.Equal(t, MetadataTypeCrossLanguage, string(nativeObj.Metadata))
	assert.NotNil(t, nativeObj.Data)

	// Verify contained object ID
	assert.Len(t, nativeObj.ContainedObjectIds, 1)

	// Deserialize
	deserialized, err := DeserializeObjectRef(nativeObj)
	require.NotNil(t, deserialized)

	// Verify round-trip
	assert.Equal(t, objectID, deserialized.ID)
	assert.Equal(t, "MyType", deserialized.Type)
	assert.NotNil(t, deserialized.Owner)
	assert.Equal(t, "192.168.1.100", deserialized.Owner.IPAddress)
	assert.Equal(t, 8080, deserialized.Owner.Port)
	assert.Equal(t, "worker-123", deserialized.Owner.WorkerID)
	assert.Equal(t, Language(LanguageGo), deserialized.Language)
}

func TestSerializedObjectRef_Serialize_Deserialize_WithoutOwner(t *testing.T) {
	// Create a test ObjectID
	objectID := ids.NewObjectID()

	// Create SerializedObjectRef without owner
	objRef := NewSerializedObjectRef(objectID, "SimpleType", nil, LanguageJava)

	// Serialize
	nativeObj, err := objRef.Serialize()
	require.NoError(t, err)

	// Deserialize
	deserialized, err := DeserializeObjectRef(nativeObj)
	require.NoError(t, err)

	// Verify
	assert.Equal(t, objectID, deserialized.ID)
	assert.Equal(t, "SimpleType", deserialized.Type)
	assert.Nil(t, deserialized.Owner)
	assert.Equal(t, Language(LanguageJava), deserialized.Language)
}

func TestSerializedObjectRef_Serialize_NilID(t *testing.T) {
	// Create SerializedObjectRef with nil ID
	objRef := &SerializedObjectRef{
		ID:       ids.NilObjectID(),
		Type:     "TestType",
		Owner:    nil,
		Language: LanguageGo,
	}

	// Serialize should fail
	_, err := objRef.Serialize()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "ObjectRef ID cannot be nil")
}

func TestSerializedObjectRef_MarshalMsgpack_UnmarshalMsgpack(t *testing.T) {
	// Create a test ObjectID
	objectID := ids.NewObjectID()

	owner := &ActorOwnerAddress{
		IPAddress: "10.0.0.1",
		Port:      9000,
		WorkerID:  "worker-456",
	}

	objRef := &SerializedObjectRef{
		ID:       objectID,
		Type:     "MsgPackType",
		Owner:    owner,
		Language: LanguagePython,
	}

	// Marshal
	data, err := objRef.MarshalMsgpack()
	require.NoError(t, err)
	require.NotNil(t, data)

	// Unmarshal
	var newObj SerializedObjectRef
	err = newObj.UnmarshalMsgpack(data)
	require.NoError(t, err)

	// Verify
	assert.Equal(t, objectID, newObj.ID)
	assert.Equal(t, "MsgPackType", newObj.Type)
	assert.NotNil(t, newObj.Owner)
	assert.Equal(t, "10.0.0.1", newObj.Owner.IPAddress)
	assert.Equal(t, 9000, newObj.Owner.Port)
	assert.Equal(t, "worker-456", newObj.Owner.WorkerID)
	assert.Equal(t, Language(LanguagePython), newObj.Language)
}

func TestSerializedObjectRef_Deserialize_InvalidMetadata(t *testing.T) {
	// Create NativeRayObject with invalid metadata
	nativeObj := &NativeRayObject{
		Data:     []byte{0x01, 0x02, 0x03},
		Metadata: []byte("INVALID_TYPE"),
	}

	_, err := DeserializeObjectRef(nativeObj)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid metadata type for SerializedObjectRef")
}

func TestSerializedObjectRef_Deserialize_NilObject(t *testing.T) {
	_, err := DeserializeObjectRef(nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "NativeRayObject is nil")
}

func TestSerializedObjectRef_Deserialize_CorruptedData(t *testing.T) {
	// Create NativeRayObject with XLANG metadata but corrupted data
	nativeObj := &NativeRayObject{
		Data:     []byte{0xFF, 0xFF, 0xFF, 0xFF}, // Invalid msgpack data
		Metadata: []byte(MetadataTypeCrossLanguage),
	}

	_, err := DeserializeObjectRef(nativeObj)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to unmarshal SerializedObjectRef data")
}

func TestSerializedObjectRef_CrossLanguageCompatibility(t *testing.T) {
	// Test SerializedObjectRef creation with different language types
	// to ensure cross-language compatibility
	languages := []Language{LanguageGo, LanguageJava, LanguagePython, LanguageCpp}

	for _, lang := range languages {
		t.Run(string(lang), func(t *testing.T) {
			objectID := ids.NewObjectID()

			objRef := NewSerializedObjectRef(objectID, "CrossLangType", nil, lang)

			// Serialize
			nativeObj, err := objRef.Serialize()
			require.NoError(t, err)

			// Deserialize
			deserialized, err := DeserializeObjectRef(nativeObj)
			require.NoError(t, err)

			// Verify language is preserved
			assert.Equal(t, Language(lang), deserialized.Language)
		})
	}
}

func TestSerializedObjectRef_ObjectSerializer_Integration(t *testing.T) {
	// Test SerializedObjectRef serialization through ObjectSerializer
	serializer := &mockSerializer{}

	objectID := ids.NewObjectID()

	objRef := NewSerializedObjectRef(objectID, "IntegrationType", nil, LanguageGo)

	// Serialize through ObjectSerializer
	nativeObj, err := serializer.Serialize(objRef)
	require.NoError(t, err)
	require.NotNil(t, nativeObj)

	// Deserialize through ObjectSerializer
	deserialized, err := serializer.Deserialize(nativeObj, &objectID, "")
	require.NoError(t, err)

	// Verify it's a SerializedObjectRef
	resultObjRef, ok := deserialized.(*SerializedObjectRef)
	require.True(t, ok)
	assert.Equal(t, objectID, resultObjRef.ID)
	assert.Equal(t, "IntegrationType", resultObjRef.Type)
}
