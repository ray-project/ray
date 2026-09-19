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
	"context"
	"encoding/json"
	"reflect"
	"testing"

	"github.com/ray-project/ray/go/pkg/ids"
	"github.com/vmihailenco/msgpack/v5"
)

// mockSerializer implements the Serializer interface for testing.
type mockSerializer struct {
	// Context management state
	containedObjectIDs []ids.ObjectID
	outerObjectID      ids.ObjectID
}

// Serialize implements the Serializer interface.
// This mock implementation serializes objects using msgpack and determines
// the correct metadata type based on the object's category.
func (m *mockSerializer) Serialize(obj interface{}) (*NativeRayObject, error) {
	// Handle different types appropriately
	var data []byte
	var err error

	// Check if it's a SerializedObjectRef
	if objRef, ok := obj.(*SerializedObjectRef); ok {
		// Use the object's own Serialize method
		nativeObj, err := objRef.Serialize()
		if err != nil {
			return nil, err
		}
		return nativeObj, nil
	}

	// Determine metadata type based on object category
	metadata := m.determineMetadata(obj)

	// Handle raw data ([]byte) - return as-is
	if string(metadata) == MetadataTypeRaw {
		return &NativeRayObject{
			Data:     obj.([]byte),
			Metadata: metadata,
		}, nil
	}

	// For Go-native types, serialize the value directly using msgpack
	data, err = msgpack.Marshal(obj)
	if err != nil {
		return nil, err
	}

	return &NativeRayObject{
		Data:     data,
		Metadata: metadata,
	}, nil
}

// determineMetadata determines the metadata type for the given object.
// Returns MetadataTypeRaw for []byte, MetadataTypeXLang for cross-language types,
// or MetadataTypeGo for Go-specific types.
func (m *mockSerializer) determineMetadata(obj interface{}) []byte {
	// Raw byte arrays are considered raw data
	if _, ok := obj.([]byte); ok {
		return []byte(MetadataTypeRaw)
	}

	// Check if it's a cross-language type
	if m.IsCrossLanguageType(obj) {
		return []byte(MetadataTypeXLang)
	}

	// Default to Go format
	return []byte(MetadataTypeGo)
}

// Deserialize implements the Serializer interface.
// This mock implementation deserializes msgpack data, handling both
// Go format and cross-language format.
// The objectType parameter is available for type-safe deserialization but not used in this mock.
func (m *mockSerializer) Deserialize(nativeObj *NativeRayObject, objectID *ids.ObjectID, objectType string) (interface{}, error) {
	if nativeObj == nil || nativeObj.Data == nil {
		return nil, nil
	}

	// Check metadata type
	metadata := string(nativeObj.Metadata)

	// Handle Go format - deserialize the value directly
	if metadata == MetadataTypeGo {
		var result interface{}
		if err := msgpack.Unmarshal(nativeObj.Data, &result); err != nil {
			return nil, nil
		}
		return result, nil
	}

	// Handle cross-language format (XLANG) - same structure but different metadata
	if metadata == MetadataTypeXLang {
		// Try to deserialize as SerializedObjectRef first
		if objRef, err := deserializeObjectRefFromGo(nativeObj.Data); err == nil && objRef != nil {
			return objRef, nil
		}

		// Otherwise try to deserialize as a regular value
		var result interface{}
		if err := msgpack.Unmarshal(nativeObj.Data, &result); err != nil {
			return nil, nil
		}
		return result, nil
	}

	// Handle raw format - return data as-is
	if metadata == MetadataTypeRaw {
		// Return the raw bytes directly
		return nativeObj.Data, nil
	}

	// For unknown formats, return nil
	return nil, nil
}

// AddContainedObjectID implements the Serializer interface.
func (m *mockSerializer) AddContainedObjectID(objectID ids.ObjectID) {
	m.containedObjectIDs = append(m.containedObjectIDs, objectID)
}

// GetAndClearContainedObjectIDs implements the Serializer interface.
func (m *mockSerializer) GetAndClearContainedObjectIDs() []ids.ObjectID {
	result := m.containedObjectIDs
	m.containedObjectIDs = nil
	return result
}

// SetOuterObjectID implements the Serializer interface.
func (m *mockSerializer) SetOuterObjectID(objectID ids.ObjectID) {
	m.outerObjectID = objectID
}

// GetOuterObjectID implements the Serializer interface.
func (m *mockSerializer) GetOuterObjectID() ids.ObjectID {
	return m.outerObjectID
}

// ResetOuterObjectID implements the Serializer interface.
func (m *mockSerializer) ResetOuterObjectID() {
	m.outerObjectID = ids.ObjectID{}
}

// EstimateBufferSize implements the Serializer interface.
func (m *mockSerializer) EstimateBufferSize(obj interface{}) int {
	return 1024
}

// GetBuffer implements the Serializer interface.
func (m *mockSerializer) GetBuffer(size int) []byte {
	return make([]byte, 0, size)
}

// PutBuffer implements the Serializer interface.
func (m *mockSerializer) PutBuffer(buf []byte) {
	// Mock implementation - do nothing
}

// IsCrossLanguageType implements the Serializer interface.
// Returns true for map and struct types, which are cross-language compatible.
func (m *mockSerializer) IsCrossLanguageType(obj interface{}) bool {
	if obj == nil {
		return false
	}
	t := reflect.TypeOf(obj)
	switch t.Kind() {
	case reflect.Map, reflect.Struct:
		return true
	default:
		return false
	}
}

// SerializeAndPut is a convenience method for testing.
func (m *mockSerializer) SerializeAndPut(store ObjectStore, obj interface{}) (*ids.ObjectID, error) {
	// Serialize the object
	nativeObj, err := m.Serialize(obj)
	if err != nil {
		return nil, err
	}

	// Put the serialized object into the store
	objectID, err := store.PutRaw(nativeObj)
	if err != nil {
		return nil, err
	}

	return objectID, nil
}

// GetAndDeserialize is a convenience method for testing.
func (m *mockSerializer) GetAndDeserialize(store ObjectStore, objectID *ids.ObjectID, timeoutMs int64) (interface{}, error) {
	// Get the serialized object from the store
	objects, err := store.GetRaw([]*ids.ObjectID{objectID}, timeoutMs, "")
	if err != nil {
		return nil, err
	}

	if len(objects) == 0 || objects[0] == nil {
		return nil, nil
	}

	// Deserialize and return
	result, err := m.Deserialize(objects[0], objectID, "")
	if err != nil {
		return nil, err
	}
	return result, nil
}

// RandomObjectID generates a new random ObjectID for testing.
func RandomObjectID() ids.ObjectID {
	return ids.NewObjectID()
}

// mockObjectStore is a mock implementation of ObjectStore for testing.
type mockObjectStore struct {
	objects map[string]*NativeRayObject
}

func newMockObjectStore() *mockObjectStore {
	return &mockObjectStore{
		objects: make(map[string]*NativeRayObject),
	}
}

func (m *mockObjectStore) PutRaw(obj *NativeRayObject) (*ids.ObjectID, error) {
	objectID := RandomObjectID()
	m.objects[objectID.String()] = obj
	return &objectID, nil
}

func (m *mockObjectStore) PutRawWithOwner(obj *NativeRayObject, ownerActorID *ids.ActorID) (*ids.ObjectID, error) {
	return m.PutRaw(obj)
}

func (m *mockObjectStore) PutRawWithID(obj *NativeRayObject, objectID *ids.ObjectID) error {
	m.objects[objectID.String()] = obj
	return nil
}

func (m *mockObjectStore) GetRaw(objectIDs []*ids.ObjectID, timeoutMs int64, objectType string) ([]*NativeRayObject, error) {
	results := make([]*NativeRayObject, len(objectIDs))
	for i, oid := range objectIDs {
		if obj, ok := m.objects[oid.String()]; ok {
			results[i] = obj
		}
	}
	return results, nil
}

func (m *mockObjectStore) GetRawWithContext(ctx context.Context, objectIDs []*ids.ObjectID, timeoutMs int64, objectType string) ([]*NativeRayObject, error) {
	// Mock implementation ignores context and just calls GetRaw
	return m.GetRaw(objectIDs, timeoutMs, objectType)
}

func (m *mockObjectStore) Wait(objectIDs []*ids.ObjectID, numObjects int, timeoutMs int64, fetchLocal bool) ([]bool, error) {
	results := make([]bool, len(objectIDs))
	for i, oid := range objectIDs {
		if _, ok := m.objects[oid.String()]; ok {
			results[i] = true
		}
	}
	return results, nil
}

func (m *mockObjectStore) WaitWithOptions(opts WaitOptions) ([]bool, error) {
	return m.Wait(opts.ObjectIDs, opts.NumObjects, opts.TimeoutMs, opts.FetchLocal)
}

func (m *mockObjectStore) Delete(objectIDs []*ids.ObjectID, localOnly bool) error {
	for _, oid := range objectIDs {
		delete(m.objects, oid.String())
	}
	return nil
}

func (m *mockObjectStore) AddLocalReference(objectID *ids.ObjectID) error {
	return nil
}

func (m *mockObjectStore) RemoveLocalReference(objectID *ids.ObjectID) error {
	return nil
}

func (m *mockObjectStore) GetOwnershipInfo(objectID *ids.ObjectID) ([]byte, error) {
	return nil, nil
}

func (m *mockObjectStore) RegisterOwnershipInfoAndResolveFuture(objectID, outerObjectID *ids.ObjectID, ownerAddress []byte) error {
	return nil
}

func (m *mockObjectStore) GetOwnerAddress(objectID *ids.ObjectID) ([]byte, error) {
	return nil, nil
}

func (m *mockObjectStore) GetAllReferenceCounts() (map[ids.ObjectID][2]int64, error) {
	return make(map[ids.ObjectID][2]int64), nil
}

// compareJSON compares two values by marshaling them to JSON and comparing the strings.
// This is useful for comparing interface{} values that may have different concrete types
// but represent the same data (e.g., int vs int64, struct vs map).
func compareJSON(t *testing.T, actual, expected interface{}) bool {
	t.Helper()
	actualJSON, err := json.Marshal(actual)
	if err != nil {
		t.Errorf("Failed to marshal actual value: %v", err)
		return false
	}
	expectedJSON, err := json.Marshal(expected)
	if err != nil {
		t.Errorf("Failed to marshal expected value: %v", err)
		return false
	}
	if string(actualJSON) != string(expectedJSON) {
		t.Errorf("JSON mismatch:\nactual  = %s\nexpected = %s", string(actualJSON), string(expectedJSON))
		return false
	}
	return true
}

// TestObjectSerializer_SerializePrimitive tests serializing primitive types.
func TestObjectSerializer_SerializePrimitive(t *testing.T) {
	serializer := &mockSerializer{}

	tests := []struct {
		name  string
		value interface{}
	}{
		{"int", int(42)},
		{"string", "hello"},
		{"float64", 3.14},
		{"bool", true},
		{"[]byte", []byte("hello")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := serializer.Serialize(tt.value)
			if err != nil {
				t.Fatalf("Serialize() error = %v", err)
			}

			if result == nil {
				t.Fatal("Serialize() returned nil result")
			}

			if len(result.Data) == 0 {
				t.Error("Serialize() returned empty data")
			}

			// Deserialize and verify
			deserialized, err := serializer.Deserialize(result, nil, "")
			if err != nil {
				t.Fatalf("Deserialize() error = %v", err)
			}

			compareJSON(t, deserialized, tt.value)
		})
	}
}

// TestObjectSerializer_SerializeComplex tests serializing complex types.
func TestObjectSerializer_SerializeComplex(t *testing.T) {
	serializer := &mockSerializer{}

	tests := []struct {
		name  string
		value interface{}
	}{
		{
			"map",
			map[string]interface{}{
				"a": 1,
				"b": "string",
				"c": []int{1, 2, 3},
			},
		},
		{
			"slice",
			[]interface{}{1, "two", 3.0, true},
		},
		{
			"struct",
			struct {
				A int
				B string
			}{42, "test"},
		},
		{
			"nested",
			map[string]map[string]int{
				"outer": {"inner": 42},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := serializer.Serialize(tt.value)
			if err != nil {
				t.Fatalf("Serialize() error = %v", err)
			}

			deserialized, err := serializer.Deserialize(result, nil, "")
			if err != nil {
				t.Fatalf("Deserialize() error = %v", err)
			}

			compareJSON(t, deserialized, tt.value)
		})
	}
}

// TestObjectSerializer_SerializeNil tests serializing nil.
func TestObjectSerializer_SerializeNil(t *testing.T) {
	serializer := &mockSerializer{}

	result, err := serializer.Serialize(nil)
	if err != nil {
		t.Fatalf("Serialize(nil) error = %v", err)
	}

	if result == nil {
		t.Fatal("Serialize(nil) returned nil result")
	}

	deserialized, err := serializer.Deserialize(result, nil, "")
	if err != nil {
		t.Fatalf("Deserialize() error = %v", err)
	}

	if deserialized != nil {
		t.Errorf("Deserialize() = %v, want nil", deserialized)
	}
}

// TestObjectSerializer_SerializeAndPut tests SerializeAndPut convenience method.
func TestObjectSerializer_SerializeAndPut(t *testing.T) {
	serializer := &mockSerializer{}
	store := newMockObjectStore()

	value := map[string]int{"a": 1, "b": 2}

	objectID, err := serializer.SerializeAndPut(store, value)
	if err != nil {
		t.Fatalf("SerializeAndPut() error = %v", err)
	}

	if objectID == nil {
		t.Fatal("SerializeAndPut() returned nil objectID")
	}

	// Verify object is stored
	objects, err := store.GetRaw([]*ids.ObjectID{objectID}, 0, "")
	if err != nil {
		t.Fatalf("GetRaw() error = %v", err)
	}

	if len(objects) == 0 || objects[0] == nil {
		t.Fatal("Object not stored")
	}

	// Deserialize and verify
	deserialized, err := serializer.Deserialize(objects[0], objectID, "")
	if err != nil {
		t.Fatalf("Deserialize() error = %v", err)
	}

	compareJSON(t, deserialized, value)
}

// TestObjectSerializer_GetAndDeserialize tests GetAndDeserialize convenience method.
func TestObjectSerializer_GetAndDeserialize(t *testing.T) {
	serializer := &mockSerializer{}
	store := newMockObjectStore()

	value := map[string]int{"a": 1, "b": 2}

	// First serialize and put
	objectID, err := serializer.SerializeAndPut(store, value)
	if err != nil {
		t.Fatalf("SerializeAndPut() error = %v", err)
	}

	// Then get and deserialize
	result, err := serializer.GetAndDeserialize(store, objectID, 0)
	if err != nil {
		t.Fatalf("GetAndDeserialize() error = %v", err)
	}

	compareJSON(t, result, value)
}

// TestObjectSerializer_MetadataTypes tests different metadata types.
func TestObjectSerializer_MetadataTypes(t *testing.T) {
	serializer := &mockSerializer{}

	tests := []struct {
		name     string
		value    interface{}
		expected string
	}{
		{"raw", []byte("raw data"), MetadataTypeRaw},
		{"xlang", map[string]int{"a": 1}, MetadataTypeXLang},
		// Note: Structs are considered cross-language (xlang) because msgpack
		// serializes them as maps which are language-agnostic
		{"go", struct{ A int }{1}, MetadataTypeXLang},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := serializer.Serialize(tt.value)
			if err != nil {
				t.Fatalf("Serialize() error = %v", err)
			}

			if string(result.Metadata) != tt.expected {
				t.Errorf("Metadata = %v, want %v", string(result.Metadata), tt.expected)
			}

			deserialized, err := serializer.Deserialize(result, nil, "")
			if err != nil {
				t.Fatalf("Deserialize() error = %v", err)
			}

			compareJSON(t, deserialized, tt.value)
		})
	}
}

// TestObjectSerializer_ConcurrentAccess tests concurrent serialization access.
func TestObjectSerializer_ConcurrentAccess(t *testing.T) {
	serializer := &mockSerializer{}
	done := make(chan bool, 10)

	for i := 0; i < 10; i++ {
		go func(id int) {
			defer func() { done <- true }()
			for j := 0; j < 100; j++ {
				value := map[string]int{"id": id, "count": j}
				result, err := serializer.Serialize(value)
				if err != nil {
					t.Errorf("Serialize() error = %v", err)
					return
				}

				deserialized, err := serializer.Deserialize(result, nil, "")
				if err != nil {
					t.Errorf("Deserialize() error = %v", err)
					return
				}

				if !compareJSON(t, deserialized, value) {
					t.Errorf("Roundtrip failed: got %v, want %v", deserialized, value)
				}
			}
		}(i)
	}

	for i := 0; i < 10; i++ {
		<-done
	}
}

// TestObjectSerializer_ContextManagement tests context management methods.
func TestObjectSerializer_ContextManagement(t *testing.T) {
	serializer := &mockSerializer{}

	// Test AddContainedObjectID and GetAndClearContainedObjectIDs
	oid1 := RandomObjectID()
	oid2 := RandomObjectID()

	serializer.AddContainedObjectID(oid1)
	serializer.AddContainedObjectID(oid2)

	containedIDs := serializer.GetAndClearContainedObjectIDs()
	if len(containedIDs) != 2 {
		t.Errorf("Expected 2 contained IDs, got %d", len(containedIDs))
	}

	// Verify cleared
	containedIDs = serializer.GetAndClearContainedObjectIDs()
	if len(containedIDs) != 0 {
		t.Errorf("Expected 0 contained IDs after clear, got %d", len(containedIDs))
	}

	// Test SetOuterObjectID and GetOuterObjectID
	serializer.SetOuterObjectID(oid1)
	retrieved := serializer.GetOuterObjectID()
	if !reflect.DeepEqual(retrieved, oid1) {
		t.Errorf("OuterObjectID mismatch: got %v, want %v", retrieved, oid1)
	}

	// Test ResetOuterObjectID
	serializer.ResetOuterObjectID()
	retrieved = serializer.GetOuterObjectID()
	// Check if the OuterObjectID is reset to zero value
	var zeroID ids.ObjectID
	if !retrieved.Equal(zeroID) {
		t.Errorf("Expected reset OuterObjectID to be zero value, got %v", retrieved)
	}
}

// BenchmarkObjectSerializer_Serialize benchmarks serialization.
func BenchmarkObjectSerializer_Serialize(b *testing.B) {
	serializer := &mockSerializer{}
	value := map[string]interface{}{
		"a": 1,
		"b": "string",
		"c": 3.14,
		"d": true,
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		serializer.Serialize(value)
	}
}

// BenchmarkObjectSerializer_Deserialize benchmarks deserialization.
func BenchmarkObjectSerializer_Deserialize(b *testing.B) {
	serializer := &mockSerializer{}
	value := map[string]interface{}{
		"a": 1,
		"b": "string",
		"c": 3.14,
		"d": true,
	}
	serialized, _ := serializer.Serialize(value)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		serializer.Deserialize(serialized, nil, "")
	}
}

// BenchmarkObjectSerializer_SerializeAndPut benchmarks SerializeAndPut.
func BenchmarkObjectSerializer_SerializeAndPut(b *testing.B) {
	serializer := &mockSerializer{}
	store := newMockObjectStore()
	value := map[string]interface{}{
		"a": 1,
		"b": "string",
		"c": 3.14,
		"d": true,
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		serializer.SerializeAndPut(store, value)
	}
}

// BenchmarkObjectSerializer_GetAndDeserialize benchmarks GetAndDeserialize.
func BenchmarkObjectSerializer_GetAndDeserialize(b *testing.B) {
	serializer := &mockSerializer{}
	store := newMockObjectStore()
	value := map[string]interface{}{
		"a": 1,
		"b": "string",
		"c": 3.14,
		"d": true,
	}
	objectID, _ := serializer.SerializeAndPut(store, value)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		serializer.GetAndDeserialize(store, objectID, 0)
	}
}
