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

	"github.com/ray-project/ray/go/pkg/ids"
	"github.com/ray-project/ray/go/pkg/runtime/object"
)

// NativeRayObject is a type alias for object.NativeRayObject.
// This allows the serializer package to work with the object package's
// NativeRayObject type without creating a circular dependency.
type NativeRayObject = object.NativeRayObject

// ObjectSerializerImpl implements the object.Serializer interface.
// This adapter wraps the internal Serializer to provide compatibility
// with the object package's abstraction layer.
type ObjectSerializerImpl struct {
	*Serializer  // Embed the existing implementation
}

// Compile-time interface compliance check.
var _ object.Serializer = (*ObjectSerializerImpl)(nil)

// NewObjectSerializerImpl creates a new ObjectSerializerImpl.
func NewObjectSerializerImpl() *ObjectSerializerImpl {
	return &ObjectSerializerImpl{
		Serializer: NewSerializer(),
	}
}

// init() automatically registers the ObjectSerializerImpl with the object package.
// This allows high-level packages to use object.GetSerializer() without
// directly depending on the serializer package, following the Dependency Inversion Principle.
func init() {
	object.SetSerializer(NewObjectSerializerImpl())
}

// Serialize implements object.Serializer.Serialize.
func (s *ObjectSerializerImpl) Serialize(obj interface{}) (*object.NativeRayObject, error) {
	// Delegate to the internal implementation
	nativeObj, err := s.Serializer.Serialize(obj)
	if err != nil {
		return nil, err
	}

	// Convert to object.NativeRayObject
	return &object.NativeRayObject{
		Data:               nativeObj.Data,
		Metadata:           nativeObj.Metadata,
		ContainedObjectIds: nativeObj.ContainedObjectIds,
	}, nil
}

// Deserialize implements object.Serializer.Deserialize.
func (s *ObjectSerializerImpl) Deserialize(nativeObj *object.NativeRayObject, objectID *ids.ObjectID, objectType string) (interface{}, error) {
	if nativeObj == nil {
		return nil, fmt.Errorf("native object is nil")
	}

	// Convert from object.NativeRayObject to internal NativeRayObject
	internalObj := &NativeRayObject{
		Data:               nativeObj.Data,
		Metadata:           nativeObj.Metadata,
		ContainedObjectIds: nativeObj.ContainedObjectIds,
	}

	// Delegate to the internal implementation with objectType
	return s.Serializer.Deserialize(internalObj, objectID, objectType)
}

// DeserializeTo implements object.Serializer.DeserializeTo.
func (s *ObjectSerializerImpl) DeserializeTo(nativeObj *object.NativeRayObject, target interface{}) error {
	if nativeObj == nil {
		return fmt.Errorf("native object is nil")
	}

	// Convert from object.NativeRayObject to internal NativeRayObject
	internalObj := &NativeRayObject{
		Data:               nativeObj.Data,
		Metadata:           nativeObj.Metadata,
		ContainedObjectIds: nativeObj.ContainedObjectIds,
	}

	// Delegate to the internal implementation
	return s.Serializer.DeserializeTo(internalObj, target)
}

// AddContainedObjectID implements object.Serializer.AddContainedObjectID.
func (s *ObjectSerializerImpl) AddContainedObjectID(objectID ids.ObjectID) {
	s.Serializer.AddContainedObjectID(objectID)
}

// GetAndClearContainedObjectIDs implements object.Serializer.GetAndClearContainedObjectIDs.
func (s *ObjectSerializerImpl) GetAndClearContainedObjectIDs() []ids.ObjectID {
	return s.Serializer.GetAndClearContainedObjectIDs()
}

// SetOuterObjectID implements object.Serializer.SetOuterObjectID.
func (s *ObjectSerializerImpl) SetOuterObjectID(objectID ids.ObjectID) {
	s.Serializer.SetOuterObjectID(objectID)
}

// GetOuterObjectID implements object.Serializer.GetOuterObjectID.
func (s *ObjectSerializerImpl) GetOuterObjectID() ids.ObjectID {
	return s.Serializer.GetOuterObjectID()
}

// ResetOuterObjectID implements object.Serializer.ResetOuterObjectID.
func (s *ObjectSerializerImpl) ResetOuterObjectID() {
	s.Serializer.ResetOuterObjectID()
}

// EstimateBufferSize implements object.Serializer.EstimateBufferSize.
func (s *ObjectSerializerImpl) EstimateBufferSize(obj interface{}) int {
	return EstimateBufferSize(obj)
}

// GetBuffer implements object.Serializer.GetBuffer.
func (s *ObjectSerializerImpl) GetBuffer(size int) []byte {
	return GetBuffer(size)
}

// PutBuffer implements object.Serializer.PutBuffer.
func (s *ObjectSerializerImpl) PutBuffer(buf []byte) {
	PutBuffer(buf)
}

// IsCrossLanguageType implements object.Serializer.IsCrossLanguageType.
func (s *ObjectSerializerImpl) IsCrossLanguageType(obj interface{}) bool {
	return IsCrossLanguageType(obj)
}
