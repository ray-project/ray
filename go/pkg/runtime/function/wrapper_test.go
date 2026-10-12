// Copyright 2026 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package function

import (
	"errors"
	"testing"

	"github.com/ray-project/ray/go/pkg/ids"
	"github.com/ray-project/ray/go/pkg/runtime/object"
)

// countingSerializer records how many values the wrapper hands to the
// serializer, so a test can assert the wrapper's output shape without pulling
// in the real msgpack serializer.
type countingSerializer struct {
	serialized int
}

func (s *countingSerializer) Serialize(obj interface{}) (*object.NativeRayObject, error) {
	s.serialized++
	return object.NewNativeRayObject([]byte("x"), []byte(object.MetadataTypeGo)), nil
}
func (s *countingSerializer) Deserialize(nativeObj *object.NativeRayObject, objectID *ids.ObjectID, objectType string) (interface{}, error) {
	return nil, nil
}
func (s *countingSerializer) DeserializeTo(nativeObj *object.NativeRayObject, target interface{}) error {
	return nil
}
func (s *countingSerializer) AddContainedObjectID(objectID ids.ObjectID)     {}
func (s *countingSerializer) GetAndClearContainedObjectIDs() []ids.ObjectID  { return nil }
func (s *countingSerializer) SetOuterObjectID(objectID ids.ObjectID)         {}
func (s *countingSerializer) GetOuterObjectID() ids.ObjectID                 { return ids.NilObjectID() }
func (s *countingSerializer) ResetOuterObjectID()                            {}
func (s *countingSerializer) EstimateBufferSize(obj interface{}) int         { return 0 }
func (s *countingSerializer) GetBuffer(size int) []byte                      { return make([]byte, size) }
func (s *countingSerializer) PutBuffer(buf []byte)                           {}
func (s *countingSerializer) IsCrossLanguageType(obj interface{}) bool       { return false }

// TestWrapGoFunctionTrailingErrorIsNotSerialized pins the trailing-error
// convention for remote task returns. A function whose last return value
// implements error must convey outcome through that value, so the wrapper
// serializes only the non-error values. Serializing the error instead would
// emit a spurious extra return object -- the driver derives numReturns from
// NonErrorReturnCount, so the object count would not match -- and a nil error
// would reach the real serializer, where reflect.TypeOf(nil) is a nil Type and
// isCrossLanguageTypeRecursive dereferences it (SIGSEGV).
func TestWrapGoFunctionTrailingErrorIsNotSerialized(t *testing.T) {
	ser := &countingSerializer{}
	object.SetSerializer(ser)

	fn := WrapGoFunction(func() (string, error) {
		return "node-1", nil
	})

	results, err := fn(nil)
	if err != nil {
		t.Fatalf("expected success, got %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 return object for a (string, error) signature, got %d", len(results))
	}
	if ser.serialized != 1 {
		t.Fatalf("expected exactly 1 value serialized, got %d", ser.serialized)
	}
}

// TestWrapGoFunctionNonNilTrailingErrorIsPropagated verifies that a non-nil
// trailing error fails the task instead of being serialized as data, so the
// caller sees the real failure.
func TestWrapGoFunctionNonNilTrailingErrorIsPropagated(t *testing.T) {
	ser := &countingSerializer{}
	object.SetSerializer(ser)

	sentinel := errors.New("boom")
	fn := WrapGoFunction(func() (string, error) {
		return "", sentinel
	})

	results, err := fn(nil)
	if !errors.Is(err, sentinel) {
		t.Fatalf("expected the sentinel error to propagate, got results=%d err=%v", len(results), err)
	}
	if ser.serialized != 0 {
		t.Fatalf("expected nothing serialized when the error propagates, got %d", ser.serialized)
	}
}
