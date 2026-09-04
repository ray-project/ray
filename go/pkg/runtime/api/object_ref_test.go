// Copyright 2025 The Ray Authors.
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

package api

import (
	"testing"

	"github.com/ray-project/ray/go/pkg/errors"
	"github.com/ray-project/ray/go/pkg/ids"
)

// TestObjectRef_String tests the String() method.
func TestObjectRef_String(t *testing.T) {
	// Create a test ObjectID (28 bytes = 56 hex characters)
	objectID, err := ids.ObjectIDFromHex("0123456789abcdef0123456789abcdef0123456789abcdef01234567")
	if err != nil {
		t.Fatalf("Failed to create ObjectID: %v", err)
	}

	// Create ObjectRef
	ref := NewObjectRef[string](objectID, "string", true)

	// Test String() method
	expected := "ObjectRef(0123456789abcdef0123456789abcdef0123456789abcdef01234567)"
	if ref.String() != expected {
		t.Errorf("String() = %q, want %q", ref.String(), expected)
	}
}

// TestObjectRef_ObjectID tests the ObjectID() method.
func TestObjectRef_ObjectID(t *testing.T) {
	objectID, err := ids.ObjectIDFromHex("0123456789abcdef0123456789abcdef0123456789abcdef01234567")
	if err != nil {
		t.Fatalf("Failed to create ObjectID: %v", err)
	}

	ref := NewObjectRef[string](objectID, "string", true)

	// Test ObjectID() method
	if ref.ObjectID() != objectID {
		t.Errorf("ObjectID() = %v, want %v", ref.ObjectID(), objectID)
	}
}

// TestObjectRef_ObjectType tests the ObjectType() method.
func TestObjectRef_ObjectType(t *testing.T) {
	objectID, err := ids.ObjectIDFromHex("0123456789abcdef0123456789abcdef0123456789abcdef01234567")
	if err != nil {
		t.Fatalf("Failed to create ObjectID: %v", err)
	}

	ref := NewObjectRef[int](objectID, "int", true)

	// Test ObjectType() method
	if ref.ObjectType() != "int" {
		t.Errorf("ObjectType() = %q, want %q", ref.ObjectType(), "int")
	}
}

// TestObjectRef_Get_WithoutInit tests Get() behavior when runtime is not initialized.
func TestObjectRef_Get_WithoutInit(t *testing.T) {
	objectID, err := ids.ObjectIDFromHex("0123456789abcdef0123456789abcdef0123456789abcdef01234567")
	if err != nil {
		t.Fatalf("Failed to create ObjectID: %v", err)
	}

	ref := NewObjectRef[string](objectID, "string", true)

	// Test Get() without runtime initialization
	_, err = ref.Get()
	if err == nil {
		t.Error("Get() should return error when runtime is not initialized")
	}
	// Check that error is ErrRuntimeNotInitialized
	if err != errors.ErrRuntimeNotInitialized {
		t.Errorf("Get() error = %q, want ErrRuntimeNotInitialized", err)
	}
}

// TestObjectRef_GetWithTimeout_WithoutInit tests GetWithTimeout() behavior when runtime is not initialized.
func TestObjectRef_GetWithTimeout_WithoutInit(t *testing.T) {
	objectID, err := ids.ObjectIDFromHex("0123456789abcdef0123456789abcdef0123456789abcdef01234567")
	if err != nil {
		t.Fatalf("Failed to create ObjectID: %v", err)
	}

	ref := NewObjectRef[string](objectID, "string", true)

	// Test GetWithTimeout() without runtime initialization
	_, err = ref.GetWithTimeout(1000)
	if err == nil {
		t.Error("GetWithTimeout() should return error when runtime is not initialized")
	}
	// Check that error is ErrRuntimeNotInitialized
	if err != errors.ErrRuntimeNotInitialized {
		t.Errorf("GetWithTimeout() error = %q, want ErrRuntimeNotInitialized", err)
	}
}

// TestObjectRef_Equal tests equality comparison between ObjectRefs.
func TestObjectRef_Equal(t *testing.T) {
	objectID1, err := ids.ObjectIDFromHex("0123456789abcdef0123456789abcdef0123456789abcdef01234567")
	if err != nil {
		t.Fatalf("Failed to create ObjectID1: %v", err)
	}

	objectID2, err := ids.ObjectIDFromHex("fedcba9876543210fedcba9876543210fedcba9876543210fedcba98")
	if err != nil {
		t.Fatalf("Failed to create ObjectID2: %v", err)
	}

	ref1 := NewObjectRef[string](objectID1, "string", true)
	ref2 := NewObjectRef[string](objectID1, "string", true)
	ref3 := NewObjectRef[string](objectID2, "string", true)

	// Test equality (same ObjectID)
	if ref1.ObjectID() != ref2.ObjectID() {
		t.Error("ObjectRefs with same ObjectID should be equal")
	}

	// Test inequality (different ObjectID)
	if ref1.ObjectID() == ref3.ObjectID() {
		t.Error("ObjectRefs with different ObjectID should not be equal")
	}
}

// TestNewObjectRef tests the NewObjectRef constructor.
func TestNewObjectRef(t *testing.T) {
	objectID, err := ids.ObjectIDFromHex("0123456789abcdef0123456789abcdef0123456789abcdef01234567")
	if err != nil {
		t.Fatalf("Failed to create ObjectID: %v", err)
	}

	// Test with skipAddingLocalRef = true
	ref1 := NewObjectRef[string](objectID, "string", true)
	if ref1.objectID != objectID {
		t.Errorf("objectID = %v, want %v", ref1.objectID, objectID)
	}
	if ref1.objectType != "string" {
		t.Errorf("objectType = %q, want %q", ref1.objectType, "string")
	}
	if ref1.skipAddingLocalRef != true {
		t.Errorf("skipAddingLocalRef = %v, want true", ref1.skipAddingLocalRef)
	}

	// Test with skipAddingLocalRef = false
	ref2 := NewObjectRef[int](objectID, "int", false)
	if ref2.skipAddingLocalRef != false {
		t.Errorf("skipAddingLocalRef = %v, want false", ref2.skipAddingLocalRef)
	}
}

// TestObjectRef_GenericTypes tests ObjectRef with different generic types.
func TestObjectRef_GenericTypes(t *testing.T) {
	objectID, err := ids.ObjectIDFromHex("0123456789abcdef0123456789abcdef0123456789abcdef01234567")
	if err != nil {
		t.Fatalf("Failed to create ObjectID: %v", err)
	}

	// Test with string type
	stringRef := NewObjectRef[string](objectID, "string", true)
	if stringRef.ObjectType() != "string" {
		t.Errorf("string ObjectType() = %q, want %q", stringRef.ObjectType(), "string")
	}

	// Test with int type
	intRef := NewObjectRef[int](objectID, "int", true)
	if intRef.ObjectType() != "int" {
		t.Errorf("int ObjectType() = %q, want %q", intRef.ObjectType(), "int")
	}

	// Test with struct type
	type TestStruct struct {
		Name string
		Age  int
	}
	structRef := NewObjectRef[TestStruct](objectID, "TestStruct", true)
	if structRef.ObjectType() != "TestStruct" {
		t.Errorf("struct ObjectType() = %q, want %q", structRef.ObjectType(), "TestStruct")
	}

	// Test with slice type
	sliceRef := NewObjectRef[[]byte](objectID, "[]byte", true)
	if sliceRef.ObjectType() != "[]byte" {
		t.Errorf("slice ObjectType() = %q, want %q", sliceRef.ObjectType(), "[]byte")
	}

	// Test with map type
	mapRef := NewObjectRef[map[string]int](objectID, "map[string]int", true)
	if mapRef.ObjectType() != "map[string]int" {
		t.Errorf("map ObjectType() = %q, want %q", mapRef.ObjectType(), "map[string]int")
	}
}
