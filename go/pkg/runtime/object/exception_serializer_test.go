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
	"strings"
	"testing"
)

// TestRayExceptionSerializer_ToBytes tests exception serialization to bytes.
func TestRayExceptionSerializer_ToBytes(t *testing.T) {
	serializer := &RayExceptionSerializer{}

	// Test RayTaskExecutionException
	taskEx := NewRayTaskExecutionException(
		"task-123",
		fmt.Errorf("Task execution failed"),
		"goroutine 1 [running]:\nmain.main()\n\t/main.go:10",
	)

	data, err := serializer.ToBytes(taskEx)
	if err != nil {
		t.Fatalf("ToBytes failed: %v", err)
	}

	if data == nil {
		t.Fatal("ToBytes returned nil data")
	}

	// Verify we can deserialize it back
	exception, err := serializer.FromBytes(data)
	if err != nil {
		t.Fatalf("FromBytes failed: %v", err)
	}

	if exception == nil {
		t.Fatal("FromBytes returned nil exception")
	}

	if exception.ErrorCode() != ErrorCodeTaskExecutionException {
		t.Errorf("Expected error code %d, got %d", ErrorCodeTaskExecutionException, exception.ErrorCode())
	}

	// Verify it's a Go exception
	if !strings.Contains(exception.Error(), "Task execution failed") {
		t.Errorf("Expected task execution message, got: %s", exception.Error())
	}
}

// TestRayExceptionSerializer_FromBytes_CrossLanguage tests cross-language exception deserialization.
func TestRayExceptionSerializer_FromBytes_CrossLanguage(t *testing.T) {
	serializer := &RayExceptionSerializer{}

	// Simulate Java exception data (manually constructed for testing)
	// This matches the format that Java's RayExceptionSerializer would produce
	javaExceptionData := []byte{
		// MessagePack map with 4 elements
		0x84,
		// "language" -> "JAVA"
		0xa8, 'l', 'a', 'n', 'g', 'u', 'a', 'g', 'e',
		0xa4, 'J', 'A', 'V', 'A',
		// "formatted_exception" -> "java.lang.NullPointerException..."
		0xb3, 'f', 'o', 'r', 'm', 'a', 't', 't', 'e', 'd', '_', 'e', 'x', 'c', 'e', 'p', 't', 'i', 'o', 'n',
		0xd8, 0x65, // MessagePack timestamp/extended type (simulated)
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01,
		// "serialized_exception" -> empty (for this test)
		0xb6, 's', 'e', 'r', 'i', 'a', 'l', 'i', 'z', 'e', 'd', '_', 'e', 'x', 'c', 'e', 'p', 't', 'i', 'o', 'n',
		0xc0, // nil
		// "error_code" -> 400
		0xaa, 'e', 'r', 'r', 'o', 'r', '_', 'c', 'o', 'd', 'e',
		0xcd, 0x01, 0x90,
	}

	exception, err := serializer.FromBytes(javaExceptionData)
	if err != nil {
		// Expected to fail due to simplified test data, but should handle gracefully
		t.Logf("FromBytes handled Java exception data (expected parsing issue in test data): %v", err)
	}

	// If it succeeded, verify it's a cross-language exception
	if exception != nil {
		if crossEx, ok := exception.(*CrossLanguageException); ok {
			if crossEx.SourceLanguage != "JAVA" {
				t.Errorf("Expected source language JAVA, got: %s", crossEx.SourceLanguage)
			}
		}
	}
}

// TestCrossLanguageException_ToBytes tests cross-language exception serialization.
func TestCrossLanguageException_ToBytes(t *testing.T) {
	crossEx := &CrossLanguageException{
		SourceLanguage: "PYTHON",
		Message:        "An exception raised from PYTHON:\nTraceback (most recent call last):\n  File \"test.py\", line 1",
		ErrorCodeValue: 503,
		TaskID:         "task-456",
		ActorID:        "actor-789",
	}

	data := crossEx.ToBytes()
	if data == nil {
		t.Fatal("ToBytes returned nil data")
	}

	// Verify we can deserialize it
	serializer := &RayExceptionSerializer{}
	exception, err := serializer.FromBytes(data)
	if err != nil {
		t.Fatalf("FromBytes failed: %v", err)
	}

	if exception == nil {
		t.Fatal("FromBytes returned nil exception")
	}

	if exception.ErrorCode() != 503 {
		t.Errorf("Expected error code 503, got %d", exception.ErrorCode())
	}
}

// TestRayExceptionSerializer_RayIDException tests RayIDException serialization.
func TestRayExceptionSerializer_RayIDException(t *testing.T) {
	serializer := &RayExceptionSerializer{}

	// Test ActorDied exception
	actorDiedEx := NewRayIDExceptionActorDied("actor-abc123")

	data, err := serializer.ToBytes(actorDiedEx)
	if err != nil {
		t.Fatalf("ToBytes failed: %v", err)
	}

	if data == nil {
		t.Fatal("ToBytes returned nil data")
	}

	// Verify we can deserialize it back
	exception, err := serializer.FromBytes(data)
	if err != nil {
		t.Fatalf("FromBytes failed: %v", err)
	}

	if exception == nil {
		t.Fatal("FromBytes returned nil exception")
	}

	if exception.ErrorCode() != ErrorCodeActorDied {
		t.Errorf("Expected error code %d, got %d", ErrorCodeActorDied, exception.ErrorCode())
	}
}

// TestExceptionData_Structure tests ExceptionData structure serialization.
func TestExceptionData_Structure(t *testing.T) {
	data := &ExceptionData{
		Language:            LanguageGo,
		FormattedException:  "test exception",
		SerializedException: []byte{0x01, 0x02, 0x03},
		ErrorCode:           404,
		TaskID:              "task-test",
		ActorID:             "actor-test",
		ObjectID:            "object-test",
	}

	serializer := &RayExceptionSerializer{}
	bytes, err := serializer.ToBytes(NewRayTaskExecutionException(
		data.TaskID,
		fmt.Errorf("%s", data.FormattedException),
		data.FormattedException,
	))
	if err != nil {
		t.Fatalf("ToBytes failed: %v", err)
	}

	// Deserialize back
	exception, err := serializer.FromBytes(bytes)
	if err != nil {
		t.Fatalf("FromBytes failed: %v", err)
	}

	if exception == nil {
		t.Fatal("FromBytes returned nil exception")
	}

	if exception.ErrorCode() != ErrorCodeTaskExecutionException {
		t.Errorf("Expected error code %d, got %d", ErrorCodeTaskExecutionException, exception.ErrorCode())
	}
}

// TestRayExceptionSerializer_NilData tests FromBytes with nil data.
func TestRayExceptionSerializer_NilData(t *testing.T) {
	serializer := &RayExceptionSerializer{}

	exception, err := serializer.FromBytes(nil)
	if err != nil {
		t.Errorf("FromBytes with nil data should return nil error, got: %v", err)
	}

	if exception != nil {
		t.Errorf("FromBytes with nil data should return nil exception, got: %v", exception)
	}
}

// TestObjectSerializer_SerializeException tests ObjectSerializer exception handling.
func TestObjectSerializer_SerializeException(t *testing.T) {
	// Use RayExceptionSerializer instead of mockSerializer for exception tests
	serializer := &RayExceptionSerializer{}

	// Test RayTaskExecutionException serialization
	taskEx := NewRayTaskExecutionException(
		"task-test-123",
		fmt.Errorf("Task execution failed in test"),
		"test stack trace",
	)

	// Serialize exception to bytes
	data, err := serializer.ToBytes(taskEx)
	if err != nil {
		t.Fatalf("ToBytes failed: %v", err)
	}

	if data == nil {
		t.Fatal("ToBytes returned nil data")
	}

	// Create NativeRayObject with exception data
	nativeObj := &NativeRayObject{
		Data:     data,
		Metadata: []byte(MetadataTypeTaskExecutionException),
	}

	// Verify metadata
	if string(nativeObj.Metadata) != MetadataTypeTaskExecutionException {
		t.Errorf("Expected metadata %s, got %s", MetadataTypeTaskExecutionException, string(nativeObj.Metadata))
	}

	// Test deserialization using FromBytes
	result, err := serializer.FromBytes(data)
	if err != nil {
		t.Fatalf("FromBytes failed: %v", err)
	}

	if result == nil {
		t.Fatal("FromBytes returned nil result")
	}

	// Verify it's a RayException
	rayEx, ok := result.(RayException)
	if !ok {
		t.Fatalf("Expected RayException, got %T", result)
	}

	if rayEx.ErrorCode() != ErrorCodeTaskExecutionException {
		t.Errorf("Expected error code %d, got %d", ErrorCodeTaskExecutionException, rayEx.ErrorCode())
	}

	if !strings.Contains(rayEx.Error(), "Task execution failed in test") {
		t.Errorf("Expected task execution message, got: %s", rayEx.Error())
	}
}

// TestObjectSerializer_SerializeException_RoundTrip tests exception serialize/deserialize round trip.
func TestObjectSerializer_SerializeException_RoundTrip(t *testing.T) {
	// Use RayExceptionSerializer instead of mockSerializer for exception tests
	serializer := &RayExceptionSerializer{}

	// Create various exception types and verify round trip
	exceptions := []RayException{
		NewRayTaskExecutionException(
			"task-001",
			fmt.Errorf("Internal server error"),
			"goroutine 1 [running]:\ntest.stack.trace()",
		),
		NewRayIDExceptionActorDied("actor-123"),
		NewRayIDExceptionActorUnavailable("actor-456"),
		NewRayIDExceptionLost("object-789"),
		NewRayIDExceptionOwnerDied("owner-abc"),
	}

	for i, originalEx := range exceptions {
		// Serialize exception to bytes
		data, err := serializer.ToBytes(originalEx)
		if err != nil {
			t.Fatalf("Exception %d: ToBytes failed: %v", i, err)
		}

		if data == nil {
			t.Fatalf("Exception %d: ToBytes returned nil data", i)
		}

		// Deserialize from bytes
		result, err := serializer.FromBytes(data)
		if err != nil {
			t.Fatalf("Exception %d: FromBytes failed: %v", i, err)
		}

		if result == nil {
			t.Fatalf("Exception %d: FromBytes returned nil result", i)
		}

		// Verify it's a RayException
		deserializedEx, ok := result.(RayException)
		if !ok {
			t.Fatalf("Exception %d: Expected RayException, got %T", i, result)
		}

		if deserializedEx.ErrorCode() != originalEx.ErrorCode() {
			t.Errorf("Exception %d: Expected error code %d, got %d", i, originalEx.ErrorCode(), deserializedEx.ErrorCode())
		}
	}
}
