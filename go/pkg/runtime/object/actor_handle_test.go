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
)

func TestNativeActorHandle_Serialize(t *testing.T) {
	jobID := ids.JobIDFromInt(100)
	taskID := ids.NilTaskID()
	actorID := ids.OfActorID(jobID, taskID, 1)

	actorHandleID := RandomObjectID()

	handle := &NativeActorHandle{
		ActorID:       actorID,
		Language:      LanguageGo,
		ActorHandleID: actorHandleID,
		OwnerAddress: &ActorOwnerAddress{
			IPAddress: "127.0.0.1",
			Port:      8080,
			WorkerID:  "test-worker",
		},
	}

	nativeObj, err := handle.Serialize()
	if err != nil {
		t.Fatalf("Serialize() error = %v", err)
	}

	if nativeObj == nil {
		t.Fatal("Serialize() result = nil")
	}

	if string(nativeObj.Metadata) != MetadataTypeActorHandle {
		t.Errorf("Serialize() metadata = %v, want %v", string(nativeObj.Metadata), MetadataTypeActorHandle)
	}

	if len(nativeObj.ContainedObjectIds) == 0 {
		t.Error("Serialize() ContainedObjectIds not set")
	}
}

func TestNativeActorHandle_SerializeNilActorID(t *testing.T) {
	handle := &NativeActorHandle{
		ActorID:  ids.NilActorID(),
		Language: LanguageGo,
	}

	_, err := handle.Serialize()
	if err == nil {
		t.Error("Serialize() expected error for nil ActorID")
	}
}

func TestDeserializeActorHandle(t *testing.T) {
	jobID := ids.JobIDFromInt(100)
	taskID := ids.NilTaskID()
	actorID := ids.OfActorID(jobID, taskID, 2)

	actorHandleID := RandomObjectID()

	originalHandle := &NativeActorHandle{
		ActorID:       actorID,
		Language:      LanguageGo,
		ActorHandleID: actorHandleID,
		OwnerAddress: &ActorOwnerAddress{
			IPAddress: "127.0.0.1",
			Port:      8080,
			WorkerID:  "test-worker",
		},
	}

	nativeObj, err := originalHandle.Serialize()
	if err != nil {
		t.Fatalf("Serialize() error = %v", err)
	}

	deserializedHandle, err := DeserializeActorHandle(nativeObj)
	if err != nil {
		t.Fatalf("DeserializeActorHandle() error = %v", err)
	}

	if deserializedHandle == nil {
		t.Fatal("DeserializeActorHandle() result = nil")
	}

	if deserializedHandle.ActorID != originalHandle.ActorID {
		t.Errorf("ActorID mismatch: got %v, want %v", deserializedHandle.ActorID, originalHandle.ActorID)
	}

	if deserializedHandle.Language != originalHandle.Language {
		t.Errorf("Language mismatch: got %v, want %v", deserializedHandle.Language, originalHandle.Language)
	}

	if deserializedHandle.ActorHandleID != originalHandle.ActorHandleID {
		t.Errorf("ActorHandleID mismatch: got %v, want %v", deserializedHandle.ActorHandleID, originalHandle.ActorHandleID)
	}

	if deserializedHandle.OwnerAddress == nil {
		t.Fatal("OwnerAddress is nil")
	}
	if deserializedHandle.OwnerAddress.IPAddress != originalHandle.OwnerAddress.IPAddress {
		t.Errorf("OwnerAddress.IPAddress mismatch: got %v, want %v",
			deserializedHandle.OwnerAddress.IPAddress, originalHandle.OwnerAddress.IPAddress)
	}
	if deserializedHandle.OwnerAddress.Port != originalHandle.OwnerAddress.Port {
		t.Errorf("OwnerAddress.Port mismatch: got %v, want %v",
			deserializedHandle.OwnerAddress.Port, originalHandle.OwnerAddress.Port)
	}
	if deserializedHandle.OwnerAddress.WorkerID != originalHandle.OwnerAddress.WorkerID {
		t.Errorf("OwnerAddress.WorkerID mismatch: got %v, want %v",
			deserializedHandle.OwnerAddress.WorkerID, originalHandle.OwnerAddress.WorkerID)
	}
}

func TestDeserializeActorHandle_InvalidMetadata(t *testing.T) {
	nativeObj := &NativeRayObject{
		Data:     []byte("test data"),
		Metadata: []byte("invalid_metadata"),
	}

	_, err := DeserializeActorHandle(nativeObj)
	if err == nil {
		t.Error("DeserializeActorHandle() expected error for invalid metadata")
	}
}

func TestDeserializeActorHandle_NilObject(t *testing.T) {
	_, err := DeserializeActorHandle(nil)
	if err == nil {
		t.Error("DeserializeActorHandle() expected error for nil object")
	}
}

func BenchmarkActorHandle_Serialize(b *testing.B) {
	jobID := ids.JobIDFromInt(100)
	taskID := ids.NilTaskID()
	actorID := ids.OfActorID(jobID, taskID, 5)
	actorHandleID := RandomObjectID()

	handle := &NativeActorHandle{
		ActorID:       actorID,
		Language:      LanguageGo,
		ActorHandleID: actorHandleID,
		OwnerAddress: &ActorOwnerAddress{
			IPAddress: "127.0.0.1",
			Port:      8080,
			WorkerID:  "test-worker",
		},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := handle.Serialize()
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkActorHandle_Deserialize(b *testing.B) {
	jobID := ids.JobIDFromInt(100)
	taskID := ids.NilTaskID()
	actorID := ids.OfActorID(jobID, taskID, 6)
	actorHandleID := RandomObjectID()

	handle := &NativeActorHandle{
		ActorID:       actorID,
		Language:      LanguageGo,
		ActorHandleID: actorHandleID,
		OwnerAddress: &ActorOwnerAddress{
			IPAddress: "127.0.0.1",
			Port:      8080,
			WorkerID:  "test-worker",
		},
	}

	nativeObj, _ := handle.Serialize()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := DeserializeActorHandle(nativeObj)
		if err != nil {
			b.Fatal(err)
		}
	}
}
