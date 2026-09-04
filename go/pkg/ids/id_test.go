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

package ids

import (
	"bytes"
	"testing"
)

func TestJobIDConstants(t *testing.T) {
	if JobIDSize != 4 {
		t.Errorf("JobIDSize should be 4, got %d", JobIDSize)
	}
}

func TestJobIDFromInt(t *testing.T) {
	tests := []struct {
		name     string
		value    uint32
		expected string
	}{
		{"zero", 0, "00000000"},
		{"one", 1, "01000000"},
		{"two_fifty_six", 256, "00010000"},
		{"max", 0xffffffff, "ffffffff"},
	}

	for _, tt := range tests {
		id := JobIDFromInt(tt.value)
		if id.Hex() != tt.expected {
			t.Errorf("%s: JobIDFromInt(%d).Hex() = %s, want %s",
				tt.name, tt.value, id.Hex(), tt.expected)
		}
		if id.ToInt() != tt.value {
			t.Errorf("%s: ToInt() = %d, want %d", tt.name, id.ToInt(), tt.value)
		}
	}
}

func TestJobIDNil(t *testing.T) {
	nilID := NilJobID()
	if !nilID.IsNil() {
		t.Error("NilJobID should be nil")
	}
	if nilID.String() != "NIL_ID" {
		t.Errorf("NilJobID.String() = %s, want NIL_ID", nilID.String())
	}

	nonNil := JobIDFromInt(1)
	if nonNil.IsNil() {
		t.Error("JobIDFromInt(1) should not be nil")
	}
}

func TestJobIDFromBinary(t *testing.T) {
	data := []byte{0x01, 0x02, 0x03, 0x04}
	id, err := JobIDFromBinary(data)
	if err != nil {
		t.Fatalf("JobIDFromBinary failed: %v", err)
	}
	if id.Hex() != "01020304" {
		t.Errorf("Hex() = %s, want 01020304", id.Hex())
	}

	// Invalid length
	_, err = JobIDFromBinary([]byte{0x01, 0x02})
	if err == nil {
		t.Error("JobIDFromBinary should fail for invalid length")
	}
}

func TestJobIDFromHex(t *testing.T) {
	id, err := JobIDFromHex("01020304")
	if err != nil {
		t.Fatalf("JobIDFromHex failed: %v", err)
	}
	if id.ToInt() != 0x04030201 {
		t.Errorf("ToInt() = %d, want %d", id.ToInt(), 0x04030201)
	}

	// Invalid length
	_, err = JobIDFromHex("0102")
	if err == nil {
		t.Error("JobIDFromHex should fail for invalid length")
	}
}

func TestJobIDEqual(t *testing.T) {
	id1 := JobIDFromInt(100)
	id2 := JobIDFromInt(100)
	id3 := JobIDFromInt(200)

	if !id1.Equal(id2) {
		t.Error("equal JobIDs should be equal")
	}
	if id1.Equal(id3) {
		t.Error("different JobIDs should not be equal")
	}
}

func TestJobIDHash(t *testing.T) {
	id := JobIDFromInt(123)
	hash := id.Hash()
	// Hash should be deterministic
	if id.Hash() != hash {
		t.Error("Hash should be deterministic")
	}
}

func TestJobIDNew(t *testing.T) {
	id1 := NewJobID()
	id2 := NewJobID()

	// Two random IDs should be different
	if id1.Equal(id2) {
		t.Error("two random JobIDs should be different")
	}

	// New JobID should not be nil
	if id1.IsNil() {
		t.Error("NewJobID should not be nil")
	}
}

func TestJobIDSize(t *testing.T) {
	id := JobIDFromInt(100)
	if id.Size() != JobIDSize {
		t.Errorf("Size() = %d, want %d", id.Size(), JobIDSize)
	}
}

func TestUniqueIDConstants(t *testing.T) {
	if UniqueIDSize != 28 {
		t.Errorf("UniqueIDSize should be 28, got %d", UniqueIDSize)
	}
}

func TestUniqueIDNil(t *testing.T) {
	nilID := NilUniqueID()
	if !nilID.IsNil() {
		t.Error("NilUniqueID should be nil")
	}
	if nilID.String() != "NIL_ID" {
		t.Errorf("NilUniqueID.String() = %s, want NIL_ID", nilID.String())
	}
}

func TestUniqueIDFromBinary(t *testing.T) {
	data := make([]byte, UniqueIDSize)
	for i := range data {
		data[i] = byte(i)
	}

	id, err := UniqueIDFromBinary(data)
	if err != nil {
		t.Fatalf("UniqueIDFromBinary failed: %v", err)
	}

	// Verify round-trip
	binary := id.Binary()
	if !bytes.Equal(binary, data) {
		t.Error("Binary round-trip failed")
	}

	// Invalid length
	_, err = UniqueIDFromBinary([]byte{0x01})
	if err == nil {
		t.Error("UniqueIDFromBinary should fail for invalid length")
	}
}

func TestUniqueIDFromHex(t *testing.T) {
	// 28 bytes = 56 hex chars
	hexStr := "0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c"
	id, err := UniqueIDFromHex(hexStr)
	if err != nil {
		t.Fatalf("UniqueIDFromHex failed: %v", err)
	}

	if id.Hex() != hexStr {
		t.Errorf("Hex round-trip failed: got %s, want %s", id.Hex(), hexStr)
	}
}

func TestUniqueIDNew(t *testing.T) {
	id1 := NewUniqueID()
	id2 := NewUniqueID()

	// Two random IDs should be different
	if id1.Equal(id2) {
		t.Error("two random UniqueIDs should be different")
	}
}

func TestUniqueIDSize(t *testing.T) {
	id := NewUniqueID()
	if id.Size() != UniqueIDSize {
		t.Errorf("Size() = %d, want %d", id.Size(), UniqueIDSize)
	}
}

func TestWorkerIDNil(t *testing.T) {
	nilID := NilWorkerID()
	if !nilID.IsNil() {
		t.Error("NilWorkerID should be nil")
	}
}

func TestWorkerIDFromBinary(t *testing.T) {
	data := make([]byte, UniqueIDSize)
	fillRandom(data)

	id, err := WorkerIDFromBinary(data)
	if err != nil {
		t.Fatalf("WorkerIDFromBinary failed: %v", err)
	}

	if !bytes.Equal(id.Binary(), data) {
		t.Error("Binary round-trip failed")
	}
}

func TestComputeDriverIdFromJob(t *testing.T) {
	jobID := JobIDFromInt(123)
	driverID := ComputeDriverIdFromJob(jobID)

	// DriverID should have JobID in first 4 bytes
	extractedJobBytes := driverID.Binary()[:JobIDSize]
	if !bytes.Equal(extractedJobBytes, jobID.Binary()) {
		t.Error("DriverID should contain JobID in first 4 bytes")
	}

	// Remaining bytes should be 0xff
	remainingBytes := driverID.Binary()[JobIDSize:]
	for _, b := range remainingBytes {
		if b != 0xff {
			t.Error("remaining bytes should be 0xff")
			break
		}
	}
}

func TestNodeIDGCSNodeID(t *testing.T) {
	gcsID := GCSNodeID()

	// GCS NodeID should be all zeros
	for i, b := range gcsID.Binary() {
		if b != 0x00 {
			t.Errorf("GCSNodeID byte %d = 0x%x, want 0x00", i, b)
		}
	}
}

func TestNodeIDNil(t *testing.T) {
	nilID := NilNodeID()
	if !nilID.IsNil() {
		t.Error("NilNodeID should be nil")
	}
}

func TestClusterIDNil(t *testing.T) {
	nilID := NilClusterID()
	if !nilID.IsNil() {
		t.Error("NilClusterID should be nil")
	}
}

func TestFunctionIDNil(t *testing.T) {
	nilID := NilFunctionID()
	if !nilID.IsNil() {
		t.Error("NilFunctionID should be nil")
	}
}

func TestActorClassIDNil(t *testing.T) {
	nilID := NilActorClassID()
	if !nilID.IsNil() {
		t.Error("NilActorClassID should be nil")
	}
}

func TestConfigIDNil(t *testing.T) {
	nilID := NilConfigID()
	if !nilID.IsNil() {
		t.Error("NilConfigID should be nil")
	}
}

func TestActorIDConstants(t *testing.T) {
	if ActorIDSize != 16 {
		t.Errorf("ActorIDSize should be 16, got %d", ActorIDSize)
	}
	if ActorIDUniqueBytesSize != 12 {
		t.Errorf("ActorIDUniqueBytesSize should be 12, got %d", ActorIDUniqueBytesSize)
	}
}

func TestActorIDNil(t *testing.T) {
	nilID := NilActorID()
	if !nilID.IsNil() {
		t.Error("NilActorID should be nil")
	}
	if nilID.String() != "NIL_ID" {
		t.Errorf("NilActorID.String() = %s, want NIL_ID", nilID.String())
	}
}

func TestActorIDFromBinary(t *testing.T) {
	data := make([]byte, ActorIDSize)
	fillRandom(data)

	id, err := ActorIDFromBinary(data)
	if err != nil {
		t.Fatalf("ActorIDFromBinary failed: %v", err)
	}

	if !bytes.Equal(id.Binary(), data) {
		t.Error("Binary round-trip failed")
	}
}

func TestActorIDNilFromJob(t *testing.T) {
	jobID := JobIDFromInt(100)
	actorID := ActorIDNilFromJob(jobID)

	extractedJobID := actorID.JobID()
	if !extractedJobID.Equal(jobID) {
		t.Error("ActorID.JobID() should return the original JobID")
	}

	uniqueBytes := actorID.Binary()[:ActorIDUniqueBytesSize]
	for _, b := range uniqueBytes {
		if b != 0xff {
			t.Error("unique bytes should be 0xff")
			break
		}
	}
}

func TestTaskIDConstants(t *testing.T) {
	if TaskIDSize != 24 {
		t.Errorf("TaskIDSize should be 24, got %d", TaskIDSize)
	}
	if TaskIDUniqueBytesSize != 8 {
		t.Errorf("TaskIDUniqueBytesSize should be 8, got %d", TaskIDUniqueBytesSize)
	}
}

func TestTaskIDNil(t *testing.T) {
	nilID := NilTaskID()
	if !nilID.IsNil() {
		t.Error("NilTaskID should be nil")
	}
}

func TestTaskIDForDriverTask(t *testing.T) {
	jobID := JobIDFromInt(100)
	taskID := TaskIDForDriverTask(jobID)

	uniqueBytes := taskID.Binary()[:TaskIDUniqueBytesSize]
	for _, b := range uniqueBytes {
		if b != 0xff {
			t.Error("DriverTask unique bytes should be 0xff")
			break
		}
	}

	actorID := taskID.ActorID()
	extractedJobID := actorID.JobID()
	if !extractedJobID.Equal(jobID) {
		t.Error("DriverTask ActorID should contain JobID")
	}
}

func TestTaskIDForActorCreationTask(t *testing.T) {
	jobID := JobIDFromInt(100)
	parentTaskID := TaskIDForDriverTask(jobID)
	actorID := OfActorID(jobID, parentTaskID, 1)

	creationTask := TaskIDForActorCreationTask(actorID)

	uniqueBytes := creationTask.Binary()[:TaskIDUniqueBytesSize]
	for _, b := range uniqueBytes {
		if b != 0xff {
			t.Error("ActorCreationTask unique bytes should be 0xff")
			break
		}
	}

	extractedActorID := creationTask.ActorID()
	if !extractedActorID.Equal(actorID) {
		t.Error("ActorCreationTask should contain correct ActorID")
	}

	if !creationTask.IsForActorCreationTask() {
		t.Error("ActorCreationTask should be for actor creation")
	}
}

func TestTaskIDForNormalTask(t *testing.T) {
	jobID := JobIDFromInt(100)
	parentTaskID := TaskIDForDriverTask(jobID)

	normalTask := TaskIDForNormalTask(jobID, parentTaskID, 1)

	actorID := normalTask.ActorID()
	extractedJobID := actorID.JobID()
	if !extractedJobID.Equal(jobID) {
		t.Error("NormalTask ActorID should contain JobID")
	}

	if normalTask.IsForActorCreationTask() {
		t.Error("NormalTask should not be for actor creation")
	}
}

func TestTaskIDForExecutionAttempt(t *testing.T) {
	jobID := JobIDFromInt(100)
	parentTaskID := TaskIDForDriverTask(jobID)

	originalTask := TaskIDForNormalTask(jobID, parentTaskID, 1)

	attempt1 := TaskIDForExecutionAttempt(originalTask, 1)
	attempt2 := TaskIDForExecutionAttempt(originalTask, 2)

	if attempt1.Equal(attempt2) {
		t.Error("different attempts should have different TaskIDs")
	}

	if !attempt1.ActorID().Equal(originalTask.ActorID()) {
		t.Error("execution attempt should preserve ActorID")
	}
}

func TestObjectIDConstants(t *testing.T) {
	if ObjectIDSize != 28 {
		t.Errorf("ObjectIDSize should be 28, got %d", ObjectIDSize)
	}
	if ObjectIDIndexSize != 4 {
		t.Errorf("ObjectIDIndexSize should be 4, got %d", ObjectIDIndexSize)
	}
}

func TestObjectIDNil(t *testing.T) {
	nilID := NilObjectID()
	if !nilID.IsNil() {
		t.Error("NilObjectID should be nil")
	}
}

func TestObjectIDFromIndex(t *testing.T) {
	jobID := JobIDFromInt(100)
	taskID := TaskIDForDriverTask(jobID)

	objID := ObjectIDFromIndex(taskID, 1)

	extractedTaskID := objID.TaskID()
	if !extractedTaskID.Equal(taskID) {
		t.Error("ObjectID.TaskID should match")
	}

	if objID.ObjectIndex() != 1 {
		t.Errorf("ObjectIndex = %d, want 1", objID.ObjectIndex())
	}

	func() {
		defer func() {
			if r := recover(); r == nil {
				t.Error("ObjectIDFromIndex should panic for invalid index")
			}
		}()
		ObjectIDFromIndex(taskID, 0) // < 1
	}()
}

func TestPlacementGroupIDConstants(t *testing.T) {
	if PlacementGroupIDSize != 18 {
		t.Errorf("PlacementGroupIDSize should be 18, got %d", PlacementGroupIDSize)
	}
	if PlacementGroupIDUniqueBytesSize != 14 {
		t.Errorf("PlacementGroupIDUniqueBytesSize should be 14, got %d", PlacementGroupIDUniqueBytesSize)
	}
}

func TestPlacementGroupIDNil(t *testing.T) {
	nilID := NilPlacementGroupID()
	if !nilID.IsNil() {
		t.Error("NilPlacementGroupID should be nil")
	}
}

func TestOfPlacementGroupID(t *testing.T) {
	jobID := JobIDFromInt(100)
	pgID := OfPlacementGroupID(jobID)

	extractedJobID := pgID.JobID()
	if !extractedJobID.Equal(jobID) {
		t.Error("PlacementGroupID.JobID should match")
	}
}

func TestObjectIDForActorHandle(t *testing.T) {
	jobID := JobIDFromInt(100)
	parentTaskID := TaskIDForDriverTask(jobID)
	actorID := OfActorID(jobID, parentTaskID, 1)

	handle := ObjectIDForActorHandle(actorID)

	if !handle.IsActorID() {
		t.Error("ActorHandle should be actor ID")
	}

	extractedActorID := handle.ToActorID()
	if !extractedActorID.Equal(actorID) {
		t.Error("ToActorID should match original ActorID")
	}
}

func TestLeaseIDConstants(t *testing.T) {
	if LeaseIDSize != 32 {
		t.Errorf("LeaseIDSize should be 32, got %d", LeaseIDSize)
	}
	if LeaseIDUniqueBytesSize != 4 {
		t.Errorf("LeaseIDUniqueBytesSize should be 4, got %d", LeaseIDUniqueBytesSize)
	}
}
func TestMurmurHash64ADeterministic(t *testing.T) {
	data := []byte{0x01, 0x02, 0x03, 0x04}
	hash1 := murmurHash64A(data, 0)
	hash2 := murmurHash64A(data, 0)
	if hash1 != hash2 {
		t.Error("Hash should be deterministic")
	}

	hashWithSeed := murmurHash64A(data, 123)
	if hash1 == hashWithSeed {
		t.Error("different seed should produce different hash")
	}

	differentData := []byte{0x05, 0x06, 0x07, 0x08}
	hashDifferent := murmurHash64A(differentData, 0)
	if hash1 == hashDifferent {
		t.Error("different input should produce different hash")
	}
}

func TestMurmurHash64AConsistency(t *testing.T) {
	tests := []struct {
		name     string
		input    []byte
		seed     uint64
		expected uint64
	}{
		{"empty", []byte{}, 0, 0x0000000000000000},

		{"single_byte_0", []byte{0x00}, 0, 0x5825f5f3bd962979},

		{"four_bytes", []byte{0x01, 0x02, 0x03, 0x04}, 0, 0xf85cff3275df7618},

		{"eight_bytes", []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08}, 0, 0x88b2a580354486b7},

		{"with_seed_123", []byte{0x01, 0x02, 0x03, 0x04}, 123, 0xdd24157c35af6563},

		{"tail_1", []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09}, 0, 0x7809ad84418c420a},
		{"tail_2", []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a}, 0, 0xb253b7a3c002ff65},
		{"tail_3", []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b}, 0, 0x1ea8a6c9357c85ea},
		{"tail_4", []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c}, 0, 0xa0bf030cff151903},
		{"tail_5", []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d}, 0, 0x76cda4eea646d3f4},
		{"tail_6", []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e}, 0, 0x7bde19805e305da3},
		{"tail_7", []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f}, 0, 0x6c03db5b0458eff9},

		{"sixteen_bytes", []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10}, 0, 0x90285e3bad6bcddb},

		{"jobid_100", []byte{0x64, 0x00, 0x00, 0x00}, 0, 0x8b86c36089fc189c},
		{"actorid_size", []byte{0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa}, 0, 0x161b5463ec6c88bc},
		{"taskid_size", []byte{0xbb, 0xbb, 0xbb, 0xbb, 0xbb, 0xbb, 0xbb, 0xbb, 0xbb, 0xbb, 0xbb, 0xbb, 0xbb, 0xbb, 0xbb, 0xbb, 0xbb, 0xbb, 0xbb, 0xbb, 0xbb, 0xbb, 0xbb, 0xbb}, 0, 0x7739043b6d04a5c8},
		{"uniqueid_size", []byte{0xcc, 0xcc, 0xcc, 0xcc, 0xcc, 0xcc, 0xcc, 0xcc, 0xcc, 0xcc, 0xcc, 0xcc, 0xcc, 0xcc, 0xcc, 0xcc, 0xcc, 0xcc, 0xcc, 0xcc, 0xcc, 0xcc, 0xcc, 0xcc, 0xcc, 0xcc, 0xcc, 0xcc}, 0, 0x857d3c0575cd5e68},
	}

	for _, tt := range tests {
		hash := murmurHash64A(tt.input, tt.seed)
		if hash != tt.expected {
			t.Errorf("%s: murmurHash64A(%v, %d) = 0x%x, want 0x%x",
				tt.name, tt.input, tt.seed, hash, tt.expected)
		}
	}
}

func TestAllIDTypesSize(t *testing.T) {
	tests := []struct {
		name     string
		size     int
		expected int
	}{
		{"JobID", JobIDSize, 4},
		{"ActorID", ActorIDSize, 16},
		{"TaskID", TaskIDSize, 24},
		{"UniqueID", UniqueIDSize, 28},
		{"ObjectID", ObjectIDSize, 28},
		{"WorkerID", UniqueIDSize, 28},
		{"NodeID", UniqueIDSize, 28},
		{"ClusterID", UniqueIDSize, 28},
		{"FunctionID", UniqueIDSize, 28},
		{"ActorClassID", UniqueIDSize, 28},
		{"ConfigID", UniqueIDSize, 28},
		{"PlacementGroupID", PlacementGroupIDSize, 18},
		{"LeaseID", LeaseIDSize, 32},
	}

	for _, tt := range tests {
		if tt.size != tt.expected {
			t.Errorf("%s size should be %d, got %d", tt.name, tt.expected, tt.size)
		}
	}
}

func TestAllIDTypesNilString(t *testing.T) {
	// JobID
	if NilJobID().String() != "NIL_ID" {
		t.Error("NilJobID.String() should be NIL_ID")
	}

	// UniqueID
	if NilUniqueID().String() != "NIL_ID" {
		t.Error("NilUniqueID.String() should be NIL_ID")
	}

	// WorkerID
	if NilWorkerID().String() != "NIL_ID" {
		t.Error("NilWorkerID.String() should be NIL_ID")
	}

	// NodeID
	if NilNodeID().String() != "NIL_ID" {
		t.Error("NilNodeID.String() should be NIL_ID")
	}

	// ClusterID
	if NilClusterID().String() != "NIL_ID" {
		t.Error("NilClusterID.String() should be NIL_ID")
	}

	// FunctionID
	if NilFunctionID().String() != "NIL_ID" {
		t.Error("NilFunctionID.String() should be NIL_ID")
	}

	// ActorClassID
	if NilActorClassID().String() != "NIL_ID" {
		t.Error("NilActorClassID.String() should be NIL_ID")
	}

	// ConfigID
	if NilConfigID().String() != "NIL_ID" {
		t.Error("NilConfigID.String() should be NIL_ID")
	}

	// ActorID
	if NilActorID().String() != "NIL_ID" {
		t.Error("NilActorID.String() should be NIL_ID")
	}

	// TaskID
	if NilTaskID().String() != "NIL_ID" {
		t.Error("NilTaskID.String() should be NIL_ID")
	}

	// ObjectID
	if NilObjectID().String() != "NIL_ID" {
		t.Error("NilObjectID.String() should be NIL_ID")
	}

	// PlacementGroupID
	if NilPlacementGroupID().String() != "NIL_ID" {
		t.Error("NilPlacementGroupID.String() should be NIL_ID")
	}

	// LeaseID
	if NilLeaseID().String() != "NIL_ID" {
		t.Error("NilLeaseID.String() should be NIL_ID")
	}
}

func TestLeaseIDNil(t *testing.T) {
	nilID := NilLeaseID()
	if !nilID.IsNil() {
		t.Error("NilLeaseID should be nil")
	}
}

func TestLeaseIDFromWorker(t *testing.T) {
	workerID := NewWorkerID()
	counter := uint32(123)

	leaseID := LeaseIDFromWorker(workerID, counter)

	extractedWorkerID := leaseID.WorkerID()
	if !extractedWorkerID.Equal(workerID) {
		t.Error("LeaseID.WorkerID should match")
	}
}

func TestLeaseIDBinaryAndHex(t *testing.T) {
	workerID := NewWorkerID()
	leaseID := LeaseIDFromWorker(workerID, 123)

	// Binary round-trip
	binary := leaseID.Binary()
	recovered, err := LeaseIDFromBinary(binary)
	if err != nil {
		t.Fatalf("LeaseIDFromBinary failed: %v", err)
	}
	if !recovered.Equal(leaseID) {
		t.Error("Binary round-trip failed")
	}

	// Hex round-trip
	hex := leaseID.Hex()
	recoveredHex, err := LeaseIDFromHex(hex)
	if err != nil {
		t.Fatalf("LeaseIDFromHex failed: %v", err)
	}
	if !recoveredHex.Equal(leaseID) {
		t.Error("Hex round-trip failed")
	}

	// Hash should be deterministic
	hash1 := leaseID.Hash()
	hash2 := leaseID.Hash()
	if hash1 != hash2 {
		t.Error("Hash should be deterministic")
	}

	// Size
	if leaseID.Size() != LeaseIDSize {
		t.Errorf("Size = %d, want %d", leaseID.Size(), LeaseIDSize)
	}
}

func TestTaskIDForActorTask(t *testing.T) {
	jobID := JobIDFromInt(100)
	parentTaskID := TaskIDForDriverTask(jobID)
	actorID := OfActorID(jobID, parentTaskID, 1)

	actorTask := TaskIDForActorTask(jobID, parentTaskID, 1, actorID)

	extractedActorID := actorTask.ActorID()
	if !extractedActorID.Equal(actorID) {
		t.Error("ActorTask should contain correct ActorID")
	}

	extractedJobID := actorTask.JobID()
	if !extractedJobID.Equal(jobID) {
		t.Error("ActorTask should contain correct JobID")
	}
}

func TestTaskIDBinaryAndHex(t *testing.T) {
	jobID := JobIDFromInt(100)
	taskID := TaskIDForDriverTask(jobID)

	// Binary round-trip
	binary := taskID.Binary()
	recovered, err := TaskIDFromBinary(binary)
	if err != nil {
		t.Fatalf("TaskIDFromBinary failed: %v", err)
	}
	if !recovered.Equal(taskID) {
		t.Error("Binary round-trip failed")
	}

	// Hex round-trip
	hex := taskID.Hex()
	recoveredHex, err := TaskIDFromHex(hex)
	if err != nil {
		t.Fatalf("TaskIDFromHex failed: %v", err)
	}
	if !recoveredHex.Equal(taskID) {
		t.Error("Hex round-trip failed")
	}

	// Hash should be deterministic
	hash1 := taskID.Hash()
	hash2 := taskID.Hash()
	if hash1 != hash2 {
		t.Error("Hash should be deterministic")
	}

	// Size
	if taskID.Size() != TaskIDSize {
		t.Errorf("Size = %d, want %d", taskID.Size(), TaskIDSize)
	}
}

func TestActorIDBinaryAndHex(t *testing.T) {
	jobID := JobIDFromInt(100)
	parentTaskID := TaskIDForDriverTask(jobID)
	actorID := OfActorID(jobID, parentTaskID, 1)

	// Binary round-trip
	binary := actorID.Binary()
	recovered, err := ActorIDFromBinary(binary)
	if err != nil {
		t.Fatalf("ActorIDFromBinary failed: %v", err)
	}
	if !recovered.Equal(actorID) {
		t.Error("Binary round-trip failed")
	}

	// Hex round-trip
	hex := actorID.Hex()
	recoveredHex, err := ActorIDFromHex(hex)
	if err != nil {
		t.Fatalf("ActorIDFromHex failed: %v", err)
	}
	if !recoveredHex.Equal(actorID) {
		t.Error("Hex round-trip failed")
	}

	// Hash should be deterministic
	hash1 := actorID.Hash()
	hash2 := actorID.Hash()
	if hash1 != hash2 {
		t.Error("Hash should be deterministic")
	}

	// Size
	if actorID.Size() != ActorIDSize {
		t.Errorf("Size = %d, want %d", actorID.Size(), ActorIDSize)
	}
}

func TestObjectIDBinaryAndHex(t *testing.T) {
	jobID := JobIDFromInt(100)
	taskID := TaskIDForDriverTask(jobID)
	objID := ObjectIDFromIndex(taskID, 1)

	// Binary round-trip
	binary := objID.Binary()
	recovered, err := ObjectIDFromBinary(binary)
	if err != nil {
		t.Fatalf("ObjectIDFromBinary failed: %v", err)
	}
	if !recovered.Equal(objID) {
		t.Error("Binary round-trip failed")
	}

	// Hex round-trip
	hex := objID.Hex()
	recoveredHex, err := ObjectIDFromHex(hex)
	if err != nil {
		t.Fatalf("ObjectIDFromHex failed: %v", err)
	}
	if !recoveredHex.Equal(objID) {
		t.Error("Hex round-trip failed")
	}

	// Hash should be deterministic
	hash1 := objID.Hash()
	hash2 := objID.Hash()
	if hash1 != hash2 {
		t.Error("Hash should be deterministic")
	}

	// Size
	if objID.Size() != ObjectIDSize {
		t.Errorf("Size = %d, want %d", objID.Size(), ObjectIDSize)
	}
}

func TestPlacementGroupIDBinaryAndHex(t *testing.T) {
	jobID := JobIDFromInt(100)
	pgID := OfPlacementGroupID(jobID)

	// Binary round-trip
	binary := pgID.Binary()
	recovered, err := PlacementGroupIDFromBinary(binary)
	if err != nil {
		t.Fatalf("PlacementGroupIDFromBinary failed: %v", err)
	}
	if !recovered.Equal(pgID) {
		t.Error("Binary round-trip failed")
	}

	// Hex round-trip
	hex := pgID.Hex()
	recoveredHex, err := PlacementGroupIDFromHex(hex)
	if err != nil {
		t.Fatalf("PlacementGroupIDFromHex failed: %v", err)
	}
	if !recoveredHex.Equal(pgID) {
		t.Error("Hex round-trip failed")
	}

	// Hash should be deterministic
	hash1 := pgID.Hash()
	hash2 := pgID.Hash()
	if hash1 != hash2 {
		t.Error("Hash should be deterministic")
	}

	// Size
	if pgID.Size() != PlacementGroupIDSize {
		t.Errorf("Size = %d, want %d", pgID.Size(), PlacementGroupIDSize)
	}
}

func TestDerivedIDTypesFull(t *testing.T) {
	// WorkerID
	workerID := NewWorkerID()
	if workerID.Hash() == 0 {
		t.Error("WorkerID.Hash should not be zero")
	}
	if workerID.Size() != UniqueIDSize {
		t.Errorf("WorkerID.Size = %d, want %d", workerID.Size(), UniqueIDSize)
	}
	workerHex := workerID.Hex()
	workerFromHex, err := WorkerIDFromHex(workerHex)
	if err != nil || !workerFromHex.Equal(workerID) {
		t.Error("WorkerID Hex round-trip failed")
	}

	// NodeID
	nodeID := NewNodeID()
	if nodeID.Hash() == 0 {
		t.Error("NodeID.Hash should not be zero")
	}
	if nodeID.Size() != UniqueIDSize {
		t.Errorf("NodeID.Size = %d, want %d", nodeID.Size(), UniqueIDSize)
	}
	nodeBinary := nodeID.Binary()
	nodeFromBinary, err := NodeIDFromBinary(nodeBinary)
	if err != nil || !nodeFromBinary.Equal(nodeID) {
		t.Error("NodeID Binary round-trip failed")
	}
	nodeHex := nodeID.Hex()
	nodeFromHex, err := NodeIDFromHex(nodeHex)
	if err != nil || !nodeFromHex.Equal(nodeID) {
		t.Error("NodeID Hex round-trip failed")
	}
	if nodeID.Equal(NilNodeID()) {
		t.Error("NewNodeID should not equal NilNodeID")
	}

	// ClusterID
	clusterID := NewClusterID()
	if clusterID.Hash() == 0 {
		t.Error("ClusterID.Hash should not be zero")
	}
	if clusterID.Size() != UniqueIDSize {
		t.Errorf("ClusterID.Size = %d, want %d", clusterID.Size(), UniqueIDSize)
	}
	clusterBinary := clusterID.Binary()
	clusterFromBinary, err := ClusterIDFromBinary(clusterBinary)
	if err != nil || !clusterFromBinary.Equal(clusterID) {
		t.Error("ClusterID Binary round-trip failed")
	}
	clusterHex := clusterID.Hex()
	clusterFromHex, err := ClusterIDFromHex(clusterHex)
	if err != nil || !clusterFromHex.Equal(clusterID) {
		t.Error("ClusterID Hex round-trip failed")
	}

	// FunctionID
	functionID := NewFunctionID()
	if functionID.Hash() == 0 {
		t.Error("FunctionID.Hash should not be zero")
	}
	if functionID.Size() != UniqueIDSize {
		t.Errorf("FunctionID.Size = %d, want %d", functionID.Size(), UniqueIDSize)
	}
	functionBinary := functionID.Binary()
	functionFromBinary, err := FunctionIDFromBinary(functionBinary)
	if err != nil || !functionFromBinary.Equal(functionID) {
		t.Error("FunctionID Binary round-trip failed")
	}
	functionHex := functionID.Hex()
	functionFromHex, err := FunctionIDFromHex(functionHex)
	if err != nil || !functionFromHex.Equal(functionID) {
		t.Error("FunctionID Hex round-trip failed")
	}

	// ActorClassID
	actorClassID := NewActorClassID()
	if actorClassID.Hash() == 0 {
		t.Error("ActorClassID.Hash should not be zero")
	}
	if actorClassID.Size() != UniqueIDSize {
		t.Errorf("ActorClassID.Size = %d, want %d", actorClassID.Size(), UniqueIDSize)
	}
	actorClassBinary := actorClassID.Binary()
	actorClassFromBinary, err := ActorClassIDFromBinary(actorClassBinary)
	if err != nil || !actorClassFromBinary.Equal(actorClassID) {
		t.Error("ActorClassID Binary round-trip failed")
	}
	actorClassHex := actorClassID.Hex()
	actorClassFromHex, err := ActorClassIDFromHex(actorClassHex)
	if err != nil || !actorClassFromHex.Equal(actorClassID) {
		t.Error("ActorClassID Hex round-trip failed")
	}

	// ConfigID
	configID := NewConfigID()
	if configID.Hash() == 0 {
		t.Error("ConfigID.Hash should not be zero")
	}
	if configID.Size() != UniqueIDSize {
		t.Errorf("ConfigID.Size = %d, want %d", configID.Size(), UniqueIDSize)
	}
	configBinary := configID.Binary()
	configFromBinary, err := ConfigIDFromBinary(configBinary)
	if err != nil || !configFromBinary.Equal(configID) {
		t.Error("ConfigID Binary round-trip failed")
	}
	configHex := configID.Hex()
	configFromHex, err := ConfigIDFromHex(configHex)
	if err != nil || !configFromHex.Equal(configID) {
		t.Error("ConfigID Hex round-trip failed")
	}
}

func TestUniqueIDHash(t *testing.T) {
	id := NewUniqueID()
	hash := id.Hash()
	if hash == 0 {
		t.Error("UniqueID.Hash should not be zero")
	}
	// Hash should be deterministic
	if id.Hash() != hash {
		t.Error("Hash should be deterministic")
	}
}
