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
	"encoding/binary"
	"errors"
)

type ObjectIDIndexType = uint32

type ObjectID struct {
	data [ObjectIDSize]byte
}

var nilObjectID = ObjectID{data: [ObjectIDSize]byte{
	0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
	0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
	0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
	0xff, 0xff, 0xff, 0xff,
}}

func NilObjectID() ObjectID { return nilObjectID }

func ObjectIDFromIndex(taskID TaskID, index ObjectIDIndexType) ObjectID {
	if index < 1 || index > uint32(MaxObjectIndex) {
		panic("invalid object index")
	}

	var id ObjectID
	copy(id.data[:TaskIDSize], taskID.data[:])
	binary.LittleEndian.PutUint32(id.data[TaskIDSize:], index)
	return id
}

func ObjectIDForActorHandle(actorID ActorID) ObjectID {
	creationTaskID := TaskIDForActorCreationTask(actorID)
	return ObjectIDFromIndex(creationTaskID, 1)
}

func ObjectIDFromBinary(data []byte) (ObjectID, error) {
	if len(data) != ObjectIDSize {
		return nilObjectID, errors.New("invalid ObjectID length")
	}
	var id ObjectID
	copy(id.data[:], data)
	return id, nil
}

// ObjectIDFromHex decodes directly into the struct array to avoid a double allocation.
func ObjectIDFromHex(hexStr string) (ObjectID, error) {
	var id ObjectID
	if err := decodeHexToBytes(id.data[:], hexStr); err != nil {
		return nilObjectID, err
	}
	return id, nil
}

// NewObjectID generates a random ObjectID for local mode testing.
// Consistent with Java's ObjectId.fromRandom() and C++ ObjectID::FromRandom.
func NewObjectID() ObjectID {
	var id ObjectID
	fillRandom(id.data[:])
	binary.LittleEndian.PutUint32(id.data[TaskIDSize:], 0)
	return id
}

func (id ObjectID) IsNil() bool {
	return bytes.Equal(id.data[:], nilObjectID.data[:])
}

func (id ObjectID) TaskID() TaskID {
	var taskID TaskID
	copy(taskID.data[:], id.data[:TaskIDSize])
	return taskID
}

func (id ObjectID) ObjectIndex() ObjectIDIndexType {
	return binary.LittleEndian.Uint32(id.data[TaskIDSize:])
}

func (id ObjectID) IsActorID() bool {
	taskID := id.TaskID()
	// An ActorID is encoded by setting the task ID's first 8 bytes (unique bytes) to all 0xff.
	return bytes.Equal(taskID.data[:TaskIDUniqueBytesSize], nilTaskID.data[:TaskIDUniqueBytesSize])
}

func (id ObjectID) ToActorID() ActorID {
	// The ActorID occupies ObjectID.data[8:24].
	var actorID ActorID
	copy(actorID.data[:], id.data[8:24])
	return actorID
}

func (id ObjectID) Binary() []byte { return id.data[:] }

func (id ObjectID) Hex() string { return idToHex(id.data[:]) }

func (id ObjectID) String() string {
	if id.IsNil() {
		return "NIL_ID"
	}
	return id.Hex()
}

func (id ObjectID) Hash() uint64 { return murmurHash64A(id.data[:], 0) }

func (id ObjectID) Size() int { return ObjectIDSize }

func (id ObjectID) Equal(other ObjectID) bool {
	return bytes.Equal(id.data[:], other.data[:])
}
