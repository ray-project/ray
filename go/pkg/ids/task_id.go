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

// TaskIDAttemptNumberMask clears the lowest byte of a TaskID's unique bytes so
// that the attempt number can be stored there.
const TaskIDAttemptNumberMask = 0xFFFFFFFFFFFFFF00

type TaskID struct {
	data [TaskIDSize]byte
}

var nilTaskID = TaskID{data: [TaskIDSize]byte{
	0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
	0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
	0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
}}

func NilTaskID() TaskID { return nilTaskID }

func TaskIDForDriverTask(jobID JobID) TaskID {
	var id TaskID
	// Set the unique bytes to all 0xff.
	copy(id.data[:TaskIDUniqueBytesSize], nilTaskID.data[:TaskIDUniqueBytesSize])
	// Use a dummy actor ID.
	dummyActorID := ActorIDNilFromJob(jobID)
	copy(id.data[TaskIDUniqueBytesSize:], dummyActorID.data[:])
	return id
}

func TaskIDForActorCreationTask(actorID ActorID) TaskID {
	var id TaskID
	// Set the unique bytes to all 0xff.
	copy(id.data[:TaskIDUniqueBytesSize], nilTaskID.data[:TaskIDUniqueBytesSize])
	copy(id.data[TaskIDUniqueBytesSize:], actorID.data[:])
	return id
}

func TaskIDForActorTask(jobID JobID, parentTaskID TaskID,
	counter uint64, actorID ActorID,
) TaskID {
	var id TaskID
	// Write directly into the destination via generateUniqueBytesInto to avoid a double allocation.
	generateUniqueBytesInto(id.data[:TaskIDUniqueBytesSize], jobID, parentTaskID, counter, 0)
	copy(id.data[TaskIDUniqueBytesSize:], actorID.data[:])
	return id
}

func TaskIDForNormalTask(jobID JobID, parentTaskID TaskID, counter uint64) TaskID {
	var id TaskID
	// Write directly into the destination via generateUniqueBytesInto to avoid a double allocation.
	generateUniqueBytesInto(id.data[:TaskIDUniqueBytesSize], jobID, parentTaskID, counter, 0)
	// Use a dummy actor ID.
	dummyActorID := ActorIDNilFromJob(jobID)
	copy(id.data[TaskIDUniqueBytesSize:], dummyActorID.data[:])
	return id
}

func TaskIDForExecutionAttempt(taskID TaskID, attemptNumber uint64) TaskID {
	var newID TaskID
	copy(newID.data[:], taskID.data[:])

	// Modify the unique bytes portion (first 8 bytes).
	uniqueBytes := binary.LittleEndian.Uint64(newID.data[:8])
	uniqueBytes &= TaskIDAttemptNumberMask // Clear the lowest byte.
	uniqueBytes += attemptNumber
	binary.LittleEndian.PutUint64(newID.data[:8], uniqueBytes)

	return newID
}

func TaskIDFromBinary(data []byte) (TaskID, error) {
	if len(data) != TaskIDSize {
		return nilTaskID, errors.New("invalid TaskID length")
	}
	var id TaskID
	copy(id.data[:], data)
	return id, nil
}

// TaskIDFromHex decodes directly into the struct array to avoid a double allocation.
func TaskIDFromHex(hexStr string) (TaskID, error) {
	var id TaskID
	if err := decodeHexToBytes(id.data[:], hexStr); err != nil {
		return nilTaskID, err
	}
	return id, nil
}

func (id TaskID) IsNil() bool {
	return bytes.Equal(id.data[:], nilTaskID.data[:])
}

func (id TaskID) ActorID() ActorID {
	var actorID ActorID
	copy(actorID.data[:], id.data[TaskIDUniqueBytesSize:])
	return actorID
}

func (id TaskID) JobID() JobID {
	var jobID JobID
	copy(jobID.data[:], id.data[TaskIDUniqueBytesSize+ActorIDUniqueBytesSize:])
	return jobID
}

func (id TaskID) IsForActorCreationTask() bool {
	// The unique bytes are all 0xff and the actor ID is non-nil.
	return bytes.Equal(id.data[:TaskIDUniqueBytesSize], nilTaskID.data[:TaskIDUniqueBytesSize]) && !id.ActorID().IsNil()
}

func (id TaskID) Binary() []byte { return id.data[:] }

func (id TaskID) Hex() string { return idToHex(id.data[:]) }

func (id TaskID) String() string {
	if id.IsNil() {
		return "NIL_ID"
	}
	return id.Hex()
}

func (id TaskID) Hash() uint64 { return murmurHash64A(id.data[:], 0) }

func (id TaskID) Size() int { return TaskIDSize }

func (id TaskID) Equal(other TaskID) bool {
	return bytes.Equal(id.data[:], other.data[:])
}
