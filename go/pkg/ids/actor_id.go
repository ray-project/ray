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
	"errors"
	"time"
)

type ActorID struct {
	data [ActorIDSize]byte
}

var nilActorID = ActorID{data: [ActorIDSize]byte{
	0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
	0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
}}

func NilActorID() ActorID { return nilActorID }

// OfActorID uses the current timestamp as the extra parameter so that the same
// combination of jobID, parentTaskID, and counter still yields a distinct ID.
func OfActorID(jobID JobID, parentTaskID TaskID, counter uint64) ActorID {
	extra := time.Now().UnixNano()

	var id ActorID
	// Write directly into the destination via generateUniqueBytesInto to avoid a double allocation.
	generateUniqueBytesInto(id.data[:ActorIDUniqueBytesSize], jobID, parentTaskID, counter, extra)
	copy(id.data[ActorIDUniqueBytesSize:], jobID.data[:])
	return id
}

func ActorIDNilFromJob(jobID JobID) ActorID {
	var id ActorID
	copy(id.data[:ActorIDUniqueBytesSize], nilActorID.data[:ActorIDUniqueBytesSize])
	copy(id.data[ActorIDUniqueBytesSize:], jobID.data[:])
	return id
}

func ActorIDFromBinary(data []byte) (ActorID, error) {
	if len(data) != ActorIDSize {
		return nilActorID, errors.New("invalid ActorID length")
	}
	var id ActorID
	copy(id.data[:], data)
	return id, nil
}

// ActorIDFromHex decodes directly into the struct array to avoid a double allocation.
func ActorIDFromHex(hexStr string) (ActorID, error) {
	var id ActorID
	if err := decodeHexToBytes(id.data[:], hexStr); err != nil {
		return nilActorID, err
	}
	return id, nil
}

func (id ActorID) IsNil() bool {
	return bytes.Equal(id.data[:], nilActorID.data[:])
}

func (id ActorID) JobID() JobID {
	if id.IsNil() {
		return nilJobID
	}
	var jobID JobID
	copy(jobID.data[:], id.data[ActorIDUniqueBytesSize:])
	return jobID
}

func (id ActorID) Binary() []byte { return id.data[:] }

func (id ActorID) Hex() string { return idToHex(id.data[:]) }

func (id ActorID) String() string {
	if id.IsNil() {
		return "NIL_ID"
	}
	return id.Hex()
}

func (id ActorID) Hash() uint64 { return murmurHash64A(id.data[:], 0) }

func (id ActorID) Size() int { return ActorIDSize }

func (id ActorID) Equal(other ActorID) bool {
	return bytes.Equal(id.data[:], other.data[:])
}
