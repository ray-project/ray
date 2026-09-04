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

type JobID struct {
	data [JobIDSize]byte
}

var nilJobID = JobID{data: [4]byte{0xff, 0xff, 0xff, 0xff}}

func NilJobID() JobID { return nilJobID }

func NewJobID() JobID {
	var id JobID
	fillRandom(id.data[:])
	return id
}

func JobIDFromInt(value uint32) JobID {
	var id JobID
	binary.LittleEndian.PutUint32(id.data[:], value)
	return id
}

func JobIDFromBinary(data []byte) (JobID, error) {
	if len(data) != JobIDSize {
		return nilJobID, errors.New("invalid JobID length")
	}
	var id JobID
	copy(id.data[:], data)
	return id, nil
}

// JobIDFromHex decodes directly into the struct array to avoid a double allocation.
func JobIDFromHex(hexStr string) (JobID, error) {
	var id JobID
	if err := decodeHexToBytes(id.data[:], hexStr); err != nil {
		return nilJobID, err
	}
	return id, nil
}

func (id JobID) IsNil() bool {
	return bytes.Equal(id.data[:], nilJobID.data[:])
}

func (id JobID) ToInt() uint32 {
	return binary.LittleEndian.Uint32(id.data[:])
}

func (id JobID) Binary() []byte {
	return id.data[:]
}

func (id JobID) Hex() string {
	return idToHex(id.data[:])
}

func (id JobID) String() string {
	if id.IsNil() {
		return "NIL_ID"
	}
	return id.Hex()
}

func (id JobID) Hash() uint64 {
	return murmurHash64A(id.data[:], 0)
}

func (id JobID) Size() int {
	return JobIDSize
}

func (id JobID) Equal(other JobID) bool {
	return bytes.Equal(id.data[:], other.data[:])
}
