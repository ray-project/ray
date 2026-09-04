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
)

type PlacementGroupID struct {
	data [PlacementGroupIDSize]byte
}

var nilPlacementGroupID = PlacementGroupID{data: [PlacementGroupIDSize]byte{
	0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
	0xff, 0xff, 0xff, 0xff,
}}

func NilPlacementGroupID() PlacementGroupID { return nilPlacementGroupID }

func OfPlacementGroupID(jobID JobID) PlacementGroupID {
	var id PlacementGroupID
	fillRandom(id.data[:PlacementGroupIDUniqueBytesSize])
	copy(id.data[PlacementGroupIDUniqueBytesSize:], jobID.data[:])
	return id
}

func PlacementGroupIDFromBinary(data []byte) (PlacementGroupID, error) {
	if len(data) != PlacementGroupIDSize {
		return nilPlacementGroupID, errors.New("invalid PlacementGroupID length")
	}
	var id PlacementGroupID
	copy(id.data[:], data)
	return id, nil
}

// PlacementGroupIDFromHex decodes directly into the struct array to avoid a double allocation.
func PlacementGroupIDFromHex(hexStr string) (PlacementGroupID, error) {
	var id PlacementGroupID
	if err := decodeHexToBytes(id.data[:], hexStr); err != nil {
		return nilPlacementGroupID, err
	}
	return id, nil
}

func (id PlacementGroupID) IsNil() bool {
	return bytes.Equal(id.data[:], nilPlacementGroupID.data[:])
}

func (id PlacementGroupID) JobID() JobID {
	if id.IsNil() {
		return nilJobID
	}
	var jobID JobID
	copy(jobID.data[:], id.data[PlacementGroupIDUniqueBytesSize:])
	return jobID
}

func (id PlacementGroupID) Binary() []byte { return id.data[:] }

func (id PlacementGroupID) Hex() string { return idToHex(id.data[:]) }

func (id PlacementGroupID) String() string {
	if id.IsNil() {
		return "NIL_ID"
	}
	return id.Hex()
}

func (id PlacementGroupID) Hash() uint64 { return murmurHash64A(id.data[:], 0) }

func (id PlacementGroupID) Size() int { return PlacementGroupIDSize }

func (id PlacementGroupID) Equal(other PlacementGroupID) bool {
	return bytes.Equal(id.data[:], other.data[:])
}
