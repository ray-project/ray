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

type UniqueID struct {
	data [UniqueIDSize]byte
}

// nilUniqueID uses nilIDBytes as the shared nil value.
var nilUniqueID = UniqueID{data: nilIDBytes}

func NilUniqueID() UniqueID { return nilUniqueID }

func NewUniqueID() UniqueID {
	var id UniqueID
	fillRandom(id.data[:])
	return id
}

func UniqueIDFromBinary(data []byte) (UniqueID, error) {
	if len(data) != UniqueIDSize {
		return nilUniqueID, errors.New("invalid UniqueID length")
	}
	var id UniqueID
	copy(id.data[:], data)
	return id, nil
}

// UniqueIDFromHex decodes directly into the struct array to avoid a double allocation.
func UniqueIDFromHex(hexStr string) (UniqueID, error) {
	var id UniqueID
	if err := decodeHexToBytes(id.data[:], hexStr); err != nil {
		return nilUniqueID, err
	}
	return id, nil
}

func (id UniqueID) IsNil() bool {
	return bytes.Equal(id.data[:], nilUniqueID.data[:])
}

func (id UniqueID) Binary() []byte {
	return id.data[:]
}

func (id UniqueID) Hex() string {
	return idToHex(id.data[:])
}

func (id UniqueID) String() string {
	if id.IsNil() {
		return "NIL_ID"
	}
	return id.Hex()
}

func (id UniqueID) Hash() uint64 {
	return murmurHash64A(id.data[:], 0)
}

func (id UniqueID) Size() int {
	return UniqueIDSize
}

func (id UniqueID) Equal(other UniqueID) bool {
	return bytes.Equal(id.data[:], other.data[:])
}
