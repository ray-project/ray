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

type LeaseID struct {
	data [LeaseIDSize]byte
}

var nilLeaseID = LeaseID{data: [LeaseIDSize]byte{
	0xff, 0xff, 0xff, 0xff,
	0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
	0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
	0xff, 0xff, 0xff, 0xff,
}}

func NilLeaseID() LeaseID { return nilLeaseID }

func LeaseIDFromWorker(workerID WorkerID, counter uint32) LeaseID {
	var id LeaseID
	binary.LittleEndian.PutUint32(id.data[:LeaseIDUniqueBytesSize], counter)
	copy(id.data[LeaseIDUniqueBytesSize:], workerID.data[:])
	return id
}

func NewLeaseID() LeaseID {
	var id LeaseID
	fillRandom(id.data[:])
	return id
}

func LeaseIDFromBinary(data []byte) (LeaseID, error) {
	if len(data) != LeaseIDSize {
		return nilLeaseID, errors.New("invalid LeaseID length")
	}
	var id LeaseID
	copy(id.data[:], data)
	return id, nil
}

// LeaseIDFromHex decodes directly into the struct array to avoid a double allocation.
func LeaseIDFromHex(hexStr string) (LeaseID, error) {
	var id LeaseID
	if err := decodeHexToBytes(id.data[:], hexStr); err != nil {
		return nilLeaseID, err
	}
	return id, nil
}

func (id LeaseID) IsNil() bool {
	return bytes.Equal(id.data[:], nilLeaseID.data[:])
}

func (id LeaseID) WorkerID() WorkerID {
	var workerID WorkerID
	copy(workerID.data[:], id.data[LeaseIDUniqueBytesSize:])
	return workerID
}

func (id LeaseID) Binary() []byte { return id.data[:] }

func (id LeaseID) Hex() string { return idToHex(id.data[:]) }

func (id LeaseID) String() string {
	if id.IsNil() {
		return "NIL_ID"
	}
	return id.Hex()
}

func (id LeaseID) Hash() uint64 { return murmurHash64A(id.data[:], 0) }

func (id LeaseID) Size() int { return LeaseIDSize }

func (id LeaseID) Equal(other LeaseID) bool {
	return bytes.Equal(id.data[:], other.data[:])
}
