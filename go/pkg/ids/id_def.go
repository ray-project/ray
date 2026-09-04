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

// nilIDBytes is the shared nil ID byte array, all 28 bytes set to 0xff.
var nilIDBytes = [UniqueIDSize]byte{
	0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
	0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
	0xff, 0xff, 0xff, 0xff,
}

type WorkerID struct {
	UniqueID
}

var nilWorkerID = WorkerID{UniqueID: UniqueID{data: nilIDBytes}}

func NilWorkerID() WorkerID { return nilWorkerID }

func NewWorkerID() WorkerID {
	return WorkerID{UniqueID: NewUniqueID()}
}

func WorkerIDFromBinary(data []byte) (WorkerID, error) {
	if len(data) != UniqueIDSize {
		return nilWorkerID, errors.New("invalid WorkerID length")
	}
	var id WorkerID
	copy(id.UniqueID.data[:], data)
	return id, nil
}

func WorkerIDFromHex(hexStr string) (WorkerID, error) {
	uid, err := UniqueIDFromHex(hexStr)
	if err != nil {
		return nilWorkerID, err
	}
	return WorkerID{UniqueID: uid}, nil
}

func ComputeDriverIdFromJob(jobID JobID) WorkerID {
	var id WorkerID
	copy(id.UniqueID.data[:JobIDSize], jobID.data[:])
	copy(id.UniqueID.data[JobIDSize:], nilWorkerID.UniqueID.data[JobIDSize:])
	return id
}

func (id WorkerID) IsNil() bool {
	return bytes.Equal(id.UniqueID.data[:], nilWorkerID.UniqueID.data[:])
}

func (id WorkerID) Equal(other WorkerID) bool {
	return bytes.Equal(id.UniqueID.data[:], other.UniqueID.data[:])
}

type NodeID struct {
	UniqueID
}

var nilNodeID = NodeID{UniqueID: UniqueID{data: nilIDBytes}}

// kGCSNodeID is a special constant: the GCS NodeID uses 28 bytes of 0x00.
var kGCSNodeID = NodeID{UniqueID: UniqueID{data: [UniqueIDSize]byte{
	0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
	0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
	0x00, 0x00, 0x00, 0x00,
}}}

func NilNodeID() NodeID { return nilNodeID }

func GCSNodeID() NodeID { return kGCSNodeID }

func NewNodeID() NodeID {
	return NodeID{UniqueID: NewUniqueID()}
}

func NodeIDFromBinary(data []byte) (NodeID, error) {
	if len(data) != UniqueIDSize {
		return nilNodeID, errors.New("invalid NodeID length")
	}
	var id NodeID
	copy(id.UniqueID.data[:], data)
	return id, nil
}

func NodeIDFromHex(hexStr string) (NodeID, error) {
	uid, err := UniqueIDFromHex(hexStr)
	if err != nil {
		return nilNodeID, err
	}
	return NodeID{UniqueID: uid}, nil
}

func (id NodeID) IsNil() bool {
	return bytes.Equal(id.UniqueID.data[:], nilNodeID.UniqueID.data[:])
}

func (id NodeID) Equal(other NodeID) bool {
	return bytes.Equal(id.UniqueID.data[:], other.UniqueID.data[:])
}

type ClusterID struct {
	UniqueID
}

var nilClusterID = ClusterID{UniqueID: UniqueID{data: nilIDBytes}}

func NilClusterID() ClusterID { return nilClusterID }

func NewClusterID() ClusterID {
	return ClusterID{UniqueID: NewUniqueID()}
}

func ClusterIDFromBinary(data []byte) (ClusterID, error) {
	if len(data) != UniqueIDSize {
		return nilClusterID, errors.New("invalid ClusterID length")
	}
	var id ClusterID
	copy(id.UniqueID.data[:], data)
	return id, nil
}

func ClusterIDFromHex(hexStr string) (ClusterID, error) {
	uid, err := UniqueIDFromHex(hexStr)
	if err != nil {
		return nilClusterID, err
	}
	return ClusterID{UniqueID: uid}, nil
}

func (id ClusterID) IsNil() bool {
	return bytes.Equal(id.UniqueID.data[:], nilClusterID.UniqueID.data[:])
}

func (id ClusterID) Equal(other ClusterID) bool {
	return bytes.Equal(id.UniqueID.data[:], other.UniqueID.data[:])
}

type FunctionID struct {
	UniqueID
}

var nilFunctionID = FunctionID{UniqueID: UniqueID{data: nilIDBytes}}

func NilFunctionID() FunctionID { return nilFunctionID }

func NewFunctionID() FunctionID {
	return FunctionID{UniqueID: NewUniqueID()}
}

func FunctionIDFromBinary(data []byte) (FunctionID, error) {
	if len(data) != UniqueIDSize {
		return nilFunctionID, errors.New("invalid FunctionID length")
	}
	var id FunctionID
	copy(id.UniqueID.data[:], data)
	return id, nil
}

func FunctionIDFromHex(hexStr string) (FunctionID, error) {
	uid, err := UniqueIDFromHex(hexStr)
	if err != nil {
		return nilFunctionID, err
	}
	return FunctionID{UniqueID: uid}, nil
}

func (id FunctionID) IsNil() bool {
	return bytes.Equal(id.UniqueID.data[:], nilFunctionID.UniqueID.data[:])
}

func (id FunctionID) Equal(other FunctionID) bool {
	return bytes.Equal(id.UniqueID.data[:], other.UniqueID.data[:])
}

type ActorClassID struct {
	UniqueID
}

var nilActorClassID = ActorClassID{UniqueID: UniqueID{data: nilIDBytes}}

func NilActorClassID() ActorClassID { return nilActorClassID }

func NewActorClassID() ActorClassID {
	return ActorClassID{UniqueID: NewUniqueID()}
}

func ActorClassIDFromBinary(data []byte) (ActorClassID, error) {
	if len(data) != UniqueIDSize {
		return nilActorClassID, errors.New("invalid ActorClassID length")
	}
	var id ActorClassID
	copy(id.UniqueID.data[:], data)
	return id, nil
}

func ActorClassIDFromHex(hexStr string) (ActorClassID, error) {
	uid, err := UniqueIDFromHex(hexStr)
	if err != nil {
		return nilActorClassID, err
	}
	return ActorClassID{UniqueID: uid}, nil
}

func (id ActorClassID) IsNil() bool {
	return bytes.Equal(id.UniqueID.data[:], nilActorClassID.UniqueID.data[:])
}

func (id ActorClassID) Equal(other ActorClassID) bool {
	return bytes.Equal(id.UniqueID.data[:], other.UniqueID.data[:])
}

type ConfigID struct {
	UniqueID
}

var nilConfigID = ConfigID{UniqueID: UniqueID{data: nilIDBytes}}

func NilConfigID() ConfigID { return nilConfigID }

func NewConfigID() ConfigID {
	return ConfigID{UniqueID: NewUniqueID()}
}

func ConfigIDFromBinary(data []byte) (ConfigID, error) {
	if len(data) != UniqueIDSize {
		return nilConfigID, errors.New("invalid ConfigID length")
	}
	var id ConfigID
	copy(id.UniqueID.data[:], data)
	return id, nil
}

func ConfigIDFromHex(hexStr string) (ConfigID, error) {
	uid, err := UniqueIDFromHex(hexStr)
	if err != nil {
		return nilConfigID, err
	}
	return ConfigID{UniqueID: uid}, nil
}

func (id ConfigID) IsNil() bool {
	return bytes.Equal(id.UniqueID.data[:], nilConfigID.UniqueID.data[:])
}

func (id ConfigID) Equal(other ConfigID) bool {
	return bytes.Equal(id.UniqueID.data[:], other.UniqueID.data[:])
}
