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

package common

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/ray-project/ray/go/pkg/gcs"
	"github.com/ray-project/ray/go/pkg/ids"
	"github.com/ray-project/ray/go/proto"
)

var State = NewGlobalState()

type GlobalState struct {
	gcsOptions          *gcs.ClientOptions
	globalStateAccessor gcs.GlobalStateAccessor
	initLock            sync.Mutex
}

// NewGlobalState creates a new *GlobalState.
//
// Initialization logic:
//  1. Create an empty GlobalState struct.
//  2. gcsOptions starts as nil and is set later by InitializeGlobalState
//     (corresponds to Python's _initialize_global_state).
//  3. globalStateAccessor starts as nil and is created on first use via
//     ConnectAndGetAccessor.
//  4. initLock guards concurrent access for thread safety.
func NewGlobalState() *GlobalState {
	return &GlobalState{
		gcsOptions:          nil,
		globalStateAccessor: nil,
		initLock:            sync.Mutex{},
	}
}

// InitializeGlobalState initializes the GCS options of GlobalState.
func (s *GlobalState) InitializeGlobalState(opts *gcs.ClientOptions) {
	s.initLock.Lock()
	defer s.initLock.Unlock()
	s.gcsOptions = opts
}

// ConnectAndGetAccessor lazily connects and returns the GCS state accessor.
//
// Core logic:
//  1. Thread safety: initLock guarantees atomic operations in a multi-threaded
//     environment.
//  2. Cache check: return the existing accessor if present, avoiding a redundant
//     connection.
//  3. Precondition check: verify gcsOptions is set (i.e. InitializeGlobalState
//     was called).
//  4. Get the accessor from the global singleton.
//  5. Connection check: attempt to connect to the GCS server, clearing state and
//     returning an error on failure.
func (s *GlobalState) ConnectAndGetAccessor() (gcs.GlobalStateAccessor, error) {
	s.initLock.Lock()
	defer s.initLock.Unlock()

	// Cache check: return the existing accessor if present.
	if s.globalStateAccessor != nil {
		return s.globalStateAccessor, nil
	}

	// Precondition check: verify gcsOptions is set.
	if s.gcsOptions == nil {
		return nil, errors.New("Ray has not been started yet. Trying to use state API before InitializeGlobalState has been called")
	}

	accessor, err := gcs.GetGlobalStateAccessor()
	if err != nil {
		return nil, err
	}

	connected, err := accessor.Connect()
	if err != nil || !connected {
		s.globalStateAccessor = nil
		// Clear the cached state when the connection fails.
		if err == nil {
			err = errors.New("failed to connect to GCS server")
		}
		return nil, err
	}

	s.globalStateAccessor = accessor
	return s.globalStateAccessor, nil
}

// Disconnect disconnects from GCS and releases resources.
//
// Clean-up operations:
// 1. Reset gcsOptions to nil, marking that re-initialization is required.
// 2. Release the globalStateAccessor reference, allowing garbage collection.
func (s *GlobalState) Disconnect() error {
	s.initLock.Lock()
	defer s.initLock.Unlock()

	s.gcsOptions = nil
	if s.globalStateAccessor != nil {
		// Close the accessor's connection.
		s.globalStateAccessor.Close()
		s.globalStateAccessor = nil
	}

	return nil
}

// AddWorker adds Worker information to the cluster.
//
// Core functionality:
// 1. Get the GCS state accessor (lazy connection).
// 2. Build the WorkerTableData protobuf message.
// 3. Set the basic Worker attributes (liveness, ID, and type).
// 4. Convert the workerInfo map into a protobuf map field.
// 5. Call accessor.AddWorkerInfo to write the data to GCS.
func (s *GlobalState) AddWorker(workerID ids.WorkerID, workerType proto.WorkerType, workerInfo map[string]string) error {
	accessor, err := s.ConnectAndGetAccessor()
	if err != nil {
		return fmt.Errorf("failed to get GCS accessor: %w", err)
	}

	// Convert workerInfo to []byte format, as required by protobuf.
	byteWorkerInfo := make(map[string][]byte)
	for k, v := range workerInfo {
		byteWorkerInfo[k] = []byte(v)
	}
	workerData := &proto.WorkerTableData{
		IsAlive:    true,
		WorkerType: workerType,
		WorkerInfo: byteWorkerInfo,
		Timestamp:  time.Now().UnixNano() / 1e6, // Timestamp in milliseconds.
	}

	// Set the Worker address (includes the Worker ID).
	// Note: WorkerId is a []byte field and requires binary data from UniqueID.
	workerData.WorkerAddress = &proto.Address{
		WorkerId: workerID.Binary(),
	}

	success, err := accessor.AddWorkerInfo(workerData)
	if err != nil {
		return fmt.Errorf("failed to add worker info to GCS: %w", err)
	}
	if !success {
		return errors.New("add worker info returned false")
	}

	return nil
}
