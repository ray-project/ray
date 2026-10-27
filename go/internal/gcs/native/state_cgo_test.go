// Copyright 2026 The Ray Authors.
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

//go:build cgo

package native

/*
#include <stdlib.h>
*/
import "C"

import (
	"context"
	"errors"
	"testing"
	"unsafe"

	"github.com/ray-project/ray/go/pkg/gcs"
	"github.com/ray-project/ray/go/pkg/ids"
	"github.com/ray-project/ray/go/proto"
)

// Test_stateCgoClient_interface verifies at compile time that stateCgoClient implements gcs.GlobalStateAccessor
func Test_stateCgoClient_interface(t *testing.T) {
	var _ gcs.GlobalStateAccessor = (*stateCgoClient)(nil)
}

// Test_newStateCgoClient verifies the constructor returns a non-nil client wrapping the underlying client
func Test_newStateCgoClient(t *testing.T) {
	mock := &stateMockClient{}
	s := newStateCgoClient(mock)
	if s == nil {
		t.Fatal("expected non-nil stateCgoClient")
	}
	if s.client != mock {
		t.Error("expected wrapped client to be the provided mock")
	}
}

// Test_stateCgoClient_Connect verifies Connect returns success when the client is connected
func Test_stateCgoClient_Connect(t *testing.T) {
	s := newStateCgoClient(&stateMockClient{})
	connected, err := s.Connect()
	if err != nil {
		t.Errorf("Connect() unexpected error: %v", err)
	}
	if !connected {
		t.Error("expected Connect() to return true")
	}
}

// Test_stateCgoClient_Connect_closed verifies Connect fails when the underlying client is closed
func Test_stateCgoClient_Connect_closed(t *testing.T) {
	mock := &stateMockClient{}
	mock.closed = true
	s := newStateCgoClient(mock)
	connected, err := s.Connect()
	if err == nil {
		t.Error("expected Connect() to return error when client is closed")
	}
	if connected {
		t.Error("expected Connect() to return false when client is closed")
	}
}

// Test_stateCgoClient_Close verifies Close is delegated to the underlying client
func Test_stateCgoClient_Close(t *testing.T) {
	mock := &stateMockClient{}
	s := newStateCgoClient(mock)
	if err := s.Close(); err != nil {
		t.Errorf("Close() unexpected error: %v", err)
	}
	if !mock.closed {
		t.Error("expected underlying client Close to be called")
	}
}

// Test_stateCgoClient_UnsupportedMethods verifies that methods routed through
// the C bridge return ErrNotImplemented when the wrapped client is not a
// *cgoClient (e.g. a test mock), consistent with the existing
// Test_stateCgoClient_GetAllResources_nonCgoClient behavior.
func Test_stateCgoClient_UnsupportedMethods(t *testing.T) {
	s := newStateCgoClient(&stateMockClient{})

	if _, err := s.GetDrainingNodes(); !errors.Is(err, gcs.ErrNotImplemented) {
		t.Errorf("GetDrainingNodes() expected ErrNotImplemented, got %v", err)
	}
	if _, err := s.GetAllResourceUsage(); !errors.Is(err, gcs.ErrNotImplemented) {
		t.Errorf("GetAllResourceUsage() expected ErrNotImplemented, got %v", err)
	}
	if _, err := s.GetAllTaskEvents(); !errors.Is(err, gcs.ErrNotImplemented) {
		t.Errorf("GetAllTaskEvents() expected ErrNotImplemented, got %v", err)
	}
	if _, err := s.GetSystemConfig(); !errors.Is(err, gcs.ErrNotImplemented) {
		t.Errorf("GetSystemConfig() expected ErrNotImplemented, got %v", err)
	}
	if _, err := s.GetWorkerDebuggerPort(ids.NilWorkerID()); !errors.Is(err, gcs.ErrNotImplemented) {
		t.Errorf("GetWorkerDebuggerPort() expected ErrNotImplemented, got %v", err)
	}
	if _, err := s.UpdateWorkerDebuggerPort(ids.NilWorkerID(), 0); !errors.Is(err, gcs.ErrNotImplemented) {
		t.Errorf("UpdateWorkerDebuggerPort() expected ErrNotImplemented, got %v", err)
	}
	if _, err := s.UpdateWorkerNumPausedThreads(ids.NilWorkerID(), 0); !errors.Is(err, gcs.ErrNotImplemented) {
		t.Errorf("UpdateWorkerNumPausedThreads() expected ErrNotImplemented, got %v", err)
	}
	if _, err := s.GetPlacementGroupByName("pg", ""); !errors.Is(err, gcs.ErrNotImplemented) {
		t.Errorf("GetPlacementGroupByName() expected ErrNotImplemented, got %v", err)
	}
}

// Test_stateCgoClient_DelegatesToClient verifies methods delegated to the underlying gcs.Client
// forward correctly. A mock client with injectable return values checks argument and result passing.
func Test_stateCgoClient_DelegatesToClient(t *testing.T) {
	jobID := ids.NewJobID()
	stateName := gcs.ActorStateNameAlive
	actorID := ids.NilActorID()
	workerID := ids.NewWorkerID()
	nodeID := ids.NewNodeID()
	workerAddress := &proto.Address{WorkerId: workerID.Binary()}

	mock := &stateMockClient{
		listJobs:      []*proto.JobTableData{{JobId: []byte("j1")}},
		nextJobID:     jobID,
		allNodeInfo:   map[ids.NodeID]*proto.GcsNodeInfo{nodeID: {NodeId: nodeID.Binary()}},
		actorInfo:     &proto.ActorTableData{ActorId: actorID.Binary()},
		allActors:     []*proto.ActorTableData{{ActorId: actorID.Binary()}},
		workerInfo:    &proto.WorkerTableData{WorkerAddress: workerAddress},
		allWorkers:    []*proto.WorkerTableData{{WorkerAddress: workerAddress}},
		allPlacements: []*proto.PlacementGroupTableData{{Name: "pg"}},
		placement:     &proto.PlacementGroupTableData{Name: "pg"},
		availableRes:  &proto.AvailableResources{NodeId: nodeID.Binary()},
		totalRes:      &proto.TotalResources{NodeId: nodeID.Binary()},
		nodeToConnect: &proto.GcsNodeInfo{NodeId: nodeID.Binary()},
		internalKv:    []byte("value"),
	}
	s := newStateCgoClient(mock)

	// GetAllJobInfo goes through the CGO bridge (needs a *cgoClient); with a
	// non-cgo client (e.g. the mock) it returns ErrNotImplemented.
	jobs, err := s.GetAllJobInfo(false, false)
	if !errors.Is(err, gcs.ErrNotImplemented) {
		t.Errorf("GetAllJobInfo() with non-cgo client expected ErrNotImplemented, got %v (len=%d)", err, len(jobs))
	}

	// GetNextJobID delegates to NextJobID
	gotJob, err := s.GetNextJobID()
	if err != nil || gotJob != jobID {
		t.Errorf("GetNextJobID() = %v, err=%v", gotJob, err)
	}

	// GetAllNodeInfo / GetNode delegate to GetAll
	nodes, err := s.GetAllNodeInfo()
	if err != nil || len(nodes) != 1 {
		t.Errorf("GetAllNodeInfo() = %d nodes, err=%v", len(nodes), err)
	}
	single, err := s.GetNode(nodeID)
	if err != nil || single == nil {
		t.Errorf("GetNode() = %v, err=%v", single, err)
	}

	// GetNodeToConnectForDriver delegates to GetNodeToConnect
	ntc, err := s.GetNodeToConnectForDriver("1.2.3.4")
	if err != nil || ntc == nil {
		t.Errorf("GetNodeToConnectForDriver() = %v, err=%v", ntc, err)
	}

	// GetInternalKV delegates to Get
	kv, err := s.GetInternalKV("ns", "key")
	if err != nil || string(kv) != "value" {
		t.Errorf("GetInternalKV() = %q, err=%v", kv, err)
	}

	// GetAllActorInfo / GetActorInfo delegate to ListActorsByFilter / GetActorInfo
	actors, err := s.GetAllActorInfo(&jobID, &stateName)
	if err != nil || len(actors) != 1 {
		t.Errorf("GetAllActorInfo() = %d actors, err=%v", len(actors), err)
	}
	actor, err := s.GetActorInfo(actorID)
	if err != nil || actor == nil {
		t.Errorf("GetActorInfo() = %v, err=%v", actor, err)
	}

	// GetAllWorkerInfo / GetWorkerInfo delegate to ListWorkers / GetWorkerInfo
	workers, err := s.GetAllWorkerInfo()
	if err != nil || len(workers) != 1 {
		t.Errorf("GetAllWorkerInfo() = %d workers, err=%v", len(workers), err)
	}
	worker, err := s.GetWorkerInfo(workerID)
	if err != nil || worker == nil {
		t.Errorf("GetWorkerInfo() = %v, err=%v", worker, err)
	}

	// GetAllPlacementGroupInfo / GetPlacementGroupInfo delegate to ListPlacementGroups / GetPlacementGroup
	pgs, err := s.GetAllPlacementGroupInfo()
	if err != nil || len(pgs) != 1 {
		t.Errorf("GetAllPlacementGroupInfo() = %d pgs, err=%v", len(pgs), err)
	}
	pg, err := s.GetPlacementGroupInfo(ids.NilPlacementGroupID())
	if err != nil || pg == nil {
		t.Errorf("GetPlacementGroupInfo() = %v, err=%v", pg, err)
	}

	// GetAllAvailableResources / GetAllTotalResources go through the batched CGO bridge (needs a *cgoClient)
	// and cannot be verified via a plain mock; see Test_stateCgoClient_GetAllResources_nonCgoClient.
	avail, err := s.GetAllAvailableResources()
	if !errors.Is(err, gcs.ErrNotImplemented) {
		t.Errorf("GetAllAvailableResources() with non-cgo client expected ErrNotImplemented, got %v (len=%d)", err, len(avail))
	}
	total, err := s.GetAllTotalResources()
	if !errors.Is(err, gcs.ErrNotImplemented) {
		t.Errorf("GetAllTotalResources() with non-cgo client expected ErrNotImplemented, got %v (len=%d)", err, len(total))
	}
}

// Test_stateCgoClient_GetAllResources_nonCgoClient verifies batched resource queries return
// ErrNotImplemented when the underlying client is not a *cgoClient (e.g. a test mock), consistent
// with methods that call the C bridge directly such as AddWorkerInfo.
func Test_stateCgoClient_GetAllResources_nonCgoClient(t *testing.T) {
	s := newStateCgoClient(&stateMockClient{})
	if _, err := s.GetAllAvailableResources(); !errors.Is(err, gcs.ErrNotImplemented) {
		t.Errorf("GetAllAvailableResources() expected ErrNotImplemented, got %v", err)
	}
	if _, err := s.GetAllTotalResources(); !errors.Is(err, gcs.ErrNotImplemented) {
		t.Errorf("GetAllTotalResources() expected ErrNotImplemented, got %v", err)
	}
}

// Test_stateCgoClient_QueryMethods_defunctCgoClient is table-driven over the nine
// query-only state accessor methods, each invoked on a real *cgoClient that has no
// live C++ client behind it: a zero value (nil C pointer) or an explicitly closed
// one. Every method must surface a plain error from the withPtr guard, must not
// panic, and must not report ErrNotImplemented — that sentinel is reserved for the
// non-cgoClient mock path (see Test_stateCgoClient_UnsupportedMethods), while a
// *cgoClient always reaches the CGO boundary. Constructing a connected *cgoClient
// is impossible without a live GCS (ray_gcs_client_create fails when Connect
// fails), so real-data assertions stay in the opt-in integration target.
func Test_stateCgoClient_QueryMethods_defunctCgoClient(t *testing.T) {
	cases := []struct {
		name string
		call func(s *stateCgoClient) error
	}{
		{"GetAllJobInfo", func(s *stateCgoClient) error {
			_, err := s.GetAllJobInfo(false, false)
			return err
		}},
		{"GetDrainingNodes", func(s *stateCgoClient) error {
			_, err := s.GetDrainingNodes()
			return err
		}},
		{"GetAllAvailableResources", func(s *stateCgoClient) error {
			_, err := s.GetAllAvailableResources()
			return err
		}},
		{"GetAllTotalResources", func(s *stateCgoClient) error {
			_, err := s.GetAllTotalResources()
			return err
		}},
		{"GetAllResourceUsage", func(s *stateCgoClient) error {
			_, err := s.GetAllResourceUsage()
			return err
		}},
		{"GetAllTaskEvents", func(s *stateCgoClient) error {
			_, err := s.GetAllTaskEvents()
			return err
		}},
		{"GetPlacementGroupByName", func(s *stateCgoClient) error {
			_, err := s.GetPlacementGroupByName("pg", "")
			return err
		}},
		{"GetSystemConfig", func(s *stateCgoClient) error {
			_, err := s.GetSystemConfig()
			return err
		}},
		{"GetWorkerDebuggerPort", func(s *stateCgoClient) error {
			_, err := s.GetWorkerDebuggerPort(ids.NewWorkerID())
			return err
		}},
	}
	states := []struct {
		name  string
		makeS func() *stateCgoClient
	}{
		// A zero-value cgoClient has a nil C pointer: withPtr rejects the call
		// before any C code runs.
		{"nilPtr", func() *stateCgoClient { return newStateCgoClient(&cgoClient{}) }},
		// An explicitly closed cgoClient takes the same withPtr rejection path.
		{"closed", func() *stateCgoClient {
			cc := &cgoClient{}
			cc.closed.Store(true)
			return newStateCgoClient(cc)
		}},
	}
	for _, state := range states {
		for _, tc := range cases {
			t.Run(state.name+"/"+tc.name, func(t *testing.T) {
				err := tc.call(state.makeS())
				if err == nil {
					t.Errorf("%s on a %s cgoClient expected an error, got nil", tc.name, state.name)
				}
				if errors.Is(err, gcs.ErrNotImplemented) {
					t.Errorf("%s on a %s cgoClient must not return ErrNotImplemented, got %v", tc.name, state.name, err)
				}
			})
		}
	}
}

// stateMockClient is a minimal gcs.Client stand-in for testing stateCgoClient delegation.
// Only the methods under test are implemented; the rest panic on call to expose unexpected access.
type stateMockClient struct {
	gcs.Client
	closed        bool
	listJobs      []*proto.JobTableData
	nextJobID     ids.JobID
	allNodeInfo   map[ids.NodeID]*proto.GcsNodeInfo
	actorInfo     *proto.ActorTableData
	allActors     []*proto.ActorTableData
	workerInfo    *proto.WorkerTableData
	allWorkers    []*proto.WorkerTableData
	allPlacements []*proto.PlacementGroupTableData
	placement     *proto.PlacementGroupTableData
	availableRes  *proto.AvailableResources
	totalRes      *proto.TotalResources
	nodeToConnect *proto.GcsNodeInfo
	internalKv    []byte
}

func (m *stateMockClient) Close() error {
	m.closed = true
	return nil
}
func (m *stateMockClient) IsClosed() bool { return m.closed }
func (m *stateMockClient) ListJobs(ctx context.Context) ([]*proto.JobTableData, error) {
	return m.listJobs, nil
}
func (m *stateMockClient) NextJobID(ctx context.Context) (ids.JobID, error) {
	return m.nextJobID, nil
}
func (m *stateMockClient) GetAll(ctx context.Context, nodeIDs []ids.NodeID) (map[ids.NodeID]*proto.GcsNodeInfo, error) {
	return m.allNodeInfo, nil
}
func (m *stateMockClient) GetNodeToConnect(ctx context.Context, nodeIpAddress string) (*proto.GcsNodeInfo, error) {
	return m.nodeToConnect, nil
}
func (m *stateMockClient) Get(ctx context.Context, ns, key string) ([]byte, error) {
	return m.internalKv, nil
}
func (m *stateMockClient) GetAvailableResources(ctx context.Context, nodeID ids.NodeID) (*proto.AvailableResources, error) {
	return m.availableRes, nil
}
func (m *stateMockClient) GetTotalResources(ctx context.Context, nodeID ids.NodeID) (*proto.TotalResources, error) {
	return m.totalRes, nil
}
func (m *stateMockClient) ListActorsByFilter(ctx context.Context, jobID *ids.JobID, actorStateName *gcs.ActorStateName) ([]*proto.ActorTableData, error) {
	return m.allActors, nil
}
func (m *stateMockClient) GetActorInfo(ctx context.Context, actorID ids.ActorID) (*proto.ActorTableData, error) {
	return m.actorInfo, nil
}
func (m *stateMockClient) ListWorkers(ctx context.Context) ([]*proto.WorkerTableData, error) {
	return m.allWorkers, nil
}
func (m *stateMockClient) GetWorkerInfo(ctx context.Context, workerID ids.WorkerID) (*proto.WorkerTableData, error) {
	return m.workerInfo, nil
}
func (m *stateMockClient) ListPlacementGroups(ctx context.Context) ([]*proto.PlacementGroupTableData, error) {
	return m.allPlacements, nil
}
func (m *stateMockClient) GetPlacementGroup(ctx context.Context, pgID ids.PlacementGroupID) (*proto.PlacementGroupTableData, error) {
	return m.placement, nil
}

// Test_validateSerializedListBounds verifies the bounds validation of batched resource arrays:
// negative cCount, cCount above the limit, and nil cSizes must return errors; valid input returns nil.
func Test_validateSerializedListBounds(t *testing.T) {
	var sizes C.int

	// valid: non-negative, within limit, cSizes non-nil
	if err := validateSerializedListBounds(&sizes, C.int(0)); err != nil {
		t.Errorf("expected no error for valid bounds, got %v", err)
	}
	if err := validateSerializedListBounds(&sizes, C.int(1)); err != nil {
		t.Errorf("expected no error for valid count, got %v", err)
	}

	// negative cCount
	if err := validateSerializedListBounds(&sizes, C.int(-1)); err == nil {
		t.Error("expected error for negative cCount")
	}

	// cCount above the limit
	if err := validateSerializedListBounds(&sizes, C.int(maxListResultCount+1)); err == nil {
		t.Error("expected error for cCount exceeding maxListResultCount")
	}

	// nil cSizes
	if err := validateSerializedListBounds(nil, C.int(1)); err == nil {
		t.Error("expected error for nil cSizes")
	}
}

// Test_freeSerializedListElements pins the batch-element release helper's branch
// behavior: count 0, negative count and a nil array must be no-ops (no panic,
// nothing freed), and a within-bounds count must free exactly that many elements.
// Each element is a C.malloc'd block; the helper frees the covered elements and
// the test frees the remainder (none, or the tail for the partial-free case) plus
// the outer array so the test itself stays leak-free.
func Test_freeSerializedListElements(t *testing.T) {
	freeP := func(p unsafe.Pointer) {
		if p != nil {
			C.free(p)
		}
	}
	allocElems := func(count C.int) (**C.char, []unsafe.Pointer) {
		arr := C.malloc(C.size_t(count) * C.size_t(unsafe.Sizeof(uintptr(0))))
		sl := unsafe.Slice((**C.char)(arr), int(count))
		ptrs := make([]unsafe.Pointer, 0, int(count))
		for i := 0; i < int(count); i++ {
			p := C.malloc(8)
			sl[i] = (*C.char)(p)
			ptrs = append(ptrs, p)
		}
		return (**C.char)(arr), ptrs
	}
	freeOuter := func(p unsafe.Pointer) {
		if p != nil {
			C.free(p)
		}
	}

	// count 0: must be a no-op (nil array too).
	freeSerializedListElements(nil, 0)
	freeSerializedListElements(nil, -1)
	arr0, _ := allocElems(0)
	freeSerializedListElements(arr0, 0)
	freeOuter(unsafe.Pointer(arr0))

	// negative count: no-op, must not panic even with a real array.
	arrNeg, ptrsNeg := allocElems(2)
	freeSerializedListElements(arrNeg, -1)
	// Elements were NOT freed by the helper (negative count is a no-op), so we
	// must free them ourselves to avoid a leak.
	for _, p := range ptrsNeg {
		freeP(p)
	}
	freeOuter(unsafe.Pointer(arrNeg))

	// within-bounds count: helper frees exactly the count elements; there are no
	// remaining ones, and the outer array is freed below.
	arr, _ := allocElems(3)
	freeSerializedListElements(arr, 3)
	freeOuter(unsafe.Pointer(arr))

	// Partial free: helper frees the first count elements; the tail stays owned
	// by us, so free it here to keep the test leak-free.
	arrP, ptrsP := allocElems(4)
	freeSerializedListElements(arrP, 2)
	for _, p := range ptrsP[2:] {
		freeP(p)
	}
	freeOuter(unsafe.Pointer(arrP))
}

// Test_freeDrainingNodeIDStrings verifies the draining-node release helper with
// the same boundary values: count 0 / negative / nil are no-ops, and a
// within-bounds count frees exactly that many element strings.
func Test_freeDrainingNodeIDStrings(t *testing.T) {
	freeP := func(p unsafe.Pointer) {
		if p != nil {
			C.free(p)
		}
	}
	allocElems := func(count C.int) (**C.char, []unsafe.Pointer) {
		arr := C.malloc(C.size_t(count) * C.size_t(unsafe.Sizeof(uintptr(0))))
		sl := unsafe.Slice((**C.char)(arr), int(count))
		ptrs := make([]unsafe.Pointer, 0, int(count))
		for i := 0; i < int(count); i++ {
			p := C.malloc(8)
			sl[i] = (*C.char)(p)
			ptrs = append(ptrs, p)
		}
		return (**C.char)(arr), ptrs
	}
	freeOuter := func(p unsafe.Pointer) {
		if p != nil {
			C.free(p)
		}
	}

	freeDrainingNodeIDStrings(nil, 0)
	freeDrainingNodeIDStrings(nil, -1)
	arr0, _ := allocElems(0)
	freeDrainingNodeIDStrings(arr0, 0)
	freeOuter(unsafe.Pointer(arr0))

	arrNeg, ptrsNeg := allocElems(2)
	freeDrainingNodeIDStrings(arrNeg, -1)
	for _, p := range ptrsNeg {
		freeP(p)
	}
	freeOuter(unsafe.Pointer(arrNeg))

	arr, _ := allocElems(3)
	freeDrainingNodeIDStrings(arr, 3)
	freeOuter(unsafe.Pointer(arr))

	arrP, ptrsP := allocElems(4)
	freeDrainingNodeIDStrings(arrP, 2)
	for _, p := range ptrsP[2:] {
		freeP(p)
	}
	freeOuter(unsafe.Pointer(arrP))
}
