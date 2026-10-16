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
#include "ray/core_worker/lib/go/gcs_client_bridge.h"
#include "ray/core_worker/lib/go/gcs_memory.h"
*/
import "C"

import (
	"context"
	"fmt"
	"strings"
	"unsafe"

	"github.com/ray-project/ray/go/pkg/gcs"
	"github.com/ray-project/ray/go/pkg/ids"
	"github.com/ray-project/ray/go/proto"
	protolib "google.golang.org/protobuf/proto"
)

// stateCgoClient implements gcs.GlobalStateAccessor using the synchronous CGO
// bridge via the existing asynchronous cgoClient. It provides the production
// implementation for the previously unimplemented global state accessor.
type stateCgoClient struct {
	client gcs.Client // wraps cgoClient for synchronous access
}

var _ gcs.GlobalStateAccessor = (*stateCgoClient)(nil)

// newStateCgoClient creates a new synchronous GlobalStateAccessor implementation.
func newStateCgoClient(client gcs.Client) *stateCgoClient {
	return &stateCgoClient{client: client}
}

// cgoClientAccessor returns the underlying *cgoClient for directly calling
// functions in the C bridge that are not exposed by the gcs.Client interface.
// It returns nil if the wrapped client is not a *cgoClient (e.g., a test mock).
func (s *stateCgoClient) cgoClientAccessor() *cgoClient {
	if cc, ok := s.client.(*cgoClient); ok {
		return cc
	}
	return nil
}

// validateSerializedListBounds validates the serialized array count and pointers
// returned by the CGO batch calls, ensuring cCount is non-negative and does not
// exceed the upper limit and cSizes is not nil, consistent with the existing
// unpacking logic in cgoClient. It only validates and does not free memory; the
// caller's defer handles unified release of the outer arrays and, on failure,
// freeSerializedListElements releases the per-element buffers.
func validateSerializedListBounds(cSizes *C.int, cCount C.int) error {
	if cCount < 0 {
		return fmt.Errorf("invalid cCount: %d", cCount)
	}
	if cCount > maxListResultCount {
		return fmt.Errorf("result count %d exceeds maximum %d", cCount, maxListResultCount)
	}
	if cSizes == nil {
		return fmt.Errorf("cSizes is nil")
	}
	return nil
}

// freeSerializedListElements frees the cCount per-element C strings in a batch
// result, without freeing the outer array. It is used on a bounds failure, where
// unpackSerializedList is never reached and the elements would otherwise leak.
// cCount <= 0 is a no-op so it is safe on the negative-count error path.
func freeSerializedListElements(cSerialized **C.char, cCount C.int) {
	if cSerialized == nil || cCount <= 0 {
		return
	}
	serializedSlice := unsafe.Slice(cSerialized, int(cCount))
	for i := 0; i < int(cCount); i++ {
		C.free(unsafe.Pointer(serializedSlice[i]))
	}
}

// messagePtr is a generic constraint: M is a pointer type implementing
// protolib.Message and pointing to the element type T.
type messagePtr[T any] interface {
	protolib.Message
	*T
}

// unpackSerializedList unmarshals a batch of serialized protobuf messages
// returned by a CGO batch call. For each element it validates the message size,
// copies the bytes to Go memory, unmarshals them and frees the C memory of the
// element. On error it frees the current and all remaining elements, so no C
// memory leaks on any path. The caller is responsible for freeing the outer
// arrays (char**/int*) via defer.
func unpackSerializedList[M messagePtr[T], T any](cSerialized **C.char, cSizes *C.int, cCount C.int, elemName string) ([]M, error) {
	result := make([]M, 0, int(cCount))
	serializedSlice := unsafe.Slice(cSerialized, int(cCount))
	sizesSlice := unsafe.Slice(cSizes, int(cCount))
	for i := 0; i < int(cCount); i++ {
		size := int(sizesSlice[i])
		if size == 0 {
			C.free(unsafe.Pointer(serializedSlice[i]))
			continue
		}
		if err := validateProtoMessageSize(sizesSlice[i]); err != nil {
			C.free(unsafe.Pointer(serializedSlice[i]))
			for j := i + 1; j < int(cCount); j++ {
				C.free(unsafe.Pointer(serializedSlice[j]))
			}
			return nil, err
		}

		serializedBytes := C.GoBytes(unsafe.Pointer(serializedSlice[i]), C.int(size))
		C.free(unsafe.Pointer(serializedSlice[i]))

		elem := M(new(T))
		if err := protolib.Unmarshal(serializedBytes, elem); err != nil {
			for j := i + 1; j < int(cCount); j++ {
				C.free(unsafe.Pointer(serializedSlice[j]))
			}
			return nil, fmt.Errorf("unmarshal %s failed: %w", elemName, err)
		}
		result = append(result, elem)
	}
	return result, nil
}

// Close closes the accessor connection.
func (s *stateCgoClient) Close() error {
	return s.client.Close()
}

// Connect establishes the connection to the GCS.
// cgoClient is already connected when created, so it returns success here
// directly; it returns failure if the underlying client has been closed.
func (s *stateCgoClient) Connect() (bool, error) {
	if s.client.IsClosed() {
		return false, fmt.Errorf("client is closed")
	}
	return true, nil
}

// GetAllJobInfo retrieves information for all jobs.
// The skip flags are passed through to the C bridge ray_gcs_client_jobs_get_all_job_info
// so callers can omit the large JobSubmissionInfo / IsRunningTasks fields to reduce
// the transferred data, matching the C++/Java semantics.
func (s *stateCgoClient) GetAllJobInfo(skipSubmissionJobInfoField, skipIsRunningTasksField bool) ([]*proto.JobTableData, error) {
	cc := s.cgoClientAccessor()
	if cc == nil {
		return nil, gcs.ErrNotImplemented
	}

	var skipSubmission C.int
	if skipSubmissionJobInfoField {
		skipSubmission = 1
	}
	var skipRunningTasks C.int
	if skipIsRunningTasksField {
		skipRunningTasks = 1
	}

	var cErr *C.char
	var cSerialized **C.char
	var cSizes *C.int
	var cCount C.int
	var empty bool

	err := cc.withPtr(func(ptr *C.CGcsClient) error {
		ok := C.ray_gcs_client_jobs_get_all_job_info(
			ptr,
			skipSubmission,
			skipRunningTasks,
			&cSerialized,
			&cSizes,
			&cCount,
			&cErr,
		)
		if cErr != nil {
			defer C.free(unsafe.Pointer(cErr))
			return fmt.Errorf("get all job info failed: %s", C.GoString(cErr))
		}
		if ok == 0 || cSerialized == nil || cCount == 0 {
			empty = true
		}
		return nil
	})
	// The outer arrays (char**/int*) are registered for release immediately
	// after the CGO call, ensuring all return paths (including empty nodes and
	// error paths) are covered. C.free(nil) is a safe no-op.
	defer C.free(unsafe.Pointer(cSerialized))
	defer C.free(unsafe.Pointer(cSizes))
	if err != nil {
		return nil, err
	}
	if empty {
		return []*proto.JobTableData{}, nil
	}

	if err := validateSerializedListBounds(cSizes, cCount); err != nil {
		freeSerializedListElements(cSerialized, cCount)
		return nil, err
	}

	result, err := unpackSerializedList[*proto.JobTableData, proto.JobTableData](cSerialized, cSizes, cCount, "job info")
	if err != nil {
		return nil, err
	}
	return result, nil
}

// GetNextJobID retrieves the next job ID.
func (s *stateCgoClient) GetNextJobID() (ids.JobID, error) {
	return s.client.NextJobID(context.Background())
}

// GetAllNodeInfo retrieves information for all nodes.
func (s *stateCgoClient) GetAllNodeInfo() (map[ids.NodeID]*proto.GcsNodeInfo, error) {
	return s.client.GetAll(context.Background(), nil)
}

// GetNode retrieves information for a single node.
func (s *stateCgoClient) GetNode(nodeID ids.NodeID) (*proto.GcsNodeInfo, error) {
	nodes, err := s.client.GetAll(context.Background(), []ids.NodeID{nodeID})
	if err != nil {
		return nil, err
	}
	node, ok := nodes[nodeID]
	if !ok {
		return nil, nil
	}
	return node, nil
}

// freeDrainingNodeIDStrings releases the per-element C strings allocated by
// the C++ side for GetDrainingNodes. It must be called on every early-return
// path so the node ID strings do not leak; the normal consumption loop frees
// each element itself.
func freeDrainingNodeIDStrings(cNodeIDs **C.char, cCount C.int) {
	if cNodeIDs == nil || cCount <= 0 {
		return
	}
	idsSlice := unsafe.Slice(cNodeIDs, int(cCount))
	for i := 0; i < int(cCount); i++ {
		C.free(unsafe.Pointer(idsSlice[i]))
	}
}

// GetDrainingNodes retrieves draining nodes and their deadlines.
// It returns a map of NodeID to drain-deadline timestamp (ms).
func (s *stateCgoClient) GetDrainingNodes() (map[ids.NodeID]int64, error) {
	cc := s.cgoClientAccessor()
	if cc == nil {
		return nil, gcs.ErrNotImplemented
	}

	var cNodeIDs **C.char
	var cDeadlines *C.int64_t
	var cCount C.int
	var cErr *C.char
	var empty bool

	err := cc.withPtr(func(ptr *C.CGcsClient) error {
		ok := C.ray_gcs_client_node_resources_get_draining_nodes(
			ptr, &cNodeIDs, &cDeadlines, &cCount, &cErr)
		if cErr != nil {
			defer C.free(unsafe.Pointer(cErr))
			return fmt.Errorf("get draining nodes failed: %s", C.GoString(cErr))
		}
		if ok == 0 || cNodeIDs == nil || cCount == 0 {
			freeDrainingNodeIDStrings(cNodeIDs, cCount)
			empty = true
			return nil
		}
		if cCount < 0 || int(cCount) > maxListResultCount {
			freeDrainingNodeIDStrings(cNodeIDs, cCount)
			return fmt.Errorf("invalid draining node count: %d", cCount)
		}
		if cDeadlines == nil {
			freeDrainingNodeIDStrings(cNodeIDs, cCount)
			return fmt.Errorf("get draining nodes: deadline array is nil")
		}
		return nil
	})
	// The outer arrays (char**/int64_t*) are registered for release immediately
	// after the CGO call; the per-element strings were already freed inside the
	// closure or by the consumption loop below.
	defer C.free(unsafe.Pointer(cNodeIDs))
	defer C.free(unsafe.Pointer(cDeadlines))
	if err != nil {
		return nil, err
	}
	if empty {
		return map[ids.NodeID]int64{}, nil
	}

	result := make(map[ids.NodeID]int64)
	nodeIDsSlice := unsafe.Slice(cNodeIDs, int(cCount))
	deadlinesSlice := unsafe.Slice(cDeadlines, int(cCount))
	for i := 0; i < int(cCount); i++ {
		nodeID, err := ids.NodeIDFromHex(C.GoString(nodeIDsSlice[i]))
		C.free(unsafe.Pointer(nodeIDsSlice[i]))
		if err != nil {
			continue
		}
		result[nodeID] = int64(deadlinesSlice[i])
	}
	return result, nil
}

// GetNodeToConnectForDriver retrieves the node to connect for the driver.
func (s *stateCgoClient) GetNodeToConnectForDriver(nodeIpAddress string) (*proto.GcsNodeInfo, error) {
	return s.client.GetNodeToConnect(context.Background(), nodeIpAddress)
}

// GetInternalKV retrieves an internal KV value.
func (s *stateCgoClient) GetInternalKV(ns string, key string) ([]byte, error) {
	return s.client.Get(context.Background(), ns, key)
}

// GetAllAvailableResources retrieves all available resources.
// It returns the available resources of all nodes in a single batch CGO call,
// avoiding N per-node RPCs across the cluster.
func (s *stateCgoClient) GetAllAvailableResources() ([]*proto.AvailableResources, error) {
	cc := s.cgoClientAccessor()
	if cc == nil {
		return nil, gcs.ErrNotImplemented
	}

	var cSerialized **C.char
	var cSizes *C.int
	var cCount C.int
	var cErr *C.char
	var empty bool

	err := cc.withPtr(func(ptr *C.CGcsClient) error {
		ok := C.ray_gcs_client_node_resources_get_all_available(
			ptr,
			&cSerialized,
			&cSizes,
			&cCount,
			&cErr,
		)
		if cErr != nil {
			defer C.free(unsafe.Pointer(cErr))
			return fmt.Errorf("get all available resources failed: %s", C.GoString(cErr))
		}
		if ok == 0 || cSerialized == nil || cCount == 0 {
			empty = true
		}
		return nil
	})
	// The outer arrays (char**/int*) are registered for release immediately
	// after the CGO call, ensuring all return paths (including empty nodes and
	// error paths) are covered. C.free(nil) is a safe no-op.
	defer C.free(unsafe.Pointer(cSerialized))
	defer C.free(unsafe.Pointer(cSizes))
	if err != nil {
		return nil, err
	}
	if empty {
		return []*proto.AvailableResources{}, nil
	}

	// Boundary validation (cCount non-negative/within limit, cSizes not nil),
	// consistent with the existing unpacking logic in cgoClient
	if err := validateSerializedListBounds(cSizes, cCount); err != nil {
		freeSerializedListElements(cSerialized, cCount)
		return nil, err
	}

	result, err := unpackSerializedList[*proto.AvailableResources, proto.AvailableResources](cSerialized, cSizes, cCount, "available resources")
	if err != nil {
		return nil, err
	}
	return result, nil
}

// GetAllTotalResources retrieves all total resources.
// It returns the total resources of all nodes in a single batch CGO call,
// avoiding N per-node RPCs across the cluster.
func (s *stateCgoClient) GetAllTotalResources() ([]*proto.TotalResources, error) {
	cc := s.cgoClientAccessor()
	if cc == nil {
		return nil, gcs.ErrNotImplemented
	}

	var cSerialized **C.char
	var cSizes *C.int
	var cCount C.int
	var cErr *C.char
	var empty bool

	err := cc.withPtr(func(ptr *C.CGcsClient) error {
		ok := C.ray_gcs_client_node_resources_get_all_total(
			ptr,
			&cSerialized,
			&cSizes,
			&cCount,
			&cErr,
		)
		if cErr != nil {
			defer C.free(unsafe.Pointer(cErr))
			return fmt.Errorf("get all total resources failed: %s", C.GoString(cErr))
		}
		if ok == 0 || cSerialized == nil || cCount == 0 {
			empty = true
		}
		return nil
	})
	defer C.free(unsafe.Pointer(cSerialized))
	defer C.free(unsafe.Pointer(cSizes))
	if err != nil {
		return nil, err
	}
	if empty {
		return []*proto.TotalResources{}, nil
	}

	// Boundary validation (cCount non-negative/within limit, cSizes not nil),
	// consistent with the existing unpacking logic in cgoClient
	if err := validateSerializedListBounds(cSizes, cCount); err != nil {
		freeSerializedListElements(cSerialized, cCount)
		return nil, err
	}

	result, err := unpackSerializedList[*proto.TotalResources, proto.TotalResources](cSerialized, cSizes, cCount, "total resources")
	if err != nil {
		return nil, err
	}
	return result, nil
}

// GetAllResourceUsage retrieves all resource usage.
func (s *stateCgoClient) GetAllResourceUsage() (*proto.ResourceUsageBatchData, error) {
	cc := s.cgoClientAccessor()
	if cc == nil {
		return nil, gcs.ErrNotImplemented
	}

	var cSerialized *C.char
	var cSize C.int
	var cErr *C.char

	err := cc.withPtr(func(ptr *C.CGcsClient) error {
		ok := C.ray_gcs_client_node_resources_get_all_resource_usage(
			ptr, &cSerialized, &cSize, &cErr)
		if cErr != nil {
			defer C.free(unsafe.Pointer(cErr))
			return fmt.Errorf("get all resource usage failed: %s", C.GoString(cErr))
		}
		if ok == 0 || cSerialized == nil || cSize == 0 {
			return nil
		}
		if err := validateProtoMessageSize(cSize); err != nil {
			C.free(unsafe.Pointer(cSerialized))
			return err
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	// Register the free on every path below (including the empty-return path:
	// a non-NULL zero-length buffer must still be released, mirroring how the
	// cErr defer is registered early).
	defer C.free(unsafe.Pointer(cSerialized))
	if cSerialized == nil || cSize == 0 {
		return nil, nil
	}

	serialized := C.GoBytes(unsafe.Pointer(cSerialized), cSize)

	var result proto.ResourceUsageBatchData
	if err := protolib.Unmarshal(serialized, &result); err != nil {
		return nil, fmt.Errorf("unmarshal resource usage batch failed: %w", err)
	}
	return &result, nil
}

// GetAllActorInfo retrieves information for all actors.
func (s *stateCgoClient) GetAllActorInfo(jobID *ids.JobID, actorStateName *gcs.ActorStateName) ([]*proto.ActorTableData, error) {
	return s.client.ListActorsByFilter(context.Background(), jobID, actorStateName)
}

// GetActorInfo retrieves information for a single actor.
func (s *stateCgoClient) GetActorInfo(actorID ids.ActorID) (*proto.ActorTableData, error) {
	return s.client.GetActorInfo(context.Background(), actorID)
}

// GetAllWorkerInfo retrieves information for all workers.
func (s *stateCgoClient) GetAllWorkerInfo() ([]*proto.WorkerTableData, error) {
	return s.client.ListWorkers(context.Background())
}

// GetWorkerInfo retrieves information for a single worker.
func (s *stateCgoClient) GetWorkerInfo(workerID ids.WorkerID) (*proto.WorkerTableData, error) {
	return s.client.GetWorkerInfo(context.Background(), workerID)
}

// AddWorkerInfo adds worker information.
// It directly calls ray_gcs_client_workers_add_worker_info in the C bridge.
func (s *stateCgoClient) AddWorkerInfo(data *proto.WorkerTableData) (bool, error) {
	cc := s.cgoClientAccessor()
	if cc == nil {
		return false, gcs.ErrNotImplemented
	}
	if data == nil {
		return false, fmt.Errorf("worker data must not be nil")
	}

	serialized, err := protolib.Marshal(data)
	if err != nil {
		return false, fmt.Errorf("marshal worker data failed: %w", err)
	}

	var cSuccess C.int
	var cErr *C.char

	cData := C.CBytes(serialized)
	defer C.free(cData)

	err = cc.withPtr(func(ptr *C.CGcsClient) error {
		ok := C.ray_gcs_client_workers_add_worker_info(
			ptr,
			(*C.char)(unsafe.Pointer(cData)),
			C.int(len(serialized)),
			&cSuccess,
			&cErr,
		)
		if cErr != nil {
			defer C.free(unsafe.Pointer(cErr))
			return fmt.Errorf("add worker info failed: %s", C.GoString(cErr))
		}
		if ok == 0 {
			return fmt.Errorf("add worker info failed: C++ returned ok=0 with no error")
		}
		return nil
	})
	if err != nil {
		return false, err
	}

	return cSuccess != 0, nil
}

// GetWorkerDebuggerPort retrieves the worker debugger port.
func (s *stateCgoClient) GetWorkerDebuggerPort(workerID ids.WorkerID) (uint32, error) {
	cc := s.cgoClientAccessor()
	if cc == nil {
		return 0, gcs.ErrNotImplemented
	}

	cWorkerID := C.CString(workerID.Hex())
	defer C.free(unsafe.Pointer(cWorkerID))

	var cPort C.uint32_t
	var cErr *C.char

	err := cc.withPtr(func(ptr *C.CGcsClient) error {
		ok := C.ray_gcs_client_workers_get_worker_debugger_port(
			ptr, cWorkerID, &cPort, &cErr)
		if cErr != nil {
			defer C.free(unsafe.Pointer(cErr))
			return fmt.Errorf("get worker debugger port failed: %s", C.GoString(cErr))
		}
		if ok == 0 {
			return fmt.Errorf("get worker debugger port failed: C++ returned ok=0 with no error")
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	return uint32(cPort), nil
}

// UpdateWorkerDebuggerPort updates the worker debugger port.
func (s *stateCgoClient) UpdateWorkerDebuggerPort(workerID ids.WorkerID, debuggerPort uint32) (bool, error) {
	cc := s.cgoClientAccessor()
	if cc == nil {
		return false, gcs.ErrNotImplemented
	}

	cWorkerID := C.CString(workerID.Hex())
	defer C.free(unsafe.Pointer(cWorkerID))

	var cSuccess C.int
	var cErr *C.char

	err := cc.withPtr(func(ptr *C.CGcsClient) error {
		ok := C.ray_gcs_client_workers_update_worker_debugger_port(
			ptr, cWorkerID, C.uint32_t(debuggerPort), &cSuccess, &cErr)
		if cErr != nil {
			defer C.free(unsafe.Pointer(cErr))
			return fmt.Errorf("update worker debugger port failed: %s", C.GoString(cErr))
		}
		if ok == 0 {
			return fmt.Errorf("update worker debugger port failed: C++ returned ok=0 with no error")
		}
		return nil
	})
	if err != nil {
		return false, err
	}
	return cSuccess != 0, nil
}

// UpdateWorkerNumPausedThreads updates the number of paused threads of a worker.
func (s *stateCgoClient) UpdateWorkerNumPausedThreads(workerID ids.WorkerID, numPausedThreadsDelta int32) (bool, error) {
	cc := s.cgoClientAccessor()
	if cc == nil {
		return false, gcs.ErrNotImplemented
	}

	cWorkerID := C.CString(workerID.Hex())
	defer C.free(unsafe.Pointer(cWorkerID))

	var cSuccess C.int
	var cErr *C.char

	err := cc.withPtr(func(ptr *C.CGcsClient) error {
		ok := C.ray_gcs_client_workers_update_worker_num_paused_threads(
			ptr, cWorkerID, C.int32_t(numPausedThreadsDelta), &cSuccess, &cErr)
		if cErr != nil {
			defer C.free(unsafe.Pointer(cErr))
			return fmt.Errorf("update worker num paused threads failed: %s", C.GoString(cErr))
		}
		if ok == 0 {
			return fmt.Errorf("update worker num paused threads failed: C++ returned ok=0 with no error")
		}
		return nil
	})
	if err != nil {
		return false, err
	}
	return cSuccess != 0, nil
}

// GetAllTaskEvents retrieves all task events.
func (s *stateCgoClient) GetAllTaskEvents() ([]*proto.TaskEvents, error) {
	cc := s.cgoClientAccessor()
	if cc == nil {
		return nil, gcs.ErrNotImplemented
	}

	var cSerialized **C.char
	var cSizes *C.int
	var cCount C.int
	var cErr *C.char
	var empty bool

	err := cc.withPtr(func(ptr *C.CGcsClient) error {
		ok := C.ray_gcs_client_task_events_get_all(
			ptr, &cSerialized, &cSizes, &cCount, &cErr)
		if cErr != nil {
			defer C.free(unsafe.Pointer(cErr))
			return fmt.Errorf("get all task events failed: %s", C.GoString(cErr))
		}
		if ok == 0 || cSerialized == nil || cCount == 0 {
			empty = true
		}
		return nil
	})
	defer C.free(unsafe.Pointer(cSerialized))
	defer C.free(unsafe.Pointer(cSizes))
	if err != nil {
		return nil, err
	}
	if empty {
		return []*proto.TaskEvents{}, nil
	}

	// Boundary validation (cCount non-negative/within limit, cSizes not nil),
	// consistent with the existing unpacking logic in cgoClient
	if err := validateSerializedListBounds(cSizes, cCount); err != nil {
		freeSerializedListElements(cSerialized, cCount)
		return nil, err
	}

	result, err := unpackSerializedList[*proto.TaskEvents, proto.TaskEvents](cSerialized, cSizes, cCount, "task events")
	if err != nil {
		return nil, err
	}
	return result, nil
}

// GetAllPlacementGroupInfo retrieves information for all placement groups.
func (s *stateCgoClient) GetAllPlacementGroupInfo() ([]*proto.PlacementGroupTableData, error) {
	return s.client.ListPlacementGroups(context.Background())
}

// GetPlacementGroupInfo retrieves placement group information by ID.
func (s *stateCgoClient) GetPlacementGroupInfo(pgID ids.PlacementGroupID) (*proto.PlacementGroupTableData, error) {
	return s.client.GetPlacementGroup(context.Background(), pgID)
}

// GetPlacementGroupByName retrieves placement group information by name.
// It delegates to the shared C bridge helper used by cgoClient. The namespace
// is always passed as a non-nil C string (an empty namespace becomes ""), so
// the C bridge never falls back to its own "default" normalization and an
// explicit empty namespace is preserved across the boundary.
func (s *stateCgoClient) GetPlacementGroupByName(name, namespace string) (*proto.PlacementGroupTableData, error) {
	cc := s.cgoClientAccessor()
	if cc == nil {
		return nil, gcs.ErrNotImplemented
	}
	if strings.TrimSpace(name) == "" {
		return nil, fmt.Errorf("placement group name is required")
	}

	cName := C.CString(name)
	defer C.free(unsafe.Pointer(cName))

	// Always pass a non-nil namespace so the C++ "default" fallback never
	// silently rewrites an explicit empty namespace.
	cNamespace := C.CString(namespace)
	defer C.free(unsafe.Pointer(cNamespace))

	return getPlacementGroupByNameC(cc, cName, cNamespace)
}

// GetSystemConfig retrieves the system configuration.
func (s *stateCgoClient) GetSystemConfig() (string, error) {
	cc := s.cgoClientAccessor()
	if cc == nil {
		return "", gcs.ErrNotImplemented
	}

	var cSerialized *C.char
	var cSize C.int
	var cErr *C.char

	err := cc.withPtr(func(ptr *C.CGcsClient) error {
		ok := C.ray_gcs_client_internal_kv_get_system_config(
			ptr, &cSerialized, &cSize, &cErr)
		if cErr != nil {
			defer C.free(unsafe.Pointer(cErr))
			return fmt.Errorf("get system config failed: %s", C.GoString(cErr))
		}
		if ok == 0 || cSerialized == nil || cSize == 0 {
			return nil
		}
		if err := validateProtoMessageSize(cSize); err != nil {
			C.free(unsafe.Pointer(cSerialized))
			return err
		}
		return nil
	})
	if err != nil {
		return "", err
	}
	// Register the free on every path below (including the empty-return path:
	// a non-NULL zero-length buffer must still be released, mirroring how the
	// cErr defer is registered early).
	defer C.free(unsafe.Pointer(cSerialized))
	if cSerialized == nil || cSize == 0 {
		return "", nil
	}

	return string(C.GoBytes(unsafe.Pointer(cSerialized), cSize)), nil
}
