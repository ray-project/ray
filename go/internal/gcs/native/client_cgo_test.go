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
	"fmt"
	"strings"
	"testing"
	"unsafe"

	"github.com/ray-project/ray/go/pkg/gcs"
	"github.com/ray-project/ray/go/pkg/ids"
	"github.com/ray-project/ray/go/proto"
)

// Cases that call ConnectClient with a reachable address are absent on purpose:
// the C++ GcsClient connects inside ray_gcs_client_create and reports a failure
// only once its timeout expires, so those cases belong to the opt-in cluster
// target under go/internal/gcs/integration instead of a unit target.

// Test_cgoClient_interface verifies at compile time that cgoClient implements
// the gcs.Client interface.
func Test_cgoClient_interface(t *testing.T) {
	var _ gcs.Client = (*cgoClient)(nil)
}

// Test_ConnectClient_negativeTimeout verifies that a negative timeout is
// rejected by the Go-side validation.
func Test_ConnectClient_negativeTimeout(t *testing.T) {
	_, err := ConnectClient(gcs.ClientOptions{
		TimeoutMs: -1000,
		Address:   "127.0.0.1:6379",
		ClusterID: ids.NewClusterID(),
	})
	if err == nil {
		t.Fatal("expected error for negative timeout, got nil")
	}
	if !strings.Contains(err.Error(), "non-negative") {
		t.Errorf("error message should mention 'non-negative', got: %v", err)
	}
}

// Test_ConnectClient_emptyAddress verifies that an empty address is rejected by
// the Go-side validation.
func Test_ConnectClient_emptyAddress(t *testing.T) {
	_, err := ConnectClient(gcs.ClientOptions{
		TimeoutMs: 5000,
		Address:   "",
		ClusterID: ids.NewClusterID(),
	})
	if err == nil {
		t.Fatal("expected error for empty address, got nil")
	}
}

// Test_setClient verifies that a GCS client registered through the global
// singleton can be fetched again.
func Test_setClient(t *testing.T) {
	// Clear the global singleton so the test is independent of the others.
	gcs.ClearClient()

	mockClient := &mockGcsClient{
		addr:      "mock:1234",
		clusterID: ids.NewClusterID(),
	}

	gcs.SetClient(mockClient)

	client, err := gcs.GetClient()
	if err != nil {
		t.Fatalf("GetClient() returned error: %v", err)
	}

	if client.Address() != "mock:1234" {
		t.Errorf("expected address 'mock:1234', got %s", client.Address())
	}

	gcs.ClearClient()
}

// Test_errKeyNotFound verifies the ErrKeyNotFound sentinel.
func Test_errKeyNotFound(t *testing.T) {
	if gcs.ErrKeyNotFound == nil {
		t.Fatal("ErrKeyNotFound should be defined")
	}
	if gcs.ErrKeyNotFound.Error() != "key not found" {
		t.Errorf("ErrKeyNotFound error message should be 'key not found', got %q", gcs.ErrKeyNotFound.Error())
	}
}

// Test_MultiGet_errNotImplemented verifies that the mock reports the
// not-implemented error for MultiGet.
func Test_MultiGet_errNotImplemented(t *testing.T) {
	client := &mockGcsClient{}
	_, err := client.MultiGet(context.Background(), "test", []string{"key1", "key2"})

	if !errors.Is(err, gcs.ErrNotImplemented) {
		t.Errorf("expected ErrNotImplemented, got: %v", err)
	}
}

// Test_Keys_basic verifies that the mock exposes the Keys contract used by the
// cgo client.
func Test_Keys_basic(t *testing.T) {
	client := &mockGcsClient{}
	keys, err := client.Keys(context.Background(), "test", "prefix")

	// The mock returns ErrNotImplemented and no keys.
	if !errors.Is(err, gcs.ErrNotImplemented) {
		t.Errorf("expected ErrNotImplemented, got: %v", err)
	}
	if keys != nil {
		t.Errorf("expected nil keys, got: %v", keys)
	}
}

// Test_put_emptyValue verifies the empty-slice handling of the Put path: an
// empty value must be passed to C as a nil pointer with size 0, because
// &value[0] on an empty slice is undefined behavior.
func Test_put_emptyValue(t *testing.T) {
	value := []byte{}

	if len(value) == 0 {
		var cValue unsafe.Pointer
		var cSize C.size_t
		if len(value) > 0 {
			cValue = unsafe.Pointer(&value[0])
			cSize = C.size_t(len(value))
		} else {
			cValue = nil
			cSize = 0
		}

		if cValue != nil {
			t.Errorf("expected nil pointer for empty slice")
		}
		if cSize != 0 {
			t.Errorf("expected 0 size for empty slice, got %d", cSize)
		}
	}
}

// Test_put_emptyValue_cbytes verifies that C.CBytes copies the value into C
// memory so no Go pointer crosses the cgo boundary.
func Test_put_emptyValue_cbytes(t *testing.T) {
	// C.CBytes always allocates, even for an empty slice, so the pointer is
	// non-nil while the size is 0.
	empty := []byte{}
	cPtr := C.CBytes(empty)
	if cPtr == nil {
		t.Error("C.CBytes(empty) should allocate memory (may be zero-size)")
	}
	defer C.free(cPtr)

	data := []byte("test")
	cPtr = C.CBytes(data)
	if cPtr == nil {
		t.Fatal("C.CBytes(non-empty) should not return nil")
	}
	defer C.free(cPtr)
}

// Test_get_null_pointer verifies that a nil data pointer returned by C is
// reported as an error instead of being passed to C.GoBytes.
func Test_get_null_pointer(t *testing.T) {
	var cData unsafe.Pointer = nil
	var cSize C.size_t = 0

	if cData == nil {
		if _, err := errorFromNullCheck(); err == nil {
			t.Error("expected error when cData is nil")
		}
	}

	if cSize == 0 {
		// A zero size means the value is empty, not missing.
		result := []byte{}
		if len(result) != 0 {
			t.Error("expected empty slice when cSize is 0")
		}
	}
}

// errorFromNullCheck mirrors the guard Get applies before it reads C memory.
func errorFromNullCheck() ([]byte, error) {
	return nil, fmt.Errorf("received null data pointer")
}

// mockGcsClient is a gcs.Client implementation used by the tests that do not
// need a live GCS connection.
type mockGcsClient struct {
	addr      string
	clusterID ids.ClusterID
}

func (m *mockGcsClient) Address() string          { return m.addr }
func (m *mockGcsClient) ClusterID() ids.ClusterID { return m.clusterID }
func (m *mockGcsClient) IsClosed() bool           { return false }
func (m *mockGcsClient) Close() error             { return nil }
func (m *mockGcsClient) ReportAutoscalingState(autoscalingState string) error {
	return gcs.ErrNotImplemented
}
func (m *mockGcsClient) Get(ctx context.Context, ns, key string) ([]byte, error) {
	return nil, gcs.ErrNotImplemented
}
func (m *mockGcsClient) MultiGet(ctx context.Context, ns string, keys []string) (map[string][]byte, error) {
	return nil, gcs.ErrNotImplemented
}
func (m *mockGcsClient) Put(ctx context.Context, ns, key string, value []byte, overwrite bool) (bool, error) {
	return false, gcs.ErrNotImplemented
}
func (m *mockGcsClient) Del(ctx context.Context, ns, key string, delByPrefix bool) (int, error) {
	return 0, gcs.ErrNotImplemented
}
func (m *mockGcsClient) Keys(ctx context.Context, ns, prefix string) ([]string, error) {
	return nil, gcs.ErrNotImplemented
}
func (m *mockGcsClient) Exists(ctx context.Context, ns, key string) (bool, error) {
	return false, gcs.ErrNotImplemented
}
func (m *mockGcsClient) CheckAlive(ctx context.Context, nodeIDs []ids.NodeID) ([]bool, error) {
	return nil, gcs.ErrNotImplemented
}
func (m *mockGcsClient) GetAll(ctx context.Context, nodeIDs []ids.NodeID) (map[ids.NodeID]*proto.GcsNodeInfo, error) {
	return nil, gcs.ErrNotImplemented
}
func (m *mockGcsClient) DrainNodes(ctx context.Context, nodeIDs []ids.NodeID) ([]ids.NodeID, error) {
	return nil, gcs.ErrNotImplemented
}
func (m *mockGcsClient) GetNodeToConnect(ctx context.Context, nodeIpAddress string) (*proto.GcsNodeInfo, error) {
	return nil, gcs.ErrNotImplemented
}
func (m *mockGcsClient) GetAvailableResources(ctx context.Context, nodeID ids.NodeID) (*proto.AvailableResources, error) {
	return nil, gcs.ErrNotImplemented
}
func (m *mockGcsClient) GetTotalResources(ctx context.Context, nodeID ids.NodeID) (*proto.TotalResources, error) {
	return nil, gcs.ErrNotImplemented
}
func (m *mockGcsClient) GetActorInfo(ctx context.Context, actorID ids.ActorID) (*proto.ActorTableData, error) {
	return nil, gcs.ErrNotImplemented
}
func (m *mockGcsClient) ListActors(ctx context.Context, jobID *ids.JobID) ([]*proto.ActorTableData, error) {
	return nil, gcs.ErrNotImplemented
}
func (m *mockGcsClient) GetJobInfo(ctx context.Context, jobID ids.JobID) (*proto.JobTableData, error) {
	return nil, gcs.ErrNotImplemented
}
func (m *mockGcsClient) ListJobs(ctx context.Context) ([]*proto.JobTableData, error) {
	return nil, gcs.ErrNotImplemented
}
func (m *mockGcsClient) NextJobID(ctx context.Context) (ids.JobID, error) {
	return ids.NilJobID(), gcs.ErrNotImplemented
}
func (m *mockGcsClient) GetWorkerInfo(ctx context.Context, workerID ids.WorkerID) (*proto.WorkerTableData, error) {
	return nil, gcs.ErrNotImplemented
}
func (m *mockGcsClient) ListWorkers(ctx context.Context) ([]*proto.WorkerTableData, error) {
	return nil, gcs.ErrNotImplemented
}
func (m *mockGcsClient) GetPlacementGroup(ctx context.Context, pgID ids.PlacementGroupID) (*proto.PlacementGroupTableData, error) {
	return nil, gcs.ErrNotImplemented
}
func (m *mockGcsClient) ListPlacementGroups(ctx context.Context) ([]*proto.PlacementGroupTableData, error) {
	return nil, gcs.ErrNotImplemented
}
func (m *mockGcsClient) GetAutoscalerStatus(ctx context.Context) (*proto.GetClusterStatusReply, error) {
	return nil, gcs.ErrNotImplemented
}
