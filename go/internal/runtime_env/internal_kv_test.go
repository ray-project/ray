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

package runtime_env

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/ray-project/ray/go/pkg/gcs"
	"github.com/ray-project/ray/go/pkg/ids"
	"github.com/ray-project/ray/go/proto"
)

// MockGCSClient is a mock GCS client used for testing.
type MockGCSClient struct {
	GetFunc       func(ctx context.Context, ns, key string) ([]byte, error)
	PutFunc       func(ctx context.Context, ns, key string, value []byte, overwrite bool) (bool, error)
	DelFunc       func(ctx context.Context, ns, key string, delByPrefix bool) (int, error)
	KeysFunc      func(ctx context.Context, ns, prefix string) ([]string, error)
	ExistsFunc    func(ctx context.Context, ns, key string) (bool, error)
	PinURIFunc    func(ctx context.Context, uri string, expirationS int) error
	AddressFunc   func() string
	ClusterIDFunc func() ids.ClusterID
	CloseFunc     func() error
	IsClosedFunc  func() bool
	closed        bool
}

// Ensure MockGCSClient implements the gcs.Client interface.
var _ gcs.Client = (*MockGCSClient)(nil)

func (m *MockGCSClient) Get(ctx context.Context, ns, key string) ([]byte, error) {
	if m.GetFunc != nil {
		return m.GetFunc(ctx, ns, key)
	}
	return nil, errors.New("Get not implemented")
}

func (m *MockGCSClient) MultiGet(ctx context.Context, ns string, keys []string) (map[string][]byte, error) {
	return nil, errors.New("MultiGet not implemented")
}

func (m *MockGCSClient) Put(ctx context.Context, ns, key string, value []byte, overwrite bool) (bool, error) {
	if m.PutFunc != nil {
		return m.PutFunc(ctx, ns, key, value, overwrite)
	}
	return false, errors.New("Put not implemented")
}

func (m *MockGCSClient) Del(ctx context.Context, ns, key string, delByPrefix bool) (int, error) {
	if m.DelFunc != nil {
		return m.DelFunc(ctx, ns, key, delByPrefix)
	}
	return 0, errors.New("Del not implemented")
}

func (m *MockGCSClient) Keys(ctx context.Context, ns, prefix string) ([]string, error) {
	if m.KeysFunc != nil {
		return m.KeysFunc(ctx, ns, prefix)
	}
	return nil, errors.New("Keys not implemented")
}

func (m *MockGCSClient) Exists(ctx context.Context, ns, key string) (bool, error) {
	if m.ExistsFunc != nil {
		return m.ExistsFunc(ctx, ns, key)
	}
	return false, errors.New("Exists not implemented")
}

func (m *MockGCSClient) PinRuntimeEnvURI(ctx context.Context, uri string, expirationS int) error {
	if m.PinURIFunc != nil {
		return m.PinURIFunc(ctx, uri, expirationS)
	}
	return errors.New("PinRuntimeEnvURI not implemented")
}

func (m *MockGCSClient) Address() string {
	if m.AddressFunc != nil {
		return m.AddressFunc()
	}
	return "mock-address"
}

func (m *MockGCSClient) ReportAutoscalingState(autoscalingState string) error {
	return gcs.ErrNotImplemented
}

func (m *MockGCSClient) ClusterID() ids.ClusterID {
	if m.ClusterIDFunc != nil {
		return m.ClusterIDFunc()
	}
	return ids.ClusterID{}
}

func (m *MockGCSClient) Close() error {
	if m.CloseFunc != nil {
		return m.CloseFunc()
	}
	m.closed = true
	return nil
}

func (m *MockGCSClient) IsClosed() bool {
	if m.IsClosedFunc != nil {
		return m.IsClosedFunc()
	}
	return m.closed
}

// NodeInfoInterface methods.
func (m *MockGCSClient) CheckAlive(ctx context.Context, nodeIDs []ids.NodeID) ([]bool, error) {
	return nil, errors.New("CheckAlive not implemented")
}

func (m *MockGCSClient) GetAll(ctx context.Context, nodeIDs []ids.NodeID) (map[ids.NodeID]*proto.GcsNodeInfo, error) {
	return nil, errors.New("GetAll not implemented")
}

func (m *MockGCSClient) DrainNodes(ctx context.Context, nodeIDs []ids.NodeID) ([]ids.NodeID, error) {
	return nil, errors.New("DrainNodes not implemented")
}

func (m *MockGCSClient) DrainNode(ctx context.Context, nodeID ids.NodeID, reason proto.DrainNodeReason, reasonMessage string, deadlineTimestampMs int64) (bool, string, error) {
	return false, "", errors.New("DrainNode not implemented")
}

func (m *MockGCSClient) GetNodeToConnect(ctx context.Context, nodeIpAddress string) (*proto.GcsNodeInfo, error) {
	return nil, errors.New("GetNodeToConnect not implemented")
}

// NodeResourceInterface methods.
func (m *MockGCSClient) GetAvailableResources(ctx context.Context, nodeID ids.NodeID) (*proto.AvailableResources, error) {
	return nil, errors.New("GetAvailableResources not implemented")
}

func (m *MockGCSClient) GetTotalResources(ctx context.Context, nodeID ids.NodeID) (*proto.TotalResources, error) {
	return nil, errors.New("GetTotalResources not implemented")
}

// ActorInfoInterface methods.
func (m *MockGCSClient) GetActorInfo(ctx context.Context, actorID ids.ActorID) (*proto.ActorTableData, error) {
	return nil, errors.New("GetActorInfo not implemented")
}

func (m *MockGCSClient) ListActors(ctx context.Context, jobID *ids.JobID) ([]*proto.ActorTableData, error) {
	return nil, errors.New("ListActors not implemented")
}

func (m *MockGCSClient) ListActorsByFilter(ctx context.Context, jobID *ids.JobID, actorStateName *gcs.ActorStateName) ([]*proto.ActorTableData, error) {
	return nil, errors.New("ListActorsByFilter not implemented")
}

// JobInfoInterface methods.
func (m *MockGCSClient) GetJobInfo(ctx context.Context, jobID ids.JobID) (*proto.JobTableData, error) {
	return nil, errors.New("GetJobInfo not implemented")
}

func (m *MockGCSClient) ListJobs(ctx context.Context) ([]*proto.JobTableData, error) {
	return nil, errors.New("ListJobs not implemented")
}

func (m *MockGCSClient) NextJobID(ctx context.Context) (ids.JobID, error) {
	return ids.NilJobID(), errors.New("NextJobID not implemented")
}

// WorkerInfoInterface methods.
func (m *MockGCSClient) GetWorkerInfo(ctx context.Context, workerID ids.WorkerID) (*proto.WorkerTableData, error) {
	return nil, errors.New("GetWorkerInfo not implemented")
}

func (m *MockGCSClient) ListWorkers(ctx context.Context) ([]*proto.WorkerTableData, error) {
	return nil, errors.New("ListWorkers not implemented")
}

// PlacementGroupInterface methods.
func (m *MockGCSClient) GetPlacementGroup(ctx context.Context, pgID ids.PlacementGroupID) (*proto.PlacementGroupTableData, error) {
	return nil, errors.New("GetPlacementGroup not implemented")
}

func (m *MockGCSClient) GetPlacementGroupByName(ctx context.Context, name, namespace string) (*proto.PlacementGroupTableData, error) {
	return nil, errors.New("GetPlacementGroupByName not implemented")
}

func (m *MockGCSClient) ListPlacementGroups(ctx context.Context) ([]*proto.PlacementGroupTableData, error) {
	return nil, errors.New("ListPlacementGroups not implemented")
}

// PublisherInterface methods.
func (m *MockGCSClient) PublishErrors(ctx context.Context) (<-chan gcs.ErrorData, error) {
	return nil, errors.New("PublishErrors not implemented")
}

func (m *MockGCSClient) PublishLogs(ctx context.Context) (<-chan gcs.LogData, error) {
	return nil, errors.New("PublishLogs not implemented")
}

// AutoscalerInterface methods.
func (m *MockGCSClient) GetAutoscalerStatus(ctx context.Context) (*proto.GetClusterStatusReply, error) {
	return nil, errors.New("GetAutoscalerStatus not implemented")
}

// ============ InternalKVReset tests ============

func TestInternalKVReset(t *testing.T) {
	// Set the initial state.
	globalGCSClient = &MockGCSClient{}

	// Call Reset.
	InternalKVReset()

	// Verify the state is reset.
	if globalGCSClient != nil {
		t.Errorf("expected globalGCSClient to be nil after reset, got %v", globalGCSClient)
	}
}

// ============ InitializeInternalKV tests ============

func TestInitializeInternalKV_Success(t *testing.T) {
	defer InternalKVReset()

	mockClient := &MockGCSClient{}
	err := InitializeInternalKV(mockClient)

	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
	if globalGCSClient != mockClient {
		t.Error("expected globalGCSClient to be set to mockClient")
	}
	if !InternalKVInitialized() {
		t.Error("expected InternalKVInitialized to return true")
	}
}

func TestInitializeInternalKV_NilClient(t *testing.T) {
	defer InternalKVReset()

	err := InitializeInternalKV(nil)

	if err == nil {
		t.Error("expected error for nil client, got nil")
	}
	if err.Error() != "GCS client is nil" {
		t.Errorf("expected 'GCS client is nil' error, got %v", err)
	}
}

// ============ InternalKVGetGCSClient tests ============

func TestInternalKVGetGCSClient(t *testing.T) {
	defer InternalKVReset()

	// Returns nil when not initialized.
	if InternalKVGetGCSClient() != nil {
		t.Error("expected nil client before initialization")
	}

	// Returns the client after initialization.
	mockClient := &MockGCSClient{}
	_ = InitializeInternalKV(mockClient)

	client := InternalKVGetGCSClient()
	if client != mockClient {
		t.Error("expected to get the initialized client")
	}
}

// ============ InternalKVInitialized tests ============

func TestInternalKVInitialized_True(t *testing.T) {
	defer InternalKVReset()

	mockClient := &MockGCSClient{}
	_ = InitializeInternalKV(mockClient)

	if !InternalKVInitialized() {
		t.Error("expected InternalKVInitialized to return true")
	}
}

func TestInternalKVInitialized_False_NoClient(t *testing.T) {
	defer InternalKVReset()

	// Not initialized.
	if InternalKVInitialized() {
		t.Error("expected InternalKVInitialized to return false when no client")
	}
}

// ============ InternalKVGet tests ============

func TestInternalKVGet_Success(t *testing.T) {
	defer InternalKVReset()

	expectedValue := []byte("test-value")
	mockClient := &MockGCSClient{
		GetFunc: func(ctx context.Context, ns, key string) ([]byte, error) {
			if ns != "test-namespace" || key != "test-key" {
				t.Errorf("unexpected namespace or key: ns=%s, key=%s", ns, key)
			}
			return expectedValue, nil
		},
	}
	_ = InitializeInternalKV(mockClient)

	ctx := context.Background()
	value, err := InternalKVGet(ctx, "test-key", "test-namespace")

	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
	if string(value) != string(expectedValue) {
		t.Errorf("expected value %s, got %s", string(expectedValue), string(value))
	}
}

func TestInternalKVGet_NoClient(t *testing.T) {
	defer InternalKVReset()

	ctx := context.Background()
	value, err := InternalKVGet(ctx, "test-key", "test-namespace")

	if err == nil {
		t.Error("expected error when no client, got nil")
	}
	if err.Error() != "GCS client is not initialized" {
		t.Errorf("expected 'GCS client is not initialized' error, got %v", err)
	}
	if value != nil {
		t.Errorf("expected nil value, got %v", value)
	}
}

func TestInternalKVGet_ClientError(t *testing.T) {
	defer InternalKVReset()

	expectedErr := errors.New("key not found")
	mockClient := &MockGCSClient{
		GetFunc: func(ctx context.Context, ns, key string) ([]byte, error) {
			return nil, expectedErr
		},
	}
	_ = InitializeInternalKV(mockClient)

	ctx := context.Background()
	value, err := InternalKVGet(ctx, "test-key", "test-namespace")

	if err != expectedErr {
		t.Errorf("expected error %v, got %v", expectedErr, err)
	}
	if value != nil {
		t.Errorf("expected nil value, got %v", value)
	}
}

// ============ InternalKVExists tests ============

func TestInternalKVExists_Success(t *testing.T) {
	defer InternalKVReset()

	mockClient := &MockGCSClient{
		ExistsFunc: func(ctx context.Context, ns, key string) (bool, error) {
			return true, nil
		},
	}
	_ = InitializeInternalKV(mockClient)

	ctx := context.Background()
	exists, err := InternalKVExists(ctx, "test-key", "test-namespace")

	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
	if !exists {
		t.Error("expected key to exist")
	}
}

func TestInternalKVExists_NotFound(t *testing.T) {
	defer InternalKVReset()

	mockClient := &MockGCSClient{
		ExistsFunc: func(ctx context.Context, ns, key string) (bool, error) {
			return false, nil
		},
	}
	_ = InitializeInternalKV(mockClient)

	ctx := context.Background()
	exists, err := InternalKVExists(ctx, "test-key", "test-namespace")

	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
	if exists {
		t.Error("expected key to not exist")
	}
}

func TestInternalKVExists_NoClient(t *testing.T) {
	defer InternalKVReset()

	ctx := context.Background()
	exists, err := InternalKVExists(ctx, "test-key", "test-namespace")

	if err == nil {
		t.Error("expected error when no client, got nil")
	}
	if exists {
		t.Error("expected exists to be false")
	}
}

// ============ PinRuntimeEnvURICall tests ============

func TestPinRuntimeEnvURICall_NotImplemented(t *testing.T) {
	defer InternalKVReset()

	mockClient := &MockGCSClient{
		PinURIFunc: func(ctx context.Context, uri string, expirationS int) error {
			return nil
		},
	}
	_ = InitializeInternalKV(mockClient)

	ctx := context.Background()
	err := PinRuntimeEnvURICall(ctx, "test-uri", 3600)

	// The gcs.Client interface exposes no pin method, so the bridge cannot yet
	// acquire a pin. It must report the gap instead of pretending success:
	// a nil error here would let the GCS runtime-env garbage collector delete
	// an uploaded package another node still references.
	if err == nil {
		t.Error("expected pin call to report the unimplemented bridge, got nil")
	}
	if !strings.Contains(err.Error(), "not implemented by the GCS bridge") {
		t.Errorf("expected 'not implemented by the GCS bridge' error, got %v", err)
	}
}

func TestPinRuntimeEnvURICall_NoClient(t *testing.T) {
	defer InternalKVReset()

	ctx := context.Background()
	err := PinRuntimeEnvURICall(ctx, "test-uri", 3600)

	if err == nil {
		t.Error("expected error when no client, got nil")
	}
	if err.Error() != "GCS client is not initialized" {
		t.Errorf("expected 'GCS client is not initialized' error, got %v", err)
	}
}

// ============ InternalKVPut tests ============

func TestInternalKVPut_Success(t *testing.T) {
	defer InternalKVReset()

	mockClient := &MockGCSClient{
		PutFunc: func(ctx context.Context, ns, key string, value []byte, overwrite bool) (bool, error) {
			if ns != "test-namespace" || key != "test-key" {
				t.Errorf("unexpected namespace or key: ns=%s, key=%s", ns, key)
			}
			if string(value) != "test-value" {
				t.Errorf("unexpected value: %s", string(value))
			}
			if !overwrite {
				t.Error("expected overwrite to be true")
			}
			return true, nil
		},
	}
	_ = InitializeInternalKV(mockClient)

	ctx := context.Background()
	created, err := InternalKVPut(ctx, "test-key", []byte("test-value"), true, "test-namespace")

	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
	if !created {
		t.Error("expected key to be created")
	}
}

func TestInternalKVPut_NoClient(t *testing.T) {
	defer InternalKVReset()

	ctx := context.Background()
	created, err := InternalKVPut(ctx, "test-key", []byte("test-value"), true, "test-namespace")

	if err == nil {
		t.Error("expected error when no client, got nil")
	}
	if created {
		t.Error("expected created to be false")
	}
}

// ============ InternalKVDel tests ============

func TestInternalKVDel_Success(t *testing.T) {
	defer InternalKVReset()

	mockClient := &MockGCSClient{
		DelFunc: func(ctx context.Context, ns, key string, delByPrefix bool) (int, error) {
			if ns != "test-namespace" || key != "test-key" {
				t.Errorf("unexpected namespace or key: ns=%s, key=%s", ns, key)
			}
			if delByPrefix {
				t.Error("expected delByPrefix to be false")
			}
			return 1, nil
		},
	}
	_ = InitializeInternalKV(mockClient)

	ctx := context.Background()
	deleted, err := InternalKVDel(ctx, "test-key", false, "test-namespace")

	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
	if deleted != 1 {
		t.Errorf("expected 1 deleted, got %d", deleted)
	}
}

func TestInternalKVDel_NoClient(t *testing.T) {
	defer InternalKVReset()

	ctx := context.Background()
	deleted, err := InternalKVDel(ctx, "test-key", false, "test-namespace")

	if err == nil {
		t.Error("expected error when no client, got nil")
	}
	if deleted != 0 {
		t.Errorf("expected 0 deleted, got %d", deleted)
	}
}

// ============ InternalKVList tests ============

func TestInternalKVList_Success(t *testing.T) {
	defer InternalKVReset()

	mockClient := &MockGCSClient{
		KeysFunc: func(ctx context.Context, ns, prefix string) ([]string, error) {
			if ns != "test-namespace" || prefix != "test-prefix" {
				t.Errorf("unexpected namespace or prefix: ns=%s, prefix=%s", ns, prefix)
			}
			return []string{"key1", "key2", "key3"}, nil
		},
	}
	_ = InitializeInternalKV(mockClient)

	ctx := context.Background()
	result, err := InternalKVList(ctx, "test-prefix", "test-namespace")

	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
	if len(result) != 3 {
		t.Errorf("expected 3 keys, got %d", len(result))
	}
	expectedKeys := [][]byte{[]byte("key1"), []byte("key2"), []byte("key3")}
	for i, key := range result {
		if string(key) != string(expectedKeys[i]) {
			t.Errorf("expected key %s at index %d, got %s", string(expectedKeys[i]), i, string(key))
		}
	}
}

func TestInternalKVList_EmptyResult(t *testing.T) {
	defer InternalKVReset()

	mockClient := &MockGCSClient{
		KeysFunc: func(ctx context.Context, ns, prefix string) ([]string, error) {
			return []string{}, nil
		},
	}
	_ = InitializeInternalKV(mockClient)

	ctx := context.Background()
	result, err := InternalKVList(ctx, "test-prefix", "test-namespace")

	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
	if len(result) != 0 {
		t.Errorf("expected 0 keys, got %d", len(result))
	}
}

func TestInternalKVList_Error(t *testing.T) {
	defer InternalKVReset()

	expectedErr := errors.New("keys lookup failed")
	mockClient := &MockGCSClient{
		KeysFunc: func(ctx context.Context, ns, prefix string) ([]string, error) {
			return nil, expectedErr
		},
	}
	_ = InitializeInternalKV(mockClient)

	ctx := context.Background()
	result, err := InternalKVList(ctx, "test-prefix", "test-namespace")

	if err != expectedErr {
		t.Errorf("expected error %v, got %v", expectedErr, err)
	}
	if result != nil {
		t.Errorf("expected nil result, got %v", result)
	}
}

func TestInternalKVList_NoClient(t *testing.T) {
	defer InternalKVReset()

	ctx := context.Background()
	result, err := InternalKVList(ctx, "test-prefix", "test-namespace")

	if err == nil {
		t.Error("expected error when no client, got nil")
	}
	if result != nil {
		t.Errorf("expected nil result, got %v", result)
	}
}
