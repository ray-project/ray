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

package usage

import (
	"context"
	"errors"
	"testing"

	"github.com/ray-project/ray/go/internal/runtime_env"
	"github.com/ray-project/ray/go/pkg/gcs"
	"github.com/ray-project/ray/go/pkg/ids"
	"github.com/ray-project/ray/go/proto"
)

// MockGCSClient is a mock GCS client used for testing.
type MockGCSClient struct {
	GetFunc                     func(ctx context.Context, ns, key string) ([]byte, error)
	MultiGetFunc                func(ctx context.Context, ns string, keys []string) (map[string][]byte, error)
	PutFunc                     func(ctx context.Context, ns, key string, value []byte, overwrite bool) (bool, error)
	DelFunc                     func(ctx context.Context, ns, key string, delByPrefix bool) (int, error)
	KeysFunc                    func(ctx context.Context, ns, prefix string) ([]string, error)
	ExistsFunc                  func(ctx context.Context, ns, key string) (bool, error)
	CheckAliveFunc              func(ctx context.Context, nodeIDs []ids.NodeID) ([]bool, error)
	GetAllFunc                  func(ctx context.Context, nodeIDs []ids.NodeID) (map[ids.NodeID]*proto.GcsNodeInfo, error)
	DrainNodesFunc              func(ctx context.Context, nodeIDs []ids.NodeID) ([]ids.NodeID, error)
	DrainNodeFunc               func(ctx context.Context, nodeID ids.NodeID, reason proto.DrainNodeReason, reasonMessage string, deadlineTimestampMs int64) (bool, string, error)
	GetAvailableResourcesFunc   func(ctx context.Context, nodeID ids.NodeID) (*proto.AvailableResources, error)
	GetTotalResourcesFunc       func(ctx context.Context, nodeID ids.NodeID) (*proto.TotalResources, error)
	GetActorInfoFunc            func(ctx context.Context, actorID ids.ActorID) (*proto.ActorTableData, error)
	ListActorsFunc              func(ctx context.Context, jobID *ids.JobID) ([]*proto.ActorTableData, error)
	ListActorsByFilterFunc      func(ctx context.Context, jobID *ids.JobID, actorStateName *gcs.ActorStateName) ([]*proto.ActorTableData, error)
	GetJobInfoFunc              func(ctx context.Context, jobID ids.JobID) (*proto.JobTableData, error)
	ListJobsFunc                func(ctx context.Context) ([]*proto.JobTableData, error)
	GetWorkerInfoFunc           func(ctx context.Context, workerID ids.WorkerID) (*proto.WorkerTableData, error)
	ListWorkersFunc             func(ctx context.Context) ([]*proto.WorkerTableData, error)
	GetPlacementGroupFunc       func(ctx context.Context, pgID ids.PlacementGroupID) (*proto.PlacementGroupTableData, error)
	GetPlacementGroupByNameFunc func(ctx context.Context, name, namespace string) (*proto.PlacementGroupTableData, error)
	ListPlacementGroupsFunc     func(ctx context.Context) ([]*proto.PlacementGroupTableData, error)
	PublishErrorsFunc           func(ctx context.Context) (<-chan gcs.ErrorData, error)
	PublishLogsFunc             func(ctx context.Context) (<-chan gcs.LogData, error)
	GetAutoscalerStatusFunc     func(ctx context.Context) (*proto.GetClusterStatusReply, error)
	AddressFunc                 func() string
	ClusterIDFunc               func() ids.ClusterID
	CloseFunc                   func() error
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
	if m.MultiGetFunc != nil {
		return m.MultiGetFunc(ctx, ns, keys)
	}
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

func (m *MockGCSClient) Address() string {
	if m.AddressFunc != nil {
		return m.AddressFunc()
	}
	return ""
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

func (m *MockGCSClient) IsClosed() bool { return false }

func (m *MockGCSClient) Close() error {
	if m.CloseFunc != nil {
		return m.CloseFunc()
	}
	return nil
}

func (m *MockGCSClient) CheckAlive(ctx context.Context, nodeIDs []ids.NodeID) ([]bool, error) {
	if m.CheckAliveFunc != nil {
		return m.CheckAliveFunc(ctx, nodeIDs)
	}
	return nil, errors.New("CheckAlive not implemented")
}

func (m *MockGCSClient) GetAll(ctx context.Context, nodeIDs []ids.NodeID) (map[ids.NodeID]*proto.GcsNodeInfo, error) {
	if m.GetAllFunc != nil {
		return m.GetAllFunc(ctx, nodeIDs)
	}
	return nil, errors.New("GetAll not implemented")
}

func (m *MockGCSClient) DrainNodes(ctx context.Context, nodeIDs []ids.NodeID) ([]ids.NodeID, error) {
	if m.DrainNodesFunc != nil {
		return m.DrainNodesFunc(ctx, nodeIDs)
	}
	return nil, errors.New("DrainNodes not implemented")
}

func (m *MockGCSClient) DrainNode(ctx context.Context, nodeID ids.NodeID, reason proto.DrainNodeReason, reasonMessage string, deadlineTimestampMs int64) (bool, string, error) {
	if m.DrainNodeFunc != nil {
		return m.DrainNodeFunc(ctx, nodeID, reason, reasonMessage, deadlineTimestampMs)
	}
	return false, "", errors.New("DrainNode not implemented")
}

func (m *MockGCSClient) GetNodeToConnect(ctx context.Context, nodeIpAddress string) (*proto.GcsNodeInfo, error) {
	return nil, errors.New("GetNodeToConnect not implemented")
}

func (m *MockGCSClient) GetAvailableResources(ctx context.Context, nodeID ids.NodeID) (*proto.AvailableResources, error) {
	if m.GetAvailableResourcesFunc != nil {
		return m.GetAvailableResourcesFunc(ctx, nodeID)
	}
	return nil, errors.New("GetAvailableResources not implemented")
}

func (m *MockGCSClient) GetTotalResources(ctx context.Context, nodeID ids.NodeID) (*proto.TotalResources, error) {
	if m.GetTotalResourcesFunc != nil {
		return m.GetTotalResourcesFunc(ctx, nodeID)
	}
	return nil, errors.New("GetTotalResources not implemented")
}

func (m *MockGCSClient) GetActorInfo(ctx context.Context, actorID ids.ActorID) (*proto.ActorTableData, error) {
	if m.GetActorInfoFunc != nil {
		return m.GetActorInfoFunc(ctx, actorID)
	}
	return nil, errors.New("GetActorInfo not implemented")
}

func (m *MockGCSClient) ListActors(ctx context.Context, jobID *ids.JobID) ([]*proto.ActorTableData, error) {
	if m.ListActorsFunc != nil {
		return m.ListActorsFunc(ctx, jobID)
	}
	return nil, errors.New("ListActors not implemented")
}

func (m *MockGCSClient) ListActorsByFilter(ctx context.Context, jobID *ids.JobID, actorStateName *gcs.ActorStateName) ([]*proto.ActorTableData, error) {
	if m.ListActorsByFilterFunc != nil {
		return m.ListActorsByFilterFunc(ctx, jobID, actorStateName)
	}
	return nil, errors.New("ListActorsByFilter not implemented")
}

func (m *MockGCSClient) GetJobInfo(ctx context.Context, jobID ids.JobID) (*proto.JobTableData, error) {
	if m.GetJobInfoFunc != nil {
		return m.GetJobInfoFunc(ctx, jobID)
	}
	return nil, errors.New("GetJobInfo not implemented")
}

func (m *MockGCSClient) ListJobs(ctx context.Context) ([]*proto.JobTableData, error) {
	if m.ListJobsFunc != nil {
		return m.ListJobsFunc(ctx)
	}
	return nil, errors.New("ListJobs not implemented")
}

func (m *MockGCSClient) NextJobID(ctx context.Context) (ids.JobID, error) {
	return ids.NilJobID(), errors.New("NextJobID not implemented")
}

func (m *MockGCSClient) GetWorkerInfo(ctx context.Context, workerID ids.WorkerID) (*proto.WorkerTableData, error) {
	if m.GetWorkerInfoFunc != nil {
		return m.GetWorkerInfoFunc(ctx, workerID)
	}
	return nil, errors.New("GetWorkerInfo not implemented")
}

func (m *MockGCSClient) ListWorkers(ctx context.Context) ([]*proto.WorkerTableData, error) {
	if m.ListWorkersFunc != nil {
		return m.ListWorkersFunc(ctx)
	}
	return nil, errors.New("ListWorkers not implemented")
}

func (m *MockGCSClient) GetPlacementGroup(ctx context.Context, pgID ids.PlacementGroupID) (*proto.PlacementGroupTableData, error) {
	if m.GetPlacementGroupFunc != nil {
		return m.GetPlacementGroupFunc(ctx, pgID)
	}
	return nil, errors.New("GetPlacementGroup not implemented")
}

func (m *MockGCSClient) GetPlacementGroupByName(ctx context.Context, name, namespace string) (*proto.PlacementGroupTableData, error) {
	if m.GetPlacementGroupByNameFunc != nil {
		return m.GetPlacementGroupByNameFunc(ctx, name, namespace)
	}
	return nil, errors.New("GetPlacementGroupByName not implemented")
}

func (m *MockGCSClient) ListPlacementGroups(ctx context.Context) ([]*proto.PlacementGroupTableData, error) {
	if m.ListPlacementGroupsFunc != nil {
		return m.ListPlacementGroupsFunc(ctx)
	}
	return nil, errors.New("ListPlacementGroups not implemented")
}

func (m *MockGCSClient) PublishErrors(ctx context.Context) (<-chan gcs.ErrorData, error) {
	if m.PublishErrorsFunc != nil {
		return m.PublishErrorsFunc(ctx)
	}
	return nil, errors.New("PublishErrors not implemented")
}

func (m *MockGCSClient) PublishLogs(ctx context.Context) (<-chan gcs.LogData, error) {
	if m.PublishLogsFunc != nil {
		return m.PublishLogsFunc(ctx)
	}
	return nil, errors.New("PublishLogs not implemented")
}

func (m *MockGCSClient) GetAutoscalerStatus(ctx context.Context) (*proto.GetClusterStatusReply, error) {
	if m.GetAutoscalerStatusFunc != nil {
		return m.GetAutoscalerStatusFunc(ctx)
	}
	return nil, errors.New("GetAutoscalerStatus not implemented")
}

// TestRecordExtraUsageTag_NoKVInitialized tests the behavior when KV is not
// initialized and no GCS client is given.
func TestRecordExtraUsageTag_NoKVInitialized(t *testing.T) {
	defer runtime_env.InternalKVReset()
	defer ResetRecordedExtraUsageTags()

	ctx := context.Background()
	err := RecordExtraUsageTag(ctx, proto.TagKey__TEST1, "test-value", nil)
	if err != nil {
		t.Errorf("expected no error when KV is not initialized and no GCS client, got: %v", err)
	}

	// Verify the tag was recorded in the in-memory cache.
	tags := GetRecordedExtraUsageTags()
	if val, ok := tags["_test1"]; !ok || val != "test-value" {
		t.Errorf("expected tag '_test1' to be recorded with value 'test-value', got: %v", tags)
	}
}

// TestRecordExtraUsageTag_WithGCSClient tests recording a tag with a GCS client.
func TestRecordExtraUsageTag_WithGCSClient(t *testing.T) {
	defer runtime_env.InternalKVReset()
	defer ResetRecordedExtraUsageTags()

	putCalled := false
	mockClient := &MockGCSClient{
		PutFunc: func(ctx context.Context, ns, key string, value []byte, overwrite bool) (bool, error) {
			putCalled = true
			expectedKey := EXTRA_USAGE_TAG_PREFIX + "_test1"
			if ns != USAGE_STATS_NAMESPACE {
				t.Errorf("expected namespace '%s', got '%s'", USAGE_STATS_NAMESPACE, ns)
			}
			if key != expectedKey {
				t.Errorf("expected key '%s', got '%s'", expectedKey, key)
			}
			if string(value) != "test-value" {
				t.Errorf("expected value 'test-value', got '%s'", string(value))
			}
			return true, nil
		},
	}

	ctx := context.Background()
	err := RecordExtraUsageTag(ctx, proto.TagKey__TEST1, "test-value", mockClient)
	if err != nil {
		t.Errorf("expected no error when using the GCS client, got: %v", err)
	}

	if !putCalled {
		t.Error("expected Put to be called on the GCS client")
	}

	// Verify the tag was recorded in the in-memory cache.
	tags := GetRecordedExtraUsageTags()
	if val, ok := tags["_test1"]; !ok || val != "test-value" {
		t.Errorf("expected tag '_test1' to be recorded with value 'test-value', got: %v", tags)
	}
}

// TestRecordExtraUsageTag_WithInternalKV tests recording a tag through the internal KV.
func TestRecordExtraUsageTag_WithInternalKV(t *testing.T) {
	defer runtime_env.InternalKVReset()
	defer ResetRecordedExtraUsageTags()

	putCalled := false
	mockClient := &MockGCSClient{
		PutFunc: func(ctx context.Context, ns, key string, value []byte, overwrite bool) (bool, error) {
			putCalled = true
			expectedKey := EXTRA_USAGE_TAG_PREFIX + "_test2"
			if ns != USAGE_STATS_NAMESPACE {
				t.Errorf("expected namespace '%s', got '%s'", USAGE_STATS_NAMESPACE, ns)
			}
			if key != expectedKey {
				t.Errorf("expected key '%s', got '%s'", expectedKey, key)
			}
			if string(value) != "another-value" {
				t.Errorf("expected value 'another-value', got '%s'", string(value))
			}
			return true, nil
		},
	}

	// Initialize the internal KV (sets the global GCS client).
	runtime_env.InitializeInternalKV(mockClient)

	ctx := context.Background()
	err := RecordExtraUsageTag(ctx, proto.TagKey__TEST2, "another-value", nil)
	if err != nil {
		t.Errorf("expected no error when using the internal KV, got: %v", err)
	}

	if !putCalled {
		t.Error("expected Put to be called on the internal KV")
	}

	// Verify the tag was recorded in the in-memory cache.
	tags := GetRecordedExtraUsageTags()
	if val, ok := tags["_test2"]; !ok || val != "another-value" {
		t.Errorf("expected tag '_test2' to be recorded with value 'another-value', got: %v", tags)
	}
}

// TestRecordExtraUsageTag_Duplicate tests recording the same tag twice.
func TestRecordExtraUsageTag_Duplicate(t *testing.T) {
	defer runtime_env.InternalKVReset()
	defer ResetRecordedExtraUsageTags()

	putCallCount := 0
	mockClient := &MockGCSClient{
		PutFunc: func(ctx context.Context, ns, key string, value []byte, overwrite bool) (bool, error) {
			putCallCount++
			return true, nil
		},
	}

	ctx := context.Background()

	// First record.
	err := RecordExtraUsageTag(ctx, proto.TagKey__TEST1, "test-value", mockClient)
	if err != nil {
		t.Errorf("expected no error on the first record, got: %v", err)
	}

	// Record the same tag and value again.
	err = RecordExtraUsageTag(ctx, proto.TagKey__TEST1, "test-value", mockClient)
	if err != nil {
		t.Errorf("expected no error on a duplicate record, got: %v", err)
	}

	// Put must have been called exactly once (deduplication).
	if putCallCount != 1 {
		t.Errorf("expected Put to be called once for duplicate records, got %d calls", putCallCount)
	}
}

// TestRecordExtraUsageTag_DifferentValue tests recording the same tag with a different value.
func TestRecordExtraUsageTag_DifferentValue(t *testing.T) {
	defer runtime_env.InternalKVReset()
	defer ResetRecordedExtraUsageTags()

	putCallCount := 0
	mockClient := &MockGCSClient{
		PutFunc: func(ctx context.Context, ns, key string, value []byte, overwrite bool) (bool, error) {
			putCallCount++
			return true, nil
		},
	}

	ctx := context.Background()

	// First record.
	err := RecordExtraUsageTag(ctx, proto.TagKey__TEST1, "value1", mockClient)
	if err != nil {
		t.Errorf("expected no error on the first record, got: %v", err)
	}

	// Record a different value.
	err = RecordExtraUsageTag(ctx, proto.TagKey__TEST1, "value2", mockClient)
	if err != nil {
		t.Errorf("expected no error when recording a different value, got: %v", err)
	}

	// Put must have been called twice because the value changed.
	if putCallCount != 2 {
		t.Errorf("expected Put to be called twice for different values, got %d calls", putCallCount)
	}

	// The final value must be the second one.
	tags := GetRecordedExtraUsageTags()
	if val, ok := tags["_test1"]; !ok || val != "value2" {
		t.Errorf("expected tag '_test1' to have final value 'value2', got: %v", tags)
	}
}

// TestRecordExtraUsageTag_GCSClientError tests the behavior when the GCS client returns an error.
func TestRecordExtraUsageTag_GCSClientError(t *testing.T) {
	defer runtime_env.InternalKVReset()
	defer ResetRecordedExtraUsageTags()

	expectedErr := errors.New("GCS put failed")
	mockClient := &MockGCSClient{
		PutFunc: func(ctx context.Context, ns, key string, value []byte, overwrite bool) (bool, error) {
			return false, expectedErr
		},
	}

	ctx := context.Background()
	err := RecordExtraUsageTag(ctx, proto.TagKey__TEST1, "test-value", mockClient)
	if err == nil {
		t.Error("expected an error when the GCS client fails, got nil")
	} else if err.Error() != "failed to put extra usage tag with GCS client: GCS put failed" {
		t.Errorf("unexpected error message: %v", err)
	}

	// The tag must still be recorded in memory even though GCS failed.
	tags := GetRecordedExtraUsageTags()
	if val, ok := tags["_test1"]; !ok || val != "test-value" {
		t.Errorf("expected tag '_test1' to be recorded in memory despite the GCS failure, got: %v", tags)
	}
}

// TestRecordExtraUsageTag_InternalKVError tests the behavior when the internal KV returns an error.
func TestRecordExtraUsageTag_InternalKVError(t *testing.T) {
	defer runtime_env.InternalKVReset()
	defer ResetRecordedExtraUsageTags()

	expectedErr := errors.New("internal KV put failed")
	mockClient := &MockGCSClient{
		PutFunc: func(ctx context.Context, ns, key string, value []byte, overwrite bool) (bool, error) {
			return false, expectedErr
		},
	}

	// Initialize the internal KV.
	runtime_env.InitializeInternalKV(mockClient)

	ctx := context.Background()
	err := RecordExtraUsageTag(ctx, proto.TagKey__TEST1, "test-value", nil)
	if err == nil {
		t.Error("expected an error when the internal KV fails, got nil")
	} else if err.Error() != "failed to put extra usage tag with internal KV: internal KV put failed" {
		t.Errorf("unexpected error message: %v", err)
	}

	// The tag must still be recorded in memory even though the internal KV failed.
	tags := GetRecordedExtraUsageTags()
	if val, ok := tags["_test1"]; !ok || val != "test-value" {
		t.Errorf("expected tag '_test1' to be recorded in memory despite the internal KV failure, got: %v", tags)
	}
}

// TestGetRecordedExtraUsageTags_Empty tests fetching an empty tag set.
func TestGetRecordedExtraUsageTags_Empty(t *testing.T) {
	defer ResetRecordedExtraUsageTags()

	tags := GetRecordedExtraUsageTags()
	if len(tags) != 0 {
		t.Errorf("expected empty tags, got: %v", tags)
	}
}

// TestGetRecordedExtraUsageTags_MultipleTags tests fetching multiple tags.
func TestGetRecordedExtraUsageTags_MultipleTags(t *testing.T) {
	defer runtime_env.InternalKVReset()
	defer ResetRecordedExtraUsageTags()

	mockClient := &MockGCSClient{
		PutFunc: func(ctx context.Context, ns, key string, value []byte, overwrite bool) (bool, error) {
			return true, nil
		},
	}

	ctx := context.Background()

	// Record multiple tags.
	RecordExtraUsageTag(ctx, proto.TagKey__TEST1, "value1", mockClient)
	RecordExtraUsageTag(ctx, proto.TagKey__TEST2, "value2", mockClient)
	RecordExtraUsageTag(ctx, proto.TagKey_RLLIB_FRAMEWORK, "pytorch", mockClient)

	tags := GetRecordedExtraUsageTags()

	expectedTags := map[string]string{
		"_test1":          "value1",
		"_test2":          "value2",
		"rllib_framework": "pytorch",
	}

	for key, expectedValue := range expectedTags {
		if actualValue, ok := tags[key]; !ok {
			t.Errorf("expected tag '%s' to exist", key)
		} else if actualValue != expectedValue {
			t.Errorf("expected tag '%s' to have value '%s', got '%s'", key, expectedValue, actualValue)
		}
	}
}

// TestGetRecordedExtraUsageTags_ReturnsCopy tests that the returned map is a
// copy rather than the original reference.
func TestGetRecordedExtraUsageTags_ReturnsCopy(t *testing.T) {
	defer runtime_env.InternalKVReset()
	defer ResetRecordedExtraUsageTags()

	mockClient := &MockGCSClient{
		PutFunc: func(ctx context.Context, ns, key string, value []byte, overwrite bool) (bool, error) {
			return true, nil
		},
	}

	ctx := context.Background()
	RecordExtraUsageTag(ctx, proto.TagKey__TEST1, "original-value", mockClient)

	// Fetch the tags and modify the returned map.
	tags := GetRecordedExtraUsageTags()
	tags["_test1"] = "modified-value"

	// Fetch again and verify the original data is unchanged.
	tags2 := GetRecordedExtraUsageTags()
	if val, ok := tags2["_test1"]; !ok || val != "original-value" {
		t.Errorf("expected the original value to remain unchanged, got: %v", tags2)
	}
}

// TestResetRecordedExtraUsageTags tests resetting the tag set.
func TestResetRecordedExtraUsageTags(t *testing.T) {
	defer runtime_env.InternalKVReset()
	defer ResetRecordedExtraUsageTags()

	mockClient := &MockGCSClient{
		PutFunc: func(ctx context.Context, ns, key string, value []byte, overwrite bool) (bool, error) {
			return true, nil
		},
	}

	ctx := context.Background()
	RecordExtraUsageTag(ctx, proto.TagKey__TEST1, "value1", mockClient)
	RecordExtraUsageTag(ctx, proto.TagKey__TEST2, "value2", mockClient)

	// Verify the tags exist.
	tags := GetRecordedExtraUsageTags()
	if len(tags) != 2 {
		t.Errorf("expected 2 tags before reset, got %d", len(tags))
	}

	// Reset.
	ResetRecordedExtraUsageTags()

	// Verify the tags are gone.
	tags = GetRecordedExtraUsageTags()
	if len(tags) != 0 {
		t.Errorf("expected empty tags after reset, got: %v", tags)
	}
}

// TestPutExtraUsageTag_WithGCSClient tests putExtraUsageTag with a GCS client.
func TestPutExtraUsageTag_WithGCSClient(t *testing.T) {
	defer runtime_env.InternalKVReset()

	putCalled := false
	mockClient := &MockGCSClient{
		PutFunc: func(ctx context.Context, ns, key string, value []byte, overwrite bool) (bool, error) {
			putCalled = true
			expectedKey := EXTRA_USAGE_TAG_PREFIX + "custom-key"
			if ns != USAGE_STATS_NAMESPACE {
				t.Errorf("expected namespace '%s', got '%s'", USAGE_STATS_NAMESPACE, ns)
			}
			if key != expectedKey {
				t.Errorf("expected key '%s', got '%s'", expectedKey, key)
			}
			if string(value) != "custom-value" {
				t.Errorf("expected value 'custom-value', got '%s'", string(value))
			}
			if !overwrite {
				t.Error("expected overwrite to be true")
			}
			return true, nil
		},
	}

	ctx := context.Background()
	err := putExtraUsageTag(ctx, "custom-key", "custom-value", mockClient)
	if err != nil {
		t.Errorf("expected no error, got: %v", err)
	}

	if !putCalled {
		t.Error("expected Put to be called on the GCS client")
	}
}

// TestPutExtraUsageTag_WithoutGCSClient tests putExtraUsageTag without a GCS
// client (the internal KV must be used).
func TestPutExtraUsageTag_WithoutGCSClient(t *testing.T) {
	defer runtime_env.InternalKVReset()

	putCalled := false
	mockClient := &MockGCSClient{
		PutFunc: func(ctx context.Context, ns, key string, value []byte, overwrite bool) (bool, error) {
			putCalled = true
			return true, nil
		},
	}

	// Initialize the internal KV.
	runtime_env.InitializeInternalKV(mockClient)

	ctx := context.Background()
	err := putExtraUsageTag(ctx, "custom-key", "custom-value", nil)
	if err != nil {
		t.Errorf("expected no error, got: %v", err)
	}

	if !putCalled {
		t.Error("expected Put to be called on the internal KV")
	}
}

// TestRecordExtraUsageTag_CaseConversion tests the TagKey-to-string conversion (lowercase).
func TestRecordExtraUsageTag_CaseConversion(t *testing.T) {
	defer runtime_env.InternalKVReset()
	defer ResetRecordedExtraUsageTags()

	mockClient := &MockGCSClient{
		PutFunc: func(ctx context.Context, ns, key string, value []byte, overwrite bool) (bool, error) {
			// The key must be lowercase.
			if key != EXTRA_USAGE_TAG_PREFIX+"_test1" {
				t.Errorf("expected the key to be lowercase, got '%s'", key)
			}
			return true, nil
		},
	}

	ctx := context.Background()
	err := RecordExtraUsageTag(ctx, proto.TagKey__TEST1, "test-value", mockClient)
	if err != nil {
		t.Errorf("expected no error, got: %v", err)
	}

	// The cached key must be lowercase too.
	tags := GetRecordedExtraUsageTags()
	if _, ok := tags["_test1"]; !ok {
		t.Errorf("expected lowercase key '_test1' in the cache, got: %v", tags)
	}
}
