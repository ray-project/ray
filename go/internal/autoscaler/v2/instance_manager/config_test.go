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

package instance_manager

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/mohae/deepcopy"
	"github.com/ray-project/ray/go/internal/autoscaler/v2/constant"
	"github.com/ray-project/ray/go/pkg/gcs"
	"github.com/ray-project/ray/go/pkg/ids"
	"github.com/ray-project/ray/go/proto"
)

// ==================== AutoscalingConfig tests ====================

func TestNewAutoscalingConfig(t *testing.T) {
	t.Run("ValidBasicConfig", func(t *testing.T) {
		configs := map[string]interface{}{
			"provider": map[string]interface{}{
				"type":       "local",
				"head_ip":    "127.0.0.1",
				"worker_ips": []interface{}{"127.0.0.2"},
			},
			"head_node_type": "local.cluster.node",
			"max_workers":    1,
		}

		asConfig, err := NewAutoscalingConfig(configs, true)
		if err != nil {
			t.Fatalf("Failed to create autoscaling config: %v", err)
		}

		if asConfig == nil {
			t.Fatal("Expected non-nil autoscaling config")
		}

		if asConfig.SyncContinuously {
			t.Error("Expected SyncContinuously to be false when skipContentHash is true")
		}

		if asConfig.Configs == nil {
			t.Error("Expected Configs to be initialized")
		}
	})

	t.Run("InvalidConfig", func(t *testing.T) {
		configs := map[string]interface{}{}

		_, err := NewAutoscalingConfig(configs, true)
		if err == nil {
			t.Fatal("Expected error for invalid config")
		}
	})
}

// ==================== FileConfigReader tests ====================

func TestNewFileConfigReader(t *testing.T) {
	t.Run("ValidConfigFile", func(t *testing.T) {
		tmpDir := t.TempDir()
		configFile := filepath.Join(tmpDir, "config.yaml")
		configContent := `
provider:
  type: local
  head_ip: 127.0.0.1
  worker_ips:
    - 127.0.0.2
head_node_type: local.cluster.node
max_workers: 1
`
		err := os.WriteFile(configFile, []byte(configContent), 0644)
		if err != nil {
			t.Fatalf("Failed to create config file: %v", err)
		}

		reader, err := NewFileConfigReader(configFile, true)
		if err != nil {
			t.Fatalf("Failed to create file config reader: %v", err)
		}

		if reader == nil {
			t.Fatal("Expected non-nil file config reader")
		}

		fcr, ok := reader.(*FileConfigReader)
		if !ok {
			t.Fatal("Expected reader to be *FileConfigReader")
		}

		if fcr.configFilePath != configFile {
			t.Errorf("Expected configFilePath to be %s, got %s", configFile, fcr.configFilePath)
		}

		if !fcr.skipContentHash {
			t.Error("Expected skipContentHash to be true")
		}

		if fcr.cachedConfig == nil {
			t.Error("Expected cachedConfig to be initialized")
		}
	})

	t.Run("NonExistentFile", func(t *testing.T) {
		_, err := NewFileConfigReader("/non/existent/file.yaml", true)
		if err == nil {
			t.Fatal("Expected error for non-existent file")
		}
	})
}

func TestFileConfigReader_GetCachedAutoscalingConfig(t *testing.T) {
	t.Run("GetInitializedConfig", func(t *testing.T) {
		tmpDir := t.TempDir()
		configFile := filepath.Join(tmpDir, "config.yaml")
		configContent := `
provider:
  type: local
  head_ip: 127.0.0.1
  worker_ips:
    - 127.0.0.2
head_node_type: local.cluster.node
max_workers: 1
`
		err := os.WriteFile(configFile, []byte(configContent), 0644)
		if err != nil {
			t.Fatalf("Failed to create config file: %v", err)
		}

		reader, err := NewFileConfigReader(configFile, true)
		if err != nil {
			t.Fatalf("Failed to create file config reader: %v", err)
		}

		fcr := reader.(*FileConfigReader)
		cachedConfig, err := fcr.GetCachedAutoscalingConfig()
		if err != nil {
			t.Fatalf("Failed to get cached config: %v", err)
		}

		if cachedConfig == nil {
			t.Fatal("Expected non-nil cached config")
		}

		if cachedConfig.Configs == nil {
			t.Error("Expected cached config to have Configs")
		}
	})

	t.Run("GetUninitializedConfig", func(t *testing.T) {
		fcr := &FileConfigReader{
			configFilePath:  "/some/path.yaml",
			skipContentHash: true,
			cachedConfig:    nil,
		}

		_, err := fcr.GetCachedAutoscalingConfig()
		if err == nil {
			t.Fatal("Expected error for uninitialized config")
		}
	})
}

// ==================== ReadOnlyProviderConfigReader tests ====================

// mockGCSClient is a mocked GCS client for tests.
type mockGCSClient struct {
	address      string
	clusterID    ids.ClusterID
	statusReply  *proto.GetClusterStatusReply
	getStatusErr error
	closeErr     error
}

func (m *mockGCSClient) GetAutoscalerStatus(ctx context.Context) (*proto.GetClusterStatusReply, error) {
	return m.statusReply, m.getStatusErr
}

func (m *mockGCSClient) Address() string {
	return m.address
}

func (m *mockGCSClient) ReportAutoscalingState(autoscalingState string) error {
	return gcs.ErrNotImplemented
}

func (m *mockGCSClient) ClusterID() ids.ClusterID {
	return m.clusterID
}

func (m *mockGCSClient) IsClosed() bool { return false }

func (m *mockGCSClient) Close() error {
	return m.closeErr
}

// InternalKVInterface methods.
func (m *mockGCSClient) Get(ctx context.Context, ns, key string) ([]byte, error) {
	return nil, gcs.ErrNotImplemented
}

func (m *mockGCSClient) MultiGet(ctx context.Context, ns string, keys []string) (map[string][]byte, error) {
	return nil, gcs.ErrNotImplemented
}

func (m *mockGCSClient) Put(ctx context.Context, ns, key string, value []byte, overwrite bool) (bool, error) {
	return false, gcs.ErrNotImplemented
}

func (m *mockGCSClient) Del(ctx context.Context, ns, key string, delByPrefix bool) (int, error) {
	return 0, gcs.ErrNotImplemented
}

func (m *mockGCSClient) Keys(ctx context.Context, ns, prefix string) ([]string, error) {
	return nil, gcs.ErrNotImplemented
}

func (m *mockGCSClient) Exists(ctx context.Context, ns, key string) (bool, error) {
	return false, gcs.ErrNotImplemented
}

// NodeInfoInterface methods.
func (m *mockGCSClient) CheckAlive(ctx context.Context, nodeIDs []ids.NodeID) ([]bool, error) {
	return nil, gcs.ErrNotImplemented
}

func (m *mockGCSClient) GetAll(ctx context.Context, nodeIDs []ids.NodeID) (map[ids.NodeID]*proto.GcsNodeInfo, error) {
	return nil, gcs.ErrNotImplemented
}

func (m *mockGCSClient) DrainNodes(ctx context.Context, nodeIDs []ids.NodeID) ([]ids.NodeID, error) {
	return nil, gcs.ErrNotImplemented
}

func (m *mockGCSClient) DrainNode(ctx context.Context, nodeID ids.NodeID, reason proto.DrainNodeReason, reasonMessage string, deadlineTimestampMs int64) (bool, string, error) {
	return false, "", gcs.ErrNotImplemented
}

func (m *mockGCSClient) GetNodeToConnect(ctx context.Context, nodeIpAddress string) (*proto.GcsNodeInfo, error) {
	return nil, gcs.ErrNotImplemented
}

// NodeResourceInterface methods.
func (m *mockGCSClient) GetAvailableResources(ctx context.Context, nodeID ids.NodeID) (*proto.AvailableResources, error) {
	return nil, gcs.ErrNotImplemented
}

func (m *mockGCSClient) GetTotalResources(ctx context.Context, nodeID ids.NodeID) (*proto.TotalResources, error) {
	return nil, gcs.ErrNotImplemented
}

// ActorInfoInterface methods.
func (m *mockGCSClient) GetActorInfo(ctx context.Context, actorID ids.ActorID) (*proto.ActorTableData, error) {
	return nil, gcs.ErrNotImplemented
}

func (m *mockGCSClient) ListActors(ctx context.Context, jobID *ids.JobID) ([]*proto.ActorTableData, error) {
	return nil, gcs.ErrNotImplemented
}

func (m *mockGCSClient) ListActorsByFilter(ctx context.Context, jobID *ids.JobID, actorStateName *gcs.ActorStateName) ([]*proto.ActorTableData, error) {
	return nil, gcs.ErrNotImplemented
}

// JobInfoInterface methods.
func (m *mockGCSClient) GetJobInfo(ctx context.Context, jobID ids.JobID) (*proto.JobTableData, error) {
	return nil, gcs.ErrNotImplemented
}

func (m *mockGCSClient) ListJobs(ctx context.Context) ([]*proto.JobTableData, error) {
	return nil, gcs.ErrNotImplemented
}

func (m *mockGCSClient) NextJobID(ctx context.Context) (ids.JobID, error) {
	return ids.NilJobID(), gcs.ErrNotImplemented
}

// WorkerInfoInterface methods.
func (m *mockGCSClient) GetWorkerInfo(ctx context.Context, workerID ids.WorkerID) (*proto.WorkerTableData, error) {
	return nil, gcs.ErrNotImplemented
}

func (m *mockGCSClient) ListWorkers(ctx context.Context) ([]*proto.WorkerTableData, error) {
	return nil, gcs.ErrNotImplemented
}

// PlacementGroupInterface methods.
func (m *mockGCSClient) GetPlacementGroup(ctx context.Context, pgID ids.PlacementGroupID) (*proto.PlacementGroupTableData, error) {
	return nil, gcs.ErrNotImplemented
}

func (m *mockGCSClient) GetPlacementGroupByName(ctx context.Context, name, namespace string) (*proto.PlacementGroupTableData, error) {
	return nil, gcs.ErrNotImplemented
}

func (m *mockGCSClient) ListPlacementGroups(ctx context.Context) ([]*proto.PlacementGroupTableData, error) {
	return nil, gcs.ErrNotImplemented
}

// PublisherInterface methods.
func (m *mockGCSClient) PublishErrors(ctx context.Context) (<-chan gcs.ErrorData, error) {
	return nil, gcs.ErrNotImplemented
}

func (m *mockGCSClient) PublishLogs(ctx context.Context) (<-chan gcs.LogData, error) {
	return nil, gcs.ErrNotImplemented
}

func TestReadOnlyProviderConfigReader_RefreshCachedAutoscalingConfig(t *testing.T) {
	t.Run("RefreshWithValidStatusReply", func(t *testing.T) {
		mockGCSClient := &mockGCSClient{
			statusReply: &proto.GetClusterStatusReply{
				ClusterResourceState: &proto.ClusterResourceState{
					NodeStates: []*proto.NodeState{
						{
							NodeId:          []byte("head-node-id"),
							RayNodeTypeName: "ray.head.default", // same as the default node type in BASE_READONLY_CONFIG
							TotalResources: map[string]float64{
								"CPU":                    2.0,
								"node:__internal_head__": 1.0,
							},
						},
						{
							NodeId:          []byte("worker-node-1"),
							RayNodeTypeName: "custom.worker.type", // a new node type
							TotalResources: map[string]float64{
								"CPU": 4.0,
							},
						},
					},
				},
			},
		}

		rop := &ReadOnlyProviderConfigReader{
			configs:   deepcopy.Copy(constant.BASE_READONLY_CONFIG).(map[string]interface{}),
			gcsClient: mockGCSClient,
		}

		err := rop.RefreshCachedAutoscalingConfig()
		if err != nil {
			t.Fatalf("Failed to refresh cached autoscaling config: %v", err)
		}

		availableNodeTypes, ok := rop.configs["available_node_types"].(map[string]interface{})
		if !ok {
			t.Fatal("Expected available_node_types to be map[string]interface{}")
		}

		// BASE_READONLY_CONFIG has 1 default node type; the ray.head.default
		// returned by GCS overwrites it and custom.worker.type is added, so
		// there are 2 in total.
		if len(availableNodeTypes) != 2 {
			t.Errorf("Expected 2 node types (1 overwritten + 1 new), got %d", len(availableNodeTypes))
		}

		headNodeType, ok := rop.configs["head_node_type"].(string)
		if !ok {
			t.Fatal("Expected head_node_type to be string")
		}
		if headNodeType != "ray.head.default" {
			t.Errorf("Expected head_node_type to be 'ray.head.default', got '%s'", headNodeType)
		}

		maxWorkers, ok := rop.configs["max_workers"].(int)
		if !ok {
			t.Fatal("Expected max_workers to be int")
		}
		// Per the code, max_workers = len(availableNodeTypes) = 2.
		if maxWorkers != 2 {
			t.Errorf("Expected max_workers to be 2, got %d", maxWorkers)
		}

		if _, exists := rop.configs["idle_timeout_minutes"]; exists {
			t.Error("Expected idle_timeout_minutes to be removed in readonly mode")
		}
	})

	t.Run("RefreshWithNilGCSClient", func(t *testing.T) {
		rop := &ReadOnlyProviderConfigReader{
			configs:   deepcopy.Copy(constant.BASE_READONLY_CONFIG).(map[string]interface{}),
			gcsClient: nil,
		}

		err := rop.RefreshCachedAutoscalingConfig()
		if err == nil {
			t.Fatal("Expected error for nil GCS client")
		}
	})

	t.Run("RefreshWithNilStatusReply", func(t *testing.T) {
		mockGCSClient := &mockGCSClient{
			statusReply: nil,
		}

		rop := &ReadOnlyProviderConfigReader{
			configs:   deepcopy.Copy(constant.BASE_READONLY_CONFIG).(map[string]interface{}),
			gcsClient: mockGCSClient,
		}

		err := rop.RefreshCachedAutoscalingConfig()
		if err == nil {
			t.Fatal("Expected error for nil status reply")
		}
	})
}

func TestReadOnlyProviderConfigReader_GetCachedAutoscalingConfig(t *testing.T) {
	t.Run("GetInitializedConfig", func(t *testing.T) {
		mockGCSClient := &mockGCSClient{}

		rop := &ReadOnlyProviderConfigReader{
			configs:   deepcopy.Copy(constant.BASE_READONLY_CONFIG).(map[string]interface{}),
			gcsClient: mockGCSClient,
		}

		asConfig, err := rop.GetCachedAutoscalingConfig()
		if err != nil {
			t.Fatalf("Failed to get cached autoscaling config: %v", err)
		}

		if asConfig == nil {
			t.Fatal("Expected non-nil autoscaling config")
		}

		if asConfig.Configs == nil {
			t.Error("Expected Configs to be populated")
		}

		if asConfig.SyncContinuously {
			t.Error("Expected SyncContinuously to be false when skipContentHash is true")
		}
	})

	t.Run("GetUninitializedConfig", func(t *testing.T) {
		rop := &ReadOnlyProviderConfigReader{
			configs: nil,
		}

		_, err := rop.GetCachedAutoscalingConfig()
		if err == nil {
			t.Fatal("Expected error for uninitialized config")
		}
	})
}

// ==================== Benchmarks ====================

func BenchmarkNewAutoscalingConfig(b *testing.B) {
	configs := map[string]interface{}{
		"provider": map[string]interface{}{
			"type": "local",
		},
		"available_node_types": map[string]interface{}{
			"head_node_type": map[string]interface{}{
				"resources": map[string]interface{}{
					"CPU": 2,
				},
				"min_workers": 0,
				"max_workers": 0,
			},
		},
		"head_node_type": "head_node_type",
		"max_workers":    1,
		"head_ip":        "127.0.0.1",
		"worker_ips":     []string{},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := NewAutoscalingConfig(configs, true)
		if err != nil {
			b.Fatalf("Failed to create autoscaling config: %v", err)
		}
	}
}

func BenchmarkReadOnlyProviderConfigReader_Refresh(b *testing.B) {
	mockGCSClient := &mockGCSClient{
		statusReply: &proto.GetClusterStatusReply{
			ClusterResourceState: &proto.ClusterResourceState{
				NodeStates: []*proto.NodeState{
					{
						NodeId:          []byte("head-node-id"),
						RayNodeTypeName: "head_node_type",
						TotalResources: map[string]float64{
							"CPU":                    2.0,
							"node:__internal_head__": 1.0,
						},
					},
					{
						NodeId:          []byte("worker-node-1"),
						RayNodeTypeName: "worker_type",
						TotalResources: map[string]float64{
							"CPU": 4.0,
						},
					},
				},
			},
		},
	}

	rop := &ReadOnlyProviderConfigReader{
		configs:   deepcopy.Copy(constant.BASE_READONLY_CONFIG).(map[string]interface{}),
		gcsClient: mockGCSClient,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := rop.RefreshCachedAutoscalingConfig()
		if err != nil {
			b.Fatalf("Failed to refresh cached autoscaling config: %v", err)
		}
	}
}
