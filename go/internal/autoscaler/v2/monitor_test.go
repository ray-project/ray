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

package v2

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"testing"

	"github.com/ray-project/ray/go/internal/autoscaler"
	"github.com/ray-project/ray/go/internal/autoscaler/v2/instance_manager"
	"github.com/ray-project/ray/go/internal/common/usage"
	"github.com/ray-project/ray/go/pkg/gcs"
	"github.com/ray-project/ray/go/pkg/ids"
	"github.com/ray-project/ray/go/proto"
	"github.com/stretchr/testify/assert"
	protolib "google.golang.org/protobuf/proto"
)

// mockGcsClient is the mock GCS client used by the tests.
// Note: this mock depends on no cgo code at all; it is implemented in pure Go.
type mockGcsClient struct {
	addr                    string
	clusterID               ids.ClusterID
	getFunc                 func(ctx context.Context, ns, key string) ([]byte, error)
	putFunc                 func(ctx context.Context, ns, key string, value []byte, overwrite bool) (bool, error)
	getAutoscalerStatusFunc func(ctx context.Context) (*proto.GetClusterStatusReply, error)
	reportFunc              func(autoscalingState string) error
}

func (m *mockGcsClient) Address() string          { return m.addr }
func (m *mockGcsClient) ClusterID() ids.ClusterID { return m.clusterID }
func (m *mockGcsClient) Close() error             { return nil }
func (m *mockGcsClient) ReportAutoscalingState(autoscalingState string) error {
	if m.reportFunc != nil {
		return m.reportFunc(autoscalingState)
	}
	return gcs.ErrNotImplemented
}
func (m *mockGcsClient) IsClosed() bool { return false }
func (m *mockGcsClient) Get(ctx context.Context, ns, key string) ([]byte, error) {
	if m.getFunc != nil {
		return m.getFunc(ctx, ns, key)
	}
	return nil, gcs.ErrNotImplemented
}
func (m *mockGcsClient) MultiGet(ctx context.Context, ns string, keys []string) (map[string][]byte, error) {
	return nil, gcs.ErrNotImplemented
}
func (m *mockGcsClient) Put(ctx context.Context, ns, key string, value []byte, overwrite bool) (bool, error) {
	if m.putFunc != nil {
		return m.putFunc(ctx, ns, key, value, overwrite)
	}
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

func (m *mockGcsClient) DrainNode(ctx context.Context, nodeID ids.NodeID, reason proto.DrainNodeReason, reasonMessage string, deadlineTimestampMs int64) (bool, string, error) {
	return false, "", gcs.ErrNotImplemented
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
func (m *mockGcsClient) ListActorsByFilter(ctx context.Context, jobID *ids.JobID, actorStateName *gcs.ActorStateName) ([]*proto.ActorTableData, error) {
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
func (m *mockGcsClient) GetPlacementGroupByName(ctx context.Context, name, namespace string) (*proto.PlacementGroupTableData, error) {
	return nil, gcs.ErrNotImplemented
}
func (m *mockGcsClient) ListPlacementGroups(ctx context.Context) ([]*proto.PlacementGroupTableData, error) {
	return nil, gcs.ErrNotImplemented
}
func (m *mockGcsClient) PublishErrors(ctx context.Context) (<-chan gcs.ErrorData, error) {
	return nil, gcs.ErrNotImplemented
}
func (m *mockGcsClient) PublishLogs(ctx context.Context) (<-chan gcs.LogData, error) {
	return nil, gcs.ErrNotImplemented
}
func (m *mockGcsClient) GetAutoscalerStatus(ctx context.Context) (*proto.GetClusterStatusReply, error) {
	if m.getAutoscalerStatusFunc != nil {
		return m.getAutoscalerStatusFunc(ctx)
	}
	return nil, gcs.ErrNotImplemented
}

// TestCreateConfigReader_NoAutoscalingConfig tests the case without a config file.
func TestCreateConfigReader_NoAutoscalingConfig(t *testing.T) {
	config := &MonitorV2Config{
		GcsAddress: "127.0.0.1:6379",
	}

	mockClient := &mockGcsClient{
		addr:      "mock:1234",
		clusterID: ids.NewClusterID(),
	}
	gcs.SetClient(mockClient)
	defer gcs.ClearClient()

	// Create the config reader with the mock GCS client.
	configReader, err := CreateConfigReader(config)

	// Verify the read-only provider config reader was created successfully.
	assert.NoError(t, err)
	assert.NotNil(t, configReader)

	// Verify the config reader is a ReadOnlyProviderConfigReader.
	_, ok := configReader.(*instance_manager.ReadOnlyProviderConfigReader)
	assert.True(t, ok, "Expected ReadOnlyProviderConfigReader type")

	// Verify the cached config is obtainable (even if it is the base config).
	cachedConfig, err := configReader.GetCachedAutoscalingConfig()
	assert.NoError(t, err)
	assert.NotNil(t, cachedConfig)

	// Verify the key fields of the base config.
	assert.Equal(t, "default", cachedConfig.Configs["cluster_name"])
	assert.Equal(t, 0, int(cachedConfig.Configs["max_workers"].(int)))
	assert.Equal(t, "readonly", cachedConfig.Configs["provider"].(map[string]interface{})["type"])
}

// TestCreateConfigReader_WithAutoscalingConfig tests the case with a config file.
func TestCreateConfigReader_WithAutoscalingConfig(t *testing.T) {
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "autoscaling.yaml")

	// Create a valid YAML config file (using the local provider manual mode).
	configContent := `
cluster_name: test-cluster
max_workers: 2
upscaling_speed: 1.0
provider:
  type: local
  head_ip: "127.0.0.1"
  worker_ips:
    - "127.0.0.2"
    - "127.0.0.3"
auth:
  ssh_user: ubuntu
file_mounts: {}
cluster_synced_files: []
initialization_commands: []
setup_commands: []
head_setup_commands: []
worker_setup_commands: []
head_start_ray_commands: []
worker_start_ray_commands: []
`
	err := os.WriteFile(configPath, []byte(configContent), 0644)
	assert.NoError(t, err)

	config := &MonitorV2Config{
		GcsAddress:        "127.0.0.1:6379",
		AutoscalingConfig: configPath,
	}

	configReader, err := CreateConfigReader(config)
	if err != nil {
		t.Fatalf("CreateConfigReader failed: %v", err)
	}
	assert.NotNil(t, configReader)

	// Verify the config reader can fetch the cached config correctly.
	cachedConfig, err := configReader.GetCachedAutoscalingConfig()
	if err != nil {
		t.Fatalf("GetCachedAutoscalingConfig failed: %v", err)
	}
	assert.NotNil(t, cachedConfig)
	assert.Equal(t, "test-cluster", cachedConfig.Configs["cluster_name"])
	assert.Equal(t, 2, int(cachedConfig.Configs["max_workers"].(int)))
	// Verify the local-mode config was converted correctly.
	assert.NotNil(t, cachedConfig.Configs["available_node_types"])
	assert.Equal(t, "local.cluster.node", cachedConfig.Configs["head_node_type"])
}

// TestCreateConfigReader_InvalidAutoscalingConfig tests an invalid config file.
func TestCreateConfigReader_InvalidAutoscalingConfig(t *testing.T) {
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "invalid.yaml")

	// Create an invalid YAML config file (missing required fields).
	configContent := `
invalid_config: true
`
	err := os.WriteFile(configPath, []byte(configContent), 0644)
	assert.NoError(t, err)

	config := &MonitorV2Config{
		GcsAddress:        "127.0.0.1:6379",
		AutoscalingConfig: configPath,
	}

	configReader, err := CreateConfigReader(config)
	assert.Error(t, err)
	assert.Nil(t, configReader)
	assert.Contains(t, err.Error(), "failed to create autoscaling config")
}

// TestCreateConfigReader_NonExistentConfigFile tests a non-existent config file.
func TestCreateConfigReader_NonExistentConfigFile(t *testing.T) {
	config := &MonitorV2Config{
		GcsAddress:        "127.0.0.1:6379",
		AutoscalingConfig: "/non/existent/path/config.yaml",
	}

	configReader, err := CreateConfigReader(config)
	assert.Error(t, err)
	assert.Nil(t, configReader)
	assert.Contains(t, err.Error(), "no such file or directory")
}

// TestCreateConfigReader_ExpandUserPath tests the expansion of a path containing ~.
func TestCreateConfigReader_ExpandUserPath(t *testing.T) {
	// Fetch the user home directory ($HOME may be undefined in the Bazel test env).
	homeDir, err := os.UserHomeDir()
	if err != nil {
		// Skip this test when the home directory is unavailable.
		t.Skipf("Skipping test: %v", err)
		return
	}

	tmpSubdir := filepath.Join(homeDir, ".ray_test_autoscaling")
	err = os.MkdirAll(tmpSubdir, 0755)
	assert.NoError(t, err)
	defer os.RemoveAll(tmpSubdir)

	configPath := filepath.Join(tmpSubdir, "autoscaling.yaml")

	// Create a valid YAML config file (using the local provider manual mode).
	configContent := `
cluster_name: test-cluster-home
max_workers: 2
upscaling_speed: 1.0
provider:
  type: local
  head_ip: "127.0.0.1"
  worker_ips:
    - "127.0.0.2"
    - "127.0.0.3"
auth:
  ssh_user: ubuntu
file_mounts: {}
cluster_synced_files: []
initialization_commands: []
setup_commands: []
head_setup_commands: []
worker_setup_commands: []
head_start_ray_commands: []
worker_start_ray_commands: []
`
	err = os.WriteFile(configPath, []byte(configContent), 0644)
	assert.NoError(t, err)

	// Use a path with ~.
	relativePath := "~/.ray_test_autoscaling/autoscaling.yaml"
	config := &MonitorV2Config{
		GcsAddress:        "127.0.0.1:6379",
		AutoscalingConfig: relativePath,
	}

	configReader, err := CreateConfigReader(config)
	assert.NoError(t, err)
	assert.NotNil(t, configReader)

	// Verify the config reader can fetch the cached config correctly.
	cachedConfig, err := configReader.GetCachedAutoscalingConfig()
	assert.NoError(t, err)
	assert.NotNil(t, cachedConfig)
	assert.Equal(t, "test-cluster-home", cachedConfig.Configs["cluster_name"])
}

// TestCreateConfigReader_EmptyConfig tests an empty config object.
func TestCreateConfigReader_EmptyConfig(t *testing.T) {
	config := &MonitorV2Config{}

	configReader, err := CreateConfigReader(config)

	// Without a GCS address and a config file this must return an error.
	assert.Error(t, err)
	assert.Nil(t, configReader)
}

// TestCreateConfigReader_MinimalValidConfig tests a minimal valid config.
func TestCreateConfigReader_MinimalValidConfig(t *testing.T) {
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "minimal.yaml")

	// Create a minimal valid YAML config file (using the local provider manual mode).
	configContent := `
cluster_name: minimal-cluster
max_workers: 1
upscaling_speed: 1.0
provider:
  type: local
  head_ip: "127.0.0.1"
  worker_ips:
    - "127.0.0.2"
auth:
  ssh_user: ubuntu
file_mounts: {}
cluster_synced_files: []
initialization_commands: []
setup_commands: []
head_setup_commands: []
worker_setup_commands: []
head_start_ray_commands: []
worker_start_ray_commands: []
`
	err := os.WriteFile(configPath, []byte(configContent), 0644)
	assert.NoError(t, err)

	config := &MonitorV2Config{
		GcsAddress:        "127.0.0.1:6379",
		AutoscalingConfig: configPath,
	}

	configReader, err := CreateConfigReader(config)
	assert.NoError(t, err)
	assert.NotNil(t, configReader)
}

// TestCreateConfigReader_ConfigWithFileMounts tests a config containing file mounts.
func TestCreateConfigReader_ConfigWithFileMounts(t *testing.T) {
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "with_mounts.yaml")

	// Create the source file used by the mount.
	sourceFile := filepath.Join(tmpDir, "source.txt")
	err := os.WriteFile(sourceFile, []byte("test content"), 0644)
	assert.NoError(t, err)

	// Create a YAML config containing file mounts (using the local provider
	// manual mode).
	configContent := `
cluster_name: mount-test-cluster
max_workers: 1
upscaling_speed: 1.0
provider:
  type: local
  head_ip: "127.0.0.1"
  worker_ips:
    - "127.0.0.2"
auth:
  ssh_user: ubuntu
file_mounts:
  /tmp/remote_file.txt: ` + sourceFile + `
cluster_synced_files: []
initialization_commands: []
setup_commands: []
head_setup_commands: []
worker_setup_commands: []
head_start_ray_commands: []
worker_start_ray_commands: []
`
	err = os.WriteFile(configPath, []byte(configContent), 0644)
	assert.NoError(t, err)

	config := &MonitorV2Config{
		GcsAddress:        "127.0.0.1:6379",
		AutoscalingConfig: configPath,
	}

	configReader, err := CreateConfigReader(config)
	assert.NoError(t, err)
	assert.NotNil(t, configReader)
}

// TestCreateConfigReader_ConfigWithCommands tests a config containing commands.
func TestCreateConfigReader_ConfigWithCommands(t *testing.T) {
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "with_commands.yaml")

	// Create a YAML config with the various command fields (using the local
	// provider manual mode).
	configContent := `
cluster_name: command-test-cluster
max_workers: 2
upscaling_speed: 1.0
provider:
  type: local
  head_ip: "127.0.0.1"
  worker_ips:
    - "127.0.0.2"
    - "127.0.0.3"
auth:
  ssh_user: ubuntu
file_mounts: {}
cluster_synced_files: []
initialization_commands:
  - echo "Initializing cluster"
setup_commands:
  - pip install ray
head_setup_commands:
  - echo "Head node setup"
worker_setup_commands:
  - echo "Worker node setup"
head_start_ray_commands:
  - ray start --head
worker_start_ray_commands:
  - ray start --address=$RAY_HEAD_IP:6379
`
	err := os.WriteFile(configPath, []byte(configContent), 0644)
	assert.NoError(t, err)

	config := &MonitorV2Config{
		GcsAddress:        "127.0.0.1:6379",
		AutoscalingConfig: configPath,
	}

	configReader, err := CreateConfigReader(config)
	assert.NoError(t, err)
	assert.NotNil(t, configReader)
}

// BenchmarkCreateConfigReader benchmarks the config reader creation performance.
func BenchmarkCreateConfigReader(b *testing.B) {
	tmpDir := b.TempDir()
	configPath := filepath.Join(tmpDir, "benchmark.yaml")

	// Create a standard YAML config file.
	configContent := `
cluster_name: benchmark-cluster
max_workers: 10
upscaling_speed: 1.0
available_node_types:
  ray.head.default:
    resources:
      CPU: 4
      memory: 16000
    max_workers: 0
  ray.worker.default:
    resources:
      CPU: 2
      memory: 8000
    max_workers: 10
head_node_type: ray.head.default
provider:
  type: local
auth:
  ssh_user: ubuntu
file_mounts: {}
cluster_synced_files: []
initialization_commands: []
setup_commands: []
head_setup_commands: []
worker_setup_commands: []
head_start_ray_commands: []
worker_start_ray_commands: []
`
	err := os.WriteFile(configPath, []byte(configContent), 0644)
	assert.NoError(b, err)

	config := &MonitorV2Config{
		GcsAddress:        "127.0.0.1:6379",
		AutoscalingConfig: configPath,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = CreateConfigReader(config)
	}
}

// TestCreateConfigReader_NoAutoscalingConfig_GcsError tests the case without a
// config file where the GCS connection fails.
func TestCreateConfigReader_NoAutoscalingConfig_GcsError(t *testing.T) {
	config := &MonitorV2Config{
		GcsAddress: "invalid-address",
	}

	// Note: this test tries to connect to an invalid GCS address and must return
	// a connection error.
	configReader, err := CreateConfigReader(config)

	// Verify the error.
	assert.Error(t, err)
	assert.Nil(t, configReader)
	assert.Contains(t, err.Error(), "failed to connect to GCS")
}

// TestCreateConfigReader_ReadOnlyProviderRefresh tests the refresh of the
// read-only provider.
func TestCreateConfigReader_ReadOnlyProviderRefresh(t *testing.T) {
	config := &MonitorV2Config{
		GcsAddress: "127.0.0.1:6379",
	}

	mockClient := &mockGcsClient{
		addr:      "mock:1234",
		clusterID: ids.NewClusterID(),
	}
	gcs.SetClient(mockClient)
	defer gcs.ClearClient()

	configReader, err := CreateConfigReader(config)
	assert.NoError(t, err)
	assert.NotNil(t, configReader)

	// Test refreshing the cached config (the mock client returns an error).
	err = configReader.RefreshCachedAutoscalingConfig()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not implemented")
}

// TestRecordAutoscalerV2Usage tests recording the v2 usage tag.
func TestRecordAutoscalerV2Usage(t *testing.T) {
	mockClient := &mockGcsClient{
		addr:      "mock:1234",
		clusterID: ids.NewClusterID(),
	}
	gcs.SetClient(mockClient)
	defer gcs.ClearClient()

	// This function must not panic or return an error.
	// Because the mock client's Put returns ErrNotImplemented, an error is only
	// logged.
	RecordAutoscalerV2Usage(mockClient)
}

// TestRecordAutoscalerV2Usage_Error tests the error handling while recording the
// v2 usage tag.
func TestRecordAutoscalerV2Usage_Error(t *testing.T) {
	// Create a mock GCS client whose Put returns an error.
	mockClient := &mockGcsClient{
		addr:      "mock:1234",
		clusterID: ids.NewClusterID(),
	}
	gcs.SetClient(mockClient)
	defer gcs.ClearClient()

	// This function must not panic; it handles failures gracefully even when the
	// underlying call fails. Because the mock client's Put returns
	// ErrNotImplemented, the error is logged but nothing panics.
	assert.NotPanics(t, func() {
		RecordAutoscalerV2Usage(mockClient)
	})
}

// TestRecordAutoscalerV2Usage_WithSuccessfulPut tests recording the v2 usage tag
// when Put succeeds.
func TestRecordAutoscalerV2Usage_WithSuccessfulPut(t *testing.T) {
	// Reset the in-memory cache so every test starts from a clean state.
	usage.ResetRecordedExtraUsageTags()

	putCalled := false
	mockClient := &mockGcsClient{
		addr:      "mock:1234",
		clusterID: ids.NewClusterID(),
		putFunc: func(ctx context.Context, ns, key string, value []byte, overwrite bool) (bool, error) {
			putCalled = true
			// Verify the arguments.
			assert.Equal(t, usage.USAGE_STATS_NAMESPACE, ns)
			assert.Equal(t, "extra_usage_tag_autoscaler_version", key)
			assert.True(t, overwrite)
			return true, nil
		},
	}
	gcs.SetClient(mockClient)
	defer gcs.ClearClient()

	// Call the function.
	RecordAutoscalerV2Usage(mockClient)

	// Verify Put was called.
	assert.True(t, putCalled, "Put method should have been called")
}

// TestGetSessionName tests fetching the session name from GCS.
func TestGetSessionName(t *testing.T) {
	t.Run("fetch the session name successfully", func(t *testing.T) {
		expectedSessionName := "test-session-123"

		mockClient := &mockGcsClient{
			addr:      "mock:1234",
			clusterID: ids.NewClusterID(),
			getFunc: func(ctx context.Context, ns, key string) ([]byte, error) {
				// The session name must be read from the "session" namespace
				// (corresponding to the Python KV_NAMESPACE_SESSION).
				assert.Equal(t, "session", ns)
				if key == "session_name" {
					return []byte(expectedSessionName), nil
				}
				return nil, gcs.ErrNotImplemented
			},
		}
		gcs.SetClient(mockClient)
		defer gcs.ClearClient()

		sessionName, err := getSessionName(mockClient)
		assert.NoError(t, err)
		assert.Equal(t, expectedSessionName, sessionName)
	})

	t.Run("GCS returns an empty value", func(t *testing.T) {
		mockClient := &mockGcsClient{
			addr:      "mock:1234",
			clusterID: ids.NewClusterID(),
			getFunc: func(ctx context.Context, ns, key string) ([]byte, error) {
				return nil, gcs.ErrKeyNotFound
			},
		}
		gcs.SetClient(mockClient)
		defer gcs.ClearClient()

		// Match Python: a missing session_name yields an empty value and the
		// monitor keeps running without an error.
		sessionName, err := getSessionName(mockClient)
		assert.NoError(t, err)
		assert.Empty(t, sessionName)
	})

	t.Run("GCS connection error", func(t *testing.T) {
		mockClient := &mockGcsClient{
			addr:      "mock:1234",
			clusterID: ids.NewClusterID(),
		}
		gcs.SetClient(mockClient)
		defer gcs.ClearClient()

		sessionName, err := getSessionName(mockClient)
		assert.Error(t, err)
		assert.Empty(t, sessionName)
	})
}

// TestNewAutoscalerMonitor_EventLoggerFailure keeps the monitor usable when the
// event logger cannot start: the constructor must skip the wrapping entirely
// (leaving eventLogger nil so the scheduler's nil-guard stays effective) rather
// than building a non-nil adapter around a nil logger, which would nil-deref on
// the first scheduling update.
// The failure is forced by placing a regular file where the events directory
// would live, so creating it fails. This test must stay ahead of
// TestNewAutoscalerMonitor in this file: the event logger registry is
// process-global, and once another test registers the AUTOSCALER source the
// forced failure can no longer be observed.
func TestNewAutoscalerMonitor_EventLoggerFailure(t *testing.T) {
	tmpDir := t.TempDir()
	eventsPath := filepath.Join(tmpDir, "events")
	assert.NoError(t, os.WriteFile(eventsPath, []byte("not a directory"), 0644))

	mockClient := &mockGcsClient{
		addr:      "127.0.0.1:6379",
		clusterID: ids.NewClusterID(),
	}
	mockClient.getFunc = func(ctx context.Context, ns, key string) ([]byte, error) {
		if key == "session_name" {
			return []byte("event-fail-session"), nil
		}
		return nil, gcs.ErrNotImplemented
	}
	// GetAutoscalerStatus returns a head-node cluster state so the reconciler
	// completes a full round through the scheduler with the nil event logger.
	mockClient.getAutoscalerStatusFunc = func(ctx context.Context) (*proto.GetClusterStatusReply, error) {
		return &proto.GetClusterStatusReply{
			ClusterResourceState: &proto.ClusterResourceState{
				ClusterResourceStateVersion: 1,
				NodeStates: []*proto.NodeState{
					{
						NodeId: []byte("head-node"),
						Status: proto.NodeStatus_RUNNING,
						TotalResources: map[string]float64{
							"node:__internal_head__": 1,
						},
					},
				},
			},
		}, nil
	}
	gcs.SetClient(mockClient)
	defer gcs.ClearClient()

	configReader, err := CreateConfigReader(&MonitorV2Config{
		GcsAddress: "127.0.0.1:6379",
	})
	assert.NoError(t, err)

	monitor, err := NewAutoscalerMonitor("127.0.0.1:6379", configReader, tmpDir, "")
	assert.NoError(t, err)
	if assert.NotNil(t, monitor) {
		assert.Nil(t, monitor.eventLogger)
	}

	// The downstream nil-guard path: one reconcile round with the nil event
	// logger must complete without a panic.
	autoscalingState, err := monitor.autoscaler.UpdateAutoscalingState()
	assert.NoError(t, err)
	if assert.NotNil(t, autoscalingState) {
		assert.Equal(t, int64(1), autoscalingState.LastSeenClusterResourceStateVersion)
	}
}

// TestNewAutoscalerMonitor tests creating an AutoscalerMonitor instance.
func TestNewAutoscalerMonitor(t *testing.T) {
	t.Run("create the monitor successfully (no config file)", func(t *testing.T) {
		mockClient := &mockGcsClient{
			addr:      "mock:1234",
			clusterID: ids.NewClusterID(),
		}
		// Override Get so it returns the session name.
		mockClient.getFunc = func(ctx context.Context, ns, key string) ([]byte, error) {
			if key == "session_name" {
				return []byte("test-session"), nil
			}
			return nil, gcs.ErrNotImplemented
		}
		gcs.SetClient(mockClient)
		defer gcs.ClearClient()

		configReader, err := CreateConfigReader(&MonitorV2Config{
			GcsAddress: "mock:1234",
		})
		assert.NoError(t, err)

		monitor, err := NewAutoscalerMonitor("mock:1234", configReader, "", "")

		fmt.Sprintf("aa %s", monitor)
		assert.NoError(t, err)
		assert.NotNil(t, monitor)
		assert.Equal(t, "mock:1234", monitor.gcsAddress)
		assert.NotNil(t, monitor.gcsClient)
		assert.NotNil(t, monitor.configReader)
		assert.Equal(t, "", monitor.logDir)
		assert.Equal(t, "", monitor.monitorIP)
		assert.Equal(t, "test-session", monitor.sessionName)
		assert.Nil(t, monitor.eventLogger) // No logDir, so the eventLogger must be nil.
		assert.NotNil(t, monitor.metricReporter)
		assert.NotNil(t, monitor.autoscaler)
	})

	t.Run("create the monitor successfully (with a config file)", func(t *testing.T) {
		// Reset DefaultServeMux at the start to avoid route conflicts with other tests.
		http.DefaultServeMux = http.NewServeMux()

		tmpDir := t.TempDir()
		configPath := filepath.Join(tmpDir, "autoscaling.yaml")

		configContent := `
cluster_name: test-cluster
max_workers: 2
upscaling_speed: 1.0
provider:
  type: readonly
type: readonly
auth:
ssh_user: ubuntu
file_mounts: {}
cluster_synced_files: []
initialization_commands: []
setup_commands: []
head_setup_commands: []
worker_setup_commands: []
head_start_ray_commands: []
worker_start_ray_commands: []
`
		err := os.WriteFile(configPath, []byte(configContent), 0644)
		assert.NoError(t, err)

		mockClient := &mockGcsClient{
			addr:      "mock:1234",
			clusterID: ids.NewClusterID(),
		}
		mockClient.getFunc = func(ctx context.Context, ns, key string) ([]byte, error) {
			if key == "session_name" {
				return []byte("test-session-file"), nil
			}
			return nil, gcs.ErrNotImplemented
		}

		mockClient.putFunc = func(ctx context.Context, ns, key string, value []byte, overwrite bool) (bool, error) {
			return true, nil
		}

		gcs.SetClient(mockClient)
		defer gcs.ClearClient()

		configReader, err := CreateConfigReader(&MonitorV2Config{
			GcsAddress:        "mock:1234",
			AutoscalingConfig: configPath,
		})
		assert.NoError(t, err)

		monitor, err := NewAutoscalerMonitor("mock:1234", configReader, tmpDir, "127.0.0.1")
		assert.NoError(t, err)
		assert.NotNil(t, monitor)
		assert.Equal(t, tmpDir, monitor.logDir)
		assert.Equal(t, "127.0.0.1", monitor.monitorIP)
		assert.NotNil(t, monitor.eventLogger)
	})

	t.Run("create the monitor even when session_name is missing", func(t *testing.T) {
		// Match Python: a missing session_name in GCS must not block the startup
		// (the metrics merely lack the session label).
		mockClient := &mockGcsClient{
			addr:      "mock:1234",
			clusterID: ids.NewClusterID(),
			getFunc: func(ctx context.Context, ns, key string) ([]byte, error) {
				return nil, gcs.ErrKeyNotFound
			},
		}
		gcs.SetClient(mockClient)
		defer gcs.ClearClient()

		configReader, err := CreateConfigReader(&MonitorV2Config{
			GcsAddress: "mock:1234",
		})
		assert.NoError(t, err)

		monitor, err := NewAutoscalerMonitor("mock:1234", configReader, "", "")
		assert.NoError(t, err)
		assert.NotNil(t, monitor)
		assert.Empty(t, monitor.sessionName)
	})

	t.Run("GCS connection failure", func(t *testing.T) {
		// Clear any existing GCS client.
		gcs.ClearClient()

		configReader, err := CreateConfigReader(&MonitorV2Config{
			GcsAddress: "invalid-address",
		})
		// Continue the test even when the config reader cannot be created.
		if err != nil {
			// Skip this test when the config reader creation failed.
			t.Skipf("Skipping test due to config reader creation failure: %v", err)
		}

		monitor, err := NewAutoscalerMonitor("invalid-address", configReader, "", "")
		assert.Error(t, err)
		assert.Nil(t, monitor)
		assert.Contains(t, err.Error(), "failed to connect to GCS")
	})

	t.Run("invalid GCS address format", func(t *testing.T) {
		gcs.ClearClient()
		mockClient := &mockGcsClient{
			addr:      "mock:1234",
			clusterID: ids.NewClusterID(),
			getFunc: func(ctx context.Context, ns, key string) ([]byte, error) {
				if key == "session_name" {
					return []byte("test-session-file"), nil
				}
				return nil, gcs.ErrNotImplemented
			},
		}
		gcs.SetClient(mockClient)
		defer gcs.ClearClient()

		configReader, err := CreateConfigReader(&MonitorV2Config{
			GcsAddress: "mock:1234",
		})
		assert.NoError(t, err)

		// Use an invalid GCS address format (missing port).
		monitor, err := NewAutoscalerMonitor("invalid", configReader, "", "")
		assert.Error(t, err)
		assert.Nil(t, monitor)
		assert.Contains(t, err.Error(), "invalid GCS address")
	})
}

// TestCreateHTTPServer tests creating the HTTP server.
func TestCreateHTTPServer(t *testing.T) {
	t.Run("create the HTTP server successfully", func(t *testing.T) {
		// Reset DefaultServeMux at the start to avoid route conflicts with other tests.
		http.DefaultServeMux = http.NewServeMux()

		mockClient := &mockGcsClient{
			addr:      "mock:1234",
			clusterID: ids.NewClusterID(),
		}
		mockClient.getFunc = func(ctx context.Context, ns, key string) ([]byte, error) {
			if key == "session_name" {
				return []byte("test-session"), nil
			}
			return nil, gcs.ErrNotImplemented
		}
		gcs.SetClient(mockClient)
		defer gcs.ClearClient()

		configReader, err := CreateConfigReader(&MonitorV2Config{
			GcsAddress: "mock:1234",
		})
		assert.NoError(t, err)

		monitor, err := NewAutoscalerMonitor("mock:1234", configReader, "", "")
		assert.NoError(t, err)

		// Create a temporary prometheus registry for the test.
		promMetrics, err := autoscaler.NewAutoscalerPrometheusMetrics("test-session")
		assert.NoError(t, err)

		server, err := monitor.createHTTPServer(8080, promMetrics.Registry, "127.0.0.1")
		assert.NoError(t, err)
		assert.NotNil(t, server)
		assert.Equal(t, "127.0.0.1:8080", server.Addr)
		assert.NotNil(t, monitor.metricsServer)
	})

	t.Run("nil registry", func(t *testing.T) {
		// Reset DefaultServeMux at the start to avoid route conflicts with other tests.
		http.DefaultServeMux = http.NewServeMux()

		mockClient := &mockGcsClient{
			addr:      "mock:1234",
			clusterID: ids.NewClusterID(),
		}
		mockClient.getFunc = func(ctx context.Context, ns, key string) ([]byte, error) {
			if key == "session_name" {
				return []byte("test-session"), nil
			}
			return nil, gcs.ErrNotImplemented
		}
		gcs.SetClient(mockClient)
		defer gcs.ClearClient()

		configReader, err := CreateConfigReader(&MonitorV2Config{
			GcsAddress: "mock:1234",
		})
		assert.NoError(t, err)

		monitor, err := NewAutoscalerMonitor("mock:1234", configReader, "", "")
		assert.NoError(t, err)

		server, err := monitor.createHTTPServer(8080, nil, "127.0.0.1")
		assert.Error(t, err)
		assert.Nil(t, server)
		assert.Contains(t, err.Error(), "registry is nil")
	})

	t.Run("invalid port number", func(t *testing.T) {
		mockClient := &mockGcsClient{
			addr:      "mock:1234",
			clusterID: ids.NewClusterID(),
		}
		mockClient.getFunc = func(ctx context.Context, ns, key string) ([]byte, error) {
			if key == "session_name" {
				return []byte("test-session"), nil
			}
			return nil, gcs.ErrNotImplemented
		}
		gcs.SetClient(mockClient)
		defer gcs.ClearClient()

		configReader, err := CreateConfigReader(&MonitorV2Config{
			GcsAddress: "mock:1234",
		})
		assert.NoError(t, err)

		monitor, err := NewAutoscalerMonitor("mock:1234", configReader, "", "")
		assert.NoError(t, err)

		promMetrics, err := autoscaler.NewAutoscalerPrometheusMetrics("test-session")
		assert.NoError(t, err)

		// Use an invalid port number (negative).
		server, err := monitor.createHTTPServer(-1, promMetrics.Registry, "127.0.0.1")
		assert.Error(t, err)
		assert.Nil(t, server)
	})
}

// TestAutoscalerMonitor_Run tests the Run method.
func TestAutoscalerMonitor_Run(t *testing.T) {
	mockClient := &mockGcsClient{
		addr:      "mock:1234",
		clusterID: ids.NewClusterID(),
	}
	mockClient.getFunc = func(ctx context.Context, ns, key string) ([]byte, error) {
		if key == "session_name" {
			return []byte("test-session"), nil
		}
		return nil, gcs.ErrNotImplemented
	}
	gcs.SetClient(mockClient)
	defer gcs.ClearClient()

	configReader, err := CreateConfigReader(&MonitorV2Config{
		GcsAddress: "mock:1234",
	})
	assert.NoError(t, err)

	monitor, err := NewAutoscalerMonitor("mock:1234", configReader, "", "")
	assert.NoError(t, err)

	// run is an infinite loop; a pre-canceled ctx makes it exit right after one round.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// With the reconciler wired, GetAutoscalerStatus returns ErrNotImplemented by
	// default, UpdateAutoscalingState swallows the error and returns (nil, nil),
	// and the loop continues until ctx is canceled and returns nil.
	err = monitor.run(ctx)
	assert.NoError(t, err)

	// Run uses constant.Ctx internally (never canceled) and would block; here only
	// the normal path of one loop round is verified.
}

// TestAutoscalerMonitor_Fields tests the AutoscalerMonitor field access.
func TestAutoscalerMonitor_Fields(t *testing.T) {
	// Reset DefaultServeMux at the start to avoid route conflicts with other tests.
	http.DefaultServeMux = http.NewServeMux()

	mockClient := &mockGcsClient{
		addr:      "mock:1234",
		clusterID: ids.NewClusterID(),
	}
	mockClient.getFunc = func(ctx context.Context, ns, key string) ([]byte, error) {
		if key == "session_name" {
			return []byte("unique-session-123"), nil
		}
		return nil, gcs.ErrNotImplemented
	}

	mockClient.putFunc = func(ctx context.Context, ns, key string, value []byte, overwrite bool) (bool, error) {
		return true, nil
	}

	gcs.SetClient(mockClient)
	defer gcs.ClearClient()

	configReader, err := CreateConfigReader(&MonitorV2Config{
		GcsAddress: "mock:1234",
	})
	assert.NoError(t, err)

	monitor, err := NewAutoscalerMonitor("mock:1234", configReader, "/tmp/logs", "192.168.1.100")
	assert.NoError(t, err)

	// Verify every field was set correctly.
	assert.Equal(t, "mock:1234", monitor.gcsAddress)
	assert.NotNil(t, monitor.gcsClient)
	assert.NotNil(t, monitor.configReader)
	assert.Equal(t, "/tmp/logs", monitor.logDir)
	assert.Equal(t, "192.168.1.100", monitor.monitorIP)
	assert.Equal(t, "unique-session-123", monitor.sessionName)
	assert.NotNil(t, monitor.eventLogger)
	assert.NotNil(t, monitor.metricReporter)
	assert.NotNil(t, monitor.autoscaler)
}

// TestCustomPanicHook tests the panic capture.
func TestCustomPanicHook(t *testing.T) {
	t.Run("no panic", func(t *testing.T) {
		// When r is nil nothing must happen.
		assert.NotPanics(t, func() {
			CustomPanicHook(nil)
		})
	})

	t.Run("capture a panic", func(t *testing.T) {
		// Simulate a panic value.
		panicValue := "test panic message"

		// Verify it does not crash the program.
		assert.NotPanics(t, func() {
			CustomPanicHook(panicValue)
		})
	})

	t.Run("capture an error-typed panic", func(t *testing.T) {
		// Simulate an error-typed panic value.
		panicValue := fmt.Errorf("test error panic")

		assert.NotPanics(t, func() {
			CustomPanicHook(panicValue)
		})
	})
}

// TestCustomErrorHook tests the error capture.
func TestCustomErrorHook(t *testing.T) {
	t.Run("no error", func(t *testing.T) {
		// When err is nil nothing must happen.
		assert.NotPanics(t, func() {
			CustomErrorHook(nil)
		})
	})

	t.Run("capture a plain error", func(t *testing.T) {
		// Simulate a plain error.
		errValue := fmt.Errorf("test error message")

		assert.NotPanics(t, func() {
			CustomErrorHook(errValue)
		})
	})

	t.Run("capture a wrapped error", func(t *testing.T) {
		// Simulate a wrapped error.
		baseErr := fmt.Errorf("base error")
		wrappedErr := fmt.Errorf("wrapped: %w", baseErr)

		assert.NotPanics(t, func() {
			CustomErrorHook(wrappedErr)
		})
	})
}

// TestDoCustomExceptHook tests the custom exception handling logic.
func TestDoCustomExceptHook(t *testing.T) {
	t.Run("driver mode with a valid workerID", func(t *testing.T) {
		// Save the original state for restoration.
		originalWorker := globalWorker

		// Set the driver mode and a valid workerID.
		globalWorker = &worker{
			workerID: ids.NewWorkerID(),
			mode:     proto.WorkerType_DRIVER,
		}

		// Note: ConnectAndGetAccessor requires GCS to be initialized, so this only
		// verifies the function does not panic.
		errorMsg := "test exception message"
		assert.NotPanics(t, func() {
			doCustomExceptHook(errorMsg)
		})

		// Restore the original state.
		globalWorker = originalWorker
	})

	t.Run("non-driver mode", func(t *testing.T) {
		// Save the original state for restoration.
		originalWorker := globalWorker

		// Set a non-driver mode (e.g. WORKER).
		globalWorker = &worker{
			workerID: ids.NewWorkerID(),
			mode:     proto.WorkerType_WORKER,
		}

		errorMsg := "test exception in worker"
		assert.NotPanics(t, func() {
			doCustomExceptHook(errorMsg)
		})

		// Restore the original state.
		globalWorker = originalWorker
	})

	t.Run("nil workerID", func(t *testing.T) {
		// Save the original state for restoration.
		originalWorker := globalWorker

		// Set a nil workerID.
		globalWorker = &worker{
			workerID: ids.WorkerID{}, // Empty WorkerID.
			mode:     proto.WorkerType_DRIVER,
		}

		errorMsg := "test exception with nil worker"
		assert.NotPanics(t, func() {
			doCustomExceptHook(errorMsg)
		})

		// Restore the original state.
		globalWorker = originalWorker
	})

	t.Run("empty exception message", func(t *testing.T) {
		// Save the original state for restoration.
		originalWorker := globalWorker

		globalWorker = &worker{
			workerID: ids.NewWorkerID(),
			mode:     proto.WorkerType_DRIVER,
		}

		assert.NotPanics(t, func() {
			doCustomExceptHook("")
		})

		// Restore the original state.
		globalWorker = originalWorker
	})
}

// TestAutoscalerMonitor_Wiring verifies the phase 7 assembly wiring:
// NewAutoscalerMonitor creates all the long-lived collaborators
// (InstanceManager/Scheduler/CloudInstanceUpdater/RayStopper/CloudResourceMonitor/
// Reconciler) and wires them into the Autoscaler; one reconcile round produces a
// non-empty AutoscalingState.
func TestAutoscalerMonitor_Wiring(t *testing.T) {
	mockClient := &mockGcsClient{
		addr:      "mock:1234",
		clusterID: ids.NewClusterID(),
	}
	mockClient.getFunc = func(ctx context.Context, ns, key string) ([]byte, error) {
		if key == "session_name" {
			return []byte("wiring-session"), nil
		}
		return nil, gcs.ErrNotImplemented
	}
	// GetAutoscalerStatus returns a cluster resource state with a head node so the
	// reconciler can complete a full round (passive sync + proactive transitions +
	// metrics reporting).
	mockClient.getAutoscalerStatusFunc = func(ctx context.Context) (*proto.GetClusterStatusReply, error) {
		return &proto.GetClusterStatusReply{
			ClusterResourceState: &proto.ClusterResourceState{
				ClusterResourceStateVersion: 1,
				NodeStates: []*proto.NodeState{
					{
						NodeId: []byte("head-node"),
						Status: proto.NodeStatus_RUNNING,
						TotalResources: map[string]float64{
							"node:__internal_head__": 1,
						},
					},
				},
			},
		}, nil
	}
	gcs.SetClient(mockClient)
	defer gcs.ClearClient()

	configReader, err := CreateConfigReader(&MonitorV2Config{
		GcsAddress: "mock:1234",
	})
	assert.NoError(t, err)

	monitor, err := NewAutoscalerMonitor("mock:1234", configReader, "", "")
	assert.NoError(t, err)
	assert.NotNil(t, monitor)
	assert.NotNil(t, monitor.autoscaler)
	// The RayStopper must have been created by NewAutoscaler and registered as an
	// InstanceManager subscriber.
	assert.NotNil(t, monitor.autoscaler.rayStopper)

	// Run one reconcile round and verify the assembly chain neither panics nor
	// fails to produce a state.
	autoscalingState, err := monitor.autoscaler.UpdateAutoscalingState()
	assert.NoError(t, err)
	assert.NotNil(t, autoscalingState)
	// LastSeenClusterResourceStateVersion must reflect the version reported by GCS.
	assert.Equal(t, int64(1), autoscalingState.LastSeenClusterResourceStateVersion)
}

// TestAutoscalerMonitor_Run_Reconciled verifies the normal monitor loop path once
// the reconciler is wired: when GetAutoscalerStatus returns an error the round is
// swallowed with a log entry only, and the loop exits normally once ctx is canceled.
func TestAutoscalerMonitor_Run_Reconciled(t *testing.T) {
	mockClient := &mockGcsClient{
		addr:      "mock:1234",
		clusterID: ids.NewClusterID(),
	}
	mockClient.getFunc = func(ctx context.Context, ns, key string) ([]byte, error) {
		if key == "session_name" {
			return []byte("run-session"), nil
		}
		return nil, gcs.ErrNotImplemented
	}
	// GetAutoscalerStatus returns an error, simulating a temporarily unavailable
	// GCS: UpdateAutoscalingState swallows the error and returns (nil, nil) so the
	// loop continues.
	mockClient.getAutoscalerStatusFunc = func(ctx context.Context) (*proto.GetClusterStatusReply, error) {
		return nil, gcs.ErrNotImplemented
	}
	gcs.SetClient(mockClient)
	defer gcs.ClearClient()

	configReader, err := CreateConfigReader(&MonitorV2Config{
		GcsAddress: "mock:1234",
	})
	assert.NoError(t, err)

	monitor, err := NewAutoscalerMonitor("mock:1234", configReader, "", "")
	assert.NoError(t, err)

	// The pre-canceled ctx makes the loop exit right after one round.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err = monitor.run(ctx)
	assert.NoError(t, err)
}

// TestNewAutoscalerMonitor_PutFalseNoError reproduces the bench crash of the
// Kuberay pod: the cgo GCS client's Put reports (false, nil) when the write does
// not take, and the metrics-address registration used to turn that into a
// (nil, nil) return of NewAutoscalerMonitor. The caller then started Run() on a
// nil *AutoscalerMonitor and panicked
// (v2.(*AutoscalerMonitor).run(0x0, ...) at the UpdateAutoscalingState call).
// Kuberay-shaped inputs: no autoscaling config, a real logs dir, a monitor IP.
func TestNewAutoscalerMonitor_PutFalseNoError(t *testing.T) {
	tmpDir := t.TempDir()

	mockClient := &mockGcsClient{
		addr:      "127.0.0.1:6379",
		clusterID: ids.NewClusterID(),
	}
	mockClient.getFunc = func(ctx context.Context, ns, key string) ([]byte, error) {
		if key == "session_name" {
			return []byte("put-false-session"), nil
		}
		return nil, gcs.ErrNotImplemented
	}
	// Mirror the cgo client behavior: the put reports "not written" with no error.
	mockClient.putFunc = func(ctx context.Context, ns, key string, value []byte, overwrite bool) (bool, error) {
		return false, nil
	}
	gcs.SetClient(mockClient)
	defer gcs.ClearClient()

	configReader, err := CreateConfigReader(&MonitorV2Config{
		GcsAddress: "127.0.0.1:6379",
	})
	assert.NoError(t, err)

	monitor, err := NewAutoscalerMonitor("127.0.0.1:6379", configReader, tmpDir, "127.0.0.1")
	if monitor == nil && err == nil {
		t.Fatal("NewAutoscalerMonitor returned (nil, nil): Run() would dereference a nil receiver and panic")
	}
	assert.NoError(t, err)
	assert.NotNil(t, monitor)
	assert.Equal(t, "127.0.0.1:6379", monitor.gcsAddress)
}

// TestReportAutoscalingState_Success verifies a successful report: the state is
// reported through gcsClient.ReportAutoscalingState, returns nil and does not
// panic (mirroring the monitor.go reportAutoscalingState semantics).
func TestReportAutoscalingState_Success(t *testing.T) {
	received := ""
	mockClient := &mockGcsClient{
		addr:      "mock:1234",
		clusterID: ids.NewClusterID(),
		reportFunc: func(autoscalingState string) error {
			received = autoscalingState
			return nil
		},
	}
	am := &AutoscalerMonitor{gcsClient: mockClient}
	// Pass a state with fields: protobuf serializes an empty message to an empty
	// string, which cannot distinguish "not called" from "called but serialized to
	// empty", so a non-empty state validates the serialization/reporting chain.
	state := &proto.AutoscalingState{
		LastSeenClusterResourceStateVersion: 1,
	}
	expectedBytes, err := protolib.Marshal(state)
	assert.NoError(t, err)
	am.reportAutoscalingState(state)
	assert.Equal(t, string(expectedBytes), received)
}

// TestReportAutoscalingState_FailureLogsOnly verifies that a report failure is
// only logged and does not panic: reportAutoscalingState does not propagate when
// GCS returns an error.
func TestReportAutoscalingState_FailureLogsOnly(t *testing.T) {
	called := false
	mockClient := &mockGcsClient{
		addr:      "mock:1234",
		clusterID: ids.NewClusterID(),
		reportFunc: func(autoscalingState string) error {
			called = true
			return gcs.ErrNotImplemented
		},
	}
	am := &AutoscalerMonitor{gcsClient: mockClient}
	assert.NotPanics(t, func() {
		am.reportAutoscalingState(&proto.AutoscalingState{})
	})
	assert.True(t, called)
}
