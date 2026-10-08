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

package autoscaler

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/ray-project/ray/go/proto"
	"github.com/stretchr/testify/assert"
)

// TestFormatReadonlyNodeType tests the node ID formatting.
func TestFormatReadonlyNodeType(t *testing.T) {
	tests := []struct {
		name     string
		nodeID   string
		expected string
	}{
		{
			name:     "ValidHexID",
			nodeID:   "abc123def456",
			expected: "node_abc123def456",
		},
		{
			name:     "EmptyID",
			nodeID:   "",
			expected: "node_",
		},
		{
			name:     "LongID",
			nodeID:   "verylongnodeid1234567890abcdef",
			expected: "node_verylongnodeid1234567890abcdef",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := FormatReadonlyNodeType(tt.nodeID)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestIsHeadNode tests the head node detection.
func TestIsHeadNode(t *testing.T) {
	tests := []struct {
		name         string
		nodeState    *proto.NodeState
		expectedHead bool
	}{
		{
			name: "HeadNodeWithInternalResource",
			nodeState: &proto.NodeState{
				TotalResources: map[string]float64{
					"node:__internal_head__": 1.0,
					"CPU":                    4.0,
					"memory":                 8192.0,
				},
			},
			expectedHead: true,
		},
		{
			name: "WorkerNodeWithoutHeadResource",
			nodeState: &proto.NodeState{
				TotalResources: map[string]float64{
					"CPU":    4.0,
					"memory": 8192.0,
				},
			},
			expectedHead: false,
		},
		{
			name: "EmptyNodeState",
			nodeState: &proto.NodeState{
				TotalResources: map[string]float64{},
			},
			expectedHead: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := IsHeadNode(tt.nodeState)
			assert.Equal(t, tt.expectedHead, result)
		})
	}
}

// TestPrepareConfig tests the config preprocessing.
func TestPrepareConfig(t *testing.T) {
	// PrepareConfig calls local.PrepareLocal, which requires head_ip and
	// worker_ips in the provider section, so only the error and success paths
	// are covered here.

	t.Run("MissingHeadIpAndWorkerIps", func(t *testing.T) {
		config := map[string]interface{}{
			"provider": map[string]interface{}{
				"type": "local",
			},
			"cluster_name": "test-cluster",
		}

		_, err := PrepareConfig(config)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "head_ip")
	})

	t.Run("WithHeadIpAndWorkerIps", func(t *testing.T) {
		config := map[string]interface{}{
			"provider": map[string]interface{}{
				"type":       "local",
				"head_ip":    "127.0.0.1",
				"worker_ips": []interface{}{"127.0.0.2"},
			},
			"cluster_name": "test-cluster",
		}

		result, err := PrepareConfig(config)
		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Contains(t, result, "provider")
	})

	t.Run("InvalidDockerConfig", func(t *testing.T) {
		config := map[string]interface{}{
			"provider": map[string]interface{}{
				"type":       "local",
				"head_ip":    "127.0.0.1",
				"worker_ips": []interface{}{"127.0.0.2"},
			},
			"cluster_name": "test-cluster",
			"docker": map[string]interface{}{
				"container_name": "test-container",
				// The missing image field fails the validation.
			},
		}

		_, err := PrepareConfig(config)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "must provide a container & image name")
	})
}

// TestValidateConfig tests the config validation.
func TestValidateConfig(t *testing.T) {
	tests := []struct {
		name        string
		config      map[string]interface{}
		expectError bool
		errorMsg    string
	}{
		{
			name:        "NilConfig",
			config:      nil,
			expectError: true,
			errorMsg:    "config is nil",
		},
		{
			name: "ValidMinimalConfig",
			config: map[string]interface{}{
				"provider": map[string]interface{}{
					"type": "local",
				},
				"cluster_name":   "test-cluster",
				"max_workers":    5,
				"head_node_type": "head.default",
				"available_node_types": map[string]interface{}{
					"head.default": map[string]interface{}{
						"min_workers": 0,
						"max_workers": 0,
					},
					"worker.default": map[string]interface{}{
						"min_workers": 0,
						"max_workers": 5,
					},
				},
				"cluster_synced_files": []interface{}{},
			},
			expectError: false,
		},
		{
			name: "MissingClusterSyncedFiles",
			config: map[string]interface{}{
				"provider": map[string]interface{}{
					"type": "local",
				},
				"cluster_name": "test-cluster",
				"max_workers":  5,
			},
			expectError: true,
			errorMsg:    "missing 'cluster_synced_files' field",
		},
		{
			name: "MissingHeadNodeType",
			config: map[string]interface{}{
				"provider": map[string]interface{}{
					"type": "local",
				},
				"cluster_name": "test-cluster",
				"max_workers":  5,
				"available_node_types": map[string]interface{}{
					"head.default":   map[string]interface{}{},
					"worker.default": map[string]interface{}{},
				},
				"cluster_synced_files": []interface{}{},
			},
			expectError: true,
			errorMsg:    "must specify `head_node_type`",
		},
		{
			name: "InvalidHeadNodeType",
			config: map[string]interface{}{
				"provider": map[string]interface{}{
					"type": "local",
				},
				"cluster_name":   "test-cluster",
				"max_workers":    5,
				"head_node_type": "nonexistent.type",
				"available_node_types": map[string]interface{}{
					"head.default":   map[string]interface{}{},
					"worker.default": map[string]interface{}{},
				},
				"cluster_synced_files": []interface{}{},
			},
			expectError: true,
			errorMsg:    "must be one of `available_node_types`",
		},
		{
			name: "MinWorkersExceedMaxWorkers",
			config: map[string]interface{}{
				"provider": map[string]interface{}{
					"type": "local",
				},
				"cluster_name":   "test-cluster",
				"max_workers":    2,
				"head_node_type": "head.default",
				"available_node_types": map[string]interface{}{
					"head.default": map[string]interface{}{
						"min_workers": 0,
						"max_workers": 0,
					},
					"worker.default": map[string]interface{}{
						"min_workers": 5, // Exceeds max_workers.
						"max_workers": 10,
					},
				},
				"cluster_synced_files": []interface{}{},
			},
			expectError: true,
			errorMsg:    "smaller than the sum of `min_workers`",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateConfig(tt.config)

			if tt.expectError {
				assert.Error(t, err)
				if tt.errorMsg != "" {
					assert.Contains(t, err.Error(), tt.errorMsg)
				}
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

// TestFilloutDefaults tests the default value filling.
func TestFilloutDefaults(t *testing.T) {
	tests := []struct {
		name        string
		config      map[string]interface{}
		expectError bool
		validate    func(t *testing.T, result map[string]interface{})
	}{
		{
			name: "BasicConfig",
			config: map[string]interface{}{
				"provider": map[string]interface{}{
					"type": "local",
				},
				"cluster_name": "test-cluster",
			},
			expectError: false,
			validate: func(t *testing.T, result map[string]interface{}) {
				assert.NotNil(t, result)
				assert.Contains(t, result, "auth")
				assert.Contains(t, result, "cluster_name")
				assert.Contains(t, result, "upscaling_speed")
			},
		},
		{
			name: "ConfigWithExistingAuth",
			config: map[string]interface{}{
				"provider": map[string]interface{}{
					"type": "local",
				},
				"cluster_name": "test-cluster",
				"auth": map[string]interface{}{
					"ssh_user": "ubuntu",
				},
			},
			expectError: false,
			validate: func(t *testing.T, result map[string]interface{}) {
				auth, ok := result["auth"].(map[string]interface{})
				assert.True(t, ok)
				assert.Equal(t, "ubuntu", auth["ssh_user"])
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := filloutDefaults(tt.config)

			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				if tt.validate != nil {
					tt.validate(t, result)
				}
			}
		})
	}
}

// TestMergeSetupCommands tests the setup command merging.
func TestMergeSetupCommands(t *testing.T) {
	tests := []struct {
		name           string
		config         map[string]interface{}
		expectedHead   []interface{}
		expectedWorker []interface{}
	}{
		{
			name: "AllCommandTypesPresent",
			config: map[string]interface{}{
				"setup_commands":        []interface{}{"command1", "command2"},
				"head_setup_commands":   []interface{}{"head_command"},
				"worker_setup_commands": []interface{}{"worker_command"},
			},
			expectedHead:   []interface{}{"command1", "command2", "head_command"},
			expectedWorker: []interface{}{"command1", "command2", "worker_command"},
		},
		{
			name: "OnlySetupCommands",
			config: map[string]interface{}{
				"setup_commands": []interface{}{"command1"},
			},
			expectedHead:   []interface{}{"command1"},
			expectedWorker: []interface{}{"command1"},
		},
		{
			name:           "NoCommands",
			config:         map[string]interface{}{},
			expectedHead:   []interface{}{},
			expectedWorker: []interface{}{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mergeSetupCommands(tt.config)

			headCommands := tt.config["head_setup_commands"].([]interface{})
			workerCommands := tt.config["worker_setup_commands"].([]interface{})

			assert.Equal(t, tt.expectedHead, headCommands)
			assert.Equal(t, tt.expectedWorker, workerCommands)
		})
	}
}

// TestFillNodeTypeMinMaxWorkers tests filling the node type min/max workers.
func TestFillNodeTypeMinMaxWorkers(t *testing.T) {
	tests := []struct {
		name           string
		config         map[string]interface{}
		expectError    bool
		validateResult func(t *testing.T, config map[string]interface{})
	}{
		{
			name: "BasicNodeTypes",
			config: map[string]interface{}{
				"max_workers":    10,
				"head_node_type": "head.default",
				"available_node_types": map[string]interface{}{
					"head.default":   map[string]interface{}{},
					"worker.default": map[string]interface{}{},
				},
			},
			expectError: false,
			validateResult: func(t *testing.T, config map[string]interface{}) {
				nodeTypes := config["available_node_types"].(map[string]interface{})

				// The head node type gets min_workers=0 and max_workers=0.
				headType := nodeTypes["head.default"].(map[string]interface{})
				assert.Equal(t, 0, headType["min_workers"])
				assert.Equal(t, 0, headType["max_workers"])

				// The worker node type gets min_workers=0 and max_workers=10.
				workerType := nodeTypes["worker.default"].(map[string]interface{})
				assert.Equal(t, 0, workerType["min_workers"])
				assert.Equal(t, 10, workerType["max_workers"])
			},
		},
		{
			name: "NodeTypesWithExistingValues",
			config: map[string]interface{}{
				"max_workers":    5,
				"head_node_type": "head.default",
				"available_node_types": map[string]interface{}{
					"head.default": map[string]interface{}{
						"min_workers": 1,
						"max_workers": 2,
					},
					"worker.default": map[string]interface{}{
						"min_workers": 2,
					},
				},
			},
			expectError: false,
			validateResult: func(t *testing.T, config map[string]interface{}) {
				nodeTypes := config["available_node_types"].(map[string]interface{})

				// Existing values are kept.
				headType := nodeTypes["head.default"].(map[string]interface{})
				assert.Equal(t, 1, headType["min_workers"])
				assert.Equal(t, 2, headType["max_workers"])

				// A worker type that only has min_workers gets max_workers.
				workerType := nodeTypes["worker.default"].(map[string]interface{})
				assert.Equal(t, 2, workerType["min_workers"])
				assert.Equal(t, 5, workerType["max_workers"])
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := fillNodeTypeMinMaxWorkers(tt.config)

			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				if tt.validateResult != nil {
					tt.validateResult(t, tt.config)
				}
			}
		})
	}
}

// TestMergeLegacyYamlWithDefaults tests the legacy YAML config merge.
func TestMergeLegacyYamlWithDefaults(t *testing.T) {
	tests := []struct {
		name        string
		config      map[string]interface{}
		expectError bool
		validate    func(t *testing.T, result map[string]interface{})
	}{
		{
			name: "LegacyConfigWithHeadAndWorkerNodes",
			config: map[string]interface{}{
				"head_node_type": "head.default",
				"available_node_types": map[string]interface{}{
					"head.default": map[string]interface{}{
						"node_config": map[string]interface{}{
							"InstanceType": "m5.large",
						},
					},
					"worker.default": map[string]interface{}{
						"node_config": map[string]interface{}{
							"InstanceType": "m5.xlarge",
						},
					},
				},
				"head_node": map[string]interface{}{
					"InstanceType": "m5.large",
					"resources": map[string]interface{}{
						"CPU": 2,
					},
				},
				"worker_nodes": map[string]interface{}{
					"InstanceType": "m5.xlarge",
					"resources": map[string]interface{}{
						"CPU": 4,
					},
				},
				"min_workers": 2,
				"max_workers": 10,
			},
			expectError: false,
			validate: func(t *testing.T, result map[string]interface{}) {
				// available_node_types must be converted correctly.
				nodeTypes := result["available_node_types"].(map[string]interface{})
				assert.Contains(t, nodeTypes, NODE_TYPE_LEGACY_HEAD)
				assert.Contains(t, nodeTypes, NODE_TYPE_LEGACY_WORKER)

				// head_node_type must be updated.
				assert.Equal(t, NODE_TYPE_LEGACY_HEAD, result["head_node_type"])

				// The resources in head_node and worker_nodes must be removed.
				headNode := result["head_node"].(map[string]interface{})
				_, hasResources := headNode["resources"]
				assert.False(t, hasResources)

				workerNodes := result["worker_nodes"].(map[string]interface{})
				_, hasResources = workerNodes["resources"]
				assert.False(t, hasResources)
			},
		},
		{
			name: "ConfigWithoutHeadNode",
			config: map[string]interface{}{
				"head_node_type": "head.default",
				"available_node_types": map[string]interface{}{
					"head.default":   map[string]interface{}{},
					"worker.default": map[string]interface{}{},
				},
				"worker_nodes": map[string]interface{}{
					"InstanceType": "m5.xlarge",
				},
				"min_workers": 2,
				"max_workers": 10,
			},
			expectError: false,
			validate: func(t *testing.T, result map[string]interface{}) {
				// The default head_node_type config is used.
				nodeTypes := result["available_node_types"].(map[string]interface{})
				assert.Contains(t, nodeTypes, NODE_TYPE_LEGACY_HEAD)
			},
		},
		{
			name: "ConfigWithoutWorkerNodes",
			config: map[string]interface{}{
				"head_node_type": "head.default",
				"available_node_types": map[string]interface{}{
					"head.default":   map[string]interface{}{},
					"worker.default": map[string]interface{}{},
				},
				"head_node": map[string]interface{}{
					"InstanceType": "m5.large",
				},
				"max_workers": 10,
			},
			expectError: false,
			validate: func(t *testing.T, result map[string]interface{}) {
				// The default worker.default config is used.
				nodeTypes := result["available_node_types"].(map[string]interface{})
				assert.Contains(t, nodeTypes, NODE_TYPE_LEGACY_WORKER)
			},
		},
		{
			name: "InvalidNodeTypesCount",
			config: map[string]interface{}{
				"head_node_type": "head.default",
				"available_node_types": map[string]interface{}{
					"head.default":   map[string]interface{}{},
					"worker.default": map[string]interface{}{},
					"extra.type":     map[string]interface{}{},
				},
			},
			expectError: true,
		},
		{
			name: "MissingMaxWorkers",
			config: map[string]interface{}{
				"head_node_type": "head.default",
				"available_node_types": map[string]interface{}{
					"head.default":   map[string]interface{}{},
					"worker.default": map[string]interface{}{},
				},
				"worker_nodes": map[string]interface{}{
					"InstanceType": "m5.xlarge",
				},
				"min_workers": 2,
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := mergeLegacyYamlWithDefaults(tt.config)

			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				if tt.validate != nil {
					tt.validate(t, result)
				}
			}
		})
	}
}

// TestTranslateTrivialLegacyConfig tests the legacy config cleanup.
func TestTranslateTrivialLegacyConfig(t *testing.T) {
	tests := []struct {
		name        string
		config      map[string]interface{}
		expectError bool
		validate    func(t *testing.T, result map[string]interface{})
	}{
		{
			name: "EmptyHeadNodeAndWorkerNodes",
			config: map[string]interface{}{
				"head_node":    map[string]interface{}{},
				"worker_nodes": map[string]interface{}{},
				"cluster_name": "test-cluster",
			},
			expectError: false,
			validate: func(t *testing.T, result map[string]interface{}) {
				// Empty head_node and worker_nodes must be removed.
				_, hasHeadNode := result["head_node"]
				_, hasWorkerNodes := result["worker_nodes"]
				assert.False(t, hasHeadNode)
				assert.False(t, hasWorkerNodes)
			},
		},
		{
			name: "NilHeadNodeAndWorkerNodes",
			config: map[string]interface{}{
				"head_node":    nil,
				"worker_nodes": nil,
				"cluster_name": "test-cluster",
			},
			expectError: false,
			validate: func(t *testing.T, result map[string]interface{}) {
				// nil head_node and worker_nodes must be removed.
				_, hasHeadNode := result["head_node"]
				_, hasWorkerNodes := result["worker_nodes"]
				assert.False(t, hasHeadNode)
				assert.False(t, hasWorkerNodes)
			},
		},
		{
			name: "NonEmptyHeadNodeAndWorkerNodes",
			config: map[string]interface{}{
				"head_node": map[string]interface{}{
					"InstanceType": "m5.large",
				},
				"worker_nodes": map[string]interface{}{
					"InstanceType": "m5.xlarge",
				},
				"cluster_name": "test-cluster",
			},
			expectError: false,
			validate: func(t *testing.T, result map[string]interface{}) {
				// Non-empty head_node and worker_nodes are kept.
				_, hasHeadNode := result["head_node"]
				_, hasWorkerNodes := result["worker_nodes"]
				assert.True(t, hasHeadNode)
				assert.True(t, hasWorkerNodes)
			},
		},
		{
			name: "NoLegacyFields",
			config: map[string]interface{}{
				"cluster_name": "test-cluster",
			},
			expectError: false,
			validate: func(t *testing.T, result map[string]interface{}) {
				// Without legacy fields the config is unchanged.
				assert.Equal(t, "test-cluster", result["cluster_name"])
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := translateTrivialLegacyConfig(tt.config)

			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				if tt.validate != nil {
					tt.validate(t, result)
				}
			}
		})
	}
}

// TestLoadRaySchemaConfig tests loading the embedded JSON schema.
func TestLoadRaySchemaConfig(t *testing.T) {
	content, err := LoadRaySchemaConfig()

	assert.NoError(t, err)
	assert.NotEmpty(t, content)

	// The embedded schema must be the Ray autoscaler draft-07 schema.
	assert.Contains(t, string(content), "http://json-schema.org/draft-07/schema#")
}

// TestHashRuntimeConf tests the runtime config hash computation.
func TestHashRuntimeConf(t *testing.T) {
	// Create temporary directories and files for the test.
	tempDir := t.TempDir()

	file1 := filepath.Join(tempDir, "file1.txt")
	file2 := filepath.Join(tempDir, "file2.txt")

	err := os.WriteFile(file1, []byte("content1"), 0644)
	assert.NoError(t, err)

	err = os.WriteFile(file2, []byte("content2"), 0644)
	assert.NoError(t, err)

	tests := []struct {
		name                           string
		fileMounts                     map[string]string
		clusterSyncedFiles             []string
		extraObjs                      [][]string
		generateFileMountsContentsHash bool
		expectError                    bool
		validateHashes                 func(t *testing.T, runtimeHash, contentsHash string)
	}{
		{
			name: "BasicFileMounts",
			fileMounts: map[string]string{
				"/remote/path": file1,
			},
			clusterSyncedFiles:             []string{},
			extraObjs:                      [][]string{},
			generateFileMountsContentsHash: false,
			expectError:                    false,
			validateHashes: func(t *testing.T, runtimeHash, contentsHash string) {
				assert.NotEmpty(t, runtimeHash)
				// contentsHash is non-empty on the first call
				// (generateFileMountsContentsHash=false still hashes the file
				// contents).
				assert.NotEmpty(t, contentsHash)
			},
		},
		{
			name: "MultipleFileMounts",
			fileMounts: map[string]string{
				"/remote/path1": file1,
				"/remote/path2": file2,
			},
			clusterSyncedFiles:             []string{},
			extraObjs:                      [][]string{},
			generateFileMountsContentsHash: true,
			expectError:                    false,
			validateHashes: func(t *testing.T, runtimeHash, contentsHash string) {
				assert.NotEmpty(t, runtimeHash)
				assert.NotEmpty(t, contentsHash)
				// Different contents must produce different hashes.
				assert.NotEqual(t, runtimeHash, contentsHash)
			},
		},
		{
			name: "WithClusterSyncedFiles",
			fileMounts: map[string]string{
				"/remote/path": file1,
			},
			clusterSyncedFiles:             []string{file2},
			extraObjs:                      [][]string{},
			generateFileMountsContentsHash: false,
			expectError:                    false,
			validateHashes: func(t *testing.T, runtimeHash, contentsHash string) {
				assert.NotEmpty(t, runtimeHash)
			},
		},
		{
			name:                           "EmptyConfig",
			fileMounts:                     map[string]string{},
			clusterSyncedFiles:             []string{},
			extraObjs:                      [][]string{},
			generateFileMountsContentsHash: false,
			expectError:                    false,
			validateHashes: func(t *testing.T, runtimeHash, contentsHash string) {
				assert.NotEmpty(t, runtimeHash)
			},
		},
		{
			name: "NonExistentFileMount",
			fileMounts: map[string]string{
				"/remote/path": "/non/existent/file",
			},
			clusterSyncedFiles:             []string{},
			extraObjs:                      [][]string{},
			generateFileMountsContentsHash: true,
			expectError:                    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Clear the cache so every test is independent.
			hashCache = make(map[string]string)

			runtimeHash, contentsHash, err := HashRuntimeConf(
				tt.fileMounts,
				tt.clusterSyncedFiles,
				tt.extraObjs,
				tt.generateFileMountsContentsHash,
			)

			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				if tt.validateHashes != nil {
					tt.validateHashes(t, runtimeHash, contentsHash)
				}
			}
		})
	}
}

// TestMarshalWithSortedKeys tests the sorted-key serialization.
func TestMarshalWithSortedKeys(t *testing.T) {
	tests := []struct {
		name     string
		data     map[string]string
		expected string
	}{
		{
			name: "UnsortedKeys",
			data: map[string]string{
				"zebra":  "animal",
				"apple":  "fruit",
				"banana": "fruit",
			},
			expected: `{"apple":"fruit","banana":"fruit","zebra":"animal"}`,
		},
		{
			name:     "EmptyMap",
			data:     map[string]string{},
			expected: `{}`,
		},
		{
			name: "SingleKey",
			data: map[string]string{
				"key": "value",
			},
			expected: `{"key":"value"}`,
		},
		{
			name: "KeysWithSpecialChars",
			data: map[string]string{
				"key with spaces":      "value1",
				"key_with_underscores": "value2",
				"key-with-dashes":      "value3",
			},
			expected: `{"key with spaces":"value1","key-with-dashes":"value3","key_with_underscores":"value2"}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := marshalWithSortedKeys(tt.data)
			assert.NoError(t, err)
			assert.Equal(t, tt.expected, string(result))
		})
	}
}

// TestHashRuntimeConfCaching tests the hash cache.
func TestHashRuntimeConfCaching(t *testing.T) {
	tempDir := t.TempDir()
	file1 := filepath.Join(tempDir, "file1.txt")
	err := os.WriteFile(file1, []byte("content1"), 0644)
	assert.NoError(t, err)

	fileMounts := map[string]string{
		"/remote/path": file1,
	}

	// Clear the cache.
	hashCache = make(map[string]string)

	// The first call computes and caches the hash.
	runtimeHash1, contentsHash1, err := HashRuntimeConf(
		fileMounts,
		[]string{},
		[][]string{},
		true,
	)
	assert.NoError(t, err)
	assert.NotEmpty(t, runtimeHash1)
	assert.NotEmpty(t, contentsHash1)

	// The second call with the same config uses the cache.
	runtimeHash2, contentsHash2, err := HashRuntimeConf(
		fileMounts,
		[]string{},
		[][]string{},
		false, // generateFileMountsContentsHash=false
	)
	assert.NoError(t, err)
	assert.Equal(t, runtimeHash1, runtimeHash2)
	// With the cache hit and generateFileMountsContentsHash=false,
	// contentsHash is empty.
	assert.Empty(t, contentsHash2)

	// The third call forces a recompute.
	runtimeHash3, contentsHash3, err := HashRuntimeConf(
		fileMounts,
		[]string{},
		[][]string{},
		true, // generateFileMountsContentsHash=true
	)
	assert.NoError(t, err)
	assert.Equal(t, runtimeHash1, runtimeHash3)
	assert.NotEmpty(t, contentsHash3)
}

// BenchmarkHashRuntimeConf is the benchmark.
func BenchmarkHashRuntimeConf(b *testing.B) {
	tempDir := b.TempDir()

	// Create several test files.
	files := make(map[string]string)
	for i := 0; i < 10; i++ {
		filename := filepath.Join(tempDir, "file"+string(rune('0'+i))+".txt")
		content := []byte("content" + string(rune('0'+i)))
		err := os.WriteFile(filename, content, 0644)
		if err != nil {
			b.Fatal(err)
		}
		files["/remote/path"+string(rune('0'+i))] = filename
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		hashCache = make(map[string]string)
		_, _, _ = HashRuntimeConf(files, []string{}, [][]string{}, true)
	}
}

// BenchmarkMarshalWithSortedKeys is the benchmark.
func BenchmarkMarshalWithSortedKeys(b *testing.B) {
	data := map[string]string{
		"key1": "value1",
		"key2": "value2",
		"key3": "value3",
		"key4": "value4",
		"key5": "value5",
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = marshalWithSortedKeys(data)
	}
}
