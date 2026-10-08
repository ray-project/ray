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

package local

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestPrepareLocal tests the PrepareLocal function.
func TestPrepareLocal(t *testing.T) {
	t.Run("RejectsLegacyFields", func(t *testing.T) {
		tests := []struct {
			name      string
			field     string
			value     interface{}
			expectErr bool
		}{
			{
				name:      "WithHeadNode",
				field:     "head_node",
				value:     map[string]interface{}{"InstanceType": "m5.large"},
				expectErr: true,
			},
			{
				name:      "WithWorkerNodes",
				field:     "worker_nodes",
				value:     map[string]interface{}{"InstanceType": "m5.xlarge"},
				expectErr: true,
			},
			{
				name:      "WithAvailableNodeTypes",
				field:     "available_node_types",
				value:     map[string]interface{}{"head.default": map[string]interface{}{}},
				expectErr: true,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				config := map[string]interface{}{
					"provider": map[string]interface{}{
						"type":       "local",
						"head_ip":    "192.168.1.1",
						"worker_ips": []interface{}{"192.168.1.2"},
					},
					"cluster_name": "test-cluster",
				}

				config[tt.field] = tt.value

				_, err := PrepareLocal(config)

				if tt.expectErr {
					assert.Error(t, err)
					assert.Contains(t, err.Error(), "not supported for on-premise clusters")
				} else {
					assert.NoError(t, err)
				}
			})
		}
	})

	t.Run("CreatesStandardizedNodeType", func(t *testing.T) {
		config := map[string]interface{}{
			"provider": map[string]interface{}{
				"type":       "local",
				"head_ip":    "192.168.1.1",
				"worker_ips": []interface{}{"192.168.1.2"},
			},
			"cluster_name": "test-cluster",
		}

		result, err := PrepareLocal(config)
		assert.NoError(t, err)
		assert.NotNil(t, result)

		// The standardized node type must be created.
		nodeTypes, ok := result["available_node_types"].(map[string]interface{})
		assert.True(t, ok)
		assert.Contains(t, nodeTypes, LOCAL_CLUSTER_NODE_TYPE)

		nodeType, ok := nodeTypes[LOCAL_CLUSTER_NODE_TYPE].(map[string]interface{})
		assert.True(t, ok)
		assert.Contains(t, nodeType, "node_config")
		assert.Contains(t, nodeType, "resources")

		// The head node type must be set.
		assert.Equal(t, LOCAL_CLUSTER_NODE_TYPE, result["head_node_type"])
	})

	t.Run("ManualModeRequiresHeadIpAndWorkerIps", func(t *testing.T) {
		config := map[string]interface{}{
			"provider": map[string]interface{}{
				"type": "local",
			},
			"cluster_name": "test-cluster",
		}

		_, err := PrepareLocal(config)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "head_ip")
		assert.Contains(t, err.Error(), "worker_ips")
	})

	t.Run("CoordinatorModeRequiresMaxWorkers", func(t *testing.T) {
		config := map[string]interface{}{
			"provider": map[string]interface{}{
				"type":                "local",
				"coordinator_address": "http://coordinator:8080",
			},
			"cluster_name": "test-cluster",
		}

		_, err := PrepareLocal(config)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "max_workers")
	})
}

// TestPrepareCoordinator tests the prepareCoordinator function.
func TestPrepareCoordinator(t *testing.T) {
	t.Run("ValidCoordinatorConfig", func(t *testing.T) {
		config := map[string]interface{}{
			"provider": map[string]interface{}{
				"type":                "local",
				"coordinator_address": "http://coordinator:8080",
			},
			"cluster_name": "test-cluster",
			"max_workers":  10,
			"min_workers":  2,
			"available_node_types": map[string]interface{}{
				LOCAL_CLUSTER_NODE_TYPE: map[string]interface{}{
					"node_config": make(map[string]interface{}),
					"resources":   make(map[string]interface{}),
				},
			},
		}

		result, err := prepareCoordinator(config)
		assert.NoError(t, err)
		assert.NotNil(t, result)

		// min_workers must move into the node type config.
		nodeTypes := result["available_node_types"].(map[string]interface{})
		nodeType := nodeTypes[LOCAL_CLUSTER_NODE_TYPE].(map[string]interface{})
		assert.Equal(t, 2, nodeType["min_workers"])

		// The global min_workers must be removed.
		_, hasMinWorkers := result["min_workers"]
		assert.False(t, hasMinWorkers)

		// max_workers must be set on the node type.
		assert.Equal(t, 10, nodeType["max_workers"])
	})

	t.Run("MissingMaxWorkers", func(t *testing.T) {
		config := map[string]interface{}{
			"provider": map[string]interface{}{
				"type":                "local",
				"coordinator_address": "http://coordinator:8080",
			},
			"cluster_name": "test-cluster",
			"available_node_types": map[string]interface{}{
				LOCAL_CLUSTER_NODE_TYPE: map[string]interface{}{},
			},
		}

		_, err := prepareCoordinator(config)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "`max_workers` is required")
	})

	t.Run("InvalidAvailableNodeTypes", func(t *testing.T) {
		config := map[string]interface{}{
			"provider": map[string]interface{}{
				"type":                "local",
				"coordinator_address": "http://coordinator:8080",
			},
			"cluster_name":         "test-cluster",
			"max_workers":          10,
			"available_node_types": "not a map",
		}

		_, err := prepareCoordinator(config)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "should be a map")
	})

	t.Run("MissingLocalClusterNodeType", func(t *testing.T) {
		config := map[string]interface{}{
			"provider": map[string]interface{}{
				"type":                "local",
				"coordinator_address": "http://coordinator:8080",
			},
			"cluster_name": "test-cluster",
			"max_workers":  10,
			"available_node_types": map[string]interface{}{
				"other.type": map[string]interface{}{},
			},
		}

		_, err := prepareCoordinator(config)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "local cluster node type config not found")
	})

	t.Run("ZeroMinWorkers", func(t *testing.T) {
		config := map[string]interface{}{
			"provider": map[string]interface{}{
				"type":                "local",
				"coordinator_address": "http://coordinator:8080",
			},
			"cluster_name": "test-cluster",
			"max_workers":  10,
			"min_workers":  0,
			"available_node_types": map[string]interface{}{
				LOCAL_CLUSTER_NODE_TYPE: map[string]interface{}{},
			},
		}

		result, err := prepareCoordinator(config)
		assert.NoError(t, err)

		nodeTypes := result["available_node_types"].(map[string]interface{})
		nodeType := nodeTypes[LOCAL_CLUSTER_NODE_TYPE].(map[string]interface{})
		assert.Equal(t, 0, nodeType["min_workers"])
	})
}

// TestPrepareManual tests the prepareManual function.
func TestPrepareManual(t *testing.T) {
	t.Run("ValidManualConfig", func(t *testing.T) {
		config := map[string]interface{}{
			"provider": map[string]interface{}{
				"type":       "local",
				"head_ip":    "192.168.1.1",
				"worker_ips": []interface{}{"192.168.1.2", "192.168.1.3"},
			},
			"cluster_name": "test-cluster",
			"max_workers":  5,
			"min_workers":  1,
			"available_node_types": map[string]interface{}{
				LOCAL_CLUSTER_NODE_TYPE: map[string]interface{}{
					"node_config": make(map[string]interface{}),
					"resources":   make(map[string]interface{}),
				},
			},
		}

		result, err := prepareManual(config)
		assert.NoError(t, err)
		assert.NotNil(t, result)

		// The node type config.
		nodeTypes := result["available_node_types"].(map[string]interface{})
		nodeType := nodeTypes[LOCAL_CLUSTER_NODE_TYPE].(map[string]interface{})

		// min_workers is kept (1 < the 2 worker IPs).
		assert.Equal(t, 1, nodeType["min_workers"])

		// max_workers is clamped (5 > the 2 worker IPs, so it becomes 2).
		assert.Equal(t, 2, nodeType["max_workers"])
		assert.Equal(t, 2, result["max_workers"])

		// The global min_workers must be removed.
		_, hasMinWorkers := result["min_workers"]
		assert.False(t, hasMinWorkers)
	})

	t.Run("MissingProvider", func(t *testing.T) {
		config := map[string]interface{}{
			"cluster_name": "test-cluster",
		}

		_, err := prepareManual(config)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "provider should be a map")
	})

	t.Run("MissingHeadIp", func(t *testing.T) {
		config := map[string]interface{}{
			"provider": map[string]interface{}{
				"type":       "local",
				"worker_ips": []interface{}{"192.168.1.2"},
			},
			"cluster_name": "test-cluster",
		}

		_, err := prepareManual(config)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "head_ip")
	})

	t.Run("MissingWorkerIps", func(t *testing.T) {
		config := map[string]interface{}{
			"provider": map[string]interface{}{
				"type":    "local",
				"head_ip": "192.168.1.1",
			},
			"cluster_name": "test-cluster",
		}

		_, err := prepareManual(config)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "worker_ips")
	})

	t.Run("MinWorkersExceedsNumIPs", func(t *testing.T) {
		config := map[string]interface{}{
			"provider": map[string]interface{}{
				"type":       "local",
				"head_ip":    "192.168.1.1",
				"worker_ips": []interface{}{"192.168.1.2"}, // Only one worker IP.
			},
			"cluster_name": "test-cluster",
			"min_workers":  5, // More than the available worker IPs.
			"available_node_types": map[string]interface{}{
				LOCAL_CLUSTER_NODE_TYPE: map[string]interface{}{
					"node_config": make(map[string]interface{}),
					"resources":   make(map[string]interface{}),
				},
			},
		}

		result, err := prepareManual(config)
		assert.NoError(t, err)

		nodeTypes := result["available_node_types"].(map[string]interface{})
		nodeType := nodeTypes[LOCAL_CLUSTER_NODE_TYPE].(map[string]interface{})

		// min_workers must be clamped to the number of worker IPs.
		assert.Equal(t, 1, nodeType["min_workers"])
	})

	t.Run("MaxWorkersExceedsNumIPs", func(t *testing.T) {
		config := map[string]interface{}{
			"provider": map[string]interface{}{
				"type":       "local",
				"head_ip":    "192.168.1.1",
				"worker_ips": []interface{}{"192.168.1.2"}, // Only one worker IP.
			},
			"cluster_name": "test-cluster",
			"max_workers":  10, // More than the available worker IPs.
			"available_node_types": map[string]interface{}{
				LOCAL_CLUSTER_NODE_TYPE: map[string]interface{}{
					"node_config": make(map[string]interface{}),
					"resources":   make(map[string]interface{}),
				},
			},
		}

		result, err := prepareManual(config)
		assert.NoError(t, err)

		nodeTypes := result["available_node_types"].(map[string]interface{})
		nodeType := nodeTypes[LOCAL_CLUSTER_NODE_TYPE].(map[string]interface{})

		// max_workers must be clamped to the number of worker IPs.
		assert.Equal(t, 1, nodeType["max_workers"])
		assert.Equal(t, 1, result["max_workers"])
	})

	t.Run("NoMaxWorkersSet", func(t *testing.T) {
		config := map[string]interface{}{
			"provider": map[string]interface{}{
				"type":       "local",
				"head_ip":    "192.168.1.1",
				"worker_ips": []interface{}{"192.168.1.2", "192.168.1.3"}, // Two worker IPs.
			},
			"cluster_name": "test-cluster",
			"available_node_types": map[string]interface{}{
				LOCAL_CLUSTER_NODE_TYPE: map[string]interface{}{
					"node_config": make(map[string]interface{}),
					"resources":   make(map[string]interface{}),
				},
			},
		}

		result, err := prepareManual(config)
		assert.NoError(t, err)

		// max_workers defaults to the number of worker IPs.
		assert.Equal(t, 2, result["max_workers"])

		nodeTypes := result["available_node_types"].(map[string]interface{})
		nodeType := nodeTypes[LOCAL_CLUSTER_NODE_TYPE].(map[string]interface{})
		assert.Equal(t, 2, nodeType["max_workers"])
	})

	t.Run("NoMinWorkersSet", func(t *testing.T) {
		config := map[string]interface{}{
			"provider": map[string]interface{}{
				"type":       "local",
				"head_ip":    "192.168.1.1",
				"worker_ips": []interface{}{"192.168.1.2", "192.168.1.3"}, // Two worker IPs.
			},
			"cluster_name": "test-cluster",
			"max_workers":  5,
			"available_node_types": map[string]interface{}{
				LOCAL_CLUSTER_NODE_TYPE: map[string]interface{}{
					"node_config": make(map[string]interface{}),
					"resources":   make(map[string]interface{}),
				},
			},
		}

		result, err := prepareManual(config)
		assert.NoError(t, err)

		nodeTypes := result["available_node_types"].(map[string]interface{})
		nodeType := nodeTypes[LOCAL_CLUSTER_NODE_TYPE].(map[string]interface{})

		// min_workers defaults to the number of worker IPs.
		assert.Equal(t, 2, nodeType["min_workers"])
	})
}

// TestLoadLocalDefaultsConfig tests loading the embedded local default config.
func TestLoadLocalDefaultsConfig(t *testing.T) {
	content, err := LoadLocalDefaultsConfig()

	assert.NoError(t, err)
	assert.NotEmpty(t, content)

	// The embedded config must keep the local cluster defaults.
	assert.Contains(t, string(content), "cluster_name: default")
}

// BenchmarkPrepareLocal is the benchmark.
func BenchmarkPrepareLocal(b *testing.B) {
	config := map[string]interface{}{
		"provider": map[string]interface{}{
			"type":       "local",
			"head_ip":    "192.168.1.1",
			"worker_ips": []interface{}{"192.168.1.2", "192.168.1.3"},
		},
		"cluster_name": "test-cluster",
		"max_workers":  5,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = PrepareLocal(config)
	}
}

// BenchmarkPrepareCoordinator is the benchmark.
func BenchmarkPrepareCoordinator(b *testing.B) {
	config := map[string]interface{}{
		"provider": map[string]interface{}{
			"type":                "local",
			"coordinator_address": "http://coordinator:8080",
		},
		"cluster_name": "test-cluster",
		"max_workers":  10,
		"min_workers":  2,
		"available_node_types": map[string]interface{}{
			LOCAL_CLUSTER_NODE_TYPE: map[string]interface{}{},
		},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = prepareCoordinator(config)
	}
}

// BenchmarkPrepareManual is the benchmark.
func BenchmarkPrepareManual(b *testing.B) {
	config := map[string]interface{}{
		"provider": map[string]interface{}{
			"type":       "local",
			"head_ip":    "192.168.1.1",
			"worker_ips": []interface{}{"192.168.1.2", "192.168.1.3"},
		},
		"cluster_name": "test-cluster",
		"max_workers":  5,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = prepareManual(config)
	}
}

// BenchmarkLoadLocalDefaultsConfig is the benchmark.
func BenchmarkLoadLocalDefaultsConfig(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = LoadLocalDefaultsConfig()
	}
}
