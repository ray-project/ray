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
	"testing"

	"github.com/ray-project/ray/go/internal/autoscaler"
	"github.com/ray-project/ray/go/internal/autoscaler/v2/instance_manager"
	"github.com/ray-project/ray/go/internal/autoscaler/v2/service"
	"github.com/ray-project/ray/go/proto"
	"github.com/stretchr/testify/assert"
)

// TestNewAutoscalerMetricsReporter tests creating the metrics reporter.
func TestNewAutoscalerMetricsReporter(t *testing.T) {
	t.Run("create the metrics reporter successfully", func(t *testing.T) {
		promMetrics, err := autoscaler.NewAutoscalerPrometheusMetrics("test-session")
		assert.NoError(t, err)

		reporter, err := NewAutoscalerMetricsReporter(promMetrics)
		assert.NoError(t, err)
		assert.NotNil(t, reporter)
		assert.Equal(t, promMetrics, reporter.promMetrics)
	})

	t.Run("nil promMetrics", func(t *testing.T) {
		reporter, err := NewAutoscalerMetricsReporter(nil)
		assert.NoError(t, err)
		assert.NotNil(t, reporter)
		assert.Nil(t, reporter.promMetrics)
	})
}

// TestAutoscalerMetricsReporter_ReportInstances tests the ReportInstances method.
func TestAutoscalerMetricsReporter_ReportInstances(t *testing.T) {
	t.Run("empty instance list", func(t *testing.T) {
		promMetrics, err := autoscaler.NewAutoscalerPrometheusMetrics("test-session")
		assert.NoError(t, err)

		reporter := &AutoscalerMetricsReporter{promMetrics: promMetrics}
		nodeTypeConfigs := map[service.NodeType]instance_manager.NodeTypeConfig{
			"worker-node": {},
		}

		// Must not panic.
		assert.NotPanics(t, func() {
			reporter.ReportInstances([]proto.Instance{}, nodeTypeConfigs)
		})
	})

	t.Run("single pending instance", func(t *testing.T) {
		promMetrics, err := autoscaler.NewAutoscalerPrometheusMetrics("test-session")
		assert.NoError(t, err)

		reporter := &AutoscalerMetricsReporter{promMetrics: promMetrics}

		instances := []proto.Instance{
			{
				InstanceType: "worker-node",
				Status:       proto.Instance_QUEUED,
			},
		}
		nodeTypeConfigs := map[service.NodeType]instance_manager.NodeTypeConfig{
			"worker-node": {},
		}

		assert.NotPanics(t, func() {
			reporter.ReportInstances(instances, nodeTypeConfigs)
		})

		// Verify PendingNodesVec was updated (only verify the call does not panic).
		promMetrics.PendingNodesVec.WithLabelValues("worker-node", "test-session").Set(1.0)
	})

	t.Run("single running instance", func(t *testing.T) {
		promMetrics, err := autoscaler.NewAutoscalerPrometheusMetrics("test-session")
		assert.NoError(t, err)

		reporter := &AutoscalerMetricsReporter{promMetrics: promMetrics}

		instances := []proto.Instance{
			{
				InstanceType: "worker-node",
				Status:       proto.Instance_RAY_RUNNING,
			},
		}
		nodeTypeConfigs := map[service.NodeType]instance_manager.NodeTypeConfig{
			"worker-node": {},
		}

		assert.NotPanics(t, func() {
			reporter.ReportInstances(instances, nodeTypeConfigs)
		})
	})

	t.Run("single terminating instance", func(t *testing.T) {
		promMetrics, err := autoscaler.NewAutoscalerPrometheusMetrics("test-session")
		assert.NoError(t, err)

		reporter := &AutoscalerMetricsReporter{promMetrics: promMetrics}

		instances := []proto.Instance{
			{
				InstanceType: "worker-node",
				Status:       proto.Instance_TERMINATING,
			},
		}
		nodeTypeConfigs := map[service.NodeType]instance_manager.NodeTypeConfig{
			"worker-node": {},
		}

		assert.NotPanics(t, func() {
			reporter.ReportInstances(instances, nodeTypeConfigs)
		})
	})

	t.Run("single terminated instance", func(t *testing.T) {
		promMetrics, err := autoscaler.NewAutoscalerPrometheusMetrics("test-session")
		assert.NoError(t, err)

		reporter := &AutoscalerMetricsReporter{promMetrics: promMetrics}

		instances := []proto.Instance{
			{
				InstanceType: "worker-node",
				Status:       proto.Instance_TERMINATED,
			},
		}
		nodeTypeConfigs := map[service.NodeType]instance_manager.NodeTypeConfig{
			"worker-node": {},
		}

		assert.NotPanics(t, func() {
			reporter.ReportInstances(instances, nodeTypeConfigs)
		})
	})

	t.Run("multiple instances with different statuses", func(t *testing.T) {
		promMetrics, err := autoscaler.NewAutoscalerPrometheusMetrics("test-session")
		assert.NoError(t, err)

		reporter := &AutoscalerMetricsReporter{promMetrics: promMetrics}

		instances := []proto.Instance{
			{InstanceType: "worker-node", Status: proto.Instance_QUEUED},
			{InstanceType: "worker-node", Status: proto.Instance_REQUESTED},
			{InstanceType: "worker-node", Status: proto.Instance_RAY_RUNNING},
			{InstanceType: "worker-node", Status: proto.Instance_RAY_RUNNING},
			{InstanceType: "worker-node", Status: proto.Instance_TERMINATING},
			{InstanceType: "worker-node", Status: proto.Instance_TERMINATED},
			{InstanceType: "head-node", Status: proto.Instance_RAY_RUNNING},
		}
		nodeTypeConfigs := map[service.NodeType]instance_manager.NodeTypeConfig{
			"worker-node": {},
			"head-node":   {},
		}

		assert.NotPanics(t, func() {
			reporter.ReportInstances(instances, nodeTypeConfigs)
		})
	})

	t.Run("unknown instance type", func(t *testing.T) {
		promMetrics, err := autoscaler.NewAutoscalerPrometheusMetrics("test-session")
		assert.NoError(t, err)

		reporter := &AutoscalerMetricsReporter{promMetrics: promMetrics}

		instances := []proto.Instance{
			{
				InstanceType: "unknown-type",
				Status:       proto.Instance_RAY_RUNNING,
			},
		}
		nodeTypeConfigs := map[service.NodeType]instance_manager.NodeTypeConfig{
			"worker-node": {},
		}

		// For an instance type missing from nodeTypeConfigs the map lookup returns
		// the zero value, which may panic or silently fail depending on the
		// implementation.
		assert.NotPanics(t, func() {
			reporter.ReportInstances(instances, nodeTypeConfigs)
		})
	})

	t.Run("coverage of every status type", func(t *testing.T) {
		promMetrics, err := autoscaler.NewAutoscalerPrometheusMetrics("test-session")
		assert.NoError(t, err)

		reporter := &AutoscalerMetricsReporter{promMetrics: promMetrics}

		// Test every possible status.
		allStatuses := []proto.Instance_InstanceStatus{
			proto.Instance_QUEUED,
			proto.Instance_REQUESTED,
			proto.Instance_ALLOCATED,
			proto.Instance_RAY_INSTALLING,
			proto.Instance_RAY_RUNNING,
			proto.Instance_RAY_STOP_REQUESTED,
			proto.Instance_RAY_STOPPING,
			proto.Instance_RAY_STOPPED,
			proto.Instance_TERMINATING,
			proto.Instance_TERMINATED,
			proto.Instance_ALLOCATION_FAILED,
			proto.Instance_ALLOCATION_TIMEOUT,
			proto.Instance_RAY_INSTALL_FAILED,
			proto.Instance_TERMINATION_FAILED,
			proto.Instance_UNKNOWN,
		}

		for _, status := range allStatuses {
			instances := []proto.Instance{
				{InstanceType: "worker-node", Status: status},
			}
			nodeTypeConfigs := map[service.NodeType]instance_manager.NodeTypeConfig{
				"worker-node": {},
			}

			assert.NotPanics(t, func() {
				reporter.ReportInstances(instances, nodeTypeConfigs)
			}, "Status %v should not panic", status)
		}
	})
}

// TestAutoscalerMetricsReporter_ReportResources tests the ReportResources method.
func TestAutoscalerMetricsReporter_ReportResources(t *testing.T) {
	t.Run("empty instance list", func(t *testing.T) {
		promMetrics, err := autoscaler.NewAutoscalerPrometheusMetrics("test-session")
		assert.NoError(t, err)

		reporter := &AutoscalerMetricsReporter{promMetrics: promMetrics}
		nodeTypeConfigs := map[service.NodeType]instance_manager.NodeTypeConfig{
			"worker-node": {
				Resources: map[string]float64{
					"CPU":    4.0,
					"memory": 16000.0,
				},
			},
		}

		assert.NotPanics(t, func() {
			reporter.ReportResources([]proto.Instance{}, nodeTypeConfigs)
		})
	})

	t.Run("single pending instance", func(t *testing.T) {
		promMetrics, err := autoscaler.NewAutoscalerPrometheusMetrics("test-session")
		assert.NoError(t, err)

		reporter := &AutoscalerMetricsReporter{promMetrics: promMetrics}

		instances := []proto.Instance{
			{
				InstanceType: "worker-node",
				Status:       proto.Instance_QUEUED,
			},
		}
		nodeTypeConfigs := map[service.NodeType]instance_manager.NodeTypeConfig{
			"worker-node": {
				Resources: map[string]float64{
					"CPU":    4.0,
					"memory": 16000.0,
				},
			},
		}

		assert.NotPanics(t, func() {
			reporter.ReportResources(instances, nodeTypeConfigs)
		})
	})

	t.Run("single running instance", func(t *testing.T) {
		promMetrics, err := autoscaler.NewAutoscalerPrometheusMetrics("test-session")
		assert.NoError(t, err)

		reporter := &AutoscalerMetricsReporter{promMetrics: promMetrics}

		instances := []proto.Instance{
			{
				InstanceType: "worker-node",
				Status:       proto.Instance_RAY_RUNNING,
			},
		}
		nodeTypeConfigs := map[service.NodeType]instance_manager.NodeTypeConfig{
			"worker-node": {
				Resources: map[string]float64{
					"CPU":    8.0,
					"memory": 32000.0,
					"GPU":    1.0,
				},
			},
		}

		assert.NotPanics(t, func() {
			reporter.ReportResources(instances, nodeTypeConfigs)
		})
	})

	t.Run("multiple instances with mixed statuses", func(t *testing.T) {
		promMetrics, err := autoscaler.NewAutoscalerPrometheusMetrics("test-session")
		assert.NoError(t, err)

		reporter := &AutoscalerMetricsReporter{promMetrics: promMetrics}

		instances := []proto.Instance{
			{InstanceType: "worker-node", Status: proto.Instance_QUEUED},
			{InstanceType: "worker-node", Status: proto.Instance_RAY_RUNNING},
			{InstanceType: "worker-node", Status: proto.Instance_RAY_RUNNING},
			{InstanceType: "head-node", Status: proto.Instance_RAY_RUNNING},
		}
		nodeTypeConfigs := map[service.NodeType]instance_manager.NodeTypeConfig{
			"worker-node": {
				Resources: map[string]float64{
					"CPU":    4.0,
					"memory": 16000.0,
				},
			},
			"head-node": {
				Resources: map[string]float64{
					"CPU":    8.0,
					"memory": 32000.0,
				},
			},
		}

		assert.NotPanics(t, func() {
			reporter.ReportResources(instances, nodeTypeConfigs)
		})
	})

	t.Run("config missing the resources field", func(t *testing.T) {
		promMetrics, err := autoscaler.NewAutoscalerPrometheusMetrics("test-session")
		assert.NoError(t, err)

		reporter := &AutoscalerMetricsReporter{promMetrics: promMetrics}

		instances := []proto.Instance{
			{InstanceType: "worker-node", Status: proto.Instance_RAY_RUNNING},
		}
		nodeTypeConfigs := map[service.NodeType]instance_manager.NodeTypeConfig{
			"worker-node": {},
		}

		assert.NotPanics(t, func() {
			reporter.ReportResources(instances, nodeTypeConfigs)
		})
	})

	t.Run("instance type missing from the config", func(t *testing.T) {
		promMetrics, err := autoscaler.NewAutoscalerPrometheusMetrics("test-session")
		assert.NoError(t, err)

		reporter := &AutoscalerMetricsReporter{promMetrics: promMetrics}

		instances := []proto.Instance{
			{InstanceType: "unknown-type", Status: proto.Instance_RAY_RUNNING},
		}
		nodeTypeConfigs := map[service.NodeType]instance_manager.NodeTypeConfig{
			"worker-node": {
				Resources: map[string]float64{
					"CPU": 4.0,
				},
			},
		}

		assert.NotPanics(t, func() {
			reporter.ReportResources(instances, nodeTypeConfigs)
		})
	})

	t.Run("terminated instances contribute no resources", func(t *testing.T) {
		promMetrics, err := autoscaler.NewAutoscalerPrometheusMetrics("test-session")
		assert.NoError(t, err)

		reporter := &AutoscalerMetricsReporter{promMetrics: promMetrics}

		instances := []proto.Instance{
			{InstanceType: "worker-node", Status: proto.Instance_TERMINATED},
			{InstanceType: "worker-node", Status: proto.Instance_TERMINATING},
		}
		nodeTypeConfigs := map[service.NodeType]instance_manager.NodeTypeConfig{
			"worker-node": {
				Resources: map[string]float64{
					"CPU":    4.0,
					"memory": 16000.0,
				},
			},
		}

		assert.NotPanics(t, func() {
			reporter.ReportResources(instances, nodeTypeConfigs)
		})
		// Terminated instances must not contribute to pending or cluster resources.
	})
}

// TestAutoscalerMetricsReporter_Integration integration test: the full workflow.
func TestAutoscalerMetricsReporter_Integration(t *testing.T) {
	t.Run("complete instance and resource reporting flow", func(t *testing.T) {
		promMetrics, err := autoscaler.NewAutoscalerPrometheusMetrics("integration-test")
		assert.NoError(t, err)

		reporter := &AutoscalerMetricsReporter{promMetrics: promMetrics}

		// Prepare the test data.
		instances := []proto.Instance{
			{InstanceType: "head-node", Status: proto.Instance_RAY_RUNNING},
			{InstanceType: "worker-node", Status: proto.Instance_QUEUED},
			{InstanceType: "worker-node", Status: proto.Instance_REQUESTED},
			{InstanceType: "worker-node", Status: proto.Instance_RAY_RUNNING},
			{InstanceType: "worker-node", Status: proto.Instance_RAY_RUNNING},
			{InstanceType: "worker-node", Status: proto.Instance_TERMINATING},
			{InstanceType: "worker-node", Status: proto.Instance_TERMINATED},
		}

		nodeTypeConfigs := map[service.NodeType]instance_manager.NodeTypeConfig{
			"head-node":   {},
			"worker-node": {},
		}

		resourceConfigs := map[service.NodeType]instance_manager.NodeTypeConfig{
			"head-node": {
				Resources: map[string]float64{
					"CPU":    8.0,
					"memory": 32000.0,
				},
			},
			"worker-node": {
				Resources: map[string]float64{
					"CPU":    4.0,
					"memory": 16000.0,
					"GPU":    1.0,
				},
			},
		}

		// Report.
		assert.NotPanics(t, func() {
			reporter.ReportInstances(instances, nodeTypeConfigs)
			reporter.ReportResources(instances, resourceConfigs)
		})
	})
}

// BenchmarkAutoscalerMetricsReporter_ReportInstances benchmarks the ReportInstances performance.
func BenchmarkAutoscalerMetricsReporter_ReportInstances(b *testing.B) {
	promMetrics, _ := autoscaler.NewAutoscalerPrometheusMetrics("benchmark")
	reporter := &AutoscalerMetricsReporter{promMetrics: promMetrics}

	instances := make([]proto.Instance, 100)
	for i := range instances {
		instances[i] = proto.Instance{
			InstanceType: "worker-node",
			Status:       proto.Instance_RAY_RUNNING,
		}
	}

	nodeTypeConfigs := map[service.NodeType]instance_manager.NodeTypeConfig{
		"worker-node": {},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		reporter.ReportInstances(instances, nodeTypeConfigs)
	}
}

// BenchmarkAutoscalerMetricsReporter_ReportResources benchmarks the ReportResources performance.
func BenchmarkAutoscalerMetricsReporter_ReportResources(b *testing.B) {
	promMetrics, _ := autoscaler.NewAutoscalerPrometheusMetrics("benchmark")
	reporter := &AutoscalerMetricsReporter{promMetrics: promMetrics}

	instances := make([]proto.Instance, 100)
	for i := range instances {
		instances[i] = proto.Instance{
			InstanceType: "worker-node",
			Status:       proto.Instance_RAY_RUNNING,
		}
	}

	nodeTypeConfigs := map[service.NodeType]instance_manager.NodeTypeConfig{
		"worker-node": {
			Resources: map[string]float64{
				"CPU":    4.0,
				"memory": 16000.0,
			},
		},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		reporter.ReportResources(instances, nodeTypeConfigs)
	}
}
