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
	"github.com/ray-project/ray/go/internal/autoscaler"
	im "github.com/ray-project/ray/go/internal/autoscaler/v2/instance_manager"
	"github.com/ray-project/ray/go/internal/autoscaler/v2/service"
	"github.com/ray-project/ray/go/proto"
)

// AutoscalerMetricsReporter reports instance metrics to Prometheus.
type AutoscalerMetricsReporter struct {
	promMetrics *autoscaler.AutoscalerPrometheusMetrics
}

// NewAutoscalerMetricsReporter creates a new metrics reporter.
func NewAutoscalerMetricsReporter(promMetrics *autoscaler.AutoscalerPrometheusMetrics) (*AutoscalerMetricsReporter, error) {
	return &AutoscalerMetricsReporter{
		promMetrics: promMetrics,
	}, nil
}

// ReportInstances records the autoscaler instance status metrics to Prometheus.
func (amr *AutoscalerMetricsReporter) ReportInstances(
	instances []proto.Instance,
	nodeTypeConfigs map[service.NodeType]im.NodeTypeConfig,
) error {
	// Per-instance-type status counters:
	// map[instance_type]map[string]int{"pending": count, "running": count, "terminating": count, "terminated": count}
	statusCountByType := make(map[service.NodeType]map[string]int)

	// Initialize the status counters of every configured instance type to 0.
	for instanceType := range nodeTypeConfigs {
		statusCountByType[instanceType] = map[string]int{
			"pending":     0, // Instances waiting to be started
			"running":     0, // Running instances
			"terminating": 0, // Instances being terminated (treated as recently failed)
			"terminated":  0, // Terminated instances (cumulative)
		}
	}

	// Traverse all instances and classify them into the status counters.
	// The read-only provider assigns anonymous node types (e.g. "node_<id>")
	// that may not exist in nodeTypeConfigs when a config file is provided;
	// lazily initialize their counters so instance metrics are still reported.
	for i := range instances {
		instance := &instances[i]
		if _, ok := statusCountByType[instance.InstanceType]; !ok {
			statusCountByType[instance.InstanceType] = map[string]int{
				"pending":     0, // Number of instances waiting to be started
				"running":     0, // Number of running instances
				"terminating": 0, // Number of instances being terminated (treated as recently failed)
				"terminated":  0, // Number of terminated instances (cumulative)
			}
		}

		if im.InstanceUtil.IsRayPending(instance.Status) {
			statusCountByType[instance.InstanceType]["pending"]++
		} else if im.InstanceUtil.IsRayRunning(instance.Status) {
			statusCountByType[instance.InstanceType]["running"]++
		} else if instance.Status == proto.Instance_TERMINATING {
			statusCountByType[instance.InstanceType]["terminating"]++
		} else if instance.Status == proto.Instance_TERMINATED {
			statusCountByType[instance.InstanceType]["terminated"]++
		}
	}

	// Publish the counters as Prometheus metrics.
	for instanceType, statusCount := range statusCountByType {
		// Pending nodes gauge (current instantaneous value).
		amr.promMetrics.PendingNodesVec.WithLabelValues(
			instanceType,
			amr.promMetrics.SessionName,
		).Set(float64(statusCount["pending"]))

		// Active nodes gauge (current instantaneous value).
		amr.promMetrics.ActiveNodesVec.WithLabelValues(
			instanceType,
			amr.promMetrics.SessionName,
		).Set(float64(statusCount["running"]))

		// Recently failed nodes gauge (may reset at undefined points).
		amr.promMetrics.RecentlyFailedNodesVec.WithLabelValues(
			instanceType,
			amr.promMetrics.SessionName,
		).Set(float64(statusCount["terminating"]))

		// Stopped nodes counter (accumulates the historical total).
		amr.promMetrics.StoppedNodes.Add(float64(statusCount["terminated"]))
	}
	return nil
}

// ReportResources records the resource-related autoscaler metrics, covering
// pending_resources and cluster_resources.
func (amr *AutoscalerMetricsReporter) ReportResources(
	instances []proto.Instance,
	nodeTypeConfigs map[service.NodeType]im.NodeTypeConfig,
) {
	// pending resources
	pendingResources := make(map[string]float64)
	// cluster resources
	clusterResources := make(map[string]float64)

	// Helper: add the resources of the given number of nodes to a resource map.
	addResources := func(resourceMap map[string]float64, nodeType service.NodeType, count float64) {
		resources := nodeTypeConfigs[nodeType].Resources
		for resourceName, resourceValue := range resources {
			if _, ok := resourceMap[resourceName]; !ok {
				resourceMap[resourceName] = 0.0
			}
			resourceMap[resourceName] += resourceValue * count
		}
	}

	for i := range instances {
		instance := &instances[i]
		if im.InstanceUtil.IsRayPending(instance.Status) {
			// Pending instances count toward pending_resources.
			addResources(pendingResources, instance.InstanceType, 1.0)
		} else if im.InstanceUtil.IsRayRunning(instance.Status) {
			// Running instances count toward cluster_resources.
			addResources(clusterResources, instance.InstanceType, 1.0)
		}
	}

	// Publish pending_resources.
	for resourceName, resourceValue := range pendingResources {
		amr.promMetrics.PendingResourcesVec.WithLabelValues(
			resourceName,
			amr.promMetrics.SessionName,
		).Set(resourceValue)
	}

	// Publish cluster_resources.
	for resourceName, resourceValue := range clusterResources {
		amr.promMetrics.ClusterResourcesVec.WithLabelValues(
			resourceName,
			amr.promMetrics.SessionName,
		).Set(resourceValue)
	}
}
