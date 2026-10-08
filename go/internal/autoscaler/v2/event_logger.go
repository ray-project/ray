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
	"fmt"
	"strings"

	"github.com/ray-project/ray/go/internal/event"
	"github.com/ray-project/ray/go/pkg/log"
	"github.com/ray-project/ray/go/proto"
)

// AutoscalerEventLogger is the autoscaler event logger.
// It records the events related to the autoscaler, including:
// - Node launch/termination operations
// - Cluster resource changes
// - Warnings about infeasible resource requests
type AutoscalerEventLogger struct {
	logger *event.EventLoggerAdapter
}

// NewAutoscalerEventLogger creates the autoscaler event logger.
func NewAutoscalerEventLogger(logger *event.EventLoggerAdapter) (*AutoscalerEventLogger, error) {
	return &AutoscalerEventLogger{
		logger: logger,
	}, nil
}

// LogClusterSchedulingUpdate records an autoscaler scheduling state update.
// It emits the following kinds of log entries:
// - INFO: node launch and termination events (aggregated by node type)
// - INFO: cluster resource summary after scaling (CPU/GPU/TPU totals)
// - WARNING: infeasible resource requests, gang requests and cluster constraints
//
// Parameters:
// - clusterResources: the current total cluster resources, keyed by resource
//   name (e.g. "CPU") with the quantity as value
// - launchRequests: the node launch requests of this scheduling step
// - terminateRequests: the node termination requests of this scheduling step
// - infeasibleRequests: the individual resource requests that cannot be satisfied
// - infeasibleGangRequests: the gang/placement-group requests that cannot be scheduled
// - infeasibleClusterResourceConstraints: the cluster-level resource constraints
//   that cannot be satisfied
func (ael *AutoscalerEventLogger) LogClusterSchedulingUpdate(
	clusterResources map[string]float64,
	launchRequests []*proto.LaunchRequest,
	terminateRequests []*proto.TerminationRequest,
	infeasibleRequests []*proto.ResourceRequest,
	infeasibleGangRequests []*proto.GangResourceRequest,
	infeasibleClusterResourceConstraints []*proto.ClusterResourceConstraint,
) error {
	// Record node launch events.
	if len(launchRequests) > 0 {
		// Count the launched nodes per node type.
		launchTypeCount := make(map[string]int32)
		for _, req := range launchRequests {
			launchTypeCount[req.InstanceType] += req.Count
		}

		// Emit a launch log entry per node type.
		for instanceType, count := range launchTypeCount {
			logStr := fmt.Sprintf("Adding %d node(s) of type %s.", count, instanceType)
			// Dual channel: write to both the structured event log and the
			// standard log.
			ael.logger.Info(logStr)
			log.Log.V(1).Info(logStr)
		}
	}

	// Record node termination events.
	if len(terminateRequests) > 0 {
		// Count the terminated nodes per (termination reason, node type).
		terminationByCausesAndType := make(map[[2]string]int)
		for _, req := range terminateRequests {
			key := [2]string{req.Cause.String(), req.InstanceType}
			terminationByCausesAndType[key]++
		}

		// Mapping from the termination reason enum to human-readable text.
		causeReasonMap := map[proto.TerminationRequest_Cause]string{
			proto.TerminationRequest_OUTDATED:              "outdated",
			proto.TerminationRequest_MAX_NUM_NODES:         "max number of worker nodes reached",
			proto.TerminationRequest_MAX_NUM_NODE_PER_TYPE: "max number of worker nodes per type reached",
			proto.TerminationRequest_IDLE:                  "idle",
		}

		// Emit a termination log entry per (reason, type) pair.
		for key, count := range terminationByCausesAndType {
			cause := proto.TerminationRequest_Cause(proto.TerminationRequest_Cause_value[key[0]])
			instanceType := key[1]
			reason, ok := causeReasonMap[cause]
			if !ok {
				reason = strings.ToLower(key[0])
			}
			logStr := fmt.Sprintf("Removing %d nodes of type %s (%s).", count, instanceType, reason)
			ael.logger.Info(logStr)
			log.Log.V(1).Info(logStr)
		}
	}

	// Record cluster shape changes (whenever there is a launch or a termination).
	if len(launchRequests) > 0 || len(terminateRequests) > 0 {
		// Total CPU resources (defaults to 0).
		numCPUs := clusterResources["CPU"]
		logStr := fmt.Sprintf("Resized to %d CPUs", int(numCPUs))

		// Append the GPU count when GPU resources exist.
		if gpuCount, ok := clusterResources["GPU"]; ok {
			logStr += fmt.Sprintf(", %d GPUs", int(gpuCount))
		}
		// Append the TPU count when TPU resources exist.
		if tpuCount, ok := clusterResources["TPU"]; ok {
			logStr += fmt.Sprintf(", %d TPUs", int(tpuCount))
		}

		// Record the scaling summary (INFO).
		ael.logger.Info(logStr + ".")
		// Record the detailed resource dict (DEBUG).
		ael.logger.Debug(fmt.Sprintf("Current cluster resources: %v.", clusterResources))
	}

	// Record infeasible individual resource requests.
	if len(infeasibleRequests) > 0 {
		// Aggregate identical resource requests with the utility type.
		requestsByCount := ResourceRequestUtil.groupByCount(infeasibleRequests)
		logStr := "No available node types can fulfill resource requests "

		// Iterate over the aggregated requests to build the resource map string.
		for idx, reqCount := range requestsByCount {
			resourceMap := ResourceRequestUtil.toResourceMap(reqCount.Request)
			logStr += fmt.Sprintf("%v*%d", resourceMap, reqCount.Count)

			// Add the separator (except after the last element).
			if idx < len(requestsByCount)-1 {
				logStr += ", "
			}

			// Parse and record label selector details when present.
			if len(reqCount.Request.LabelSelectors) > 0 {
				selectorStrs := []string{}
				for _, selector := range reqCount.Request.LabelSelectors {
					for _, constraint := range selector.LabelConstraints {
						// The enum name of the operator (e.g. "LABEL_OPERATOR_IN").
						op := constraint.Operator.String()
						// Join the label values with commas.
						values := strings.Join(constraint.LabelValues, ",")
						selectorStrs = append(selectorStrs,
							fmt.Sprintf("%s %s [%s]", constraint.LabelKey, op, values))
					}
				}
				if len(selectorStrs) > 0 {
					logStr += " with label selectors: [" + strings.Join(selectorStrs, "; ") + "]"
				}
			}
		}

		// Append the resolution hint.
		logStr += ". Add suitable node types to this cluster to resolve this issue."
		// Record the infeasible requests at WARNING level.
		ael.logger.Warning(logStr)
	}

	// Record infeasible gang/placement-group requests.
	if len(infeasibleGangRequests) > 0 {
		// Log each placement-group request separately.
		for _, gangRequest := range infeasibleGangRequests {
			// Build the base message with the request details.
			logStr := fmt.Sprintf(
				"No available node types can fulfill placement group requests (detail=%s): ",
				gangRequest.Details,
			)
			// Aggregate the resource requests inside the placement group.
			requestsByCount := ResourceRequestUtil.groupByCount(gangRequest.Requests)
			// Iterate over the aggregated requests to build the resource map string.
			for idx, reqCount := range requestsByCount {
				resourceMap := ResourceRequestUtil.toResourceMap(reqCount.Request)
				logStr += fmt.Sprintf("%v*%d", resourceMap, reqCount.Count)
				if idx < len(requestsByCount)-1 {
					logStr += ", "
				}
			}

			// Append the resolution hint.
			logStr += ". Add suitable node types to this cluster to resolve this issue."
			// Record at WARNING level.
			ael.logger.Warning(logStr)
		}
	}

	// Record infeasible cluster-level resource constraints.
	if len(infeasibleClusterResourceConstraints) > 0 {
		// At most one cluster resource constraint exists today (from the latest
		// request_resources() SDK call).
		for _, infeasibleConstraint := range infeasibleClusterResourceConstraints {
			logStr := "No available node types can fulfill cluster constraint: "
			// Iterate over the resource request list of the constraint.
			for i, requestsByCount := range infeasibleConstraint.ResourceRequests {
				// Convert the ResourceRequest into a resource map.
				resourceMap := ResourceRequestUtil.toResourceMap(requestsByCount.Request)
				logStr += fmt.Sprintf("%v*%d", resourceMap, requestsByCount.Count)
				if i < len(infeasibleConstraint.ResourceRequests)-1 {
					logStr += ", "
				}
			}

			// Append the resolution hint.
			logStr += ". Add suitable node types to this cluster to resolve this issue."
			// Record at WARNING level.
			ael.logger.Warning(logStr)
		}
	}

	return nil
}
