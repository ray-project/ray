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

package scheduler

import (
	"math"

	"github.com/google/uuid"
	"github.com/ray-project/ray/go/internal/autoscaler/v2/instance_manager"
	"github.com/ray-project/ray/go/pkg/log"
	"github.com/ray-project/ray/go/proto"
)

// SchedulingNode is the node abstraction scheduled by the resource scheduler.
// Corresponds to Python scheduler.SchedulingNode (scheduler.py:137-693).
// Typical usage:
//
//	node = SchedulingNode.NewNodeFromInstance(instance, nodeConfigs, disableCheck)
//	remaining, score = node.TrySchedule(requests, source)
//	... use score ...
type SchedulingNode struct {
	// NodeType is the node type name.
	NodeType string
	// Status is the node status.
	Status SchedulingNodeStatus
	// TotalResources is the node's resource capacity.
	TotalResources map[string]float64
	// Labels are the node labels (static or dynamic; dynamic labels are a
	// deprecated feature kept only for the autoscaler's strict-spread
	// placement group scheduling (antiaffinity)).
	Labels map[string]string
	// LaunchReason is an observable description of why the node was launched.
	LaunchReason string
	// TerminationRequest is the termination request, nil when the node was not
	// terminated.
	TerminationRequest *proto.TerminationRequest
	// IMInstanceID is the instance ID in the instance manager, empty when the
	// instance has not entered the IM yet.
	IMInstanceID string
	// IMInstanceStatus is the IM instance status, the zero value when it has
	// not been assigned to an IM instance yet.
	IMInstanceStatus proto.Instance_InstanceStatus
	// RayNodeID is the Ray node ID, empty when the node has not shown up in
	// the GCS report (not running Ray).
	RayNodeID string
	// IdleDurationMS is the idle duration in milliseconds, non-idle by default.
	IdleDurationMS int64
	// LaunchConfigHash is the hash of the node's launch config.
	LaunchConfigHash string
	// NodeKind is the node kind.
	NodeKind proto.NodeKind

	// schedRequests holds the scheduled resource requests per source.
	schedRequests map[ResourceRequestSource][]*proto.ResourceRequest
	// availableForSched holds the available resources per source.
	availableForSched map[ResourceRequestSource]map[string]float64
}

// NewSchedulingNode creates a scheduling node.
func NewSchedulingNode(
	nodeType string,
	totalResources map[string]float64,
	availableResources map[string]float64,
	labels map[string]string,
	status SchedulingNodeStatus,
	imInstanceID string,
	imInstanceStatus proto.Instance_InstanceStatus,
	rayNodeID string,
	idleDurationMS int64,
	launchConfigHash string,
	nodeKind proto.NodeKind,
	terminationRequest *proto.TerminationRequest,
) *SchedulingNode {
	node := &SchedulingNode{
		NodeType:           nodeType,
		TotalResources:     totalResources,
		Labels:             labels,
		Status:             status,
		IMInstanceID:       imInstanceID,
		IMInstanceStatus:   imInstanceStatus,
		RayNodeID:          rayNodeID,
		IdleDurationMS:     idleDurationMS,
		LaunchConfigHash:   launchConfigHash,
		NodeKind:           nodeKind,
		TerminationRequest: terminationRequest,
		schedRequests: map[ResourceRequestSource][]*proto.ResourceRequest{
			PendingDemand:             {},
			ClusterResourceConstraint: {},
		},
		availableForSched: map[ResourceRequestSource]map[string]float64{
			// The demand source uses the actually available resources.
			PendingDemand: cloneFloatMap(availableResources),
			// The cluster constraint source uses the total resources
			// (constraints usually require whole-node resources).
			ClusterResourceConstraint: cloneFloatMap(totalResources),
		},
	}
	return node
}

// GetAvailableResources returns the available resources of the given source.
func (n *SchedulingNode) GetAvailableResources(source ResourceRequestSource) map[string]float64 {
	return n.availableForSched[source]
}

// GetSchedRequests returns the scheduled resource requests of the given source.
func (n *SchedulingNode) GetSchedRequests(source ResourceRequestSource) []*proto.ResourceRequest {
	return n.schedRequests[source]
}

// AddSchedRequest adds a resource request to the node.
func (n *SchedulingNode) AddSchedRequest(request *proto.ResourceRequest, source ResourceRequestSource) {
	n.schedRequests[source] = append(n.schedRequests[source], request)
}

// IsSchedulable reports whether the instance can be scheduled by the IM.
// Corresponds to Python SchedulingNode.is_schedulable:
//   - Instances that have not entered the IM are not schedulable (out-of-band
//     Ray nodes, cloud instances not yet discovered by the Reconciler, and
//     cloud instances already terminated whose Ray status lags behind are all
//     excluded from scheduling).
//   - Instances whose status can transition to RAY_RUNNING are schedulable.
func IsSchedulable(instance *AutoscalerInstance) bool {
	if instance.IMInstance == nil {
		return false
	}
	return instance_manager.InstanceUtil.IsRayRunningReachable(instance.IMInstance.Status)
}

// NewNodeFromInstance creates a scheduling node from an autoscaler instance.
// Corresponds to Python SchedulingNode.new:
//   - Returns nil when the instance is not schedulable.
//   - Instances running Ray: builds the schedulable node from the Ray node's
//     live resources/labels.
//   - Instances pending Ray: builds the schedulable node from the node type
//     config; when the config is missing and the launch config check is not
//     disabled, returns a node to terminate (OUTDATED).
func NewNodeFromInstance(
	instance *AutoscalerInstance,
	nodeTypeConfigs map[string]*instance_manager.NodeTypeConfig,
	disableLaunchConfigCheck bool,
) *SchedulingNode {
	if !IsSchedulable(instance) {
		return nil
	}

	if instance.IMInstance.Status == proto.Instance_RAY_RUNNING {
		if instance.RayNode == nil {
			// Python asserts ray_node is not nil; Go defensively returns nil
			// and logs instead of crashing.
			log.Log.Error(nil, "ray node should not be nil when the instance is running ray",
				"instanceId", instance.IMInstance.InstanceId)
			return nil
		}
		// Running Ray node.
		labels := make(map[string]string)
		for k, v := range instance.RayNode.Labels {
			labels[k] = v
		}
		// DEPRECATED: dynamic labels are a deprecated feature kept only for
		// strict-spread placement group scheduling.
		for k, v := range instance.RayNode.DynamicLabels {
			labels[k] = v
		}
		return NewSchedulingNode(
			instance.IMInstance.InstanceType,
			cloneFloatMap(instance.RayNode.TotalResources),
			cloneFloatMap(instance.RayNode.AvailableResources),
			labels,
			Schedulable,
			instance.IMInstance.InstanceId,
			instance.IMInstance.Status,
			instance.IMInstance.GetNodeId(),
			instance.RayNode.IdleDurationMs,
			instance.IMInstance.LaunchConfigHash,
			instance.IMInstance.NodeKind,
			nil,
		)
	}

	// Instance pending Ray: build from the node type config.
	nodeConfig := nodeTypeConfigs[instance.IMInstance.InstanceType]
	if nodeConfig == nil {
		if disableLaunchConfigCheck {
			log.Log.Info("Node config is missing, but not terminating the outdated node because disable_launch_config_check is enabled",
				"nodeType", instance.IMInstance.InstanceType)
			return nil
		}
		// The config may have been updated and this node type no longer has a
		// config, so the node should be terminated.
		return NewSchedulingNode(
			instance.IMInstance.InstanceType,
			map[string]float64{},
			map[string]float64{},
			map[string]string{},
			ToTerminate,
			instance.IMInstance.InstanceId,
			instance.IMInstance.Status,
			"",
			0,
			"",
			proto.NodeKind_WORKER,
			&proto.TerminationRequest{
				Id:             uuid.NewString(),
				InstanceId:     instance.IMInstance.InstanceId,
				InstanceStatus: instance.IMInstance.Status,
				Cause:          proto.TerminationRequest_OUTDATED,
				InstanceType:   instance.IMInstance.InstanceType,
			},
		)
	}

	return FromNodeConfig(
		nodeConfig,
		Schedulable,
		instance.IMInstance.NodeKind,
		instance.IMInstance.InstanceId,
		instance.IMInstance.Status,
	)
}

// FromNodeConfig creates a scheduling node from a node type config.
// Corresponds to Python SchedulingNode.from_node_config.
func FromNodeConfig(
	nodeConfig *instance_manager.NodeTypeConfig,
	status SchedulingNodeStatus,
	nodeKind proto.NodeKind,
	imInstanceID string,
	imInstanceStatus proto.Instance_InstanceStatus,
) *SchedulingNode {
	return NewSchedulingNode(
		string(nodeConfig.Name),
		cloneFloatMap(nodeConfig.Resources),
		cloneFloatMap(nodeConfig.Resources),
		cloneStringMap(nodeConfig.Labels),
		status,
		imInstanceID,
		imInstanceStatus,
		"",
		0,
		"",
		nodeKind,
		nil,
	)
}

// TrySchedule attempts to schedule the resource requests on the node.
// Corresponds to Python SchedulingNode.try_schedule:
// requests are scheduled one by one in sorted order without backtracking;
// requests that cannot be scheduled are returned as remaining.
// If schedulable, the node's available resources are mutated.
func (n *SchedulingNode) TrySchedule(requests []*proto.ResourceRequest, source ResourceRequestSource) ([]*proto.ResourceRequest, UtilizationScore) {
	unschedulable := make([]*proto.ResourceRequest, 0)
	for _, r := range requests {
		if !n.tryScheduleOne(r, source) {
			unschedulable = append(unschedulable, r)
		}
	}
	score := n.computeScore(source)
	return unschedulable, score
}

// computeScore computes the node's utilization score.
// Corresponds to Python SchedulingNode._compute_score; see the UtilizationScore
// doc for the score semantics.
func (n *SchedulingNode) computeScore(source ResourceRequestSource) UtilizationScore {
	schedRequests := n.GetSchedRequests(source)
	available := n.GetAvailableResources(source)

	// Count the number of scheduled resource types.
	numMatching := 0
	schedResourceTypes := map[string]bool{}
	for _, req := range schedRequests {
		for name, v := range req.ResourcesBundle {
			if v > 0 {
				schedResourceTypes[name] = true
			}
		}
	}
	for t := range schedResourceTypes {
		if _, ok := n.TotalResources[t]; ok {
			numMatching++
		}
	}

	// Compute the utilization per resource type (v1's
	// _resource_based_utilization_scorer).
	utilByResources := make([]float64, 0)
	for k, v := range n.TotalResources {
		if v == 0 {
			continue
		}
		if _, ok := available[k]; ok {
			util := (v - available[k]) / v
			utilByResources = append(utilByResources, v*math.Pow(util, 3))
		}
	}

	// Prefer not launching GPU nodes for requests without GPU demand.
	gpuOK := true
	if conserveGpuNodes {
		isGPUNode := n.TotalResources["GPU"] > 0
		anyGPURequests := false
		for _, r := range schedRequests {
			if _, ok := r.ResourcesBundle["GPU"]; ok {
				anyGPURequests = true
				break
			}
		}
		if isGPUNode && !anyGPURequests {
			gpuOK = false
		}
	}

	matchesLabels := n.satisfiesLabelConstraints(schedRequests)

	score := UtilizationScore{
		MatchesLabels:            matchesLabels,
		GpuOK:                    gpuOK,
		NumMatchingResourceTypes: numMatching,
	}
	if len(utilByResources) > 0 {
		minUtil := utilByResources[0]
		sum := 0.0
		for _, u := range utilByResources {
			if u < minUtil {
				minUtil = u
			}
			sum += u
		}
		score.MinUtilization = minUtil
		score.AvgUtilization = sum / float64(len(utilByResources))
	}
	return score
}

// satisfiesLabelConstraints returns the label selector priority satisfied by
// the node.
// Corresponds to Python SchedulingNode._satisfies_label_constraints:
// returns numSelectors-i when the i-th (0-based) selector is satisfied (the
// earlier the selector, the higher the priority).
func (n *SchedulingNode) satisfiesLabelConstraints(requests []*proto.ResourceRequest) int {
	for _, req := range requests {
		numSelectors := len(req.LabelSelectors)
		for i, selector := range req.LabelSelectors {
			allPass := true
			for _, constraint := range selector.LabelConstraints {
				key := constraint.LabelKey
				values := make(map[string]bool, len(constraint.LabelValues))
				for _, v := range constraint.LabelValues {
					values[v] = true
				}
				nodeVal, hasKey := n.Labels[key]
				switch constraint.Operator {
				case proto.LabelSelectorOperator_LABEL_OPERATOR_IN:
					if !hasKey || !values[nodeVal] {
						allPass = false
					}
				case proto.LabelSelectorOperator_LABEL_OPERATOR_NOT_IN:
					if hasKey && values[nodeVal] {
						allPass = false
					}
				default:
					allPass = false
				}
				if !allPass {
					break
				}
			}
			if allPass {
				return numSelectors - i
			}
		}
	}
	return 0
}

// tryScheduleOne attempts to schedule a single resource request on the node.
// Corresponds to Python SchedulingNode._try_schedule_one.
func (n *SchedulingNode) tryScheduleOne(request *proto.ResourceRequest, source ResourceRequestSource) bool {
	// Mandatory label selector constraints.
	if len(request.LabelSelectors) > 0 && n.satisfiesLabelConstraints([]*proto.ResourceRequest{request}) == 0 {
		return false // the node satisfies none of the request's label selectors
	}

	// Check unsatisfied placement constraints.
	for _, constraint := range request.PlacementConstraints {
		if constraint.AntiAffinity != nil {
			antiAffinity := constraint.AntiAffinity
			if val, ok := n.Labels[antiAffinity.LabelName]; ok && antiAffinity.LabelValue == val {
				// The node already has a label matching the anti-affinity.
				return false
			}
		}
		// Affinity constraints were already merged into the same request by
		// combine_requests_with_affinity, so nothing to check here.
	}

	available := n.GetAvailableResources(source)

	// Check that the resources suffice.
	if !fits(available, request.ResourcesBundle) {
		return false
	}

	// Schedule the request and update the resources.
	inplaceSubtract(available, request.ResourcesBundle)
	n.AddSchedRequest(request, source)

	// If the request carries placement group constraints, update the node
	// labels.
	for _, constraint := range request.PlacementConstraints {
		if constraint.AntiAffinity != nil {
			antiAffinity := constraint.AntiAffinity
			n.addLabel(antiAffinity.LabelName, antiAffinity.LabelValue)
		}
	}

	return true
}

// addLabel adds a label to the node (assuming one label key has one value).
// Corresponds to Python SchedulingNode._add_label (Python asserts no conflict;
// Go logs and keeps the old value so the whole autoscaler does not crash).
func (n *SchedulingNode) addLabel(labelName, labelValue string) {
	if old, ok := n.Labels[labelName]; ok && old != labelValue {
		log.Log.Info("Label already exists with a different value, cannot set",
			"labelName", labelName, "oldValue", old, "newValue", labelValue)
		return
	}
	n.Labels[labelName] = labelValue
}

// clone deep-copies the node (including maps and proto objects).
// Corresponds to Python copy.deepcopy(node); used during scheduling attempts
// to avoid polluting the original node.
func (n *SchedulingNode) clone() *SchedulingNode {
	c := *n
	c.TotalResources = cloneFloatMap(n.TotalResources)
	c.Labels = cloneStringMap(n.Labels)
	c.availableForSched = make(map[ResourceRequestSource]map[string]float64, len(n.availableForSched))
	for src, m := range n.availableForSched {
		c.availableForSched[src] = cloneFloatMap(m)
	}
	c.schedRequests = make(map[ResourceRequestSource][]*proto.ResourceRequest, len(n.schedRequests))
	for src, reqs := range n.schedRequests {
		c.schedRequests[src] = append([]*proto.ResourceRequest{}, reqs...)
	}
	if n.TerminationRequest != nil {
		c.TerminationRequest = n.TerminationRequest
	}
	return &c
}

func cloneFloatMap(m map[string]float64) map[string]float64 {
	c := make(map[string]float64, len(m))
	for k, v := range m {
		c[k] = v
	}
	return c
}

func cloneStringMap(m map[string]string) map[string]string {
	c := make(map[string]string, len(m))
	for k, v := range m {
		c[k] = v
	}
	return c
}
