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

// Package scheduler implements the resource scheduling service
// (ResourceSchedulerService).
// It corresponds to the Python side python/ray/autoscaler/v2/scheduler.py
// (1902 lines).
// This file holds the public types: requests/replies/interfaces/enums and the
// underlying helper functions, corresponding to scheduler.py:40-135 (module
// level data types) plus _fits/_inplace_subtract reused from v1 and
// utils.py's combine_requests_with_affinity.
//
// The scheduler answers one question: given the current cluster state and the
// resource demands, what is the target cluster shape?
// It is a pure computation module that touches no IO (it only optionally
// writes the event log), and is the dependency input of the Phase 6
// Reconciler's proactive scaling subtasks.
package scheduler

import (
	"reflect"
	"sort"
	"strings"

	"github.com/ray-project/ray/go/internal/autoscaler/v2/instance_manager"
	"github.com/ray-project/ray/go/proto"
)

// implicitResourcePrefix is the implicit resource prefix.
// Corresponds to Python ray._raylet.IMPLICIT_RESOURCE_PREFIX
// ("node:__internal_implicit_resource_").
// Implicit resources (such as CPU groups) default to 1 when a node does not
// declare them, used by _fits/_inplace_subtract.
const implicitResourcePrefix = "node:__internal_implicit_resource_"

// conserveGpuNodes controls whether launching GPU nodes for resource requests
// without GPU demand is avoided.
// Corresponds to Python AUTOSCALER_CONSERVE_GPU_NODES (default 1).
const conserveGpuNodes = true

// ResourceRequestSource is the source of a resource request.
// Corresponds to Python scheduler.ResourceRequestSource.
type ResourceRequestSource int

const (
	// PendingDemand is a regular demand from Ray tasks/actors/placement groups.
	PendingDemand ResourceRequestSource = iota
	// ClusterResourceConstraint is a cluster level resource constraint (such
	// as ray.autoscaler.sdk.request_resources).
	ClusterResourceConstraint
)

// SchedulingNodeStatus is the scheduling node status.
// Corresponds to Python scheduler.SchedulingNodeStatus.
type SchedulingNodeStatus int

const (
	// ToLaunch is a node newly added by the ResourceDemandScheduler, pending
	// launch.
	ToLaunch SchedulingNodeStatus = iota
	// Schedulable is an existing instance: running Ray or about to run Ray,
	// able to take new resource requests.
	Schedulable
	// ToTerminate is a node marked for termination by the
	// ResourceDemandScheduler.
	ToTerminate
)

// AutoscalerInstance represents an instance managed by the autoscaler.
// Corresponds to Python schema.AutoscalerInstance, carrying the instance
// manager's cloud instance state and the Ray node state.
// The possible combinations of IMInstance and RayNode are described in the
// Python comments (schema.py:261-270).
type AutoscalerInstance struct {
	// CloudInstanceID is the cloud instance ID, empty when no cloud instance
	// has been assigned yet.
	CloudInstanceID string
	// RayNode is the Ray node state, nil when no Ray is or was running.
	RayNode *proto.NodeState
	// IMInstance is the instance state in the instance manager.
	IMInstance *proto.Instance
}

// UtilizationScore is the node utilization score.
// Corresponds to the 5-tuple returned by Python
// scheduler.SchedulingNode._compute_score.
// A "higher" score means the node fits the current resource request better;
// scores compare lexicographically in descending order:
//  1. MatchesLabels: the label selector priority satisfied by the node
//     (0 means no selector is satisfied)
//  2. GpuOK: whether it is a GPU node while the request has no GPU demand
//     (avoid launching GPU nodes for workloads without GPU demand)
//  3. NumMatchingResourceTypes: the number of scheduled resource types
//  4. MinUtilization: the minimum utilization across all resource types
//  5. AvgUtilization: the average utilization across all resource types
type UtilizationScore struct {
	MatchesLabels            int
	GpuOK                    bool
	NumMatchingResourceTypes int
	MinUtilization           float64
	AvgUtilization           float64
}

// greater returns whether this score is strictly "higher" than other (used for
// descending sorts).
// The semantics match Python's lexicographic tuple comparison with
// reverse=True.
func (s UtilizationScore) greater(other UtilizationScore) bool {
	if s.MatchesLabels != other.MatchesLabels {
		return s.MatchesLabels > other.MatchesLabels
	}
	if s.GpuOK != other.GpuOK {
		return s.GpuOK
	}
	if s.NumMatchingResourceTypes != other.NumMatchingResourceTypes {
		return s.NumMatchingResourceTypes > other.NumMatchingResourceTypes
	}
	if s.MinUtilization != other.MinUtilization {
		return s.MinUtilization > other.MinUtilization
	}
	return s.AvgUtilization > other.AvgUtilization
}

// equals reports whether two scores are equal (the score together with cloud
// availability forms the full sort key).
func (s UtilizationScore) equals(other UtilizationScore) bool {
	return s.MatchesLabels == other.MatchesLabels &&
		s.GpuOK == other.GpuOK &&
		s.NumMatchingResourceTypes == other.NumMatchingResourceTypes &&
		s.MinUtilization == other.MinUtilization &&
		s.AvgUtilization == other.AvgUtilization
}

// SchedulingRequest is a scheduling request.
// Corresponds to Python scheduler.SchedulingRequest.
type SchedulingRequest struct {
	// DisableLaunchConfigCheck disables checking stale nodes via the launch
	// config.
	DisableLaunchConfigCheck bool
	// NodeTypeConfigs are the available node type configs.
	NodeTypeConfigs map[string]*instance_manager.NodeTypeConfig
	// NodeTypeOrder is the node type order (declaration order), matching the
	// Python dict insertion order.
	// The traversal order of node types during scheduling affects tie-breaking
	// (the earlier one wins on equal scores); when empty it falls back to the
	// sorted NodeTypeConfigs keys to stay deterministic.
	NodeTypeOrder []string
	// MaxNumNodes is the cluster-wide max number of worker nodes, nil means
	// unlimited.
	MaxNumNodes *int64
	// IdleTimeoutS is the idle timeout in seconds.
	IdleTimeoutS *float64
	// ResourceRequests are the Ray resource demands aggregated by count.
	ResourceRequests []*proto.ResourceRequestByCount
	// GangResourceRequests are the gang resource demands (placement groups).
	GangResourceRequests []*proto.GangResourceRequest
	// ClusterResourceConstraints are the cluster resource constraints.
	ClusterResourceConstraints []*proto.ClusterResourceConstraint
	// CurrentInstances is the current autoscaler instance snapshot.
	CurrentInstances []*AutoscalerInstance
	// CloudResourceAvailabilities are the cloud resource availability scores
	// per node type; a low score means the type recently failed allocation and
	// should be down-weighted during scheduling.
	CloudResourceAvailabilities map[string]float64
}

// SchedulingReply is a scheduling reply.
// Corresponds to Python scheduler.SchedulingReply.
type SchedulingReply struct {
	// ToLaunch is the set of nodes to launch (aggregated by node type).
	ToLaunch []*proto.LaunchRequest
	// ToTerminate is the set of nodes to terminate.
	ToTerminate []*proto.TerminationRequest
	// InfeasibleResourceRequests are the resource demands that cannot be
	// satisfied.
	InfeasibleResourceRequests []*proto.ResourceRequest
	// InfeasibleGangResourceRequests are the gang demands that cannot be
	// satisfied.
	InfeasibleGangResourceRequests []*proto.GangResourceRequest
	// InfeasibleClusterResourceConstraints are the cluster constraints that
	// cannot be satisfied.
	InfeasibleClusterResourceConstraints []*proto.ClusterResourceConstraint
}

// IResourceScheduler is the resource scheduler interface.
// Corresponds to Python scheduler.IResourceScheduler, implementing the
// ResourceSchedulerService interface of instance_manager.proto.
type IResourceScheduler interface {
	// Schedule computes the target cluster shape from the resource requests
	// and the current cluster state.
	Schedule(request *SchedulingRequest) *SchedulingReply
}

// EventLogger is the optional scheduling event logger interface.
// It is injected by the assembling side (the v2 package) so the scheduler
// package does not depend on v2, avoiding an import cycle.
// Corresponds to Python AutoscalerEventLogger.log_cluster_scheduling_update.
type EventLogger interface {
	LogClusterSchedulingUpdate(
		launchRequests []*proto.LaunchRequest,
		terminateRequests []*proto.TerminationRequest,
		infeasibleRequests []*proto.ResourceRequest,
		infeasibleGangRequests []*proto.GangResourceRequest,
		infeasibleClusterResourceConstraints []*proto.ClusterResourceConstraint,
		clusterResources map[string]float64,
	)
}

// fits checks whether the node has enough resources to hold resources.
// Corresponds to Python resource_demand_scheduler._fits: an implicit resource
// request counts as 1.0 when the node does not declare it.
func fits(node map[string]float64, resources map[string]float64) bool {
	for k, v := range resources {
		available, ok := node[k]
		if !ok {
			available = 0.0
			if strings.HasPrefix(k, implicitResourcePrefix) {
				available = 1.0
			}
		}
		if v > available {
			return false
		}
	}
	return true
}

// inplaceSubtract subtracts resources from node.
// Corresponds to Python resource_demand_scheduler._inplace_subtract: an
// implicit resource the node does not declare is initialized to 1.0 before
// subtracting.
// The caller must guarantee fits passed (enough resources), so the result is
// never negative.
func inplaceSubtract(node map[string]float64, resources map[string]float64) {
	for k, v := range resources {
		if v == 0 {
			continue
		}
		if _, ok := node[k]; !ok {
			node[k] = 1.0
		}
		node[k] -= v
	}
}

// combineRequestsWithAffinity merges resource requests with affinity
// constraints into single requests.
// Corresponds to Python ResourceRequestUtil.combine_requests_with_affinity:
// requests with affinity constraints are co-placed (STRICT_PACK).
// Anti-affinity constraints are not merged (STRICT_SPREAD is implemented via
// node labels).
func combineRequestsWithAffinity(resourceRequests []*proto.ResourceRequest) []*proto.ResourceRequest {
	// affinityKey identifies a group of affinity constraints to merge.
	type affinityKey struct {
		labelName   string
		labelValue  string
		selectorKey string
	}

	requestsByAffinity := map[affinityKey][]*proto.ResourceRequest{}
	combinedRequests := []*proto.ResourceRequest{}

	for _, request := range resourceRequests {
		if len(request.PlacementConstraints) > 1 {
			// Python asserts at most one placement constraint; Go defensively
			// skips invalid requests.
			continue
		}
		if len(request.PlacementConstraints) == 0 {
			combinedRequests = append(combinedRequests, request)
			continue
		}
		constraint := request.PlacementConstraints[0]
		if constraint.Affinity != nil {
			affinity := constraint.Affinity
			key := affinityKey{
				labelName:   affinity.LabelName,
				labelValue:  affinity.LabelValue,
				selectorKey: labelSelectorKey(request.LabelSelectors),
			}
			requestsByAffinity[key] = append(requestsByAffinity[key], request)
		} else {
			// Anti-affinity is not merged.
			combinedRequests = append(combinedRequests, request)
		}
	}

	// Merge the resource bundles and dedupe the placement constraints.
	for key, requests := range requestsByAffinity {
		combinedRequest := &proto.ResourceRequest{
			ResourcesBundle: map[string]float64{},
		}
		// Matching Python: aggregate based on the resource keys of the first
		// request.
		for k := range requests[0].ResourcesBundle {
			sum := 0.0
			for _, request := range requests {
				sum += request.ResourcesBundle[k]
			}
			combinedRequest.ResourcesBundle[k] = sum
		}
		combinedRequest.PlacementConstraints = []*proto.PlacementConstraint{
			{Affinity: &proto.AffinityConstraint{LabelName: key.labelName, LabelValue: key.labelValue}},
		}
		combinedRequest.LabelSelectors = requests[0].LabelSelectors
		combinedRequests = append(combinedRequests, combinedRequest)
	}

	return combinedRequests
}

// labelSelectorKey serializes label selectors into a comparable string key.
// Corresponds to Python ResourceRequestUtil._label_selector_key.
func labelSelectorKey(selectors []*proto.LabelSelector) string {
	var sb strings.Builder
	for _, selector := range selectors {
		for _, constraint := range selector.LabelConstraints {
			sb.WriteString(constraint.LabelKey)
			sb.WriteByte('|')
			sb.WriteString(constraint.Operator.String())
			sb.WriteByte('|')
			for _, v := range constraint.LabelValues {
				sb.WriteString(v)
				sb.WriteByte(',')
			}
			sb.WriteByte(';')
		}
	}
	return sb.String()
}

// resourceRequestCompare compares the sort keys of two resource requests and
// returns -1/0/1.
// Corresponds to Python scheduler._try_schedule._sort_resource_request, used
// for deterministic bin packing:
//  1. Number of placement constraints (descending)
//  2. Number of constraints of the first label selector (descending)
//  3. Number of requested resource types (descending)
//  4. Total requested resources (descending)
//  5. Lexicographic order by (resource name, value) for a stable order
func resourceRequestCompare(a, b *proto.ResourceRequest) int {
	aLabelLen, bLabelLen := 0, 0
	if len(a.LabelSelectors) > 0 {
		aLabelLen = len(a.LabelSelectors[0].LabelConstraints)
	}
	if len(b.LabelSelectors) > 0 {
		bLabelLen = len(b.LabelSelectors[0].LabelConstraints)
	}
	if x := len(a.PlacementConstraints) - len(b.PlacementConstraints); x != 0 {
		return sign(x)
	}
	if x := aLabelLen - bLabelLen; x != 0 {
		return sign(x)
	}
	if x := len(a.ResourcesBundle) - len(b.ResourcesBundle); x != 0 {
		return sign(x)
	}
	aSum, bSum := resourceTotal(a.ResourcesBundle), resourceTotal(b.ResourcesBundle)
	if aSum > bSum {
		return 1
	}
	if aSum < bSum {
		return -1
	}
	return compareSortedResourceItems(a.ResourcesBundle, b.ResourcesBundle)
}

func sign(x int) int {
	if x > 0 {
		return 1
	}
	if x < 0 {
		return -1
	}
	return 0
}

func resourceTotal(m map[string]float64) float64 {
	total := 0.0
	for _, v := range m {
		total += v
	}
	return total
}

// compareSortedResourceItems compares two resource maps lexicographically
// after sorting by (resource name, value).
func compareSortedResourceItems(a, b map[string]float64) int {
	type item struct {
		key   string
		value float64
	}
	itemsA := make([]item, 0, len(a))
	for k, v := range a {
		itemsA = append(itemsA, item{k, v})
	}
	itemsB := make([]item, 0, len(b))
	for k, v := range b {
		itemsB = append(itemsB, item{k, v})
	}
	sort.Slice(itemsA, func(i, j int) bool {
		if itemsA[i].key != itemsA[j].key {
			return itemsA[i].key < itemsA[j].key
		}
		return itemsA[i].value < itemsA[j].value
	})
	sort.Slice(itemsB, func(i, j int) bool {
		if itemsB[i].key != itemsB[j].key {
			return itemsB[i].key < itemsB[j].key
		}
		return itemsB[i].value < itemsB[j].value
	})
	for i := 0; i < len(itemsA) && i < len(itemsB); i++ {
		if itemsA[i].key != itemsB[i].key {
			if itemsA[i].key < itemsB[i].key {
				return -1
			}
			return 1
		}
		if itemsA[i].value != itemsB[i].value {
			if itemsA[i].value < itemsB[i].value {
				return -1
			}
			return 1
		}
	}
	if len(itemsA) != len(itemsB) {
		return sign(len(itemsA) - len(itemsB))
	}
	return 0
}

// mapsEqual reports whether two string->float maps are equal.
// Corresponds to Python's dict != comparison (used to detect a fully idle
// node).
func mapsEqual(a, b map[string]float64) bool {
	return reflect.DeepEqual(a, b)
}

// ungroupByCount expands count-aggregated resource demands into a flat list.
// Corresponds to Python ResourceRequestUtil.ungroup_by_count.
func ungroupByCount(requests []*proto.ResourceRequestByCount) []*proto.ResourceRequest {
	reqs := make([]*proto.ResourceRequest, 0)
	for _, r := range requests {
		for i := int64(0); i < r.Count; i++ {
			reqs = append(reqs, r.Request)
		}
	}
	return reqs
}
