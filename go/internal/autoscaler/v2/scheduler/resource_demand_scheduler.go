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
	"fmt"
	"sort"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/ray-project/ray/go/internal/autoscaler/v2/instance_manager"
	"github.com/ray-project/ray/go/pkg/log"
	"github.com/ray-project/ray/go/proto"
)

// ResourceDemandScheduler is the resource demand scheduler.
// Corresponds to Python scheduler.ResourceDemandScheduler
// (scheduler.py:695-1902).
// The scheduling rules run in order:
//  1. Enforce the minimum number of nodes per worker node type
//  2. Enforce the cluster resource constraints
//  3. Schedule the gang resource requests
//  4. Schedule the task/actor resource requests
//
// Plus the finishing passes: guaranteed idle node counts, idle timeout
// termination, outdated node termination, and the max cap constraints.
type ResourceDemandScheduler struct {
	eventLogger EventLogger
}

// NewResourceDemandScheduler creates a resource demand scheduler.
// eventLogger may be nil (in which case no event log is written); it is
// injected by the assembling side to avoid an import cycle.
func NewResourceDemandScheduler(eventLogger EventLogger) *ResourceDemandScheduler {
	return &ResourceDemandScheduler{eventLogger: eventLogger}
}

// scheduleContext wraps the processing context of one scheduling request.
// Corresponds to Python ResourceDemandScheduler.ScheduleContext:
// it provides read/write access to the scheduling nodes while preventing
// external code from accidentally mutating the internal state.
type scheduleContext struct {
	nodeTypeConfigs             map[string]*instance_manager.NodeTypeConfig
	nodeTypeOrder               []string
	disableLaunchConfigCheck    bool
	maxNumNodes                 *int64
	idleTimeoutS                *float64
	nodes                       []*SchedulingNode
	nodeTypeAvailable           map[string]int
	cloudResourceAvailabilities map[string]float64
}

// fromScheduleRequest initializes the scheduling context from a scheduling
// request.
// Corresponds to Python ScheduleContext.from_schedule_request.
func (ctx *scheduleContext) fromScheduleRequest(req *SchedulingRequest) {
	nodes := make([]*SchedulingNode, 0)
	for _, instance := range req.CurrentInstances {
		node := NewNodeFromInstance(instance, req.NodeTypeConfigs, req.DisableLaunchConfigCheck)
		if node != nil {
			nodes = append(nodes, node)
		}
	}

	ctx.nodeTypeConfigs = req.NodeTypeConfigs
	ctx.disableLaunchConfigCheck = req.DisableLaunchConfigCheck
	ctx.maxNumNodes = req.MaxNumNodes
	ctx.idleTimeoutS = req.IdleTimeoutS
	ctx.cloudResourceAvailabilities = req.CloudResourceAvailabilities
	ctx.nodes = nodes
	ctx.nodeTypeConfigs = req.NodeTypeConfigs
	ctx.nodeTypeOrder = req.NodeTypeOrder
	if len(ctx.nodeTypeOrder) == 0 {
		// Sort by name when no explicit order is provided, to stay
		// deterministic.
		ctx.nodeTypeOrder = make([]string, 0, len(ctx.nodeTypeConfigs))
		for name := range ctx.nodeTypeConfigs {
			ctx.nodeTypeOrder = append(ctx.nodeTypeOrder, name)
		}
		sort.Strings(ctx.nodeTypeOrder)
	}
	ctx.nodeTypeAvailable = computeAvailableNodeTypes(nodes, req.NodeTypeConfigs)
}

// nodeOrderIndex returns the index of the node type in the declaration order.
func (ctx *scheduleContext) nodeOrderIndex(nodeType string) int {
	for i, name := range ctx.nodeTypeOrder {
		if name == nodeType {
			return i
		}
	}
	return len(ctx.nodeTypeOrder)
}

// computeAvailableNodeTypes computes how many nodes of each type can be
// launched.
// Corresponds to Python ScheduleContext._compute_available_node_types:
// launchable = configured max_worker_nodes - currently existing nodes.
func computeAvailableNodeTypes(nodes []*SchedulingNode, nodeTypeConfigs map[string]*instance_manager.NodeTypeConfig) map[string]int {
	nodeTypeExisting := map[string]int{}
	for _, node := range nodes {
		nodeTypeExisting[node.NodeType]++
	}
	available := make(map[string]int, len(nodeTypeConfigs))
	for nodeType, config := range nodeTypeConfigs {
		available[nodeType] = int(config.MaxWorkerNodes) - nodeTypeExisting[nodeType]
	}
	return available
}

// getNodes returns a deep copy of the current nodes.
// Corresponds to Python ScheduleContext.get_nodes (copy.deepcopy).
func (ctx *scheduleContext) getNodes() []*SchedulingNode {
	nodes := make([]*SchedulingNode, 0, len(ctx.nodes))
	for _, node := range ctx.nodes {
		nodes = append(nodes, node.clone())
	}
	return nodes
}

// getNodeTypeConfigs returns the node type configs.
func (ctx *scheduleContext) getNodeTypeConfigs() map[string]*instance_manager.NodeTypeConfig {
	return ctx.nodeTypeConfigs
}

// getMaxNumNodes returns the cluster-wide max node count (nil means
// unlimited).
func (ctx *scheduleContext) getMaxNumNodes() *int64 {
	return ctx.maxNumNodes
}

// getIdleTimeoutS returns the idle timeout in seconds (nil means unset).
func (ctx *scheduleContext) getIdleTimeoutS() *float64 {
	return ctx.idleTimeoutS
}

// getCloudResourceAvailabilities returns the cloud resource availability score
// per node type.
func (ctx *scheduleContext) getCloudResourceAvailabilities() map[string]float64 {
	return ctx.cloudResourceAvailabilities
}

// getClusterShape returns the node count per node type (excluding nodes to
// terminate).
// Corresponds to Python ScheduleContext.get_cluster_shape.
func (ctx *scheduleContext) getClusterShape() map[string]int {
	clusterShape := map[string]int{}
	for _, node := range ctx.nodes {
		if node.Status == ToTerminate {
			continue
		}
		clusterShape[node.NodeType]++
	}
	return clusterShape
}

// getClusterResources aggregates the cluster total resources (excluding nodes
// to terminate).
// Corresponds to Python ScheduleContext.get_cluster_resources.
func (ctx *scheduleContext) getClusterResources() map[string]float64 {
	clusterResources := map[string]float64{}
	for _, node := range ctx.nodes {
		if node.Status == ToTerminate {
			continue
		}
		for key, value := range node.TotalResources {
			clusterResources[key] += value
		}
	}
	return clusterResources
}

// update replaces the context node list and recomputes the available node
// types.
// Corresponds to Python ScheduleContext.update.
func (ctx *scheduleContext) update(newNodes []*SchedulingNode) {
	ctx.nodes = newNodes
	ctx.nodeTypeAvailable = computeAvailableNodeTypes(ctx.nodes, ctx.nodeTypeConfigs)
}

// getLaunchRequests returns the launch requests (aggregated by type).
// Corresponds to Python ScheduleContext.get_launch_requests.
func (ctx *scheduleContext) getLaunchRequests() []*proto.LaunchRequest {
	launchByType := map[string]int{}
	for _, node := range ctx.nodes {
		if node.Status == ToLaunch {
			launchByType[node.NodeType]++
		}
	}

	launchRequests := make([]*proto.LaunchRequest, 0, len(launchByType))
	for instanceType, count := range launchByType {
		launchRequests = append(launchRequests, &proto.LaunchRequest{
			InstanceType: instanceType,
			Count:        int32(count),
			Id:           uuid.NewString(),
			RequestTsMs:  time.Now().UnixMilli(),
		})
	}
	return launchRequests
}

// getTerminateRequests returns the termination requests.
// Corresponds to Python ScheduleContext.get_terminate_requests.
func (ctx *scheduleContext) getTerminateRequests() []*proto.TerminationRequest {
	terminate := make([]*proto.TerminationRequest, 0)
	for _, node := range ctx.nodes {
		if node.TerminationRequest != nil {
			terminate = append(terminate, node.TerminationRequest)
		}
	}
	return terminate
}

// getNodeTypeCounts counts the allocated/idle nodes per node type.
// Corresponds to Python ScheduleContext.get_node_type_counts:
// used by the guaranteed idle nodes (_enforce_idle_worker_nodes_per_type) and
// the idle termination (_enforce_idle_termination).
// Head nodes are not counted; nodes to terminate are not counted.
func (ctx *scheduleContext) getNodeTypeCounts() map[string]map[string]int {
	counts := map[string]map[string]int{}
	for _, node := range ctx.nodes {
		if node.NodeKind == proto.NodeKind_HEAD {
			continue
		}
		if node.Status == ToTerminate || node.TerminationRequest != nil {
			continue
		}
		entry := counts[node.NodeType]
		if entry == nil {
			entry = map[string]int{}
			counts[node.NodeType] = entry
		}
		if len(node.GetSchedRequests(PendingDemand)) > 0 || len(node.GetSchedRequests(ClusterResourceConstraint)) > 0 {
			entry["allocated"]++
		} else if node.IdleDurationMS > 0 {
			entry["idle"]++
		} else {
			// The node has no scheduled requests and is not idle: if the
			// available resources differ from the total resources, some
			// request was placed but not recorded (should not happen), so
			// treat it as allocated; otherwise treat it as idle.
			if !mapsEqual(node.GetAvailableResources(PendingDemand), node.TotalResources) {
				entry["allocated"]++
			} else {
				entry["idle"]++
			}
		}
	}
	return counts
}

// Schedule schedules a request and computes the target cluster shape.
// Corresponds to Python ResourceDemandScheduler.schedule
// (scheduler.py:971-1036); the 10-step order is fixed and must not be
// reordered.
func (s *ResourceDemandScheduler) Schedule(request *SchedulingRequest) *SchedulingReply {
	log.Log.V(1).Info("Scheduling for request",
		"resourceRequest", len(request.ResourceRequests),
		"gangResourceRequest", len(request.GangResourceRequests),
		"clusterConstraint", len(request.ClusterResourceConstraints))

	ctx := &scheduleContext{}
	ctx.fromScheduleRequest(request)

	// Terminate outdated nodes (the launch config changed).
	s.terminateOutdatedNodes(ctx)

	// Minimum number of nodes per worker node type.
	s.enforceMinWorkersPerType(ctx)

	// Maximum number of nodes per node type.
	s.enforceMaxWorkersPerType(ctx)

	// Cluster-wide maximum number of nodes.
	s.enforceMaxWorkersGlobal(ctx)

	// Cluster resource constraints.
	infeasibleConstraints := s.enforceResourceConstraints(ctx, request.ClusterResourceConstraints)

	// Gang resource requests.
	infeasibleGangRequests := s.schedGangResourceRequests(ctx, request.GangResourceRequests)

	// Regular task/actor resource requests.
	infeasibleRequests := s.schedResourceRequests(ctx, ungroupByCount(request.ResourceRequests))

	// Guaranteed idle node count.
	s.enforceIdleWorkerNodesPerType(ctx)

	// Idle timeout termination (nodes without constrained demand and beyond
	// the min_worker guarantee).
	s.enforceIdleTermination(ctx)

	reply := &SchedulingReply{
		InfeasibleResourceRequests:           infeasibleRequests,
		InfeasibleGangResourceRequests:       infeasibleGangRequests,
		InfeasibleClusterResourceConstraints: infeasibleConstraints,
		ToLaunch:                             ctx.getLaunchRequests(),
		ToTerminate:                          ctx.getTerminateRequests(),
	}

	if s.eventLogger != nil {
		s.eventLogger.LogClusterSchedulingUpdate(
			reply.ToLaunch,
			reply.ToTerminate,
			infeasibleRequests,
			infeasibleGangRequests,
			infeasibleConstraints,
			ctx.getClusterResources(),
		)
	}

	return reply
}

// terminateOutdatedNodes terminates outdated nodes (the launch config was
// updated).
// Corresponds to Python _terminate_outdated_nodes. The head node is never
// terminated even when outdated.
func (s *ResourceDemandScheduler) terminateOutdatedNodes(ctx *scheduleContext) {
	nodes := ctx.getNodes()

	if ctx.disableLaunchConfigCheck {
		return
	}

	for _, node := range nodes {
		if node.Status != Schedulable {
			continue
		}
		if node.NodeKind == proto.NodeKind_HEAD {
			log.Log.V(1).Info("Head node is outdated with node config changes, autoscaler is not able to shutdown the outdated head node",
				"imInstanceId", node.IMInstanceID, "rayNodeId", node.RayNodeID)
			continue
		}
		nodeType := node.NodeType
		nodeTypeConfig := ctx.getNodeTypeConfigs()[nodeType]
		if nodeTypeConfig == nil || (nodeTypeConfig.LaunchConfigHash != "" && nodeTypeConfig.LaunchConfigHash != node.LaunchConfigHash) {
			node.Status = ToTerminate
			node.TerminationRequest = &proto.TerminationRequest{
				Id:             strconv.FormatInt(time.Now().UnixNano(), 10),
				InstanceId:     node.IMInstanceID,
				RayNodeId:      node.RayNodeID,
				InstanceType:   node.NodeType,
				InstanceStatus: node.IMInstanceStatus,
				Cause:          proto.TerminationRequest_OUTDATED,
				Details:        fmt.Sprintf("node from %s has outdated config", node.NodeType),
			}
		}
	}

	ctx.update(nodes)
}

// enforceMinWorkersPerType enforces the minimum number of nodes per worker
// node type.
// Corresponds to Python _enforce_min_workers_per_type.
func (s *ResourceDemandScheduler) enforceMinWorkersPerType(ctx *scheduleContext) {
	countByNodeType := ctx.getClusterShape()

	newNodes := make([]*SchedulingNode, 0)
	for nodeType, config := range ctx.getNodeTypeConfigs() {
		curCount := countByNodeType[nodeType]
		minCount := int(config.MinWorkerNodes)
		if curCount < minCount {
			log.Log.Info("Adding nodes to satisfy min count for node type",
				"nodeType", nodeType, "count", minCount-curCount)
			for i := 0; i < minCount-curCount; i++ {
				newNodes = append(newNodes, FromNodeConfig(
					config,
					ToLaunch,
					proto.NodeKind_WORKER,
					"",
					0,
				))
			}
		}
	}
	// Note: assumes the sum of all node types' min does not exceed any global
	// max_num_nodes constraint.

	ctx.update(append(newNodes, ctx.getNodes()...))
}

// enforceMaxWorkersPerType enforces the maximum number of nodes per node type.
// Corresponds to Python _enforce_max_workers_per_type. Nodes beyond the max
// are selected for termination in sorted order.
func (s *ResourceDemandScheduler) enforceMaxWorkersPerType(ctx *scheduleContext) {
	allNodes := ctx.getNodes()

	nonTerminatingByType := map[string][]*SchedulingNode{}
	terminatingNodes := make([]*SchedulingNode, 0)
	for _, node := range allNodes {
		if node.Status == ToTerminate {
			terminatingNodes = append(terminatingNodes, node)
		} else {
			nonTerminatingByType[node.NodeType] = append(nonTerminatingByType[node.NodeType], node)
		}
	}

	for nodeType, nodesOfType := range nonTerminatingByType {
		config := ctx.getNodeTypeConfigs()[nodeType]
		if config == nil {
			continue
		}
		numMaxNodesPerType := int(config.MaxWorkerNodes)
		numExtraNodes := len(nodesOfType) - numMaxNodesPerType
		if numExtraNodes <= 0 {
			continue
		}

		toTerminate, remained := selectNodesToTerminate(
			nodesOfType,
			numExtraNodes,
			proto.TerminationRequest_MAX_NUM_NODE_PER_TYPE,
			nil,
			&numMaxNodesPerType,
		)
		nonTerminatingByType[nodeType] = remained
		terminatingNodes = append(terminatingNodes, toTerminate...)
	}

	nonTerminatingNodes := make([]*SchedulingNode, 0)
	for _, nodes := range nonTerminatingByType {
		nonTerminatingNodes = append(nonTerminatingNodes, nodes...)
	}

	ctx.update(append(terminatingNodes, nonTerminatingNodes...))

	if len(terminatingNodes) > 0 {
		log.Log.V(1).Info("Terminating nodes for per node type max num node's constraints",
			"count", len(terminatingNodes))
	}
}

// enforceMaxWorkersGlobal enforces the cluster-wide maximum number of nodes.
// Corresponds to Python _enforce_max_workers_global.
func (s *ResourceDemandScheduler) enforceMaxWorkersGlobal(ctx *scheduleContext) {
	allNodes := ctx.getNodes()

	terminatingNodes := make([]*SchedulingNode, 0)
	nonTerminatingNodes := make([]*SchedulingNode, 0)
	for _, node := range allNodes {
		if node.Status == ToTerminate {
			terminatingNodes = append(terminatingNodes, node)
		} else {
			nonTerminatingNodes = append(nonTerminatingNodes, node)
		}
	}

	maxNumNodes := ctx.getMaxNumNodes()
	numMaxNodes := 0
	if maxNumNodes != nil {
		numMaxNodes = int(*maxNumNodes)
	}
	numToTerminate := 0
	if numMaxNodes > 0 {
		numToTerminate = len(nonTerminatingNodes) - numMaxNodes
		if numToTerminate < 0 {
			numToTerminate = 0
		}
	}
	if numToTerminate <= 0 {
		return
	}

	toTerminateNodes, nonTerminatingNodes := selectNodesToTerminate(
		nonTerminatingNodes,
		numToTerminate,
		proto.TerminationRequest_MAX_NUM_NODES,
		&numMaxNodes,
		nil,
	)

	terminatingNodes = append(terminatingNodes, toTerminateNodes...)

	allNodes = append(terminatingNodes, nonTerminatingNodes...)
	ctx.update(allNodes)
}

// selectNodesToTerminate selects the nodes to terminate from a node list
// (never the head node).
// Corresponds to Python _select_nodes_to_terminate. Nodes are sorted by
// _sortNodesForTermination; the earliest in the order terminate first
// (not running Ray first, then idle longer, then lower resource utilization).
func selectNodesToTerminate(
	nodes []*SchedulingNode,
	numToTerminate int,
	cause proto.TerminationRequest_Cause,
	maxNumNodes *int,
	maxNumNodesPerType *int,
) ([]*SchedulingNode, []*SchedulingNode) {
	sort.Slice(nodes, func(i, j int) bool {
		return lessForTermination(nodes[i], nodes[j])
	})

	// Remove the head node from the list.
	headNode := (*SchedulingNode)(nil)
	for i, node := range nodes {
		if node.NodeKind == proto.NodeKind_HEAD {
			headNode = nodes[i]
			nodes = append(nodes[:i], nodes[i+1:]...)
			break
		}
	}

	terminatedNodes := nodes[:numToTerminate]
	remainedNodes := nodes[numToTerminate:]
	if headNode != nil {
		remainedNodes = append(remainedNodes, headNode)
	}

	for _, node := range terminatedNodes {
		node.Status = ToTerminate
		termination := &proto.TerminationRequest{
			Id:             uuid.NewString(),
			InstanceId:     node.IMInstanceID,
			RayNodeId:      node.RayNodeID,
			Cause:          cause,
			InstanceType:   node.NodeType,
			InstanceStatus: node.IMInstanceStatus,
			Details: fmt.Sprintf("Terminating node due to %s: max_num_nodes=%v, max_num_nodes_per_type=%v",
				cause.String(), maxNumNodes, maxNumNodesPerType),
		}
		if cause == proto.TerminationRequest_MAX_NUM_NODES && maxNumNodes != nil {
			v := uint32(*maxNumNodes)
			termination.MaxNumNodes = &v
		} else if cause == proto.TerminationRequest_MAX_NUM_NODE_PER_TYPE && maxNumNodesPerType != nil {
			v := uint32(*maxNumNodesPerType)
			termination.MaxNumNodesPerType = &v
		}
		node.TerminationRequest = termination
	}

	return terminatedNodes, remainedNodes
}

// lessForTermination is the termination sort comparison for nodes.
// Corresponds to Python _sort_nodes_for_termination, ascending (the earliest
// terminates first):
//  1. First compare whether Ray is already running (false first, i.e. nodes
//     not running Ray terminate first)
//  2. Then compare the idle duration (idle longer terminates first, note the
//     negation)
//  3. Then compare the average resource utilization (lower utilization
//     terminates first)
func lessForTermination(a, b *SchedulingNode) bool {
	aRunning := a.RayNodeID != ""
	bRunning := b.RayNodeID != ""
	if aRunning != bRunning {
		return !aRunning
	}
	if a.IdleDurationMS != b.IdleDurationMS {
		return a.IdleDurationMS > b.IdleDurationMS // idle longer first (same as Python -idle ascending)
	}
	return avgUtilization(a) < avgUtilization(b)
}

// avgUtilization computes the node's average resource utilization.
func avgUtilization(node *SchedulingNode) float64 {
	available := node.GetAvailableResources(PendingDemand)
	utilsPerResources := map[string]float64{}
	for resource, total := range node.TotalResources {
		if total <= 0 {
			continue
		}
		utilsPerResources[resource] = (total - available[resource]) / total
	}
	if len(utilsPerResources) == 0 {
		return 0
	}
	sum := 0.0
	for _, u := range utilsPerResources {
		sum += u
	}
	return sum / float64(len(utilsPerResources))
}

// enforceResourceConstraints enforces the cluster resource constraints.
// Corresponds to Python _enforce_resource_constraints:
// unlike the other scheduling functions, it does not really schedule requests
// but asks whether the cluster could scale to the shape that satisfies the
// constraint.
// Note: currently at most 1 constraint is supported.
func (s *ResourceDemandScheduler) enforceResourceConstraints(
	ctx *scheduleContext,
	constraints []*proto.ClusterResourceConstraint,
) []*proto.ClusterResourceConstraint {
	if len(constraints) > 1 {
		log.Log.Error(nil, "Max 1 cluster resource constraint is supported, ignoring extra constraints",
			"count", len(constraints))
	}
	if len(constraints) == 0 {
		return nil
	}

	constraint := constraints[0]
	requests := ungroupByCount(constraint.ResourceRequests)

	scheduledNodes, infeasible := s.trySchedule(ctx, requests, ClusterResourceConstraint)

	if len(infeasible) > 0 {
		return []*proto.ClusterResourceConstraint{constraint}
	}

	ctx.update(scheduledNodes)
	return nil
}

// schedResourceRequests schedules the regular resource requests.
// Corresponds to Python _sched_resource_requests.
func (s *ResourceDemandScheduler) schedResourceRequests(ctx *scheduleContext, requests []*proto.ResourceRequest) []*proto.ResourceRequest {
	nodes, infeasible := s.trySchedule(ctx, requests, PendingDemand)
	ctx.update(nodes)
	return infeasible
}

// schedGangResourceRequests schedules the gang resource requests.
// Corresponds to Python _sched_gang_resource_requests:
// gang requests must be scheduled atomically (all satisfied or all rejected),
// never partially.
// STRICT_PACK is implemented by merging requests; STRICT_SPREAD is implemented
// via node labels (antiaffinity).
func (s *ResourceDemandScheduler) schedGangResourceRequests(ctx *scheduleContext, gangRequests []*proto.GangResourceRequest) []*proto.GangResourceRequest {
	// Sort key: first by the total number of placement constraints, then by the
	// number of resource requests (descending, more constraints tried first).
	sorted := make([]*proto.GangResourceRequest, len(gangRequests))
	copy(sorted, gangRequests)
	sort.Slice(sorted, func(i, j int) bool {
		return gangSortKeyGreater(sorted[i], sorted[j])
	})

	infeasibleGang := make([]*proto.GangResourceRequest, 0)
	for _, gangReq := range sorted {
		var requests []*proto.ResourceRequest
		if len(gangReq.BundleSelectors) > 0 {
			// Currently only a single bundle_selector is supported
			// (fallback_strategy is reserved).
			requests = gangReq.BundleSelectors[0].ResourceRequests
		} else {
			requests = gangReq.Requests
		}
		// Merge the requests with affinity constraints.
		requests = combineRequestsWithAffinity(requests)

		nodes, infeasible := s.trySchedule(ctx, requests, PendingDemand)

		if len(infeasible) > 0 {
			// Cannot be satisfied; skip the whole gang request without
			// updating the context.
			infeasibleGang = append(infeasibleGang, gangReq)
			continue
		}

		ctx.update(nodes)
	}
	return infeasibleGang
}

// gangSortKeyGreater is the gang request sort comparison (both the total
// placement constraints and the request count descending).
// Corresponds to Python
// _sched_gang_resource_requests._sort_gang_resource_requests.
func gangSortKeyGreater(a, b *proto.GangResourceRequest) bool {
	aConstraints, bConstraints := 0, 0
	for _, req := range a.Requests {
		aConstraints += len(req.PlacementConstraints)
	}
	for _, req := range b.Requests {
		bConstraints += len(req.PlacementConstraints)
	}
	if aConstraints != bConstraints {
		return aConstraints > bConstraints
	}
	return len(a.Requests) > len(b.Requests)
}

// trySchedule attempts to schedule the resource requests on the scheduling
// context.
// Corresponds to Python _try_schedule:
// it first tries the existing nodes, then the new nodes (limited by
// node_type_available and max_num_nodes).
// Returns the target node list and the requests that cannot be scheduled.
func (s *ResourceDemandScheduler) trySchedule(
	ctx *scheduleContext,
	requestsToSched []*proto.ResourceRequest,
	source ResourceRequestSource,
) ([]*SchedulingNode, []*proto.ResourceRequest) {
	// First sort by the deterministic bin-packing key (descending).
	sort.Slice(requestsToSched, func(i, j int) bool {
		return resourceRequestCompare(requestsToSched[i], requestsToSched[j]) > 0
	})

	existingNodes := ctx.getNodes()
	nodeTypeAvailable := ctx.nodeTypeAvailable

	targetNodes := make([]*SchedulingNode, 0)

	// Schedule on the existing nodes first.
	for len(requestsToSched) > 0 && len(existingNodes) > 0 {
		bestNode, remaining, restNodes := s.schedBestNode(
			requestsToSched, existingNodes, source, ctx.getCloudResourceAvailabilities())
		if bestNode == nil {
			break
		}
		requestsToSched = remaining
		existingNodes = restNodes
		targetNodes = append(targetNodes, bestNode)
	}

	targetNodes = append(targetNodes, existingNodes...)

	// Then schedule on the new nodes.
	// nodePools is built in the node type declaration order, matching the
	// Python dict insertion order (deterministic bin packing).
	nodePools := make([]*SchedulingNode, 0, len(ctx.nodeTypeOrder))
	for _, nodeType := range ctx.nodeTypeOrder {
		if nodeTypeAvailable[nodeType] <= 0 {
			continue
		}
		config := ctx.getNodeTypeConfigs()[nodeType]
		if config == nil {
			continue
		}
		nodePools = append(nodePools, FromNodeConfig(config, ToLaunch, proto.NodeKind_WORKER, "", 0))
	}

	for len(requestsToSched) > 0 && len(nodePools) > 0 {
		// The max node count limit is reached.
		maxNumNodes := ctx.getMaxNumNodes()
		if maxNumNodes != nil && len(targetNodes) >= int(*maxNumNodes) {
			log.Log.V(1).Info("Max number of nodes reached, cannot launch more nodes",
				"maxNumNodes", *maxNumNodes)
			break
		}

		bestNode, remaining, restPools := s.schedBestNode(
			requestsToSched, nodePools, source, ctx.getCloudResourceAvailabilities())
		if bestNode == nil {
			break
		}
		requestsToSched = remaining
		nodePools = restPools
		targetNodes = append(targetNodes, bestNode)

		// If more nodes of the same type can be launched, add one more node
		// pool candidate.
		nodeTypeAvailable[bestNode.NodeType]--
		if nodeTypeAvailable[bestNode.NodeType] > 0 {
			config := ctx.getNodeTypeConfigs()[bestNode.NodeType]
			if config != nil {
				nodePools = append(nodePools, FromNodeConfig(config, ToLaunch, proto.NodeKind_WORKER, "", 0))
			}
		}
	}

	return targetNodes, requestsToSched
}

// schedResult is the result of one node scheduling attempt.
// Corresponds to the local ScheduleResult of Python _sched_best_node.
type schedResult struct {
	node               *SchedulingNode
	infeasibleRequests []*proto.ResourceRequest
	idx                int
	score              UtilizationScore
}

// schedBestNode selects the best candidate node to schedule the requests.
// Corresponds to Python _sched_best_node:
//  1. Tries to schedule on every candidate node (on deep copies, so the
//     original nodes are not polluted)
//  2. Picks the best by score (utilization score + cloud resource
//     availability), descending
//  3. Removes the selected node from the node list
func (s *ResourceDemandScheduler) schedBestNode(
	requests []*proto.ResourceRequest,
	nodes []*SchedulingNode,
	source ResourceRequestSource,
	cloudResourceAvailabilities map[string]float64,
) (*SchedulingNode, []*proto.ResourceRequest, []*SchedulingNode) {
	results := make([]schedResult, 0)

	nodesCopy := make([]*SchedulingNode, 0, len(nodes))
	for _, node := range nodes {
		nodesCopy = append(nodesCopy, node.clone())
	}

	// Try scheduling on each node.
	for idx, node := range nodesCopy {
		remaining, score := node.TrySchedule(requests, source)
		if len(remaining) == len(requests) {
			// The node cannot schedule any request.
			continue
		}
		results = append(results, schedResult{node, remaining, idx, score})
	}

	if len(results) == 0 {
		log.Log.V(1).Info("No nodes can schedule the requests", "requestCount", len(requests))
		return nil, requests, nodes
	}

	// Sort by (score, cloud_avail) descending.
	cloudAvail := func(nodeType string) float64 {
		if v, ok := cloudResourceAvailabilities[nodeType]; ok {
			return v
		}
		return 1
	}
	sort.Slice(results, func(i, j int) bool {
		if !results[i].score.equals(results[j].score) {
			return results[i].score.greater(results[j].score)
		}
		if cloudAvail(results[i].node.NodeType) != cloudAvail(results[j].node.NodeType) {
			return cloudAvail(results[i].node.NodeType) > cloudAvail(results[j].node.NodeType)
		}
		// Keep the original index order (Python's sorted is a stable sort).
		return results[i].idx < results[j].idx
	})

	bestResult := results[0]
	// Remove the best node from the original node list.
	nodes = append(nodes[:bestResult.idx], nodes[bestResult.idx+1:]...)

	log.Log.V(1).Info("Best node selected",
		"nodeType", bestResult.node.NodeType,
		"score", bestResult.score,
		"remainingRequests", len(bestResult.infeasibleRequests))

	return bestResult.node, bestResult.infeasibleRequests, nodes
}

// enforceIdleWorkerNodesPerType enforces the guaranteed idle node count per
// node type.
// Corresponds to Python _enforce_idle_worker_nodes_per_type.
func (s *ResourceDemandScheduler) enforceIdleWorkerNodesPerType(ctx *scheduleContext) {
	nodeTypeConfigs := ctx.getNodeTypeConfigs()
	nodeTypeCounts := ctx.getNodeTypeCounts()

	newNodes := make([]*SchedulingNode, 0)
	for nodeType, config := range nodeTypeConfigs {
		if config.IdleWorkerNodes <= 0 {
			continue
		}
		countData := nodeTypeCounts[nodeType]
		currentIdle := countData["idle"]
		currentTotal := currentIdle + countData["allocated"]

		maxPossible := int(config.MaxWorkerNodes) - currentTotal
		nodesNeeded := int(config.IdleWorkerNodes) - currentIdle
		nodesToAdd := min(max(nodesNeeded, 0), max(maxPossible, 0))

		log.Log.V(1).Info("Ensuring idle worker type",
			"nodeType", nodeType,
			"idleWorkerNodesConfig", config.IdleWorkerNodes,
			"nodeCounts", countData,
			"nodesToAdd", nodesToAdd)
		if nodesToAdd <= 0 {
			continue
		}

		for i := 0; i < nodesToAdd; i++ {
			newNodes = append(newNodes, FromNodeConfig(config, ToLaunch, proto.NodeKind_WORKER, "", 0))
		}
	}

	ctx.update(append(newNodes, ctx.getNodes()...))
}

// enforceIdleTermination terminates nodes idle beyond the timeout and no
// longer needed.
// Corresponds to Python _enforce_idle_termination:
// excludes nodes still used by regular demands/cluster constraints and nodes
// guaranteed by min_worker_nodes or idle_worker_nodes.
func (s *ResourceDemandScheduler) enforceIdleTermination(ctx *scheduleContext) {
	countByNodeType := ctx.getClusterShape()
	nodeTypeConfigs := ctx.getNodeTypeConfigs()
	terminateCountByType := map[string]int{}
	nodeTypeCounts := ctx.getNodeTypeCounts()

	nodes := ctx.getNodes()
	const msPerSecond = 1000

	for _, node := range nodes {
		if node.Status != Schedulable {
			continue
		}
		if node.NodeKind == proto.NodeKind_HEAD {
			continue
		}

		idleTimeoutS := ctx.getIdleTimeoutS()
		nodeType := node.NodeType
		if config, ok := nodeTypeConfigs[nodeType]; ok && config.IdleTimeoutS != 0 {
			// The node type specific timeout overrides the global setting.
			// Note: a config.IdleTimeoutS of 0 means unset; keep the global
			// value.
			v := config.IdleTimeoutS
			idleTimeoutS = &v
		}
		if idleTimeoutS == nil {
			continue
		}

		if node.IdleDurationMS <= int64(*idleTimeoutS*msPerSecond) {
			continue
		}

		if len(node.GetSchedRequests(PendingDemand)) > 0 {
			// Used by pending requests.
			log.Log.V(1).Info("Node is needed by the pending requests, skip idle termination",
				"rayNodeId", node.RayNodeID)
			continue
		}
		if len(node.GetSchedRequests(ClusterResourceConstraint)) > 0 {
			// Used by the cluster resource constraints.
			log.Log.V(1).Info("Node is needed by the cluster resource constraints, skip idle termination",
				"rayNodeId", node.RayNodeID)
			continue
		}

		// Also respect min_worker_nodes and idle_worker_nodes; the guaranteed
		// count is the max of the two:
		//   min_worker_nodes: the minimum node count of the type
		//   idle_worker_nodes + allocated: the expected idle count + the used
		//   count
		minCount := 0
		if config, ok := nodeTypeConfigs[nodeType]; ok {
			expectedIdle := int(config.IdleWorkerNodes) + nodeTypeCounts[nodeType]["allocated"]
			if int(config.MinWorkerNodes) > expectedIdle {
				minCount = int(config.MinWorkerNodes)
			} else {
				minCount = expectedIdle
			}
		}
		if countByNodeType[nodeType]-terminateCountByType[nodeType] <= minCount {
			log.Log.V(1).Info("Node is required by min_worker_nodes, skipping idle termination",
				"rayNodeId", node.RayNodeID, "nodeType", nodeType)
			continue
		}

		terminateCountByType[nodeType]++
		idleDurationMS := uint64(node.IdleDurationMS)
		node.Status = ToTerminate
		node.TerminationRequest = &proto.TerminationRequest{
			Id:             uuid.NewString(),
			InstanceId:     node.IMInstanceID,
			RayNodeId:      node.RayNodeID,
			Cause:          proto.TerminationRequest_IDLE,
			InstanceType:   node.NodeType,
			InstanceStatus: node.IMInstanceStatus,
			IdleDurationMs: &idleDurationMS,
			Details: fmt.Sprintf("idle for %f secs > timeout=%f secs",
				float64(node.IdleDurationMS)/msPerSecond, *idleTimeoutS),
		}
	}

	ctx.update(nodes)
}
