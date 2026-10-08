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
	"sort"
	"testing"

	"github.com/ray-project/ray/go/internal/autoscaler/v2/instance_manager"
	"github.com/ray-project/ray/go/proto"
	"github.com/stretchr/testify/assert"
)

// The helpers below correspond to Python test_scheduler.py's
// sched_request/schedule.
// The test coverage is ported from
// python/ray/autoscaler/v2/tests/test_scheduler.py.

func strPtr(s string) *string       { return &s }
func int64Ptr(v int64) *int64       { return &v }
func float64Ptr(v float64) *float64 { return &v }
func u32Ptr(v uint32) *uint32       { return &v }

// nodeTypeConfigs builds the node type configs.
func nodeTypeConfigs() map[string]*instance_manager.NodeTypeConfig {
	return map[string]*instance_manager.NodeTypeConfig{}
}

// schedRequest builds a scheduling request, corresponding to Python's
// sched_request.
func schedRequest(
	nodeTypeConfigs map[string]*instance_manager.NodeTypeConfig,
	maxNumNodes *int64,
	resourceRequests []*proto.ResourceRequest,
	gangResourceRequests [][]*proto.ResourceRequest,
	clusterResourceConstraints []*proto.ResourceRequest,
	instances []*AutoscalerInstance,
	idleTimeoutS *float64,
	disableLaunchConfigCheck bool,
	cloudResourceAvailabilities map[string]float64,
) *SchedulingRequest {
	grouped := make([]*proto.ResourceRequestByCount, 0, len(resourceRequests))
	for _, r := range resourceRequests {
		grouped = append(grouped, &proto.ResourceRequestByCount{Request: r, Count: 1})
	}

	gangs := make([]*proto.GangResourceRequest, 0, len(gangResourceRequests))
	for _, reqs := range gangResourceRequests {
		gangs = append(gangs, &proto.GangResourceRequest{Requests: reqs})
	}

	constraints := make([]*proto.ClusterResourceConstraint, 0)
	if len(clusterResourceConstraints) > 0 {
		groupedConstraints := make([]*proto.ResourceRequestByCount, 0, len(clusterResourceConstraints))
		for _, r := range clusterResourceConstraints {
			groupedConstraints = append(groupedConstraints, &proto.ResourceRequestByCount{Request: r, Count: 1})
		}
		constraints = append(constraints, &proto.ClusterResourceConstraint{
			ResourceRequests: groupedConstraints,
		})
	}

	if cloudResourceAvailabilities == nil {
		cloudResourceAvailabilities = map[string]float64{}
	}

	// NodeTypeOrder takes the NodeTypeConfigs keys sorted by name.
	// This order drives the nodePools construction and the tie-breaking;
	// sorting by name keeps it deterministic.
	order := make([]string, 0, len(nodeTypeConfigs))
	for name := range nodeTypeConfigs {
		order = append(order, name)
	}
	sort.Strings(order)

	return &SchedulingRequest{
		NodeTypeConfigs:             nodeTypeConfigs,
		NodeTypeOrder:               order,
		MaxNumNodes:                 maxNumNodes,
		IdleTimeoutS:                idleTimeoutS,
		ResourceRequests:            grouped,
		GangResourceRequests:        gangs,
		ClusterResourceConstraints:  constraints,
		CurrentInstances:            instances,
		DisableLaunchConfigCheck:    disableLaunchConfigCheck,
		CloudResourceAvailabilities: cloudResourceAvailabilities,
	}
}

// launchAndTerminate extracts the reply's launch/terminate info, corresponding
// to Python's _launch_and_terminate.
func launchAndTerminate(reply *SchedulingReply) (map[string]int, [][3]interface{}) {
	toLaunch := make(map[string]int, len(reply.ToLaunch))
	for _, r := range reply.ToLaunch {
		toLaunch[r.InstanceType] = int(r.Count)
	}
	toTerminate := make([][3]interface{}, 0, len(reply.ToTerminate))
	for _, r := range reply.ToTerminate {
		toTerminate = append(toTerminate, [3]interface{}{r.InstanceId, r.RayNodeId, r.Cause})
	}
	return toLaunch, toTerminate
}

// makeIMInstance builds an instance manager instance (the IM part of
// `make_autoscaler_instance`).
func makeIMInstance(instanceType string, status proto.Instance_InstanceStatus, instanceId string) *proto.Instance {
	return &proto.Instance{
		InstanceId:   instanceId,
		InstanceType: instanceType,
		Status:       status,
		NodeKind:     proto.NodeKind_WORKER,
	}
}

// makeRayNodeState builds a Ray node state.
func makeRayNodeState(
	nodeID string,
	instanceType string,
	available map[string]float64,
	total map[string]float64,
	idleDurationMs int64,
	labels map[string]string,
) *proto.NodeState {
	node := &proto.NodeState{
		NodeId:             []byte(nodeID),
		RayNodeTypeName:    instanceType,
		AvailableResources: available,
		TotalResources:     total,
		IdleDurationMs:     idleDurationMs,
		Status:             proto.NodeStatus_RUNNING,
	}
	if len(labels) > 0 {
		node.Labels = labels
	}
	return node
}

// makeAutoscalerInstance builds an autoscaler instance, corresponding to
// Python's make_autoscaler_instance.
// When rayNode exists and the IM instance has no explicit NodeId, the Ray node
// id is synced to the IM instance automatically
// (the Python scheduler reads the Ray node id from im_instance.node_id).
func makeAutoscalerInstance(
	im *proto.Instance,
	rayNode *proto.NodeState,
	cloudInstanceID string,
) *AutoscalerInstance {
	if cloudInstanceID != "" {
		if im != nil {
			im.CloudInstanceId = &cloudInstanceID
		}
		if rayNode != nil {
			rayNode.InstanceId = cloudInstanceID
		}
	}
	if rayNode != nil && im != nil && im.GetNodeId() == "" {
		nodeID := string(rayNode.NodeId)
		im.NodeId = &nodeID
	}
	return &AutoscalerInstance{
		CloudInstanceID: cloudInstanceID,
		RayNode:         rayNode,
		IMInstance:      im,
	}
}

// schedule is a convenience function corresponding to Python's schedule.
// nodeTypeOrder is optional; when given it drives the node type traversal
// order (matching the Python dict insertion order) and the tie-break
// determinism; when nil it falls back to name sorting.
func schedule(
	nodeTypeConfigs map[string]*instance_manager.NodeTypeConfig,
	currentNodesAvailableCount map[string]int,
	resourceRequests []*proto.ResourceRequest,
	antiAffinity bool,
	maxNodes *int64,
	cloudResourceAvailabilities map[string]float64,
	nodeTypeOrder ...[]string,
) *SchedulingReply {
	instances := make([]*AutoscalerInstance, 0)
	for nodeType, count := range currentNodesAvailableCount {
		for i := 0; i < count; i++ {
			instanceID := nodeType + "-" + intToString(i)
			nodeID := "r" + intToString(i) + nodeType
			config := nodeTypeConfigs[nodeType]
			im := &proto.Instance{
				InstanceType: nodeType,
				Status:       proto.Instance_RAY_RUNNING,
				InstanceId:   instanceID,
				NodeKind:     proto.NodeKind_WORKER,
			}
			nodeIDStr := nodeID
			im.NodeId = &nodeIDStr
			available := cloneFloatMap(config.Resources)
			total := cloneFloatMap(config.Resources)
			rayNode := makeRayNodeState(nodeID, nodeType, available, total, 0, nil)
			instances = append(instances, makeAutoscalerInstance(im, rayNode, "c-"+nodeType+"-"+intToString(i)))
		}
	}

	var order []string
	if len(nodeTypeOrder) > 0 {
		order = nodeTypeOrder[0]
	} else {
		order = sortedKeys(nodeTypeConfigs)
	}

	if antiAffinity {
		gangReq := make([][]*proto.ResourceRequest, 0, 1)
		reqs := make([]*proto.ResourceRequest, 0, len(resourceRequests))
		for _, r := range resourceRequests {
			reqs = append(reqs, makeWithPlacementConstraint(r, "af", "af"))
		}
		gangReq = append(gangReq, reqs)
		req := schedRequest(nodeTypeConfigs, maxNodes, nil, gangReq, nil, instances, nil, false, cloudResourceAvailabilities)
		req.NodeTypeOrder = order
		return NewResourceDemandScheduler(nil).Schedule(req)
	}

	req := schedRequest(nodeTypeConfigs, maxNodes, resourceRequests, nil, nil, instances, nil, false, cloudResourceAvailabilities)
	req.NodeTypeOrder = order
	return NewResourceDemandScheduler(nil).Schedule(req)
}

// sortedKeys returns the map keys sorted by name, providing a deterministic
// default traversal order.
func sortedKeys(m map[string]*instance_manager.NodeTypeConfig) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func intToString(i int) string {
	if i == 0 {
		return "0"
	}
	return string(rune('0' + i))
}

// makeRequest builds a simple resource request.
func makeRequest(resources map[string]float64) *proto.ResourceRequest {
	return &proto.ResourceRequest{ResourcesBundle: resources}
}

// makeWithPlacementConstraint builds a resource request with a placement
// constraint.
func makeWithPlacementConstraint(request *proto.ResourceRequest, labelName, labelValue string) *proto.ResourceRequest {
	cp := cloneRequest(request)
	cp.PlacementConstraints = []*proto.PlacementConstraint{
		{AntiAffinity: &proto.AntiAffinityConstraint{LabelName: labelName, LabelValue: labelValue}},
	}
	return cp
}

// makeWithAffinity builds a resource request with an affinity placement
// constraint.
func makeWithAffinity(request *proto.ResourceRequest, labelName, labelValue string) *proto.ResourceRequest {
	cp := cloneRequest(request)
	cp.PlacementConstraints = []*proto.PlacementConstraint{
		{Affinity: &proto.AffinityConstraint{LabelName: labelName, LabelValue: labelValue}},
	}
	return cp
}

func cloneRequest(r *proto.ResourceRequest) *proto.ResourceRequest {
	out := &proto.ResourceRequest{
		ResourcesBundle: map[string]float64{},
	}
	for k, v := range r.ResourcesBundle {
		out.ResourcesBundle[k] = v
	}
	if len(r.PlacementConstraints) > 0 {
		out.PlacementConstraints = make([]*proto.PlacementConstraint, len(r.PlacementConstraints))
		for i, c := range r.PlacementConstraints {
			cp := &proto.PlacementConstraint{}
			if c.Affinity != nil {
				cp.Affinity = &proto.AffinityConstraint{LabelName: c.Affinity.LabelName, LabelValue: c.Affinity.LabelValue}
			}
			if c.AntiAffinity != nil {
				cp.AntiAffinity = &proto.AntiAffinityConstraint{LabelName: c.AntiAffinity.LabelName, LabelValue: c.AntiAffinity.LabelValue}
			}
			out.PlacementConstraints[i] = cp
		}
	}
	if len(r.LabelSelectors) > 0 {
		out.LabelSelectors = make([]*proto.LabelSelector, len(r.LabelSelectors))
		for i, ls := range r.LabelSelectors {
			lsCopy := &proto.LabelSelector{}
			for _, c := range ls.LabelConstraints {
				lsCopy.LabelConstraints = append(lsCopy.LabelConstraints, &proto.LabelSelectorConstraint{
					LabelKey:    c.LabelKey,
					Operator:    c.Operator,
					LabelValues: append([]string{}, c.LabelValues...),
				})
			}
			out.LabelSelectors[i] = lsCopy
		}
	}
	return out
}

// testNodeTypeConfig builds a node type config, corresponding to Python's
// NodeTypeConfig.
func testNodeTypeConfig(
	name string,
	resources map[string]float64,
	minWorkers, maxWorkers int32,
	labels map[string]string,
	idleWorkers int32,
	idleTimeoutS float64,
) *instance_manager.NodeTypeConfig {
	return &instance_manager.NodeTypeConfig{
		Name:            name,
		Resources:       resources,
		MinWorkerNodes:  minWorkers,
		MaxWorkerNodes:  maxWorkers,
		Labels:          labels,
		IdleWorkerNodes: idleWorkers,
		IdleTimeoutS:    idleTimeoutS,
	}
}

func TestSchedulingNode_IsSchedulable(t *testing.T) {
	// im_instance=None -> not schedulable
	instance := makeAutoscalerInstance(nil, nil, "")
	assert.False(t, IsSchedulable(instance))

	positive := map[proto.Instance_InstanceStatus]bool{
		proto.Instance_QUEUED:             true,
		proto.Instance_REQUESTED:          true,
		proto.Instance_ALLOCATED:          true,
		proto.Instance_RAY_INSTALLING:     true,
		proto.Instance_RAY_RUNNING:        true,
		proto.Instance_RAY_STOP_REQUESTED: true,
	}
	negative := map[proto.Instance_InstanceStatus]bool{
		proto.Instance_UNKNOWN:            true,
		proto.Instance_RAY_STOPPING:       true,
		proto.Instance_RAY_STOPPED:        true,
		proto.Instance_TERMINATING:        true,
		proto.Instance_TERMINATED:         true,
		proto.Instance_ALLOCATION_FAILED:  true,
		proto.Instance_RAY_INSTALL_FAILED: true,
		proto.Instance_TERMINATION_FAILED: true,
		proto.Instance_ALLOCATION_TIMEOUT: true,
	}

	for _, status := range proto.Instance_InstanceStatus_value {
		s := proto.Instance_InstanceStatus(status)
		im := makeIMInstance("type_1", s, "")
		instance := makeAutoscalerInstance(im, nil, "")
		if positive[s] {
			assert.True(t, IsSchedulable(instance), "expected schedulable for status %v", s)
		} else if negative[s] {
			assert.False(t, IsSchedulable(instance), "expected not schedulable for status %v", s)
		} else {
			t.Fatalf("Unknown status %v", s)
		}
	}
}

func TestSchedulingNode_NewNode(t *testing.T) {
	nodeTypeConfigs := map[string]*instance_manager.NodeTypeConfig{
		"type_1": testNodeTypeConfig(
			"type_1", map[string]float64{"CPU": 1}, 0, 10,
			map[string]string{"foo": "foo"}, 0, 0,
		),
	}

	// None IM instance -> nil
	instance := makeAutoscalerInstance(nil, nil, "")
	assert.Nil(t, NewNodeFromInstance(instance, nodeTypeConfigs, false))

	// A running ray node
	im := makeIMInstance("type_1", proto.Instance_RAY_RUNNING, "1")
	nodeID := "r1"
	im.NodeId = &nodeID
	rayNode := makeRayNodeState("r1", "type_1",
		map[string]float64{"CPU": 0},
		map[string]float64{"CPU": 1},
		0,
		nil)
	rayNode.DynamicLabels = map[string]string{"foo": "bar"}
	instance = makeAutoscalerInstance(im, rayNode, "")
	node := NewNodeFromInstance(instance, nodeTypeConfigs, false)
	assert.NotNil(t, node)
	assert.Equal(t, "type_1", node.NodeType)
	assert.Equal(t, Schedulable, node.Status)
	assert.Equal(t, "r1", node.RayNodeID)
	assert.Equal(t, "1", node.IMInstanceID)
	assert.Equal(t, map[string]float64{"CPU": 0}, node.GetAvailableResources(PendingDemand))
	assert.Equal(t, map[string]float64{"CPU": 1}, node.GetAvailableResources(ClusterResourceConstraint))
	assert.Equal(t, map[string]float64{"CPU": 1}, node.TotalResources)
	assert.Equal(t, map[string]string{"foo": "bar"}, node.Labels)

	// An outdated node
	im = makeIMInstance("type_no_longer_exists", proto.Instance_REQUESTED, "1")
	instance = makeAutoscalerInstance(im, nil, "")
	node = NewNodeFromInstance(instance, nodeTypeConfigs, false)
	assert.NotNil(t, node)
	assert.Equal(t, "type_no_longer_exists", node.NodeType)
	assert.Equal(t, ToTerminate, node.Status)
	assert.NotNil(t, node.TerminationRequest)
	assert.Equal(t, proto.TerminationRequest_OUTDATED, node.TerminationRequest.Cause)

	node = NewNodeFromInstance(instance, nodeTypeConfigs, true)
	assert.Nil(t, node)

	// A pending ray node
	im = makeIMInstance("type_1", proto.Instance_REQUESTED, "1")
	instance = makeAutoscalerInstance(im, nil, "")
	node = NewNodeFromInstance(instance, nodeTypeConfigs, false)
	assert.NotNil(t, node)
	assert.Equal(t, "type_1", node.NodeType)
	assert.Equal(t, Schedulable, node.Status)
	assert.Equal(t, map[string]float64{"CPU": 1}, node.GetAvailableResources(PendingDemand))
	assert.Equal(t, map[string]float64{"CPU": 1}, node.GetAvailableResources(ClusterResourceConstraint))
	assert.Equal(t, map[string]float64{"CPU": 1}, node.TotalResources)
	assert.Equal(t, map[string]string{"foo": "foo"}, node.Labels)
}

func TestSchedulingNode_NewHeadNode(t *testing.T) {
	nodeTypeConfigs := map[string]*instance_manager.NodeTypeConfig{
		"head": testNodeTypeConfig(
			"head", map[string]float64{"CPU": 1}, 0, 1, nil, 0, 0,
		),
	}

	// An allocated head node
	im := makeIMInstance("head", proto.Instance_ALLOCATED, "1")
	im.NodeKind = proto.NodeKind_HEAD
	instance := makeAutoscalerInstance(im, nil, "")
	node := NewNodeFromInstance(instance, nodeTypeConfigs, false)
	assert.NotNil(t, node)
	assert.Equal(t, proto.NodeKind_HEAD, node.NodeKind)
	assert.Equal(t, Schedulable, node.Status)

	// A running head node
	rayNode := makeRayNodeState("r1", "head",
		map[string]float64{"CPU": 0},
		map[string]float64{"CPU": 1},
		0, nil)
	im = makeIMInstance("head", proto.Instance_RAY_RUNNING, "1")
	im.NodeKind = proto.NodeKind_HEAD
	nodeID := "r1"
	im.NodeId = &nodeID
	instance = makeAutoscalerInstance(im, rayNode, "")
	node = NewNodeFromInstance(instance, nodeTypeConfigs, false)
	assert.NotNil(t, node)
	assert.Equal(t, proto.NodeKind_HEAD, node.NodeKind)
	assert.Equal(t, Schedulable, node.Status)
}

func TestMinWorkerNodes(t *testing.T) {
	scheduler := NewResourceDemandScheduler(nil)
	nodeTypeConfigs := map[string]*instance_manager.NodeTypeConfig{
		"type_1": testNodeTypeConfig("type_1", map[string]float64{"CPU": 1}, 1, 10, nil, 0, 0),
		"type_2": testNodeTypeConfig("type_2", map[string]float64{"CPU": 1}, 0, 10, nil, 0, 0),
		"type_3": testNodeTypeConfig("type_3", map[string]float64{"CPU": 1}, 2, 10, nil, 0, 0),
	}

	// With empty cluster
	req := schedRequest(nodeTypeConfigs, nil, nil, nil, nil, nil, nil, false, nil)
	reply := scheduler.Schedule(req)
	toLaunch, _ := launchAndTerminate(reply)
	assert.Equal(t, map[string]int{"type_1": 1, "type_3": 2}, toLaunch)

	// With existing ray nodes
	instances := []*AutoscalerInstance{
		makeAutoscalerInstance(
			makeIMInstance("type_1", proto.Instance_RAY_RUNNING, ""),
			makeRayNodeState("", "type_1", nil, nil, 0, nil),
			"",
		),
		makeAutoscalerInstance(
			makeIMInstance("type_1", proto.Instance_RAY_RUNNING, ""),
			makeRayNodeState("", "type_1", nil, nil, 0, nil),
			"",
		),
	}
	req = schedRequest(nodeTypeConfigs, nil, nil, nil, nil, instances, nil, false, nil)
	reply = scheduler.Schedule(req)
	toLaunch, _ = launchAndTerminate(reply)
	assert.Equal(t, map[string]int{"type_3": 2}, toLaunch)

	// With existing instances pending
	instances = []*AutoscalerInstance{
		makeAutoscalerInstance(makeIMInstance("type_1", proto.Instance_REQUESTED, ""), nil, ""),
		makeAutoscalerInstance(makeIMInstance("type_1", proto.Instance_ALLOCATED, ""), nil, ""),
		makeAutoscalerInstance(makeIMInstance("type_no_longer_exists", proto.Instance_REQUESTED, "0"), nil, ""),
	}
	req = schedRequest(nodeTypeConfigs, nil, nil, nil, nil, instances, nil, false, nil)
	reply = scheduler.Schedule(req)
	toLaunch, _ = launchAndTerminate(reply)
	assert.Equal(t, map[string]int{"type_3": 2}, toLaunch)
}

func TestMaxWorkersHeadNodeType(t *testing.T) {
	scheduler := NewResourceDemandScheduler(nil)
	nodeTypeConfigs := map[string]*instance_manager.NodeTypeConfig{
		"head_type": testNodeTypeConfig("head_type", map[string]float64{}, 0, 2, nil, 0, 0),
	}
	instances := []*AutoscalerInstance{
		makeAutoscalerInstance(func() *proto.Instance {
			im := makeIMInstance("head_type", proto.Instance_ALLOCATED, "0")
			im.NodeKind = proto.NodeKind_HEAD
			return im
		}(), nil, ""),
		makeAutoscalerInstance(makeIMInstance("head_type", proto.Instance_ALLOCATED, "1"), nil, ""),
		makeAutoscalerInstance(makeIMInstance("head_type", proto.Instance_ALLOCATED, "2"), nil, ""),
	}

	req := schedRequest(nodeTypeConfigs, nil, nil, nil, nil, instances, nil, false, nil)
	reply := scheduler.Schedule(req)
	_, toTerminate := launchAndTerminate(reply)
	assert.Len(t, toTerminate, 1)
	instanceID := toTerminate[0][0].(string)
	assert.Contains(t, []string{"1", "2"}, instanceID)
	assert.Equal(t, proto.TerminationRequest_MAX_NUM_NODE_PER_TYPE, toTerminate[0][2])
}

func TestMaxWorkersPerType(t *testing.T) {
	scheduler := NewResourceDemandScheduler(nil)
	nodeTypeConfigs := map[string]*instance_manager.NodeTypeConfig{
		"type_1": testNodeTypeConfig("type_1", map[string]float64{"CPU": 1}, 2, 2, nil, 0, 0),
	}

	req := schedRequest(nodeTypeConfigs, nil, nil, nil, nil, nil, nil, false, nil)
	reply := scheduler.Schedule(req)
	_, toTerminate := launchAndTerminate(reply)
	assert.Len(t, toTerminate, 0)

	// 3 instances (1 pending allocated + 2 running) with max 2 for type_1
	instances := []*AutoscalerInstance{
		makeAutoscalerInstance(makeIMInstance("type_1", proto.Instance_ALLOCATED, "0"), nil, ""),
		makeAutoscalerInstance(
			makeIMInstance("type_1", proto.Instance_RAY_RUNNING, "1"),
			makeRayNodeState("r1", "type_1",
				map[string]float64{"CPU": 1}, map[string]float64{"CPU": 1}, 0, nil),
			"",
		),
		makeAutoscalerInstance(
			makeIMInstance("type_1", proto.Instance_RAY_RUNNING, "2"),
			makeRayNodeState("r2", "type_1",
				map[string]float64{"CPU": 0.5}, map[string]float64{"CPU": 1}, 0, nil),
			"",
		),
	}
	// With min_worker_nodes=2, max_worker_nodes=2, 3 instances -> terminate the extra one
	req = schedRequest(
		map[string]*instance_manager.NodeTypeConfig{
			"type_1": testNodeTypeConfig("type_1", map[string]float64{"CPU": 1}, 2, 2, nil, 0, 0),
		},
		nil, nil, nil, nil, instances, nil, false, nil)
	reply = scheduler.Schedule(req)
	_, toTerminate = launchAndTerminate(reply)
	assert.Equal(t, [][3]interface{}{
		{"0", "", proto.TerminationRequest_MAX_NUM_NODE_PER_TYPE},
	}, toTerminate)

	// max_worker_nodes=1 -> terminate 2 nodes (lower util first)
	nodeTypeConfigs = map[string]*instance_manager.NodeTypeConfig{
		"type_1": testNodeTypeConfig("type_1", map[string]float64{"CPU": 1}, 0, 1, nil, 0, 0),
	}
	req = schedRequest(nodeTypeConfigs, nil, nil, nil, nil, instances, nil, false, nil)
	reply = scheduler.Schedule(req)
	_, toTerminate = launchAndTerminate(reply)
	assert.Equal(t, [][3]interface{}{
		{"0", "", proto.TerminationRequest_MAX_NUM_NODE_PER_TYPE},
		{"1", "r1", proto.TerminationRequest_MAX_NUM_NODE_PER_TYPE},
	}, toTerminate)
}

func TestTerminateMaxAllocatedWorkersPerType(t *testing.T) {
	scheduler := NewResourceDemandScheduler(nil)
	nodeTypeConfigs := map[string]*instance_manager.NodeTypeConfig{
		"type_1": testNodeTypeConfig("type_1", map[string]float64{"CPU": 1}, 0, 2, nil, 0, 0),
	}

	req := schedRequest(nodeTypeConfigs, nil, nil, nil, nil, nil, nil, false, nil)
	reply := scheduler.Schedule(req)
	_, toTerminate := launchAndTerminate(reply)
	assert.Len(t, toTerminate, 0)

	// 2 allocated instances with max 2 -> keep both
	instances := []*AutoscalerInstance{
		makeAutoscalerInstance(makeIMInstance("type_1", proto.Instance_ALLOCATED, "0"), nil, ""),
		makeAutoscalerInstance(makeIMInstance("type_1", proto.Instance_ALLOCATED, "1"), nil, ""),
	}
	req = schedRequest(nodeTypeConfigs, nil, nil, nil, nil, instances, nil, false, nil)
	reply = scheduler.Schedule(req)
	_, toTerminate = launchAndTerminate(reply)
	assert.Len(t, toTerminate, 0)

	// max_worker_nodes=0 -> terminate both
	nodeTypeConfigs = map[string]*instance_manager.NodeTypeConfig{
		"type_1": testNodeTypeConfig("type_1", map[string]float64{"CPU": 1}, 0, 0, nil, 0, 0),
	}
	req = schedRequest(nodeTypeConfigs, nil, nil, nil, nil, instances, nil, false, nil)
	reply = scheduler.Schedule(req)
	_, toTerminate = launchAndTerminate(reply)
	assert.Equal(t, [][3]interface{}{
		{"0", "", proto.TerminationRequest_MAX_NUM_NODE_PER_TYPE},
		{"1", "", proto.TerminationRequest_MAX_NUM_NODE_PER_TYPE},
	}, toTerminate)
}

func TestMaxNumNodes(t *testing.T) {
	scheduler := NewResourceDemandScheduler(nil)
	nodeTypeConfigs := map[string]*instance_manager.NodeTypeConfig{
		"type_1": testNodeTypeConfig("type_1", map[string]float64{"CPU": 1}, 0, 2, nil, 0, 0),
		"type_2": testNodeTypeConfig("type_2", map[string]float64{"CPU": 1}, 0, 2, nil, 0, 0),
	}

	req := schedRequest(nodeTypeConfigs, int64Ptr(1), nil, nil, nil, nil, nil, false, nil)
	reply := scheduler.Schedule(req)
	_, toTerminate := launchAndTerminate(reply)
	assert.Len(t, toTerminate, 0)

	// 4 running instances with various idle/util
	instances := []*AutoscalerInstance{
		makeAutoscalerInstance(makeIMInstance("type_1", proto.Instance_ALLOCATED, "0"), nil, ""),
		makeAutoscalerInstance(
			makeIMInstance("type_1", proto.Instance_RAY_RUNNING, "1"),
			makeRayNodeState("r1", "type_1",
				map[string]float64{"CPU": 1}, map[string]float64{"CPU": 1}, 10, nil),
			"",
		),
		makeAutoscalerInstance(
			makeIMInstance("type_2", proto.Instance_RAY_RUNNING, "2"),
			makeRayNodeState("r2", "type_2",
				map[string]float64{"CPU": 0.5}, map[string]float64{"CPU": 1}, 0, nil),
			"",
		),
		makeAutoscalerInstance(
			makeIMInstance("type_2", proto.Instance_RAY_RUNNING, "3"),
			makeRayNodeState("r3", "type_2",
				map[string]float64{"CPU": 0.0}, map[string]float64{"CPU": 1}, 0, nil),
			"",
		),
	}

	// 4 max -> no termination
	req = schedRequest(nodeTypeConfigs, int64Ptr(4), nil, nil, nil, instances, nil, false, nil)
	reply = scheduler.Schedule(req)
	_, toTerminate = launchAndTerminate(reply)
	assert.Len(t, toTerminate, 0)

	// 3 max -> terminate non-ray-running first
	req = schedRequest(nodeTypeConfigs, int64Ptr(3), nil, nil, nil, instances, nil, false, nil)
	reply = scheduler.Schedule(req)
	_, toTerminate = launchAndTerminate(reply)
	assert.Equal(t, [][3]interface{}{
		{"0", "", proto.TerminationRequest_MAX_NUM_NODES},
	}, toTerminate)

	// 2 max -> terminate non-ray-running + idle
	req = schedRequest(nodeTypeConfigs, int64Ptr(2), nil, nil, nil, instances, nil, false, nil)
	reply = scheduler.Schedule(req)
	_, toTerminate = launchAndTerminate(reply)
	assert.Equal(t, [][3]interface{}{
		{"0", "", proto.TerminationRequest_MAX_NUM_NODES},
		{"1", "r1", proto.TerminationRequest_MAX_NUM_NODES},
	}, toTerminate)

	// 1 max -> terminate 3 nodes
	req = schedRequest(nodeTypeConfigs, int64Ptr(1), nil, nil, nil, instances, nil, false, nil)
	reply = scheduler.Schedule(req)
	_, toTerminate = launchAndTerminate(reply)
	assert.Equal(t, [][3]interface{}{
		{"0", "", proto.TerminationRequest_MAX_NUM_NODES},
		{"1", "r1", proto.TerminationRequest_MAX_NUM_NODES},
		{"2", "r2", proto.TerminationRequest_MAX_NUM_NODES},
	}, toTerminate)

	// Combine max_num_nodes with max_num_nodes_per_type
	nodeTypeConfigs = map[string]*instance_manager.NodeTypeConfig{
		"type_1": testNodeTypeConfig("type_1", map[string]float64{"CPU": 1}, 0, 2, nil, 0, 0),
		"type_2": testNodeTypeConfig("type_2", map[string]float64{"CPU": 1}, 0, 0, nil, 0, 0),
	}
	req = schedRequest(nodeTypeConfigs, int64Ptr(1), nil, nil, nil, instances, nil, false, nil)
	reply = scheduler.Schedule(req)
	_, toTerminate = launchAndTerminate(reply)
	// sort toTerminate by instance_id for deterministic assertion
	toTerminate = sortedByInstanceID(toTerminate)
	assert.Equal(t, [][3]interface{}{
		{"0", "", proto.TerminationRequest_MAX_NUM_NODES},
		{"2", "r2", proto.TerminationRequest_MAX_NUM_NODE_PER_TYPE},
		{"3", "r3", proto.TerminationRequest_MAX_NUM_NODE_PER_TYPE},
	}, toTerminate)
}

func sortedByInstanceID(terms [][3]interface{}) [][3]interface{} {
	out := make([][3]interface{}, len(terms))
	copy(out, terms)
	for i := 0; i < len(out); i++ {
		for j := i + 1; j < len(out); j++ {
			if out[j][0].(string) < out[i][0].(string) {
				out[i], out[j] = out[j], out[i]
			}
		}
	}
	return out
}

func TestSingleResources(t *testing.T) {
	scheduler := NewResourceDemandScheduler(nil)
	nodeTypeConfigs := map[string]*instance_manager.NodeTypeConfig{
		"type_1": testNodeTypeConfig("type_1", map[string]float64{"CPU": 1}, 0, 10, nil, 0, 0),
	}

	// Request 1 CPU should start a node.
	req := schedRequest(nodeTypeConfigs, nil, []*proto.ResourceRequest{
		makeRequest(map[string]float64{"CPU": 1}),
	}, nil, nil, nil, nil, false, nil)
	reply := scheduler.Schedule(req)
	toLaunch, _ := launchAndTerminate(reply)
	assert.Equal(t, map[string]int{"type_1": 1}, toLaunch)

	// Request multiple CPUs should start multiple nodes
	req = schedRequest(nodeTypeConfigs, nil, []*proto.ResourceRequest{
		makeRequest(map[string]float64{"CPU": 1}),
		makeRequest(map[string]float64{"CPU": 1}),
		makeRequest(map[string]float64{"CPU": 1}),
	}, nil, nil, nil, nil, false, nil)
	reply = scheduler.Schedule(req)
	toLaunch, _ = launchAndTerminate(reply)
	assert.Equal(t, map[string]int{"type_1": 3}, toLaunch)

	// Existing node with sufficient resources
	instances := []*AutoscalerInstance{
		makeAutoscalerInstance(
			makeIMInstance("type_1", proto.Instance_RAY_RUNNING, "1"),
			makeRayNodeState("r1", "type_1",
				map[string]float64{"CPU": 1}, map[string]float64{"CPU": 1}, 0, nil),
			"",
		),
	}
	req = schedRequest(nodeTypeConfigs, nil, []*proto.ResourceRequest{
		makeRequest(map[string]float64{"CPU": 1}),
	}, nil, nil, instances, nil, false, nil)
	reply = scheduler.Schedule(req)
	toLaunch, _ = launchAndTerminate(reply)
	assert.Equal(t, map[string]int{}, toLaunch)

	// Existing node insufficient resources -> launch new node
	instances = []*AutoscalerInstance{
		makeAutoscalerInstance(
			makeIMInstance("type_1", proto.Instance_RAY_RUNNING, "1"),
			makeRayNodeState("r1", "type_1",
				map[string]float64{"CPU": 0.9}, map[string]float64{"CPU": 1}, 0, nil),
			"",
		),
	}
	req = schedRequest(nodeTypeConfigs, nil, []*proto.ResourceRequest{
		makeRequest(map[string]float64{"CPU": 1}),
	}, nil, nil, instances, nil, false, nil)
	reply = scheduler.Schedule(req)
	toLaunch, _ = launchAndTerminate(reply)
	assert.Equal(t, map[string]int{"type_1": 1}, toLaunch)

	// Existing pending node should NOT launch new nodes
	instances = []*AutoscalerInstance{
		makeAutoscalerInstance(makeIMInstance("type_1", proto.Instance_REQUESTED, "0"), nil, ""),
	}
	req = schedRequest(nodeTypeConfigs, nil, []*proto.ResourceRequest{
		makeRequest(map[string]float64{"CPU": 1}),
	}, nil, nil, instances, nil, false, nil)
	reply = scheduler.Schedule(req)
	toLaunch, _ = launchAndTerminate(reply)
	assert.Equal(t, map[string]int{}, toLaunch)
}

func TestImplicitResources(t *testing.T) {
	scheduler := NewResourceDemandScheduler(nil)
	nodeTypeConfigs := map[string]*instance_manager.NodeTypeConfig{
		"type_1": testNodeTypeConfig("type_1", map[string]float64{"CPU": 1}, 0, 10, nil, 0, 0),
	}
	implicitResource := implicitResourcePrefix + "a"

	// implicit resources should scale up clusters
	req := schedRequest(nodeTypeConfigs, nil, []*proto.ResourceRequest{
		makeRequest(map[string]float64{implicitResource: 1}),
	}, nil, nil, nil, nil, false, nil)
	reply := scheduler.Schedule(req)
	toLaunch, _ := launchAndTerminate(reply)
	assert.Equal(t, map[string]int{"type_1": 1}, toLaunch)

	// implicit resources should be satisfied by existing node
	instances := []*AutoscalerInstance{
		makeAutoscalerInstance(
			makeIMInstance("type_1", proto.Instance_RAY_RUNNING, "1"),
			makeRayNodeState("r1", "type_1",
				map[string]float64{"CPU": 1}, map[string]float64{"CPU": 1}, 0, nil),
			"",
		),
	}
	req = schedRequest(nodeTypeConfigs, nil, []*proto.ResourceRequest{
		makeRequest(map[string]float64{implicitResource: 1}),
		makeRequest(map[string]float64{"CPU": 1}),
	}, nil, nil, instances, nil, false, nil)
	reply = scheduler.Schedule(req)
	toLaunch, _ = launchAndTerminate(reply)
	assert.Equal(t, map[string]int{}, toLaunch)
}

func TestMaxWorkerNumEnforceWithResourceRequests(t *testing.T) {
	scheduler := NewResourceDemandScheduler(nil)
	nodeTypeConfigs := map[string]*instance_manager.NodeTypeConfig{
		"type_1": testNodeTypeConfig("type_1", map[string]float64{"CPU": 1}, 0, 10, nil, 0, 0),
	}
	var maxNumNodes int64 = 2

	// Request 10 CPUs should start at most 2 nodes (but existing 1 node + max 2 = 1 new)
	instances := []*AutoscalerInstance{
		makeAutoscalerInstance(
			makeIMInstance("type_1", proto.Instance_RAY_RUNNING, "1"),
			makeRayNodeState("r1", "type_1",
				map[string]float64{"CPU": 1}, map[string]float64{"CPU": 1}, 0, nil),
			"",
		),
	}
	req := schedRequest(nodeTypeConfigs, &maxNumNodes, []*proto.ResourceRequest{
		makeRequest(map[string]float64{"CPU": 1}),
		makeRequest(map[string]float64{"CPU": 1}),
		makeRequest(map[string]float64{"CPU": 1}),
	}, nil, nil, instances, nil, false, nil)
	reply := scheduler.Schedule(req)
	toLaunch, _ := launchAndTerminate(reply)
	assert.Equal(t, map[string]int{"type_1": 1}, toLaunch)
}

func TestMultiRequestsFittable(t *testing.T) {
	scheduler := NewResourceDemandScheduler(nil)
	nodeTypeConfigs := map[string]*instance_manager.NodeTypeConfig{
		"type_1": testNodeTypeConfig("type_1", map[string]float64{"CPU": 1, "GPU": 1}, 0, 1, nil, 0, 0),
		"type_2": testNodeTypeConfig("type_2", map[string]float64{"CPU": 3}, 0, 1, nil, 0, 0),
	}

	req := schedRequest(nodeTypeConfigs, nil, []*proto.ResourceRequest{
		makeRequest(map[string]float64{"CPU": 1}),
		makeRequest(map[string]float64{"CPU": 1}),
		makeRequest(map[string]float64{"CPU": 1}),
		makeRequest(map[string]float64{"CPU": 1, "GPU": 1}),
	}, nil, nil, nil, nil, false, nil)
	reply := scheduler.Schedule(req)
	toLaunch, _ := launchAndTerminate(reply)
	assert.Equal(t, map[string]int{"type_1": 1, "type_2": 1}, toLaunch)
	assert.Len(t, reply.InfeasibleResourceRequests, 0)

	// Change ordering
	req = schedRequest(nodeTypeConfigs, nil, []*proto.ResourceRequest{
		makeRequest(map[string]float64{"CPU": 1, "GPU": 1}),
		makeRequest(map[string]float64{"CPU": 1}),
		makeRequest(map[string]float64{"CPU": 1}),
		makeRequest(map[string]float64{"CPU": 1}),
	}, nil, nil, nil, nil, false, nil)
	reply = scheduler.Schedule(req)
	toLaunch, _ = launchAndTerminate(reply)
	assert.Equal(t, map[string]int{"type_1": 1, "type_2": 1}, toLaunch)
	assert.Len(t, reply.InfeasibleResourceRequests, 0)

	// Fragmentation case
	instances := []*AutoscalerInstance{
		makeAutoscalerInstance(
			makeIMInstance("type_1", proto.Instance_RAY_RUNNING, "1"),
			makeRayNodeState("r1", "type_1",
				map[string]float64{"CPU": 0, "GPU": 1}, map[string]float64{"CPU": 1, "GPU": 1}, 0, nil),
			"",
		),
	}
	req = schedRequest(nodeTypeConfigs, nil, []*proto.ResourceRequest{
		makeRequest(map[string]float64{"CPU": 1}),
		makeRequest(map[string]float64{"CPU": 1}),
		makeRequest(map[string]float64{"CPU": 1, "GPU": 1}),
	}, nil, nil, instances, nil, false, nil)
	reply = scheduler.Schedule(req)
	toLaunch, _ = launchAndTerminate(reply)
	assert.Equal(t, map[string]int{"type_2": 1}, toLaunch)
	assert.Len(t, reply.InfeasibleResourceRequests, 1)
}

func TestMultiNodeTypesScore(t *testing.T) {
	scheduler := NewResourceDemandScheduler(nil)
	nodeTypeConfigs := map[string]*instance_manager.NodeTypeConfig{
		"type_large": testNodeTypeConfig("type_large", map[string]float64{"CPU": 10}, 0, 1, nil, 0, 0),
		"type_small": testNodeTypeConfig("type_small", map[string]float64{"CPU": 5}, 0, 1, nil, 0, 0),
		"type_gpu":   testNodeTypeConfig("type_gpu", map[string]float64{"CPU": 2, "GPU": 2}, 0, 1, nil, 0, 0),
	}

	// Request 1 CPU should just start the small machine
	req := schedRequest(nodeTypeConfigs, nil, []*proto.ResourceRequest{
		makeRequest(map[string]float64{"CPU": 1}),
	}, nil, nil, nil, nil, false, nil)
	reply := scheduler.Schedule(req)
	toLaunch, _ := launchAndTerminate(reply)
	assert.Equal(t, map[string]int{"type_small": 1}, toLaunch)

	// type_small should be preferred over type_large
	req = schedRequest(nodeTypeConfigs, nil, []*proto.ResourceRequest{
		makeRequest(map[string]float64{"CPU": 2}),
	}, nil, nil, nil, nil, false, nil)
	reply = scheduler.Schedule(req)
	toLaunch, _ = launchAndTerminate(reply)
	assert.Equal(t, map[string]int{"type_small": 1}, toLaunch)
}

func TestMultiNodeTypesScoreWithGPU(t *testing.T) {
	scheduler := NewResourceDemandScheduler(nil)
	nodeTypeConfigs := map[string]*instance_manager.NodeTypeConfig{
		"type_gpu":   testNodeTypeConfig("type_gpu", map[string]float64{"CPU": 1, "GPU": 2}, 0, 1, nil, 0, 0),
		"type_multi": testNodeTypeConfig("type_multi", map[string]float64{"CPU": 2, "XXX": 2}, 0, 1, nil, 0, 0),
	}

	req := schedRequest(nodeTypeConfigs, nil, []*proto.ResourceRequest{
		makeRequest(map[string]float64{"CPU": 1}),
	}, nil, nil, nil, nil, false, nil)
	reply := scheduler.Schedule(req)
	toLaunch, _ := launchAndTerminate(reply)
	// with conserve GPU nodes, prefer the non-GPU node
	assert.Equal(t, map[string]int{"type_multi": 1}, toLaunch)
}
func TestResourceConstraints(t *testing.T) {
	scheduler := NewResourceDemandScheduler(nil)
	nodeTypeConfigs := map[string]*instance_manager.NodeTypeConfig{
		"type_cpu": testNodeTypeConfig("type_cpu", map[string]float64{"CPU": 1}, 1, 5, nil, 0, 0),
		"type_gpu": testNodeTypeConfig("type_gpu", map[string]float64{"CPU": 1, "GPU": 2}, 0, 1, nil, 0, 0),
	}

	// Resource constraints should not launch extra with min_nodes
	req := schedRequest(nodeTypeConfigs, nil, nil, nil, []*proto.ResourceRequest{
		makeRequest(map[string]float64{"CPU": 1}),
	}, nil, nil, false, nil)
	reply := scheduler.Schedule(req)
	toLaunch, _ := launchAndTerminate(reply)
	assert.Equal(t, map[string]int{"type_cpu": 1}, toLaunch)

	// Constraints should launch extra nodes
	req = schedRequest(nodeTypeConfigs, nil, nil, nil, []*proto.ResourceRequest{
		makeRequest(map[string]float64{"CPU": 1}),
		makeRequest(map[string]float64{"CPU": 1}),
		makeRequest(map[string]float64{"GPU": 1}),
	}, nil, nil, false, nil)
	reply = scheduler.Schedule(req)
	toLaunch, _ = launchAndTerminate(reply)
	assert.Equal(t, map[string]int{"type_cpu": 1, "type_gpu": 1}, toLaunch)

	// Resource constraints should not launch extra with max_nodes: infeasible atomically
	req = schedRequest(nodeTypeConfigs, nil, nil, nil, []*proto.ResourceRequest{
		makeRequest(map[string]float64{"CPU": 1}),
		makeRequest(map[string]float64{"CPU": 1}),
		makeRequest(map[string]float64{"GPU": 2}),
		makeRequest(map[string]float64{"GPU": 2}),
	}, nil, nil, false, nil)
	reply = scheduler.Schedule(req)
	toLaunch, _ = launchAndTerminate(reply)
	assert.Equal(t, map[string]int{"type_cpu": 1}, toLaunch)
	assert.Len(t, reply.InfeasibleClusterResourceConstraints, 1)
}

type outdatedTestCase struct {
	disableLaunchConfigCheck bool
}

func TestOutdatedNodes(t *testing.T) {
	for _, tc := range []outdatedTestCase{{false}, {true}} {
		scheduler := NewResourceDemandScheduler(nil)
		nodeTypeConfigs := map[string]*instance_manager.NodeTypeConfig{
			"type_cpu": func() *instance_manager.NodeTypeConfig {
				c := testNodeTypeConfig("type_cpu", map[string]float64{"CPU": 1}, 2, 5, nil, 0, 0)
				c.LaunchConfigHash = "hash1"
				return c
			}(),
			"head_node": func() *instance_manager.NodeTypeConfig {
				c := testNodeTypeConfig("head_node", map[string]float64{"CPU": 0}, 0, 1, nil, 0, 0)
				c.LaunchConfigHash = "hash2"
				return c
			}(),
		}

		mkRunningInstance := func(instanceID, launchHash, nodeKind string, instanceType string) *AutoscalerInstance {
			im := makeIMInstance(instanceType, proto.Instance_RAY_RUNNING, instanceID)
			launchHashCp := launchHash
			im.LaunchConfigHash = launchHashCp
			im.NodeKind = proto.NodeKind_WORKER
			if nodeKind == "HEAD" {
				im.NodeKind = proto.NodeKind_HEAD
			}
			rNode := makeRayNodeState(instanceID, instanceType,
				map[string]float64{"CPU": 1}, map[string]float64{"CPU": 1}, 0, nil)
			im.NodeId = &instanceID
			return makeAutoscalerInstance(im, rNode, "c-"+instanceID)
		}

		instances := []*AutoscalerInstance{
			mkRunningInstance("i-1", "hash2", "WORKER", "type_cpu"), // outdated
			mkRunningInstance("i-2", "hash1", "WORKER", "type_cpu"), // matched
			mkRunningInstance("i-3", "hash1", "HEAD", "head_node"),  // mismatched head
		}

		req := schedRequest(nodeTypeConfigs, nil, nil, nil, nil, instances, nil, tc.disableLaunchConfigCheck, nil)
		reply := scheduler.Schedule(req)
		toLaunch, toTerminate := launchAndTerminate(reply)

		if !tc.disableLaunchConfigCheck {
			assert.Equal(t, [][3]interface{}{
				{"i-1", "i-1", proto.TerminationRequest_OUTDATED},
			}, toTerminate)
			assert.Equal(t, map[string]int{"type_cpu": 1}, toLaunch) // launch 1 to replace outdated
		} else {
			assert.Len(t, toTerminate, 0)
			assert.Equal(t, map[string]int{}, toLaunch)
		}
	}
}

func TestIdleTermination(t *testing.T) {
	for _, idleTimeoutS := range []float64{1, 2, 10} {
		for _, hasResourceConstraints := range []bool{true, false} {
			for _, hasResourceRequests := range []bool{true, false} {
				for _, hasGangResourceRequests := range []bool{true, false} {
					runIdleTerminationCase(t, idleTimeoutS, hasResourceConstraints, hasResourceRequests, hasGangResourceRequests)
				}
			}
		}
	}
}

func runIdleTerminationCase(
	t *testing.T,
	idleTimeoutS float64,
	hasResourceConstraints, hasResourceRequests, hasGangResourceRequests bool,
) {
	scheduler := NewResourceDemandScheduler(nil)
	nodeTypeConfigs := map[string]*instance_manager.NodeTypeConfig{
		"type_cpu": func() *instance_manager.NodeTypeConfig {
			c := testNodeTypeConfig("type_cpu", map[string]float64{"CPU": 1}, 0, 5, nil, 0, 0)
			c.LaunchConfigHash = "hash1"
			return c
		}(),
		"head_node": func() *instance_manager.NodeTypeConfig {
			c := testNodeTypeConfig("head_node", map[string]float64{"CPU": 0}, 0, 1, nil, 0, 0)
			c.LaunchConfigHash = "hash2"
			return c
		}(),
	}

	idleTimeS := 5
	var constraints []*proto.ResourceRequest
	if hasResourceConstraints {
		constraints = []*proto.ResourceRequest{
			makeRequest(map[string]float64{"CPU": 1}),
			makeRequest(map[string]float64{"CPU": 1}),
		}
	}
	var resourceRequests []*proto.ResourceRequest
	if hasResourceRequests {
		resourceRequests = []*proto.ResourceRequest{
			makeRequest(map[string]float64{"CPU": 1}),
			makeRequest(map[string]float64{"CPU": 1}),
		}
	}
	var gangResourceRequests [][]*proto.ResourceRequest
	if hasGangResourceRequests {
		gangResourceRequests = [][]*proto.ResourceRequest{
			{
				makeWithPlacementConstraint(makeRequest(map[string]float64{"CPU": 1}), "pg", ""),
				makeWithPlacementConstraint(makeRequest(map[string]float64{"CPU": 1}), "pg", ""),
			},
		}
	}

	mkInstance := func(instanceID string, instanceType string, available, total map[string]float64, idleMS int64, nodeKind proto.NodeKind) *AutoscalerInstance {
		im := makeIMInstance(instanceType, proto.Instance_RAY_RUNNING, instanceID)
		hash := "hash1"
		if instanceType == "head_node" {
			hash = "hash2"
		}
		im.LaunchConfigHash = hash
		im.NodeKind = nodeKind
		im.NodeId = &instanceID
		rayNode := makeRayNodeState(instanceID, instanceType, available, total, idleMS, nil)
		return makeAutoscalerInstance(im, rayNode, "c-"+instanceID)
	}

	instances := []*AutoscalerInstance{
		mkInstance("i-1", "type_cpu", map[string]float64{"CPU": 0}, map[string]float64{"CPU": 1}, 0, proto.NodeKind_WORKER),
		mkInstance("i-2", "type_cpu", map[string]float64{"CPU": 1}, map[string]float64{"CPU": 1}, int64(idleTimeS*1000), proto.NodeKind_WORKER),
		mkInstance("i-3", "head_node", map[string]float64{"CPU": 0}, map[string]float64{"CPU": 0}, 999*1000, proto.NodeKind_HEAD),
	}

	var timeoutPtr *float64 = &idleTimeoutS
	req := schedRequest(nodeTypeConfigs, nil, resourceRequests, gangResourceRequests, constraints, instances, timeoutPtr, false, nil)
	reply := scheduler.Schedule(req)
	_, toTerminate := launchAndTerminate(reply)

	if idleTimeoutS <= float64(idleTimeS) &&
		!hasResourceConstraints && !hasResourceRequests && !hasGangResourceRequests {
		assert.Equal(t, [][3]interface{}{
			{"i-2", "i-2", proto.TerminationRequest_IDLE},
		}, toTerminate)
	} else {
		assert.Len(t, toTerminate, 0)
	}
}

func TestIdleTerminationWithMinWorker(t *testing.T) {
	for _, minWorkers := range []int32{0, 1} {
		idleTimeoutS := 1.0
		scheduler := NewResourceDemandScheduler(nil)
		nodeTypeConfigs := map[string]*instance_manager.NodeTypeConfig{
			"type_cpu": func() *instance_manager.NodeTypeConfig {
				c := testNodeTypeConfig("type_cpu", map[string]float64{"CPU": 1}, minWorkers, 5, nil, 0, 0)
				c.LaunchConfigHash = "hash1"
				return c
			}(),
			"head_node": func() *instance_manager.NodeTypeConfig {
				c := testNodeTypeConfig("head_node", map[string]float64{"CPU": 0}, 0, 1, nil, 0, 0)
				c.LaunchConfigHash = "hash2"
				return c
			}(),
		}
		idleTimeS := 5

		mkInstance := func(instanceID string, instanceType string, idleMS int64, nodeKind proto.NodeKind) *AutoscalerInstance {
			im := makeIMInstance(instanceType, proto.Instance_RAY_RUNNING, instanceID)
			hash := "hash1"
			if instanceType == "head_node" {
				hash = "hash2"
			}
			im.LaunchConfigHash = hash
			im.NodeKind = nodeKind
			im.NodeId = &instanceID
			return makeAutoscalerInstance(im, makeRayNodeState(instanceID, instanceType,
				map[string]float64{"CPU": 1}, map[string]float64{"CPU": 1}, idleMS, nil), "c-"+instanceID)
		}

		instances := []*AutoscalerInstance{
			mkInstance("i-1", "type_cpu", int64(idleTimeS*1000), proto.NodeKind_WORKER),
			mkInstance("i-2", "head_node", 999*1000, proto.NodeKind_HEAD),
		}

		req := schedRequest(nodeTypeConfigs, nil, nil, nil, nil, instances, &idleTimeoutS, false, nil)
		reply := scheduler.Schedule(req)
		_, toTerminate := launchAndTerminate(reply)

		if minWorkers == 0 {
			assert.Equal(t, [][3]interface{}{
				{"i-1", "i-1", proto.TerminationRequest_IDLE},
			}, toTerminate)
		} else {
			assert.Len(t, toTerminate, 0)
		}
	}
}

// TestIdleWorkerNodes covers the various idle_worker_nodes scenarios.
func TestIdleWorkerNodesNormalScaleUp(t *testing.T) {
	scheduler := NewResourceDemandScheduler(nil)
	nodeTypeConfigs := map[string]*instance_manager.NodeTypeConfig{
		"type_1": testNodeTypeConfig("type_1", map[string]float64{"CPU": 1}, 0, 10, nil, 2, 0),
	}

	// Currently 2 busy nodes (available=0, no resource requests but available
	// != total); request 3 workers.
	instances := []*AutoscalerInstance{
		mkBusyInstance("0", "type_1"),
		mkBusyInstance("1", "type_1"),
	}
	req := schedRequest(nodeTypeConfigs, nil, []*proto.ResourceRequest{
		makeRequest(map[string]float64{"CPU": 1}),
		makeRequest(map[string]float64{"CPU": 1}),
		makeRequest(map[string]float64{"CPU": 1}),
	}, nil, nil, instances, nil, false, nil)
	reply := scheduler.Schedule(req)
	toLaunch, _ := launchAndTerminate(reply)
	assert.Equal(t, map[string]int{"type_1": 5}, toLaunch)
}

func mkIdleInstance(instanceID, instanceType string, idleMS int64) *AutoscalerInstance {
	im := makeIMInstance(instanceType, proto.Instance_RAY_RUNNING, instanceID)
	avail := map[string]float64{"CPU": 1}
	total := map[string]float64{"CPU": 1}
	return makeAutoscalerInstance(im, makeRayNodeState(instanceID, instanceType, avail, total, idleMS, nil), "")
}

func TestIdleWorkerNodesScaleDown(t *testing.T) {
	scheduler := NewResourceDemandScheduler(nil)
	nodeTypeConfigs := map[string]*instance_manager.NodeTypeConfig{
		"type_1": func() *instance_manager.NodeTypeConfig {
			c := testNodeTypeConfig("type_1", map[string]float64{"CPU": 1}, 1, 10, nil, 2, 0)
			c.IdleTimeoutS = 300
			return c
		}(),
	}

	// 5 nodes: 3 idle beyond the 400s timeout, 2 busy.
	instances := []*AutoscalerInstance{
		mkIdleInstance("0", "type_1", 400*1000),
		mkIdleInstance("1", "type_1", 400*1000),
		mkIdleInstance("2", "type_1", 400*1000),
		mkBusyInstance("3", "type_1"),
		mkBusyInstance("4", "type_1"),
	}

	idleTimeoutS := 300.0
	req := schedRequest(nodeTypeConfigs, nil, nil, nil, nil, instances, &idleTimeoutS, false, nil)
	reply := scheduler.Schedule(req)
	_, toTerminate := launchAndTerminate(reply)

	// Only 1 idle node should terminate; keep 2 idle workers
	// (min_worker=1 but idle_worker=2 must be kept).
	assert.Len(t, toTerminate, 1)
	terminatedID := toTerminate[0][0].(string)
	assert.Contains(t, []string{"0", "1", "2"}, terminatedID)
}

func mkBusyInstance(instanceID, instanceType string) *AutoscalerInstance {
	im := makeIMInstance(instanceType, proto.Instance_RAY_RUNNING, instanceID)
	avail := map[string]float64{"CPU": 0}
	total := map[string]float64{"CPU": 1}
	return makeAutoscalerInstance(im, makeRayNodeState(instanceID, instanceType, avail, total, 0, nil), "")
}

func TestIdleWorkerNodesZero(t *testing.T) {
	scheduler := NewResourceDemandScheduler(nil)
	nodeTypeConfigs := map[string]*instance_manager.NodeTypeConfig{
		"type_1": testNodeTypeConfig("type_1", map[string]float64{"CPU": 1}, 0, 10, nil, 0, 0),
	}

	instances := []*AutoscalerInstance{mkBusyInstance("0", "type_1")}
	req := schedRequest(nodeTypeConfigs, nil, []*proto.ResourceRequest{
		makeRequest(map[string]float64{"CPU": 1}),
		makeRequest(map[string]float64{"CPU": 1}),
	}, nil, nil, instances, nil, false, nil)
	reply := scheduler.Schedule(req)
	toLaunch, _ := launchAndTerminate(reply)
	assert.Equal(t, map[string]int{"type_1": 2}, toLaunch)
}

func TestIdleWorkerNodesEqualMin(t *testing.T) {
	scheduler := NewResourceDemandScheduler(nil)
	nodeTypeConfigs := map[string]*instance_manager.NodeTypeConfig{
		"type_1": testNodeTypeConfig("type_1", map[string]float64{"CPU": 1}, 2, 10, nil, 2, 0),
	}

	// 2 idle nodes (400ms), no resource requests, idle_timeout=300s.
	instances := []*AutoscalerInstance{
		mkIdleInstance("0", "type_1", 400),
		mkIdleInstance("1", "type_1", 400),
	}
	idleTimeoutS := 300.0
	req := schedRequest(nodeTypeConfigs, nil, nil, nil, nil, instances, &idleTimeoutS, false, nil)
	reply := scheduler.Schedule(req)
	toLaunch, _ := launchAndTerminate(reply)
	assert.Equal(t, map[string]int{}, toLaunch)
}

func TestIdleWorkerNodesGreaterThanMin(t *testing.T) {
	scheduler := NewResourceDemandScheduler(nil)
	nodeTypeConfigs := map[string]*instance_manager.NodeTypeConfig{
		"type_1": testNodeTypeConfig("type_1", map[string]float64{"CPU": 1}, 2, 10, nil, 3, 0),
	}

	instances := []*AutoscalerInstance{
		mkIdleInstance("0", "type_1", 0),
		mkIdleInstance("1", "type_1", 0),
	}
	req := schedRequest(nodeTypeConfigs, nil, nil, nil, nil, instances, nil, false, nil)
	reply := scheduler.Schedule(req)
	toLaunch, _ := launchAndTerminate(reply)
	assert.Equal(t, map[string]int{"type_1": 1}, toLaunch)
}

func TestIdleWorkerNodesExceedMax(t *testing.T) {
	scheduler := NewResourceDemandScheduler(nil)
	nodeTypeConfigs := map[string]*instance_manager.NodeTypeConfig{
		"type_1": testNodeTypeConfig("type_1", map[string]float64{"CPU": 1}, 0, 5, nil, 10, 0),
	}

	instances := []*AutoscalerInstance{
		mkIdleInstance("0", "type_1", 0),
		mkIdleInstance("1", "type_1", 0),
		mkIdleInstance("2", "type_1", 0),
	}
	req := schedRequest(nodeTypeConfigs, nil, nil, nil, nil, instances, nil, false, nil)
	reply := scheduler.Schedule(req)
	toLaunch, _ := launchAndTerminate(reply)
	assert.Equal(t, map[string]int{"type_1": 2}, toLaunch)
}

func TestIdleWorkerNodesMultipleTypes(t *testing.T) {
	scheduler := NewResourceDemandScheduler(nil)
	nodeTypeConfigs := map[string]*instance_manager.NodeTypeConfig{
		"cpu_worker": testNodeTypeConfig("cpu_worker", map[string]float64{"CPU": 1}, 1, 10, nil, 2, 0),
		"gpu_worker": testNodeTypeConfig("gpu_worker", map[string]float64{"GPU": 1}, 1, 10, nil, 1, 0),
	}

	instances := []*AutoscalerInstance{
		mkIdleInstance("0", "cpu_worker", 0),
		mkIdleInstance("1", "gpu_worker", 0),
	}
	req := schedRequest(nodeTypeConfigs, nil, nil, nil, nil, instances, nil, false, nil)
	reply := scheduler.Schedule(req)
	toLaunch, _ := launchAndTerminate(reply)
	assert.Equal(t, map[string]int{"cpu_worker": 1}, toLaunch)
}

func TestIdleTerminationWithNodeTypeIdleTimeout(t *testing.T) {
	for _, nodeTypeIdleTimeoutS := range []float64{1, 2, 10} {
		scheduler := NewResourceDemandScheduler(nil)
		nodeTypeConfigs := map[string]*instance_manager.NodeTypeConfig{
			"type_cpu_with_idle_timeout": func() *instance_manager.NodeTypeConfig {
				c := testNodeTypeConfig("type_cpu_with_idle_timeout", map[string]float64{"CPU": 1}, 0, 5, nil, 0, 0)
				c.LaunchConfigHash = "hash1"
				c.IdleTimeoutS = nodeTypeIdleTimeoutS
				return c
			}(),
		}
		idleTimeS := 5

		mkInstance := func(instanceID string, idleMS int64) *AutoscalerInstance {
			im := makeIMInstance("type_cpu_with_idle_timeout", proto.Instance_RAY_RUNNING, instanceID)
			im.LaunchConfigHash = "hash1"
			im.NodeId = &instanceID
			return makeAutoscalerInstance(im, makeRayNodeState(instanceID, "type_cpu_with_idle_timeout",
				map[string]float64{"CPU": 1}, map[string]float64{"CPU": 1}, idleMS, nil), "c-"+instanceID)
		}

		instances := []*AutoscalerInstance{
			mkInstance("i-1", 0),
			mkInstance("i-2", int64(idleTimeS*1000)),
		}
		// The autoscaler global idle_timeout is set much larger than the node
		// type one.
		globalTimeout := float64(idleTimeS * 1000)
		req := schedRequest(nodeTypeConfigs, nil, nil, nil, nil, instances, &globalTimeout, false, nil)
		reply := scheduler.Schedule(req)
		_, toTerminate := launchAndTerminate(reply)

		if nodeTypeIdleTimeoutS <= float64(idleTimeS) {
			assert.Equal(t, [][3]interface{}{
				{"i-2", "i-2", proto.TerminationRequest_IDLE},
			}, toTerminate)
		} else {
			assert.Len(t, toTerminate, 0)
		}
	}
}

func TestGangScheduling(t *testing.T) {
	scheduler := NewResourceDemandScheduler(nil)
	nodeTypeConfigs := map[string]*instance_manager.NodeTypeConfig{
		"type_cpu": testNodeTypeConfig("type_cpu", map[string]float64{"CPU": 2}, 0, 5, nil, 0, 0),
	}

	// Affinity should be grouped on the same node
	req := schedRequest(nodeTypeConfigs, nil, nil, [][]*proto.ResourceRequest{
		{
			makeWithAffinity(makeRequest(map[string]float64{"CPU": 1}), "pg", ""),
			makeWithAffinity(makeRequest(map[string]float64{"CPU": 1}), "pg", ""),
		},
	}, nil, nil, nil, false, nil)
	reply := scheduler.Schedule(req)
	toLaunch, _ := launchAndTerminate(reply)
	assert.Equal(t, map[string]int{"type_cpu": 1}, toLaunch)

	// Anti-affinity should be placed on different nodes
	req = schedRequest(nodeTypeConfigs, nil, nil, [][]*proto.ResourceRequest{
		{
			makeWithPlacementConstraint(makeRequest(map[string]float64{"CPU": 1}), "pg", ""),
			makeWithPlacementConstraint(makeRequest(map[string]float64{"CPU": 1}), "pg", ""),
		},
	}, nil, nil, nil, false, nil)
	reply = scheduler.Schedule(req)
	toLaunch, _ = launchAndTerminate(reply)
	assert.Equal(t, map[string]int{"type_cpu": 2}, toLaunch)

	// Atomic gang scheduling
	req = schedRequest(nodeTypeConfigs, nil, nil, [][]*proto.ResourceRequest{
		{
			makeWithAffinity(makeRequest(map[string]float64{"CPU": 3}), "pg", ""),
			makeWithAffinity(makeRequest(map[string]float64{"CPU": 3}), "pg", ""),
		},
	}, nil, nil, nil, false, nil)
	reply = scheduler.Schedule(req)
	toLaunch, _ = launchAndTerminate(reply)
	assert.Equal(t, map[string]int{}, toLaunch)
	assert.Len(t, reply.InfeasibleGangResourceRequests, 1)
}

func TestGangSchedulingWithOthers(t *testing.T) {
	scheduler := NewResourceDemandScheduler(nil)
	nodeTypeConfigs := map[string]*instance_manager.NodeTypeConfig{
		"type_1": func() *instance_manager.NodeTypeConfig {
			c := testNodeTypeConfig("type_1", map[string]float64{"CPU": 4}, 2, 4, nil, 0, 0)
			c.LaunchConfigHash = "hash1"
			return c
		}(),
		"type_2": func() *instance_manager.NodeTypeConfig {
			c := testNodeTypeConfig("type_2", map[string]float64{"CPU": 1, "GPU": 1}, 0, 10, nil, 0, 0)
			c.LaunchConfigHash = "hash2"
			return c
		}(),
	}

	gangRequests := [][]*proto.ResourceRequest{
		{ // anti affinity, 4 requests
			makeWithPlacementConstraint(makeRequest(map[string]float64{"CPU": 2}), "ak", "av"),
			makeWithPlacementConstraint(makeRequest(map[string]float64{"CPU": 2}), "ak", "av"),
			makeWithPlacementConstraint(makeRequest(map[string]float64{"CPU": 2}), "ak", "av"),
			makeWithPlacementConstraint(makeRequest(map[string]float64{"CPU": 2}), "ak", "av"),
		},
		{ // affinity
			makeWithAffinity(makeRequest(map[string]float64{"CPU": 3}), "c", "c1"),
			makeWithAffinity(makeRequest(map[string]float64{"CPU": 3}), "c", "c1"),
		},
		{ // no constraint
			makeRequest(map[string]float64{"CPU": 1}),
			makeRequest(map[string]float64{"CPU": 1}),
			makeRequest(map[string]float64{"CPU": 1}),
		},
	}

	resourceRequests := []*proto.ResourceRequest{
		makeRequest(map[string]float64{"CPU": 2}),
		makeRequest(map[string]float64{"GPU": 1, "CPU": 1}),
		makeRequest(map[string]float64{"GPU": 1}),
	}

	var constraints []*proto.ResourceRequest
	for i := 0; i < 10; i++ {
		constraints = append(constraints, makeRequest(map[string]float64{"CPU": 1}))
	}

	// Existing nodes.
	im1 := makeIMInstance("type_1", proto.Instance_RAY_RUNNING, "i-1")
	im1.LaunchConfigHash = "hash1"
	im1.NodeId = strPtr("r-1")
	inst1 := makeAutoscalerInstance(im1, makeRayNodeState("r-1", "type_1",
		map[string]float64{"CPU": 2}, map[string]float64{"CPU": 4}, 0, nil), "c-1")

	im2 := makeIMInstance("type_2", proto.Instance_RAY_RUNNING, "i-2")
	im2.LaunchConfigHash = "hash2"
	im2.NodeId = strPtr("r-2")
	inst2 := makeAutoscalerInstance(im2, makeRayNodeState("r-2", "type_2",
		map[string]float64{"CPU": 1, "GPU": 1}, map[string]float64{"CPU": 1, "GPU": 1}, 0, nil), "c-2")

	instances := []*AutoscalerInstance{inst1, inst2}

	idleTimeout := 999.0
	req := schedRequest(nodeTypeConfigs, nil, resourceRequests, gangRequests, constraints, instances, &idleTimeout, false, nil)
	reply := scheduler.Schedule(req)
	toLaunch, _ := launchAndTerminate(reply)

	// - added 3 type_1, 1 type_2
	assert.Equal(t, map[string]int{"type_1": 3, "type_2": 1}, toLaunch)
	assert.Len(t, reply.InfeasibleGangResourceRequests, 1)
	assert.Len(t, reply.InfeasibleResourceRequests, 0)
}

func TestBinPack(t *testing.T) {
	binPackResidual := func(nodeResources map[string]map[string]float64, resourceMaps []map[string]float64, antiAffinity bool) []map[string]float64 {
		nodeTypeConfigs := map[string]*instance_manager.NodeTypeConfig{}
		for typeName, res := range nodeResources {
			nodeTypeConfigs[typeName] = testNodeTypeConfig(typeName, res, 0, 1, nil, 0, 0)
		}
		requests := make([]*proto.ResourceRequest, 0, len(resourceMaps))
		for _, r := range resourceMaps {
			requests = append(requests, makeRequest(r))
		}
		reply := schedule(nodeTypeConfigs, map[string]int{}, requests, antiAffinity, nil, nil)
		if antiAffinity {
			infeasible := []map[string]float64{}
			for _, r := range reply.InfeasibleGangResourceRequests {
				for _, req := range r.Requests {
					infeasible = append(infeasible, req.ResourcesBundle)
				}
			}
			return infeasible
		}
		out := make([]map[string]float64, 0, len(reply.InfeasibleResourceRequests))
		for _, r := range reply.InfeasibleResourceRequests {
			out = append(out, r.ResourcesBundle)
		}
		return out
	}

	assert.Equal(t, []map[string]float64{{"GPU": 2}, {"GPU": 2}},
		binPackResidual(map[string]map[string]float64{"type_1": {"CPU": 0}}, []map[string]float64{{"GPU": 2}, {"GPU": 2}}, false))
	assert.Equal(t, []map[string]float64{{"GPU": 2}},
		binPackResidual(map[string]map[string]float64{"type_1": {"GPU": 2}}, []map[string]float64{{"GPU": 2}, {"GPU": 2}}, false))
	assert.Equal(t, []map[string]float64{},
		binPackResidual(map[string]map[string]float64{"type_1": {"GPU": 4}}, []map[string]float64{{"GPU": 2}, {"GPU": 2}}, false))

	assert.Equal(t, []map[string]float64{},
		binPackResidual(map[string]map[string]float64{
			"type_1": {"GPU": 2},
			"type_2": {"GPU": 2, "CPU": 2},
		}, []map[string]float64{{"GPU": 2}, {"GPU": 2}}, false))
	assert.Equal(t, []map[string]float64{{"GPU": 2}},
		binPackResidual(map[string]map[string]float64{
			"type_1": {"GPU": 2},
			"type_2": {"CPU": 2},
		}, []map[string]float64{{"GPU": 2}, {"GPU": 2}}, false))

	assert.Equal(t, []map[string]float64{{"GPU": 1}, {"GPU": 1}},
		binPackResidual(map[string]map[string]float64{"type_1": {"GPU": 3}}, []map[string]float64{{"GPU": 1}, {"GPU": 1}}, true))
	assert.Equal(t, []map[string]float64{},
		binPackResidual(map[string]map[string]float64{"type_1": {"GPU": 3}}, []map[string]float64{{"GPU": 1}, {"GPU": 1}}, false))

	implicitResource := implicitResourcePrefix + "a"
	assert.Equal(t, []map[string]float64{},
		binPackResidual(map[string]map[string]float64{"type_1": {"CPU": 1}},
			[]map[string]float64{{implicitResource: 0.5}, {implicitResource: 0.5}}, false))
	assert.Equal(t, []map[string]float64{{implicitResource: 0.5}},
		binPackResidual(map[string]map[string]float64{"type_1": {"CPU": 1}},
			[]map[string]float64{{implicitResource: 1}, {implicitResource: 0.5}}, false))
}

func TestNodeScheduleScore(t *testing.T) {
	trySchedule := func(nodeResources map[string]float64, resourceMaps []map[string]float64, source ResourceRequestSource) ([]map[string]float64, UtilizationScore) {
		config := testNodeTypeConfig("type_1", nodeResources, 0, 1, nil, 0, 0)
		node := FromNodeConfig(config, Schedulable, proto.NodeKind_WORKER, "", 0)
		requests := make([]*proto.ResourceRequest, 0, len(resourceMaps))
		for _, r := range resourceMaps {
			requests = append(requests, makeRequest(r))
		}
		infeasible, score := node.TrySchedule(requests, source)
		out := make([]map[string]float64, 0, len(infeasible))
		for _, r := range infeasible {
			out = append(out, r.ResourcesBundle)
		}
		return out, score
	}

	// Base scores.
	_, score := trySchedule(map[string]float64{"CPU": 1}, []map[string]float64{{"CPU": 1}}, PendingDemand)
	assert.Equal(t, UtilizationScore{0, true, 1, 1.0, 1.0}, score)

	_, score = trySchedule(map[string]float64{"GPU": 4}, []map[string]float64{{"GPU": 2}}, PendingDemand)
	assert.Equal(t, UtilizationScore{0, true, 1, 0.5, 0.5}, score)

	_, score = trySchedule(map[string]float64{"GPU": 4}, []map[string]float64{{"GPU": 1}, {"GPU": 1}}, PendingDemand)
	assert.Equal(t, UtilizationScore{0, true, 1, 0.5, 0.5}, score)

	_, score = trySchedule(map[string]float64{"GPU": 2}, []map[string]float64{{"GPU": 2}}, PendingDemand)
	assert.Equal(t, UtilizationScore{0, true, 1, 2.0, 2.0}, score)

	_, score = trySchedule(map[string]float64{"GPU": 2}, []map[string]float64{{"GPU": 1}, {"GPU": 1}}, PendingDemand)
	assert.Equal(t, UtilizationScore{0, true, 1, 2.0, 2.0}, score)

	// Scores on a GPU node.
	_, score = trySchedule(map[string]float64{"GPU": 1, "CPU": 1}, []map[string]float64{{"CPU": 1}}, PendingDemand)
	assert.Equal(t, UtilizationScore{0, false, 1, 0.0, 0.5}, score)

	_, score = trySchedule(map[string]float64{"GPU": 1, "CPU": 1}, []map[string]float64{{"CPU": 1, "GPU": 1}}, PendingDemand)
	assert.Equal(t, UtilizationScore{0, true, 2, 1.0, 1.0}, score)

	// Zero resources.
	_, score = trySchedule(map[string]float64{"CPU": 0, "custom": 1}, []map[string]float64{{"custom": 1}}, PendingDemand)
	assert.Equal(t, UtilizationScore{0, true, 1, 1, 1}, score)

	infeasible, _ := trySchedule(map[string]float64{"CPU": 0, "custom": 1}, []map[string]float64{{"CPU": 1}}, PendingDemand)
	assert.Equal(t, []map[string]float64{{"CPU": 1}}, infeasible)

	// Implicit resources.
	implicitResource := implicitResourcePrefix + "a"
	_, score = trySchedule(map[string]float64{"CPU": 1}, []map[string]float64{{implicitResource: 1}}, PendingDemand)
	assert.Equal(t, UtilizationScore{0, true, 0, 0.0, 0.0}, score)

	infeasible, _ = trySchedule(map[string]float64{"CPU": 1},
		[]map[string]float64{{implicitResource: 1}, {implicitResource: 1}}, PendingDemand)
	assert.Equal(t, []map[string]float64{{implicitResource: 1}}, infeasible)
}

func TestNodeScheduleLabelSelectorScore(t *testing.T) {
	tryScheduleLS := func(nodeResources map[string]float64, nodeLabels map[string]string, selectors [][][3]interface{}) ([]map[string]float64, UtilizationScore) {
		config := testNodeTypeConfig("type_1", nodeResources, 0, 1, nodeLabels, 0, 0)
		node := FromNodeConfig(config, Schedulable, proto.NodeKind_WORKER, "", 0)

		req := makeWithLabelSelectors(map[string]float64{"CPU": 1}, selectors)
		infeasible, score := node.TrySchedule([]*proto.ResourceRequest{req}, PendingDemand)
		out := make([]map[string]float64, 0, len(infeasible))
		for _, r := range infeasible {
			out = append(out, r.ResourcesBundle)
		}
		return out, score
	}

	labels := map[string]string{"ray.io/accelerator-type": "A100"}

	// 1) A matching label selector.
	sel1 := [][][3]interface{}{
		{{"ray.io/accelerator-type", proto.LabelSelectorOperator_LABEL_OPERATOR_IN, []string{"TPU-v6e"}}},
		{{"ray.io/accelerator-type", proto.LabelSelectorOperator_LABEL_OPERATOR_IN, []string{"B200"}}},
		{{"ray.io/accelerator-type", proto.LabelSelectorOperator_LABEL_OPERATOR_IN, []string{"A100"}}},
	}
	infeasible, score := tryScheduleLS(map[string]float64{"CPU": 1}, labels, sel1)
	assert.Equal(t, []map[string]float64{}, infeasible)
	assert.Equal(t, UtilizationScore{1, true, 1, 1.0, 1.0}, score)

	// 2) A non-matching label selector.
	sel2 := [][][3]interface{}{
		{{"ray.io/accelerator-type", proto.LabelSelectorOperator_LABEL_OPERATOR_IN, []string{"B200"}}},
	}
	infeasible, score = tryScheduleLS(map[string]float64{"CPU": 1}, labels, sel2)
	assert.Equal(t, []map[string]float64{{"CPU": 1}}, infeasible)
	assert.Equal(t, UtilizationScore{0, true, 0, 0.0, 0.0}, score)
}

// makeWithLabelSelectors builds a resource request with label selectors.
// The selectors parameter has the shape [][][3]interface{}; each selector layer
// holds several (labelKey, operator, values) constraints.
func makeWithLabelSelectors(resources map[string]float64, selectors [][][3]interface{}) *proto.ResourceRequest {
	req := &proto.ResourceRequest{ResourcesBundle: resources}
	for _, selectorConstraints := range selectors {
		ls := &proto.LabelSelector{}
		for _, c := range selectorConstraints {
			key := c[0].(string)
			op := c[1].(proto.LabelSelectorOperator)
			valuesIface := c[2].([]string)
			ls.LabelConstraints = append(ls.LabelConstraints, &proto.LabelSelectorConstraint{
				LabelKey:    key,
				Operator:    op,
				LabelValues: valuesIface,
			})
		}
		req.LabelSelectors = append(req.LabelSelectors, ls)
	}
	return req
}

func TestGetNodesPackingHeuristic(t *testing.T) {
	nodeTypeConfigs := map[string]*instance_manager.NodeTypeConfig{
		"m4.large":    testNodeTypeConfig("m4.large", map[string]float64{"CPU": 2}, 0, 10, nil, 0, 0),
		"m4.4xlarge":  testNodeTypeConfig("m4.4xlarge", map[string]float64{"CPU": 16}, 0, 8, nil, 0, 0),
		"m4.16xlarge": testNodeTypeConfig("m4.16xlarge", map[string]float64{"CPU": 64}, 0, 4, nil, 0, 0),
		"p2.xlarge":   testNodeTypeConfig("p2.xlarge", map[string]float64{"CPU": 16, "GPU": 1}, 0, 10, nil, 0, 0),
		"p2.8xlarge":  testNodeTypeConfig("p2.8xlarge", map[string]float64{"CPU": 32, "GPU": 8}, 0, 4, nil, 0, 0),
	}

	// Same as the Python declaration order (insertion order): m4.large,
	// m4.4xlarge, m4.16xlarge, p2.xlarge, p2.8xlarge.
	pythonOrder := []string{"m4.large", "m4.4xlarge", "m4.16xlarge", "p2.xlarge", "p2.8xlarge"}
	getNodesFor := func(resourceMaps []map[string]float64, antiAffinity bool, maxNodes *int64, currentNodes map[string]int) map[string]int {
		requests := make([]*proto.ResourceRequest, 0, len(resourceMaps))
		for _, r := range resourceMaps {
			requests = append(requests, makeRequest(r))
		}
		reply := schedule(nodeTypeConfigs, currentNodes, requests, antiAffinity, maxNodes, nil, pythonOrder)
		toLaunch, _ := launchAndTerminate(reply)
		return toLaunch
	}

	assert.Equal(t, map[string]int{"p2.8xlarge": 1}, getNodesFor([]map[string]float64{{"GPU": 8}}, false, nil, nil))
	assert.Equal(t, map[string]int{"p2.8xlarge": 1}, getNodesFor([]map[string]float64{{"GPU": 1}, {"GPU": 1}, {"GPU": 1}, {"GPU": 1}, {"GPU": 1}, {"GPU": 1}}, false, nil, nil))
	assert.Equal(t, map[string]int{"p2.xlarge": 4}, getNodesFor([]map[string]float64{{"GPU": 1}, {"GPU": 1}, {"GPU": 1}, {"GPU": 1}}, false, nil, nil))
	assert.Equal(t, map[string]int{"p2.8xlarge": 3}, getNodesFor([]map[string]float64{{"CPU": 32, "GPU": 1}, {"CPU": 32, "GPU": 1}, {"CPU": 32, "GPU": 1}}, false, nil, nil))
	assert.Equal(t, map[string]int{}, getNodesFor([]map[string]float64{{"CPU": 64, "GPU": 1}, {"CPU": 64, "GPU": 1}, {"CPU": 64, "GPU": 1}}, false, nil, nil))
	assert.Equal(t, map[string]int{"m4.16xlarge": 3}, getNodesFor([]map[string]float64{{"CPU": 64}, {"CPU": 64}, {"CPU": 64}}, false, nil, nil))
	assert.Equal(t, map[string]int{"m4.16xlarge": 1, "m4.large": 1}, getNodesFor([]map[string]float64{{"CPU": 64}, {"CPU": 1}}, false, nil, nil))

	// [{"CPU": 16}]*5 => 1 * 16xlarge + 1 * 4xlarge
	assert.Equal(t, map[string]int{"m4.16xlarge": 1, "m4.4xlarge": 1}, getNodesFor([]map[string]float64{
		{"CPU": 16}, {"CPU": 16}, {"CPU": 16}, {"CPU": 16}, {"CPU": 16},
	}, false, nil, nil))

	// GPU/CPU scheduling.
	gpuCPUCfg := map[string]*instance_manager.NodeTypeConfig{
		"cpu": testNodeTypeConfig("cpu", map[string]float64{"CPU": 16}, 0, 10, nil, 0, 0),
		"gpu": testNodeTypeConfig("gpu", map[string]float64{"CPU": 16, "GPU": 1}, 0, 10, nil, 0, 0),
	}
	getNodesFor2 := func(resourceMaps []map[string]float64, maxNodes *int64) map[string]int {
		requests := make([]*proto.ResourceRequest, 0, len(resourceMaps))
		for _, r := range resourceMaps {
			requests = append(requests, makeRequest(r))
		}
		reply := schedule(gpuCPUCfg, map[string]int{}, requests, false, maxNodes, nil)
		toLaunch, _ := launchAndTerminate(reply)
		return toLaunch
	}

	assert.Equal(t, map[string]int{"cpu": 1}, getNodesFor2([]map[string]float64{{"CPU": 16}}, nil))
	assert.Equal(t, map[string]int{"cpu": 1, "gpu": 1}, getNodesFor2([]map[string]float64{
		{"CPU": 1}, {"CPU": 1}, {"CPU": 1}, {"CPU": 1}, {"CPU": 1},
		{"CPU": 1}, {"CPU": 1}, {"CPU": 1}, {"CPU": 1}, {"CPU": 1},
		{"CPU": 1}, {"CPU": 1}, {"CPU": 1}, {"CPU": 1}, {"CPU": 1},
		{"CPU": 1}, {"CPU": 1}, {"CPU": 1}, {"CPU": 1}, {"CPU": 1},
		{"CPU": 1}, {"CPU": 1}, {"CPU": 1}, {"CPU": 1}, {"CPU": 1},
		{"CPU": 1}, {"CPU": 1}, {"CPU": 1}, {"CPU": 1}, {"CPU": 1},
		{"GPU": 1, "CPU": 1},
	}, nil))

	// GPU nodes should be avoided.
	avoidGPUCfg := map[string]*instance_manager.NodeTypeConfig{
		"cpu": testNodeTypeConfig("cpu", map[string]float64{"CPU": 1}, 0, 10, nil, 0, 0),
		"gpu": testNodeTypeConfig("gpu", map[string]float64{"CPU": 100, "GPU": 1}, 0, 10, nil, 0, 0),
	}
	getNodesFor3 := func(resourceMaps []map[string]float64, maxNodes *int64) map[string]int {
		requests := make([]*proto.ResourceRequest, 0, len(resourceMaps))
		for _, r := range resourceMaps {
			requests = append(requests, makeRequest(r))
		}
		reply := schedule(avoidGPUCfg, map[string]int{}, requests, false, maxNodes, nil)
		toLaunch, _ := launchAndTerminate(reply)
		return toLaunch
	}

	maxNodes10 := int64(10)
	cpu100 := make([]map[string]float64, 0, 100)
	for i := 0; i < 100; i++ {
		cpu100 = append(cpu100, map[string]float64{"CPU": 1})
	}
	assert.Equal(t, map[string]int{"cpu": 10}, getNodesFor3(cpu100, &maxNodes10))

	// The max limit should be respected.
	limitedCfg := map[string]*instance_manager.NodeTypeConfig{
		"m4.large": testNodeTypeConfig("m4.large", map[string]float64{"CPU": 2}, 0, 10, nil, 0, 0),
	}
	getNodesFor4 := func(resourceMaps []map[string]float64, maxNodes *int64, current map[string]int) map[string]int {
		requests := make([]*proto.ResourceRequest, 0, len(resourceMaps))
		for _, r := range resourceMaps {
			requests = append(requests, makeRequest(r))
		}
		reply := schedule(limitedCfg, current, requests, false, maxNodes, nil)
		toLaunch, _ := launchAndTerminate(reply)
		return toLaunch
	}
	cpu10 := make([]map[string]float64, 0, 10)
	for i := 0; i < 10; i++ {
		cpu10 = append(cpu10, map[string]float64{"CPU": 1})
	}
	maxNodes2 := int64(2)
	assert.Equal(t, map[string]int{"m4.large": 2}, getNodesFor4(cpu10, &maxNodes2, nil))
	maxNodes10v2 := int64(10)
	assert.Equal(t, map[string]int{}, getNodesFor4(cpu10, &maxNodes10v2, map[string]int{"m4.large": 10}))
	// 10 CPU requests with 2 CPU per node need 5 nodes.
	req10 := make([]map[string]float64, 0, 10)
	for i := 0; i < 10; i++ {
		req10 = append(req10, map[string]float64{"CPU": 1})
	}
	assert.Equal(t, map[string]int{"m4.large": 5}, getNodesFor4(req10, &maxNodes10v2, nil))

	// Min workers
	minCfg := map[string]*instance_manager.NodeTypeConfig{
		"m2.large": testNodeTypeConfig("m2.large", map[string]float64{"CPU": 1}, 5, 10, nil, 0, 0),
		"m4.large": testNodeTypeConfig("m4.large", map[string]float64{"CPU": 2}, 0, 10, nil, 0, 0),
		"gpu":      testNodeTypeConfig("gpu", map[string]float64{"GPU": 2}, 2, 2, nil, 0, 0),
		"gpubla":   testNodeTypeConfig("gpubla", map[string]float64{"GPU": 1}, 0, 0, nil, 0, 0),
	}
	getNodesForMin := func(resourceMaps []map[string]float64) map[string]int {
		requests := make([]*proto.ResourceRequest, 0, len(resourceMaps))
		for _, r := range resourceMaps {
			requests = append(requests, makeRequest(r))
		}
		reply := schedule(minCfg, map[string]int{}, requests, false, nil, nil)
		toLaunch, _ := launchAndTerminate(reply)
		return toLaunch
	}
	assert.Equal(t, map[string]int{"m2.large": 5, "m4.large": 5, "gpu": 2}, getNodesForMin([]map[string]float64{{"CPU": 2}, {"CPU": 2}, {"CPU": 2}, {"CPU": 2}, {"CPU": 2}}))
}

func TestMinWorkersAndOthers(t *testing.T) {
	nodeTypeConfigs := map[string]*instance_manager.NodeTypeConfig{
		"p2.8xlarge": testNodeTypeConfig("p2.8xlarge", map[string]float64{"CPU": 32, "GPU": 8}, 2, 4, nil, 0, 0),
		"p2.xlarge":  testNodeTypeConfig("p2.xlarge", map[string]float64{"CPU": 16, "GPU": 1}, 0, 10, nil, 0, 0),
	}

	getNodesFor := func(resourceMaps []map[string]float64, currentNodes map[string]int, maxNodes *int64) (map[string]int, []map[string]float64) {
		requests := make([]*proto.ResourceRequest, 0, len(resourceMaps))
		for _, r := range resourceMaps {
			requests = append(requests, makeRequest(r))
		}
		reply := schedule(nodeTypeConfigs, currentNodes, requests, false, maxNodes, nil)
		toLaunch, _ := launchAndTerminate(reply)
		infeasible := make([]map[string]float64, 0, len(reply.InfeasibleResourceRequests))
		for _, r := range reply.InfeasibleResourceRequests {
			infeasible = append(infeasible, r.ResourcesBundle)
		}
		return toLaunch, infeasible
	}

	toLaunch, infeasible := getNodesFor([]map[string]float64{{"GPU": 8}}, nil, nil)
	assert.Equal(t, map[string]int{"p2.8xlarge": 2}, toLaunch)
	assert.Equal(t, []map[string]float64{}, infeasible)

	// 2 GPU:8 requests -> in 2 nodes
	toLaunch, infeasible = getNodesFor([]map[string]float64{{"GPU": 8}, {"GPU": 8}}, nil, nil)
	assert.Equal(t, map[string]int{"p2.8xlarge": 2}, toLaunch)
	assert.Equal(t, []map[string]float64{}, infeasible)

	// 4 GPU:8 requests -> 4 nodes
	toLaunch, infeasible = getNodesFor([]map[string]float64{{"GPU": 8}, {"GPU": 8}, {"GPU": 8}, {"GPU": 8}}, nil, nil)
	assert.Equal(t, map[string]int{"p2.8xlarge": 4}, toLaunch)
	assert.Equal(t, []map[string]float64{}, infeasible)

	// 8 GPU:8 requests -> max 4 nodes
	toLaunch, infeasible = getNodesFor([]map[string]float64{
		{"GPU": 8}, {"GPU": 8}, {"GPU": 8}, {"GPU": 8},
		{"GPU": 8}, {"GPU": 8}, {"GPU": 8}, {"GPU": 8},
	}, nil, nil)
	assert.Equal(t, map[string]int{"p2.8xlarge": 4}, toLaunch)
	assert.Equal(t, []map[string]float64{{"GPU": 8}, {"GPU": 8}, {"GPU": 8}, {"GPU": 8}}, infeasible)

	// min workers + max_num_nodes
	toLaunch, infeasible = getNodesFor([]map[string]float64{
		{"GPU": 8}, {"GPU": 8}, {"GPU": 8}, {"GPU": 1},
	}, map[string]int{"p2.8xlarge": 1}, nil)
	assert.Equal(t, map[string]int{"p2.xlarge": 1, "p2.8xlarge": 2}, toLaunch)
	assert.Equal(t, []map[string]float64{}, infeasible)
}

func TestGangSchedulingComplex(t *testing.T) {
	nodeTypeConfigs := map[string]*instance_manager.NodeTypeConfig{
		"m4.large":   testNodeTypeConfig("m4.large", map[string]float64{"CPU": 2}, 0, 10, nil, 0, 0),
		"p2.8xlarge": testNodeTypeConfig("p2.8xlarge", map[string]float64{"CPU": 32, "GPU": 8}, 0, 4, nil, 0, 0),
	}

	// Affinity merges onto one node.
	req := schedRequest(nodeTypeConfigs, nil, nil, [][]*proto.ResourceRequest{
		{
			makeWithAffinity(makeRequest(map[string]float64{"CPU": 2}), "pg", "pg"),
			makeWithAffinity(makeRequest(map[string]float64{"CPU": 2}), "pg", "pg"),
		},
	}, nil, nil, nil, false, nil)
	reply := NewResourceDemandScheduler(nil).Schedule(req)
	toLaunch, _ := launchAndTerminate(reply)
	assert.Equal(t, map[string]int{"p2.8xlarge": 1}, toLaunch)

	// Infeasible affinity -> infeasible.
	req = schedRequest(nodeTypeConfigs, nil, nil, [][]*proto.ResourceRequest{
		{
			makeWithAffinity(makeRequest(map[string]float64{"CPU": 32}), "pg", "pg"),
			makeWithAffinity(makeRequest(map[string]float64{"CPU": 32}), "pg", "pg"),
		},
	}, nil, nil, nil, false, nil)
	reply = NewResourceDemandScheduler(nil).Schedule(req)
	toLaunch, infeasible := launchAndTerminate(reply)
	assert.Equal(t, map[string]int{}, toLaunch)
	assert.Len(t, infeasible, 0)
	assert.Len(t, reply.InfeasibleGangResourceRequests, 1)

	// Multiple anti-affinities.
	req = schedRequest(nodeTypeConfigs, nil, nil, [][]*proto.ResourceRequest{
		{
			makeWithPlacementConstraint(makeRequest(map[string]float64{"CPU": 4}), "pg", "pg"),
			makeWithPlacementConstraint(makeRequest(map[string]float64{"CPU": 4}), "pg", "pg"),
		},
		{
			makeWithPlacementConstraint(makeRequest(map[string]float64{"CPU": 4}), "pg2", "pg2"),
			makeWithPlacementConstraint(makeRequest(map[string]float64{"CPU": 4}), "pg2", "pg2"),
		},
	}, nil, nil, nil, false, nil)
	reply = NewResourceDemandScheduler(nil).Schedule(req)
	toLaunch, _ = launchAndTerminate(reply)
	// 4 anti-affinity gangs, each with 2 CPU: 4 bundles.
	// p2.8xlarge has 32 CPUs, so 2 nodes hold all 8 bundles (4 per node, with
	// the 2 bundles of the same gang forced onto different nodes by
	// anti-affinity), matching the long tail of bare expression calls at the
	// end of Python's test_gang_scheduling_complex.
	assert.Equal(t, map[string]int{"p2.8xlarge": 2}, toLaunch)
}

func TestScheduleNodeWithMatchingLabels(t *testing.T) {
	scheduler := NewResourceDemandScheduler(nil)
	nodeTypeConfigs := map[string]*instance_manager.NodeTypeConfig{
		"labelled_node": testNodeTypeConfig("labelled_node", map[string]float64{"CPU": 1}, 0, 10, map[string]string{"accelerator": "A100"}, 0, 0),
	}

	im := makeIMInstance("labelled_node", proto.Instance_RAY_RUNNING, "1")
	im.NodeId = strPtr("r-1")
	labels := map[string]string{"accelerator": "A100"}
	instance := makeAutoscalerInstance(im, makeRayNodeState("r-1", "labelled_node",
		map[string]float64{"CPU": 1}, map[string]float64{"CPU": 1}, 0, labels), "c-1")

	resourceRequest := makeWithLabelSelectors(map[string]float64{"CPU": 1}, [][][3]interface{}{
		{{"accelerator", proto.LabelSelectorOperator_LABEL_OPERATOR_IN, []string{"A100"}}},
	})

	req := schedRequest(nodeTypeConfigs, nil, []*proto.ResourceRequest{resourceRequest}, nil, nil, []*AutoscalerInstance{instance}, nil, false, nil)
	reply := scheduler.Schedule(req)
	toLaunch, _ := launchAndTerminate(reply)
	assert.Equal(t, map[string]int{}, toLaunch)
}

func TestScaleUpNodeToSatisfyLabels(t *testing.T) {
	scheduler := NewResourceDemandScheduler(nil)
	nodeTypeConfigs := map[string]*instance_manager.NodeTypeConfig{
		"tpu_node": testNodeTypeConfig("tpu_node", map[string]float64{"CPU": 1}, 0, 10, map[string]string{"accelerator": "TPU"}, 0, 0),
		"gpu_node": testNodeTypeConfig("gpu_node", map[string]float64{"CPU": 1}, 0, 10, map[string]string{"accelerator": "A100"}, 0, 0),
	}

	resourceRequest := makeWithLabelSelectors(map[string]float64{"CPU": 1}, [][][3]interface{}{
		{{"accelerator", proto.LabelSelectorOperator_LABEL_OPERATOR_IN, []string{"A100"}}},
	})

	req := schedRequest(nodeTypeConfigs, nil, []*proto.ResourceRequest{resourceRequest}, nil, nil, nil, nil, false, nil)
	reply := scheduler.Schedule(req)
	toLaunch, _ := launchAndTerminate(reply)
	assert.Equal(t, map[string]int{"gpu_node": 1}, toLaunch)
}

func TestLabelSelectorFallbackPriority(t *testing.T) {
	scheduler := NewResourceDemandScheduler(nil)
	nodeTypeConfigs := map[string]*instance_manager.NodeTypeConfig{
		"tpu_node": testNodeTypeConfig("tpu_node", map[string]float64{"CPU": 1}, 0, 10, map[string]string{"accelerator-type": "TPU"}, 0, 0),
		"gpu_node": testNodeTypeConfig("gpu_node", map[string]float64{"CPU": 1}, 0, 10, map[string]string{"accelerator-type": "A100"}, 0, 0),
	}

	// 1) TPU node scaled up to satisfy first label selector
	req1 := makeWithLabelSelectors(map[string]float64{"CPU": 1}, [][][3]interface{}{
		{{"accelerator-type", proto.LabelSelectorOperator_LABEL_OPERATOR_IN, []string{"TPU"}}},
		{{"accelerator-type", proto.LabelSelectorOperator_LABEL_OPERATOR_IN, []string{"A100"}}},
	})
	reply1 := scheduler.Schedule(schedRequest(nodeTypeConfigs, nil, []*proto.ResourceRequest{req1}, nil, nil, nil, nil, false, nil))
	toLaunch1, _ := launchAndTerminate(reply1)
	assert.Equal(t, map[string]int{"tpu_node": 1}, toLaunch1)

	// 2) fallback to second priority, scale up A100
	req2 := makeWithLabelSelectors(map[string]float64{"CPU": 1}, [][][3]interface{}{
		{{"accelerator-type", proto.LabelSelectorOperator_LABEL_OPERATOR_IN, []string{"B200"}}},
		{{"accelerator-type", proto.LabelSelectorOperator_LABEL_OPERATOR_IN, []string{"A100"}}},
	})
	reply2 := scheduler.Schedule(schedRequest(nodeTypeConfigs, nil, []*proto.ResourceRequest{req2}, nil, nil, nil, nil, false, nil))
	toLaunch2, _ := launchAndTerminate(reply2)
	assert.Equal(t, map[string]int{"gpu_node": 1}, toLaunch2)
}

func TestPGWithBundleInfeasibleLabelSelectors(t *testing.T) {
	scheduler := NewResourceDemandScheduler(nil)
	nodeTypeConfigs := map[string]*instance_manager.NodeTypeConfig{
		"gpu_node": testNodeTypeConfig("gpu_node", map[string]float64{"CPU": 4, "GPU": 1}, 0, 5, map[string]string{"accelerator": "A100"}, 0, 0),
		"tpu_node": testNodeTypeConfig("tpu_node", map[string]float64{"CPU": 4}, 0, 5, map[string]string{"accelerator": "TPU"}, 0, 0),
	}

	gpuRequest := makeWithAffinity(makeWithLabelSelectors(map[string]float64{"CPU": 2, "GPU": 1}, [][][3]interface{}{
		{{"accelerator", proto.LabelSelectorOperator_LABEL_OPERATOR_IN, []string{"A100"}}},
	}), "pg-1", "")
	tpuRequest := makeWithAffinity(makeWithLabelSelectors(map[string]float64{"CPU": 2}, [][][3]interface{}{
		{{"accelerator", proto.LabelSelectorOperator_LABEL_OPERATOR_IN, []string{"TPU"}}},
	}), "pg-1", "")

	req := schedRequest(nodeTypeConfigs, nil, nil, [][]*proto.ResourceRequest{{gpuRequest, tpuRequest}}, nil, nil, nil, false, nil)
	reply := scheduler.Schedule(req)
	toLaunch, _ := launchAndTerminate(reply)
	assert.Equal(t, map[string]int{"gpu_node": 1, "tpu_node": 1}, toLaunch)

	// Both bundles require A100, no node has enough resources -> infeasible
	infeasibleGPU := makeWithAffinity(makeWithLabelSelectors(map[string]float64{"CPU": 3, "GPU": 1}, [][][3]interface{}{
		{{"accelerator", proto.LabelSelectorOperator_LABEL_OPERATOR_IN, []string{"A100"}}},
	}), "pg-2", "")

	req = schedRequest(nodeTypeConfigs, nil, nil, [][]*proto.ResourceRequest{{infeasibleGPU, infeasibleGPU}}, nil, nil, nil, false, nil)
	reply = scheduler.Schedule(req)
	toLaunch, _ = launchAndTerminate(reply)
	assert.Equal(t, map[string]int{}, toLaunch)
	assert.Len(t, reply.InfeasibleGangResourceRequests, 1)
}

func TestGetNodesWithResourceAvailabilities(t *testing.T) {
	nodeTypeConfigs := map[string]*instance_manager.NodeTypeConfig{
		"type_gpu1": testNodeTypeConfig("type_gpu1", map[string]float64{"CPU": 8, "GPU": 1, "gpu1": 1}, 0, 10, nil, 0, 0),
		"type_gpu2": testNodeTypeConfig("type_gpu2", map[string]float64{"CPU": 8, "GPU": 1, "gpu2": 1}, 0, 10, nil, 0, 0),
		"type_gpu3": testNodeTypeConfig("type_gpu3", map[string]float64{"CPU": 8, "GPU": 1, "gpu3": 1}, 0, 10, nil, 0, 0),
		"type_gpu4": testNodeTypeConfig("type_gpu4", map[string]float64{"CPU": 1, "GPU": 1, "gpu4": 1}, 0, 10, nil, 0, 0),
	}

	getNodesFor := func(resourceMaps []map[string]float64, availabilities map[string]float64) (map[string]int, []map[string]float64) {
		requests := make([]*proto.ResourceRequest, 0, len(resourceMaps))
		for _, r := range resourceMaps {
			requests = append(requests, makeRequest(r))
		}
		reply := schedule(nodeTypeConfigs, map[string]int{}, requests, false, nil, availabilities)
		toLaunch, _ := launchAndTerminate(reply)
		infeasible := make([]map[string]float64, 0, len(reply.InfeasibleResourceRequests))
		for _, r := range reply.InfeasibleResourceRequests {
			infeasible = append(infeasible, r.ResourcesBundle)
		}
		return toLaunch, infeasible
	}

	// With equal utilization scores, pick the highest availability.
	toLaunch, _ := getNodesFor([]map[string]float64{{"CPU": 8, "GPU": 1}}, map[string]float64{
		"type_gpu1": 0.1, "type_gpu2": 1, "type_gpu3": 0.2,
	})
	assert.Equal(t, map[string]int{"type_gpu2": 1}, toLaunch)

	// The default availability is 1.
	toLaunch, _ = getNodesFor([]map[string]float64{{"CPU": 8, "GPU": 1}}, map[string]float64{
		"type_gpu2": 0.1, "type_gpu3": 0.2,
	})
	assert.Equal(t, map[string]int{"type_gpu1": 1}, toLaunch)

	// Higher availability wins.
	toLaunch, _ = getNodesFor([]map[string]float64{{"CPU": 8, "GPU": 1}, {"CPU": 8, "GPU": 1}}, map[string]float64{
		"type_gpu1": 0.1, "type_gpu2": 0.1, "type_gpu3": 1,
	})
	assert.Equal(t, map[string]int{"type_gpu3": 2}, toLaunch)

	// The utilization score wins.
	toLaunch, _ = getNodesFor([]map[string]float64{{"CPU": 1, "GPU": 1}}, map[string]float64{})
	assert.Equal(t, map[string]int{"type_gpu4": 1}, toLaunch)
}
