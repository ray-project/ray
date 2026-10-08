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

// Tests of the reconciler, corresponding to Python test_reconciler.py
// (29 cases).
// Written as an internal package test so the unexported methods can be called
// directly (matching the Python tests calling private methods such as
// Reconciler._is_head_node_running).
package reconciler

import (
	"encoding/hex"
	"math"
	"strconv"
	"testing"
	"time"

	"github.com/ray-project/ray/go/internal/autoscaler"
	"github.com/ray-project/ray/go/internal/autoscaler/v2/instance_manager"
	"github.com/ray-project/ray/go/internal/autoscaler/v2/instance_manager/subscribers"
	"github.com/ray-project/ray/go/internal/autoscaler/v2/scheduler"
	"github.com/ray-project/ray/go/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const sToNs = int64(time.Second)

// ---- Test stubs ----

// mockSubscriber is a subscriber recording all the update events.
type mockSubscriber struct {
	events []*proto.InstanceUpdateEvent
}

func (m *mockSubscriber) Notify(events []*proto.InstanceUpdateEvent) {
	m.events = append(m.events, events...)
}

func (m *mockSubscriber) clear() {
	m.events = nil
}

func (m *mockSubscriber) eventsById(instanceId string) []*proto.InstanceUpdateEvent {
	out := make([]*proto.InstanceUpdateEvent, 0)
	for _, e := range m.events {
		if e.InstanceId == instanceId {
			out = append(out, e)
		}
	}
	return out
}

// mockScheduler is a scheduler returning a preset reply.
type mockScheduler struct {
	reply *scheduler.SchedulingReply
}

func (m *mockScheduler) Schedule(request *scheduler.SchedulingRequest) *scheduler.SchedulingReply {
	if m.reply == nil {
		return &scheduler.SchedulingReply{}
	}
	return m.reply
}

// statusTime is a status and timestamp pair, matching the status_times
// argument of the Python tests.
type statusTime struct {
	status proto.Instance_InstanceStatus
	tsNs   int64
}

// instanceSpec holds the parameters building an instance, corresponding to
// Python util.create_instance.
type instanceSpec struct {
	id              string
	status          proto.Instance_InstanceStatus
	instanceType    string
	statusTimes     []statusTime
	launchRequestId string
	cloudInstanceId string
	rayNodeId       string
	nodeKind        proto.NodeKind
}

// createInstance builds a test instance.
// When statusTimes is unspecified it defaults to [(status, current time)].
func createInstance(spec instanceSpec) *proto.Instance {
	status := spec.status
	instanceType := spec.instanceType
	if instanceType == "" {
		instanceType = "worker_nodes1"
	}
	statusTimes := spec.statusTimes
	if statusTimes == nil {
		statusTimes = []statusTime{{status, instance_manager.NowUnixNano()}}
	}
	nodeKind := spec.nodeKind
	if nodeKind == proto.NodeKind_UNMANAGED {
		nodeKind = proto.NodeKind_WORKER
	}
	instance := &proto.Instance{
		InstanceId:      spec.id,
		Status:          status,
		InstanceType:    instanceType,
		LaunchRequestId: spec.launchRequestId,
		NodeKind:        nodeKind,
	}
	if spec.cloudInstanceId != "" {
		instance.CloudInstanceId = &spec.cloudInstanceId
	}
	if spec.rayNodeId != "" {
		instance.NodeId = &spec.rayNodeId
	}
	for _, st := range statusTimes {
		instance.StatusHistory = append(instance.StatusHistory, &proto.Instance_StatusHistory{
			InstanceStatus: st.status,
			TimestampNs:    st.tsNs,
		})
	}
	return instance
}

// cloudInstance builds a cloud instance.
func cloudInstance(id, nodeType string, isRunning bool, nodeKind proto.NodeKind) autoscaler.CloudInstance {
	return autoscaler.CloudInstance{
		CloudInstanceId: id,
		NodeType:        nodeType,
		NodeKind:        nodeKind,
		IsRunning:       isRunning,
	}
}

// rayNode builds a Ray node state.
func rayNode(nodeId string, status proto.NodeStatus, instanceId string, rayNodeTypeName string) *proto.NodeState {
	return &proto.NodeState{
		NodeId:          []byte(nodeId),
		Status:          status,
		InstanceId:      instanceId,
		RayNodeTypeName: rayNodeTypeName,
	}
}

// nodeHex converts the binary Ray node ID to a hex string, corresponding to
// Python binary_to_hex.
func nodeHex(nodeId string) string {
	return hex.EncodeToString([]byte(nodeId))
}

// testEnv is the shared environment of each test, corresponding to the Python
// setup fixture.
type testEnv struct {
	instanceManager      *instance_manager.InstanceManager
	instanceStorage      *instance_manager.InstanceStorage
	subscriber           *mockSubscriber
	cloudResourceMonitor *subscribers.CloudResourceMonitor
	reconciler           *Reconciler
	scheduler            *mockScheduler
}

func newTestEnv(t *testing.T) *testEnv {
	instanceStorage := instance_manager.NewInstanceStorage(
		"test_cluster_id", instance_manager.NewInMemoryStorage())
	subscriber := &mockSubscriber{}
	instanceManager := instance_manager.NewInstanceManager(
		instanceStorage, []instance_manager.InstanceUpdatedSubscriber{subscriber})
	monitor := subscribers.NewCloudResourceMonitor()
	sched := &mockScheduler{}
	return &testEnv{
		instanceManager:      instanceManager,
		instanceStorage:      instanceStorage,
		subscriber:           subscriber,
		cloudResourceMonitor: monitor,
		reconciler:           NewReconciler(instanceManager, sched, nil, monitor, nil),
		scheduler:            sched,
	}
}

// reconcileOpts holds the mutable inputs of one reconcile round.
type reconcileOpts struct {
	rayNodes                []*proto.NodeState
	cloudInstances          map[string]autoscaler.CloudInstance
	cloudProviderErrors     []error
	rayInstallErrors        []error
	rayStopErrors           []error
	configs                 map[string]interface{}
	clusterResourceStateVer int64
}

// reconcile runs one reconcile round.
func (e *testEnv) reconcile(t *testing.T, opts reconcileOpts) *proto.AutoscalingState {
	state, err := e.reconciler.Reconcile(&autoscaler.ReconcileArgs{
		RayClusterResourceState: &proto.ClusterResourceState{
			NodeStates:                  opts.rayNodes,
			ClusterResourceStateVersion: opts.clusterResourceStateVer,
		},
		NonTerminatedCloudInstances: opts.cloudInstances,
		CloudProviderErrors:         opts.cloudProviderErrors,
		RayInstallErrors:            opts.rayInstallErrors,
		RayStopErrors:               opts.rayStopErrors,
		AutoscalingConfig:           newTestConfig(opts.configs),
	})
	require.NoError(t, err)
	return state
}

// newTestConfig builds the test config.
// Corresponds to Python MockAutoscalingConfig: the node updaters are disabled
// by default; the "disable_node_updaters" entry maps into the provider config
// (where the real getter reads it).
func newTestConfig(configs map[string]interface{}) *instance_manager.AutoscalingConfig {
	providerConfig := map[string]interface{}{"disable_node_updaters": true}
	cfg := map[string]interface{}{"provider": providerConfig}
	for k, v := range configs {
		if k == "disable_node_updaters" {
			providerConfig["disable_node_updaters"] = v
			continue
		}
		cfg[k] = v
	}
	return &instance_manager.AutoscalingConfig{Configs: cfg}
}

// mockTime injects a fixed time, corresponding to the Python tests mocking
// time.time_ns.
func mockTime(t *testing.T, curTimeNs int64) {
	orig := instance_manager.NowUnixNano
	instance_manager.NowUnixNano = func() int64 { return curTimeNs }
	t.Cleanup(func() { instance_manager.NowUnixNano = orig })
}

// setReconcileEnv sets the instance reconcile config via env vars (only the
// non-zero fields are set).
// Corresponds to the Python tests passing instance_reconcile_config to
// MockAutoscalingConfig.
func setReconcileEnv(t *testing.T, config *instance_manager.InstanceReconcileConfig) {
	if config.RequestStatusTimeoutS != 0 {
		t.Setenv("RAY_AUTOSCALER_RECONCILE_REQUEST_STATUS_TIMEOUT_S", strconv.Itoa(config.RequestStatusTimeoutS))
	}
	if config.AllocateStatusTimeoutS != 0 {
		t.Setenv("RAY_AUTOSCALER_RECONCILE_ALLOCATE_STATUS_TIMEOUT_S", strconv.Itoa(config.AllocateStatusTimeoutS))
	}
	if config.RayInstallStatusTimeoutS != 0 {
		t.Setenv("RAY_AUTOSCALER_RECONCILE_RAY_INSTALL_STATUS_TIMEOUT_S", strconv.Itoa(config.RayInstallStatusTimeoutS))
	}
	if config.TerminatingStatusTimeoutS != 0 {
		t.Setenv("RAY_AUTOSCALER_RECONCILE_TERMINATING_STATUS_TIMEOUT_S", strconv.Itoa(config.TerminatingStatusTimeoutS))
	}
	if config.RayStopRequestedStatusTimeoutS != 0 {
		t.Setenv("RAY_AUTOSCALER_RECONCILE_RAY_STOP_REQUESTED_STATUS_TIMEOUT_S", strconv.Itoa(config.RayStopRequestedStatusTimeoutS))
	}
	if config.TransientStatusWarnIntervalS != 0 {
		t.Setenv("RAY_AUTOSCALER_RECONCILE_TRANSIENT_STATUS_WARN_INTERVAL_S", strconv.Itoa(config.TransientStatusWarnIntervalS))
	}
	if config.MaxNumRetryRequestToAllocate != 0 {
		t.Setenv("RAY_AUTOSCALER_RECONCILE_MAX_NUM_RETRY_REQUEST_TO_ALLOCATE", strconv.Itoa(config.MaxNumRetryRequestToAllocate))
	}
}

// preventLaunches sets the max concurrent launches to 0 to block launches.
func preventLaunches(t *testing.T) {
	t.Setenv("AUTOSCALER_MAX_CONCURRENT_LAUNCHES", "0")
}

// addInstances adds the instances to the instance storage.
func addInstances(t *testing.T, storage *instance_manager.InstanceStorage, instances ...*proto.Instance) {
	for _, instance := range instances {
		status := storage.UpsertInstance(instance, nil, nil)
		require.True(t, status.Success, "failed to upsert instance %s", instance.InstanceId)
	}
}

// getInstances reads all the instances from the instance storage.
func getInstances(t *testing.T, storage *instance_manager.InstanceStorage) map[string]*proto.Instance {
	instances, _ := storage.GetInstances(nil, nil)
	return instances
}

// ---- Test cases ----

// TestRequestedInstanceNoOp a REQUESTED instance that has not timed out does
// not change.
func TestRequestedInstanceNoOp(t *testing.T) {
	e := newTestEnv(t)
	addInstances(t, e.instanceStorage, createInstance(instanceSpec{
		id:              "i-1",
		status:          proto.Instance_REQUESTED,
		instanceType:    "type-1",
		launchRequestId: "l1",
		statusTimes:     []statusTime{{proto.Instance_REQUESTED, instance_manager.NowUnixNano()}},
	}))

	e.reconcile(t, reconcileOpts{})

	instances := getInstances(t, e.instanceStorage)
	assert.Len(t, instances, 1)
	assert.Equal(t, proto.Instance_REQUESTED, instances["i-1"].Status)
}

// TestRequestedInstanceToAllocated REQUESTED -> ALLOCATED when a matching
// cloud instance exists.
func TestRequestedInstanceToAllocated(t *testing.T) {
	e := newTestEnv(t)
	addInstances(t, e.instanceStorage,
		createInstance(instanceSpec{
			id:              "i-1",
			status:          proto.Instance_REQUESTED,
			instanceType:    "type-1",
			launchRequestId: "l1",
			statusTimes:     []statusTime{{proto.Instance_REQUESTED, instance_manager.NowUnixNano()}},
		}),
		createInstance(instanceSpec{
			id:              "i-2",
			status:          proto.Instance_REQUESTED,
			instanceType:    "type-2",
			launchRequestId: "l2",
			statusTimes:     []statusTime{{proto.Instance_REQUESTED, instance_manager.NowUnixNano()}},
		}),
	)

	cloudInstances := map[string]autoscaler.CloudInstance{
		"c-1": cloudInstance("c-1", "type-1", true, proto.NodeKind_WORKER),
	}

	e.reconcile(t, reconcileOpts{cloudInstances: cloudInstances})

	instances := getInstances(t, e.instanceStorage)
	assert.Len(t, instances, 2)
	assert.Equal(t, proto.Instance_ALLOCATED, instances["i-1"].Status)
	assert.Equal(t, "c-1", instances["i-1"].GetCloudInstanceId())
	assert.Equal(t, proto.Instance_REQUESTED, instances["i-2"].Status)
}

// TestRequestedInstanceToAllocationFailed REQUESTED -> ALLOCATION_FAILED when
// the launch failed.
func TestRequestedInstanceToAllocationFailed(t *testing.T) {
	e := newTestEnv(t)
	addInstances(t, e.instanceStorage,
		createInstance(instanceSpec{
			id:              "i-1",
			status:          proto.Instance_REQUESTED,
			instanceType:    "type-1",
			launchRequestId: "l1",
		}),
		createInstance(instanceSpec{
			id:              "i-2",
			status:          proto.Instance_REQUESTED,
			instanceType:    "type-2",
			launchRequestId: "l1",
		}),
	)

	launchError := &autoscaler.LaunchNodeError{
		CloudInstanceProviderError: autoscaler.CloudInstanceProviderError{TimestampNs: 1},
		RequestId:                  "l1",
		Count:                      1,
		NodeType:                   "type-2",
	}

	cloudInstances := map[string]autoscaler.CloudInstance{
		"c-1": cloudInstance("c-1", "type-1", true, proto.NodeKind_WORKER),
	}
	e.reconcile(t, reconcileOpts{
		cloudInstances:      cloudInstances,
		cloudProviderErrors: []error{launchError},
	})

	instances := getInstances(t, e.instanceStorage)
	assert.Len(t, instances, 2)
	assert.Equal(t, proto.Instance_ALLOCATED, instances["i-1"].Status)
	assert.Equal(t, "c-1", instances["i-1"].GetCloudInstanceId())
	assert.Equal(t, proto.Instance_ALLOCATION_FAILED, instances["i-2"].Status)
}

// TestReconcileTerminatedCloudInstances covers the status transitions when a
// cloud instance disappears or its termination fails.
func TestReconcileTerminatedCloudInstances(t *testing.T) {
	e := newTestEnv(t)
	addInstances(t, e.instanceStorage,
		createInstance(instanceSpec{
			id:              "i-1",
			status:          proto.Instance_ALLOCATED,
			instanceType:    "type-1",
			cloudInstanceId: "c-1",
		}),
		createInstance(instanceSpec{
			id:              "i-2",
			status:          proto.Instance_TERMINATING,
			instanceType:    "type-2",
			cloudInstanceId: "c-2",
		}),
		createInstance(instanceSpec{
			id:              "i-3",
			status:          proto.Instance_TERMINATED,
			instanceType:    "type-2",
			cloudInstanceId: "c-3",
		}),
	)

	cloudInstances := map[string]autoscaler.CloudInstance{
		"c-2": cloudInstance("c-2", "type-2", false, proto.NodeKind_WORKER),
	}

	terminationError := &autoscaler.TerminateNodeError{
		CloudInstanceProviderError: autoscaler.CloudInstanceProviderError{TimestampNs: 1},
		CloudInstanceId:            "c-2",
		RequestId:                  "t1",
	}

	e.subscriber.clear()
	e.reconcile(t, reconcileOpts{
		cloudInstances:      cloudInstances,
		cloudProviderErrors: []error{terminationError},
	})

	instances := getInstances(t, e.instanceStorage)
	assert.Len(t, instances, 3)
	assert.Equal(t, proto.Instance_TERMINATED, instances["i-1"].Status)
	assert.Equal(t, proto.Instance_TERMINATING, instances["i-2"].Status)

	events := e.subscriber.events
	assert.Len(t, events, 3)

	eventsI1 := e.subscriber.eventsById("i-1")
	assert.Len(t, eventsI1, 1)
	assert.Equal(t, proto.Instance_TERMINATED, eventsI1[0].NewInstanceStatus)
	assert.Equal(t, "i-1", eventsI1[0].InstanceId)

	eventsI2 := e.subscriber.eventsById("i-2")
	assert.Len(t, eventsI2, 2)
	assert.Equal(t, proto.Instance_TERMINATION_FAILED, eventsI2[0].NewInstanceStatus)
	assert.Equal(t, proto.Instance_TERMINATING, eventsI2[1].NewInstanceStatus)
}

// TestRayReconcilerNoOp produces no events without Ray nodes or with an
// unknown Ray node status.
func TestRayReconcilerNoOp(t *testing.T) {
	e := newTestEnv(t)
	addInstances(t, e.instanceStorage, createInstance(instanceSpec{
		id:              "i-1",
		status:          proto.Instance_ALLOCATED,
		launchRequestId: "l1",
		instanceType:    "type-1",
		cloudInstanceId: "c-1",
	}))
	cloudInstances := map[string]autoscaler.CloudInstance{
		"c-1": cloudInstance("c-1", "type-1", true, proto.NodeKind_WORKER),
	}

	e.subscriber.clear()
	e.reconcile(t, reconcileOpts{cloudInstances: cloudInstances})

	// No changes.
	assert.Empty(t, e.subscriber.events)

	// Unknown Ray node status: Python raises ValueError; Go logs and skips,
	// likewise with no events.
	e.reconcile(t, reconcileOpts{
		rayNodes:       []*proto.NodeState{rayNode("r-1", proto.NodeStatus_UNSPECIFIED, "c-1", "")},
		cloudInstances: cloudInstances,
	})

	assert.Empty(t, e.subscriber.events)
}

// TestRayReconcilerNewRay a Ray node joins the cluster -> RAY_RUNNING.
func TestRayReconcilerNewRay(t *testing.T) {
	e := newTestEnv(t)
	nodeStates := []*proto.NodeState{rayNode("r-1", proto.NodeStatus_RUNNING, "c-1", "")}
	addInstances(t, e.instanceStorage,
		// A TERMINATED instance should not be matched.
		createInstance(instanceSpec{id: "i-0", status: proto.Instance_TERMINATED, cloudInstanceId: "c-1"}),
		createInstance(instanceSpec{id: "i-1", status: proto.Instance_ALLOCATED, cloudInstanceId: "c-1"}),
	)
	cloudInstances := map[string]autoscaler.CloudInstance{
		"c-1": cloudInstance("c-1", "type-1", true, proto.NodeKind_WORKER),
	}

	e.reconcile(t, reconcileOpts{rayNodes: nodeStates, cloudInstances: cloudInstances})

	instances := getInstances(t, e.instanceStorage)
	assert.Len(t, instances, 2)
	assert.Equal(t, proto.Instance_TERMINATED, instances["i-0"].Status)
	assert.Equal(t, proto.Instance_RAY_RUNNING, instances["i-1"].Status)
	assert.Equal(t, nodeHex("r-1"), instances["i-1"].GetNodeId())
}

// TestRayReconcilerAlreadyRayRunning an already reconciled running node
// produces no events.
func TestRayReconcilerAlreadyRayRunning(t *testing.T) {
	e := newTestEnv(t)
	addInstances(t, e.instanceStorage,
		createInstance(instanceSpec{id: "i-1", status: proto.Instance_RAY_RUNNING, cloudInstanceId: "c-1"}),
		createInstance(instanceSpec{id: "i-2", status: proto.Instance_TERMINATING, cloudInstanceId: "c-2"}),
	)
	rayNodes := []*proto.NodeState{
		rayNode("r-1", proto.NodeStatus_IDLE, "c-1", ""),
		rayNode("r-2", proto.NodeStatus_IDLE, "c-2", ""),
	}
	cloudInstances := map[string]autoscaler.CloudInstance{
		"c-1": cloudInstance("c-1", "type-1", true, proto.NodeKind_WORKER),
		"c-2": cloudInstance("c-2", "type-2", true, proto.NodeKind_WORKER),
	}

	e.subscriber.clear()
	e.reconcile(t, reconcileOpts{rayNodes: rayNodes, cloudInstances: cloudInstances})

	assert.Empty(t, e.subscriber.events)
}

// TestRayReconcilerStoppingRay a draining Ray node -> RAY_STOPPING.
func TestRayReconcilerStoppingRay(t *testing.T) {
	e := newTestEnv(t)
	addInstances(t, e.instanceStorage,
		createInstance(instanceSpec{id: "i-1", status: proto.Instance_RAY_RUNNING, cloudInstanceId: "c-1"}),
		createInstance(instanceSpec{id: "i-2", status: proto.Instance_RAY_STOPPING, cloudInstanceId: "c-2"}),
		createInstance(instanceSpec{id: "i-3", status: proto.Instance_TERMINATING, cloudInstanceId: "c-3"}),
	)
	rayNodes := []*proto.NodeState{
		rayNode("r-1", proto.NodeStatus_DRAINING, "c-1", ""),
		rayNode("r-2", proto.NodeStatus_DRAINING, "c-2", ""),
		rayNode("r-3", proto.NodeStatus_DRAINING, "c-3", ""),
	}
	cloudInstances := map[string]autoscaler.CloudInstance{
		"c-1": cloudInstance("c-1", "type-1", true, proto.NodeKind_WORKER),
		"c-2": cloudInstance("c-2", "type-2", true, proto.NodeKind_WORKER),
		"c-3": cloudInstance("c-3", "type-3", true, proto.NodeKind_WORKER),
	}

	e.reconcile(t, reconcileOpts{rayNodes: rayNodes, cloudInstances: cloudInstances})

	instances := getInstances(t, e.instanceStorage)
	assert.Len(t, instances, 3)
	assert.Equal(t, proto.Instance_RAY_STOPPING, instances["i-1"].Status)
	assert.Equal(t, proto.Instance_RAY_STOPPING, instances["i-2"].Status)
	assert.Equal(t, proto.Instance_TERMINATING, instances["i-3"].Status)
}

// TestRayReconcilerStoppedRay a stopped Ray node -> RAY_STOPPED then
// terminated.
func TestRayReconcilerStoppedRay(t *testing.T) {
	e := newTestEnv(t)
	addInstances(t, e.instanceStorage,
		createInstance(instanceSpec{id: "i-1", status: proto.Instance_ALLOCATED, cloudInstanceId: "c-1"}),
		createInstance(instanceSpec{id: "i-2", status: proto.Instance_RAY_STOPPING, cloudInstanceId: "c-2"}),
		createInstance(instanceSpec{id: "i-3", status: proto.Instance_TERMINATING, cloudInstanceId: "c-3"}),
	)
	rayNodes := []*proto.NodeState{
		rayNode("r-1", proto.NodeStatus_DEAD, "c-1", ""),
		rayNode("r-2", proto.NodeStatus_DEAD, "c-2", ""),
		rayNode("r-3", proto.NodeStatus_DEAD, "c-3", ""),
	}
	cloudInstances := map[string]autoscaler.CloudInstance{
		"c-1": cloudInstance("c-1", "type-1", true, proto.NodeKind_WORKER),
		"c-2": cloudInstance("c-2", "type-2", true, proto.NodeKind_WORKER),
		"c-3": cloudInstance("c-3", "type-3", true, proto.NodeKind_WORKER),
	}

	e.reconcile(t, reconcileOpts{rayNodes: rayNodes, cloudInstances: cloudInstances})

	instances := getInstances(t, e.instanceStorage)
	assert.Len(t, instances, 3)
	assert.Equal(t, proto.Instance_TERMINATING, instances["i-1"].Status)
	assert.Equal(t, proto.Instance_TERMINATING, instances["i-2"].Status)
	assert.Equal(t, proto.Instance_TERMINATING, instances["i-3"].Status)
}

// TestReconcileRayInstallerFailures an install failure -> RAY_INSTALL_FAILED
// then terminated.
func TestReconcileRayInstallerFailures(t *testing.T) {
	e := newTestEnv(t)
	addInstances(t, e.instanceStorage, createInstance(instanceSpec{
		id:              "i-1",
		status:          proto.Instance_RAY_INSTALLING,
		cloudInstanceId: "c-1",
	}))

	rayInstallErrors := []error{subscribers.RayInstallError{
		ImInstanceId: "i-1",
		Details:      "failed to install",
	}}

	cloudInstances := map[string]autoscaler.CloudInstance{
		"c-1": cloudInstance("c-1", "type-1", true, proto.NodeKind_WORKER),
	}

	e.reconcile(t, reconcileOpts{
		cloudInstances:   cloudInstances,
		rayInstallErrors: rayInstallErrors,
	})

	instances := getInstances(t, e.instanceStorage)
	assert.Len(t, instances, 1)
	assert.Equal(t, proto.Instance_TERMINATING, instances["i-1"].Status)
}

// TestDrainingRayNodeAlsoTerminated a draining node is also marked TERMINATED
// when its cloud instance has terminated.
func TestDrainingRayNodeAlsoTerminated(t *testing.T) {
	e := newTestEnv(t)
	addInstances(t, e.instanceStorage,
		createInstance(instanceSpec{
			id:              "i-1",
			status:          proto.Instance_RAY_RUNNING,
			cloudInstanceId: "c-1",
			rayNodeId:       nodeHex("r-1"),
		}),
		createInstance(instanceSpec{
			id:              "i-2",
			status:          proto.Instance_RAY_RUNNING,
			cloudInstanceId: "c-2",
			rayNodeId:       nodeHex("r-2"),
		}),
	)
	rayNodes := []*proto.NodeState{
		rayNode("r-1", proto.NodeStatus_DEAD, "c-1", ""),
		rayNode("r-2", proto.NodeStatus_DRAINING, "c-2", ""),
	}

	// Both cloud instances have terminated.
	e.reconcile(t, reconcileOpts{rayNodes: rayNodes})

	instances := getInstances(t, e.instanceStorage)
	assert.Len(t, instances, 2)
	assert.Equal(t, proto.Instance_TERMINATED, instances["i-1"].Status)
	assert.Equal(t, proto.Instance_TERMINATED, instances["i-2"].Status)
}

// TestMaxConcurrentLaunches covers the concurrent launch cap and the upscaling
// speed constraints.
func TestMaxConcurrentLaunches(t *testing.T) {
	cases := []struct {
		maxConcurrentLaunches int
		numAllocated          int
		numRequested          int
		numRunning            int
	}{
		{1, 0, 0, 0},
		{10, 0, 0, 0},
		{1, 0, 1, 1},
		{1, 1, 0, 1},
		{1, 10, 0, 1},
		{10, 1, 0, 1},
		{10, 5, 5, 5},
	}
	upscalingSpeeds := []float64{0.0, 0.1, 0.5, 1.0, 100.0}

	for _, c := range cases {
		for _, upscalingSpeed := range upscalingSpeeds {
			t.Run(
				strconv.Itoa(c.maxConcurrentLaunches)+"_"+strconv.Itoa(c.numAllocated)+"_"+
					strconv.Itoa(c.numRequested)+"_"+strconv.Itoa(c.numRunning)+"_"+
					strconv.FormatFloat(upscalingSpeed, 'f', -1, 64),
				func(t *testing.T) {
					e := newTestEnv(t)
					t.Setenv("AUTOSCALER_MAX_CONCURRENT_LAUNCHES", strconv.Itoa(c.maxConcurrentLaunches))
					nextId := 0

					// Add the ALLOCATED instances.
					cloudInstances := make(map[string]autoscaler.CloudInstance)
					for i := 0; i < c.numAllocated; i++ {
						cloudInstanceId := "c-" + strconv.Itoa(nextId)
						addInstances(t, e.instanceStorage, createInstance(instanceSpec{
							id:              strconv.Itoa(nextId),
							status:          proto.Instance_ALLOCATED,
							instanceType:    "type-1",
							cloudInstanceId: cloudInstanceId,
						}))
						cloudInstances[cloudInstanceId] = cloudInstance(cloudInstanceId, "type-1", true, proto.NodeKind_WORKER)
						nextId++
					}

					// Add the REQUESTED instances.
					for i := 0; i < c.numRequested; i++ {
						addInstances(t, e.instanceStorage, createInstance(instanceSpec{
							id:              strconv.Itoa(nextId),
							status:          proto.Instance_REQUESTED,
							instanceType:    "type-1",
							launchRequestId: "l-1",
						}))
						nextId++
					}

					// Add many QUEUED instances.
					queuedInstances := make([]*proto.Instance, 0, 1000)
					for i := 0; i < 1000; i++ {
						queuedInstances = append(queuedInstances, createInstance(instanceSpec{
							id:           strconv.Itoa(i + nextId),
							status:       proto.Instance_QUEUED,
							instanceType: "type-1",
						}))
					}
					addInstances(t, e.instanceStorage, queuedInstances...)

					// Add the RAY_RUNNING instances.
					for i := 0; i < c.numRunning; i++ {
						addInstances(t, e.instanceStorage, createInstance(instanceSpec{
							id:              strconv.Itoa(nextId),
							status:          proto.Instance_RAY_RUNNING,
							instanceType:    "type-1",
							launchRequestId: "l-1",
						}))
						nextId++
					}

					numDesiredUpscale := int(math.Ceil(upscalingSpeed * math.Max(float64(c.numRunning), 1)))
					if numDesiredUpscale < 1 {
						numDesiredUpscale = 1
					}
					globalLimit := c.maxConcurrentLaunches - c.numRequested
					if globalLimit < 0 {
						globalLimit = 0
					}
					expectedLaunchNum := numDesiredUpscale
					if globalLimit < expectedLaunchNum {
						expectedLaunchNum = globalLimit
					}

					e.subscriber.clear()
					e.reconcile(t, reconcileOpts{
						cloudInstances: cloudInstances,
						configs:        map[string]interface{}{"upscaling_speed": upscalingSpeed},
					})

					instances := getInstances(t, e.instanceStorage)
					assert.Len(t, e.subscriber.events, expectedLaunchNum)
					for _, event := range e.subscriber.events {
						assert.Equal(t, proto.Instance_REQUESTED, event.NewInstanceStatus)
						assert.Equal(t, instances[event.InstanceId].LaunchRequestId, event.GetLaunchRequestId())
						assert.Equal(t, "type-1", event.GetInstanceType())
					}
				})
		}
	}
}

// TestStuckInstancesRequested covers the retries and failures of a REQUESTED
// allocation timeout.
func TestStuckInstancesRequested(t *testing.T) {
	e := newTestEnv(t)
	curTimeS := int64(10)
	mockTime(t, curTimeS*sToNs)

	setReconcileEnv(t, &instance_manager.InstanceReconcileConfig{
		RequestStatusTimeoutS:        5,
		MaxNumRetryRequestToAllocate: 1,
	})
	preventLaunches(t)

	addInstances(t, e.instanceStorage,
		createInstance(instanceSpec{
			id:              "no-update",
			status:          proto.Instance_REQUESTED,
			statusTimes:     []statusTime{{proto.Instance_REQUESTED, 9 * sToNs}},
			launchRequestId: "l1",
		}),
		createInstance(instanceSpec{
			id:              "retry",
			status:          proto.Instance_REQUESTED,
			statusTimes:     []statusTime{{proto.Instance_REQUESTED, 2 * sToNs}},
			launchRequestId: "l2",
		}),
		createInstance(instanceSpec{
			id:              "failed",
			status:          proto.Instance_REQUESTED,
			statusTimes:     []statusTime{{proto.Instance_REQUESTED, 1 * sToNs}, {proto.Instance_REQUESTED, 2 * sToNs}},
			launchRequestId: "l3",
		}),
	)

	e.reconcile(t, reconcileOpts{})

	instances := getInstances(t, e.instanceStorage)
	assert.Equal(t, proto.Instance_REQUESTED, instances["no-update"].Status)
	assert.Equal(t, proto.Instance_QUEUED, instances["retry"].Status)
	assert.Equal(t, proto.Instance_ALLOCATION_FAILED, instances["failed"].Status)
}

// TestStuckInstancesRayStopRequested a RAY_STOP_REQUESTED timeout falls back to
// RAY_RUNNING.
func TestStuckInstancesRayStopRequested(t *testing.T) {
	e := newTestEnv(t)
	timeoutS := int64(5)
	curTimeS := int64(20)
	mockTime(t, curTimeS*sToNs)

	setReconcileEnv(t, &instance_manager.InstanceReconcileConfig{
		RayStopRequestedStatusTimeoutS: int(timeoutS),
	})
	preventLaunches(t)

	curStatus := proto.Instance_RAY_STOP_REQUESTED
	addInstances(t, e.instanceStorage,
		createInstance(instanceSpec{
			id:          "no-update",
			status:      curStatus,
			statusTimes: []statusTime{{curStatus, (curTimeS - timeoutS + 1) * sToNs}},
			rayNodeId:   "r-1",
		}),
		createInstance(instanceSpec{
			id:          "updated",
			status:      curStatus,
			statusTimes: []statusTime{{curStatus, (curTimeS - timeoutS - 1) * sToNs}},
			rayNodeId:   "r-2",
		}),
	)

	e.reconcile(t, reconcileOpts{})

	instances := getInstances(t, e.instanceStorage)
	assert.Equal(t, curStatus, instances["no-update"].Status)
	assert.Equal(t, proto.Instance_RAY_RUNNING, instances["updated"].Status)
}

// TestRayStopRequestedFail a Ray stop failure falls back to RAY_RUNNING.
func TestRayStopRequestedFail(t *testing.T) {
	e := newTestEnv(t)
	mockTime(t, 10*sToNs)

	addInstances(t, e.instanceStorage,
		createInstance(instanceSpec{
			id:              "i-1",
			status:          proto.Instance_RAY_STOP_REQUESTED,
			rayNodeId:       nodeHex("r-1"),
			cloudInstanceId: "c-1",
			statusTimes:     []statusTime{{proto.Instance_RAY_STOP_REQUESTED, 10 * sToNs}},
		}),
		createInstance(instanceSpec{
			id:              "i-2",
			status:          proto.Instance_RAY_STOP_REQUESTED,
			rayNodeId:       nodeHex("r-2"),
			cloudInstanceId: "c-2",
			statusTimes:     []statusTime{{proto.Instance_RAY_STOP_REQUESTED, 10 * sToNs}},
		}),
	)

	rayNodes := []*proto.NodeState{
		rayNode("r-1", proto.NodeStatus_RUNNING, "c-1", ""),
		rayNode("r-2", proto.NodeStatus_RUNNING, "c-2", ""),
	}

	rayStopErrors := []error{subscribers.RayStopError{ImInstanceId: "i-1"}}

	cloudInstances := map[string]autoscaler.CloudInstance{
		"c-1": cloudInstance("c-1", "type-1", true, proto.NodeKind_WORKER),
		"c-2": cloudInstance("c-2", "type-2", true, proto.NodeKind_WORKER),
	}

	e.reconcile(t, reconcileOpts{
		rayNodes:       rayNodes,
		cloudInstances: cloudInstances,
		rayStopErrors:  rayStopErrors,
	})

	instances := getInstances(t, e.instanceStorage)
	assert.Len(t, instances, 2)
	assert.Equal(t, proto.Instance_RAY_RUNNING, instances["i-1"].Status)
	assert.Equal(t, proto.Instance_RAY_STOP_REQUESTED, instances["i-2"].Status)
}

// TestStuckInstances covers the failure transitions after each status times
// out.
func TestStuckInstances(t *testing.T) {
	cases := []struct {
		curStatus    proto.Instance_InstanceStatus
		expectStatus proto.Instance_InstanceStatus
	}{
		{proto.Instance_ALLOCATED, proto.Instance_TERMINATING},
		{proto.Instance_RAY_INSTALLING, proto.Instance_TERMINATING},
		{proto.Instance_TERMINATING, proto.Instance_TERMINATING},
	}
	for _, c := range cases {
		t.Run(c.curStatus.String(), func(t *testing.T) {
			e := newTestEnv(t)
			timeoutS := int64(5)
			curTimeS := int64(20)
			mockTime(t, curTimeS*sToNs)

			setReconcileEnv(t, &instance_manager.InstanceReconcileConfig{
				AllocateStatusTimeoutS:    int(timeoutS),
				TerminatingStatusTimeoutS: int(timeoutS),
				RayInstallStatusTimeoutS:  int(timeoutS),
			})
			preventLaunches(t)

			addInstances(t, e.instanceStorage,
				createInstance(instanceSpec{
					id:              "no-update",
					status:          c.curStatus,
					statusTimes:     []statusTime{{c.curStatus, (curTimeS - timeoutS + 1) * sToNs}},
					instanceType:    "type-1",
					cloudInstanceId: "c-1",
				}),
				createInstance(instanceSpec{
					id:              "updated",
					status:          c.curStatus,
					statusTimes:     []statusTime{{c.curStatus, (curTimeS - timeoutS - 1) * sToNs}},
					instanceType:    "type-1",
					cloudInstanceId: "c-2",
				}),
			)

			cloudInstances := map[string]autoscaler.CloudInstance{
				"c-1": cloudInstance("c-1", "type-1", true, proto.NodeKind_WORKER),
				"c-2": cloudInstance("c-2", "type-1", true, proto.NodeKind_WORKER),
			}

			e.reconcile(t, reconcileOpts{cloudInstances: cloudInstances})

			instances := getInstances(t, e.instanceStorage)
			assert.Equal(t, c.curStatus, instances["no-update"].Status)
			assert.Equal(t, c.expectStatus, instances["updated"].Status)
		})
	}
}

// TestWarnStuckTransientInstances covers warning about the instances stuck in
// a transient status.
// It calls warnStuckInstances directly and asserts the warning count
// (corresponding to the Python tests counting warning calls via a mock
// logger).
func TestWarnStuckTransientInstances(t *testing.T) {
	for _, status := range []proto.Instance_InstanceStatus{
		proto.Instance_RAY_STOPPING,
		proto.Instance_QUEUED,
	} {
		t.Run(status.String(), func(t *testing.T) {
			curTimeS := int64(10)
			mockTime(t, curTimeS*int64(time.Second))
			timeoutS := int64(5)

			instances := []*proto.Instance{
				createInstance(instanceSpec{
					id:          "no-warn",
					status:      status,
					statusTimes: []statusTime{{status, (curTimeS - timeoutS + 1) * sToNs}},
				}),
				createInstance(instanceSpec{
					id:          "warn",
					status:      status,
					statusTimes: []statusTime{{status, (curTimeS - timeoutS - 1) * sToNs}},
				}),
			}

			warned := warnStuckInstances(instances, status, int(timeoutS))
			assert.Equal(t, 1, warned)
		})
	}
}

// TestStuckInstancesNoOp statuses without a timeout mechanism do not change.
func TestStuckInstancesNoOp(t *testing.T) {
	e := newTestEnv(t)
	// A time large enough to trigger every timeout.
	mockTime(t, 999999*sToNs)
	preventLaunches(t)

	// Statuses other than reconciled_stuck and transient neither transition nor
	// warn.
	noOpStatuses := []proto.Instance_InstanceStatus{
		proto.Instance_UNKNOWN,
		proto.Instance_RAY_RUNNING,
		proto.Instance_TERMINATED,
		proto.Instance_ALLOCATION_FAILED,
	}
	for _, status := range noOpStatuses {
		addInstances(t, e.instanceStorage, createInstance(instanceSpec{
			id:          "no-op-" + status.String(),
			status:      status,
			statusTimes: []statusTime{{status, 1 * sToNs}},
		}))
	}

	e.subscriber.clear()
	e.reconcile(t, reconcileOpts{})

	assert.Empty(t, e.subscriber.events)
}

// TestIsHeadNodeRunning covers the head node running status judgment.
func TestIsHeadNodeRunning(t *testing.T) {
	cases := []struct {
		status          proto.Instance_InstanceStatus
		expectedRunning bool
	}{
		{proto.Instance_RAY_RUNNING, true},
		{proto.Instance_ALLOCATED, false},
	}
	for _, c := range cases {
		t.Run(c.status.String(), func(t *testing.T) {
			e := newTestEnv(t)
			addInstances(t, e.instanceStorage, createInstance(instanceSpec{
				id:              "i-1",
				status:          c.status,
				cloudInstanceId: "c-1",
				nodeKind:        proto.NodeKind_HEAD,
			}))
			assert.Equal(t, c.expectedRunning, e.reconciler.isHeadNodeRunning())
		})
	}
}

// TestScalingUpdates scales up and down per the scheduler decisions.
func TestScalingUpdates(t *testing.T) {
	e := newTestEnv(t)
	addInstances(t, e.instanceStorage,
		createInstance(instanceSpec{
			id:              "head",
			status:          proto.Instance_RAY_RUNNING,
			cloudInstanceId: "c-0",
			rayNodeId:       nodeHex("r-0"),
			nodeKind:        proto.NodeKind_HEAD,
		}),
		createInstance(instanceSpec{
			id:              "i-1",
			status:          proto.Instance_RAY_RUNNING,
			cloudInstanceId: "c-1",
			rayNodeId:       nodeHex("r-1"),
			nodeKind:        proto.NodeKind_WORKER,
		}),
	)

	rayNodes := []*proto.NodeState{
		rayNode("r-0", proto.NodeStatus_RUNNING, "c-1", ""),
		rayNode("r-1", proto.NodeStatus_RUNNING, "c-1", ""),
	}

	cloudInstances := map[string]autoscaler.CloudInstance{
		"c-0": cloudInstance("c-0", "head", true, proto.NodeKind_HEAD),
		"c-1": cloudInstance("c-1", "type-1", true, proto.NodeKind_WORKER),
	}

	e.scheduler.reply = &scheduler.SchedulingReply{
		ToLaunch: []*proto.LaunchRequest{
			{InstanceType: "type-1", Count: 2},
		},
		ToTerminate: []*proto.TerminationRequest{
			{
				Id:             "t1",
				RayNodeId:      "r-1",
				InstanceId:     "i-1",
				Cause:          proto.TerminationRequest_IDLE,
				IdleDurationMs: uint64ptr(1000),
			},
		},
		InfeasibleGangResourceRequests: []*proto.GangResourceRequest{
			{Requests: []*proto.ResourceRequest{{ResourcesBundle: map[string]float64{"CPU": 1}}}},
		},
	}

	preventLaunches(t)
	state := e.reconcile(t, reconcileOpts{
		rayNodes:       rayNodes,
		cloudInstances: cloudInstances,
	})

	instances := getInstances(t, e.instanceStorage)

	assert.Len(t, instances, 3+1) // including the head node
	for id, instance := range instances {
		if id == "head" {
			assert.Equal(t, proto.Instance_RAY_RUNNING, instance.Status)
		} else if id == "i-1" {
			assert.Equal(t, proto.Instance_RAY_STOP_REQUESTED, instance.Status)
		} else {
			assert.Equal(t, proto.Instance_QUEUED, instance.Status)
			assert.Equal(t, "type-1", instance.InstanceType)
		}
	}

	assert.Len(t, state.InfeasibleGangResourceRequests, 1)
}

// TestTerminatingInstances pre-termination statuses -> TERMINATING.
func TestTerminatingInstances(t *testing.T) {
	e := newTestEnv(t)
	addInstances(t, e.instanceStorage,
		createInstance(instanceSpec{id: "i-1", status: proto.Instance_RAY_STOPPED, cloudInstanceId: "c-1"}),
		createInstance(instanceSpec{id: "i-2", status: proto.Instance_RAY_INSTALL_FAILED, cloudInstanceId: "c-2"}),
		createInstance(instanceSpec{id: "i-3", status: proto.Instance_TERMINATION_FAILED, cloudInstanceId: "c-3"}),
	)

	cloudInstances := map[string]autoscaler.CloudInstance{
		"c-1": cloudInstance("c-1", "type-1", true, proto.NodeKind_WORKER),
		"c-2": cloudInstance("c-2", "type-2", true, proto.NodeKind_WORKER),
		"c-3": cloudInstance("c-3", "type-3", true, proto.NodeKind_WORKER),
	}

	e.reconcile(t, reconcileOpts{cloudInstances: cloudInstances})

	instances := getInstances(t, e.instanceStorage)
	assert.Equal(t, proto.Instance_TERMINATING, instances["i-1"].Status)
	assert.Equal(t, proto.Instance_TERMINATING, instances["i-2"].Status)
	assert.Equal(t, proto.Instance_TERMINATING, instances["i-3"].Status)
}

// TestRayInstall installs Ray per the config and the cloud instance state.
func TestRayInstall(t *testing.T) {
	for _, disableNodeUpdaters := range []bool{true, false} {
		for _, cloudInstanceRunning := range []bool{true, false} {
			t.Run(
				"disable_node_updaters_"+strconv.FormatBool(disableNodeUpdaters)+
					"_cloud_instance_running_"+strconv.FormatBool(cloudInstanceRunning),
				func(t *testing.T) {
					e := newTestEnv(t)
					addInstances(t, e.instanceStorage, createInstance(instanceSpec{
						id:              "i-1",
						status:          proto.Instance_ALLOCATED,
						instanceType:    "type-1",
						launchRequestId: "l1",
						cloudInstanceId: "c-1",
					}))

					cloudInstances := map[string]autoscaler.CloudInstance{
						"c-1": cloudInstance("c-1", "type-1", cloudInstanceRunning, proto.NodeKind_WORKER),
					}

					e.reconcile(t, reconcileOpts{
						cloudInstances: cloudInstances,
						configs: map[string]interface{}{
							"disable_node_updaters": disableNodeUpdaters,
						},
					})

					instances := getInstances(t, e.instanceStorage)
					if disableNodeUpdaters || !cloudInstanceRunning {
						assert.Equal(t, proto.Instance_ALLOCATED, instances["i-1"].Status)
					} else {
						assert.Equal(t, proto.Instance_RAY_INSTALLING, instances["i-1"].Status)
					}
				})
		}
	}
}

// TestAutoscalerState covers the autoscaling state summary.
func TestAutoscalerState(t *testing.T) {
	e := newTestEnv(t)
	mockTime(t, 5)

	addInstances(t, e.instanceStorage,
		createInstance(instanceSpec{
			id:              "head",
			status:          proto.Instance_RAY_RUNNING,
			cloudInstanceId: "c-0",
			rayNodeId:       nodeHex("r-0"),
			nodeKind:        proto.NodeKind_HEAD,
		}),
		createInstance(instanceSpec{
			id:              "i-1",
			status:          proto.Instance_ALLOCATED,
			instanceType:    "type-1",
			cloudInstanceId: "c-1",
			launchRequestId: "l1",
			statusTimes: []statusTime{
				{proto.Instance_QUEUED, 0},
				{proto.Instance_REQUESTED, 1},
				{proto.Instance_ALLOCATED, 2},
			},
		}),
		createInstance(instanceSpec{
			id:              "i-2",
			status:          proto.Instance_REQUESTED,
			instanceType:    "type-2",
			launchRequestId: "l2",
			statusTimes:     []statusTime{{proto.Instance_QUEUED, 0}, {proto.Instance_REQUESTED, 1}},
		}),
		createInstance(instanceSpec{
			id:              "i-3",
			status:          proto.Instance_QUEUED,
			instanceType:    "type-3",
			launchRequestId: "l3",
			statusTimes:     []statusTime{{proto.Instance_QUEUED, 0}},
		}),
		createInstance(instanceSpec{
			id:              "i-4",
			status:          proto.Instance_ALLOCATION_FAILED,
			instanceType:    "type-4",
			launchRequestId: "l4",
			statusTimes: []statusTime{
				{proto.Instance_QUEUED, 0},
				{proto.Instance_REQUESTED, 1},
				{proto.Instance_ALLOCATION_FAILED, 2},
			},
		}),
		createInstance(instanceSpec{
			id:              "i-5",
			status:          proto.Instance_RAY_INSTALLING,
			instanceType:    "type-5",
			launchRequestId: "l5",
			cloudInstanceId: "c-5",
			statusTimes: []statusTime{
				{proto.Instance_QUEUED, 0},
				{proto.Instance_REQUESTED, 1},
				{proto.Instance_ALLOCATED, 2},
				{proto.Instance_RAY_INSTALLING, 3},
			},
		}),
	)

	cloudInstances := map[string]autoscaler.CloudInstance{
		"c-0": cloudInstance("c-0", "head", true, proto.NodeKind_HEAD),
		"c-1": cloudInstance("c-1", "type-1", true, proto.NodeKind_WORKER),
		"c-5": cloudInstance("c-5", "type-5", true, proto.NodeKind_WORKER),
	}

	e.scheduler.reply = &scheduler.SchedulingReply{
		InfeasibleGangResourceRequests: []*proto.GangResourceRequest{
			{Requests: []*proto.ResourceRequest{{ResourcesBundle: map[string]float64{"CPU": 1}}}},
		},
		InfeasibleResourceRequests: []*proto.ResourceRequest{
			{ResourcesBundle: map[string]float64{"CPU": 1}},
		},
	}

	preventLaunches(t)
	autoscalingState := e.reconcile(t, reconcileOpts{
		cloudInstances:          cloudInstances,
		clusterResourceStateVer: 1,
	})

	assert.Equal(t, int64(1), autoscalingState.LastSeenClusterResourceStateVersion)
	assert.Len(t, autoscalingState.InfeasibleGangResourceRequests, 1)
	assert.Len(t, autoscalingState.InfeasibleResourceRequests, 1)
	assert.Len(t, autoscalingState.PendingInstances, 2)
	pendingInstances := make(map[string]bool)
	for _, i := range autoscalingState.PendingInstances {
		pendingInstances[i.InstanceId] = true
	}
	assert.Equal(t, map[string]bool{"i-1": true, "i-5": true}, pendingInstances)

	pendingInstanceRequests := make(map[string]int32)
	for _, r := range autoscalingState.PendingInstanceRequests {
		pendingInstanceRequests[r.RayNodeTypeName] += r.Count
	}
	failedInstanceRequests := make(map[string]int32)
	for _, r := range autoscalingState.FailedInstanceRequests {
		failedInstanceRequests[r.RayNodeTypeName] += r.Count
	}
	assert.Equal(t, map[string]int32{"type-2": 1, "type-3": 1}, pendingInstanceRequests)
	assert.Equal(t, map[string]int32{"type-4": 1}, failedInstanceRequests)
}

// TestExtraCloudInstancesCloudProvider covers backfilling the unmanaged cloud
// instances/Ray nodes.
func TestExtraCloudInstancesCloudProvider(t *testing.T) {
	e := newTestEnv(t)
	addInstances(t, e.instanceStorage, createInstance(instanceSpec{
		id:              "i-1",
		status:          proto.Instance_RAY_RUNNING,
		cloudInstanceId: "c-1",
	}))

	rayNodes := []*proto.NodeState{
		rayNode("r-1", proto.NodeStatus_RUNNING, "c-1", ""),
		// An out-of-band Ray node.
		rayNode("r-2", proto.NodeStatus_RUNNING, "c-3", "type-1"),
	}

	cloudInstances := map[string]autoscaler.CloudInstance{
		"c-1": cloudInstance("c-1", "type-1", true, proto.NodeKind_WORKER),
		// An out-of-band cloud instance.
		"c-2": cloudInstance("c-2", "type-2", true, proto.NodeKind_WORKER),
	}

	e.subscriber.clear()
	e.reconcile(t, reconcileOpts{rayNodes: rayNodes, cloudInstances: cloudInstances})

	for _, e := range e.subscriber.events {
		if e.NewInstanceStatus == proto.Instance_ALLOCATED {
			assert.Contains(t, []string{"c-2", "c-3"}, e.GetCloudInstanceId())
		} else {
			assert.Equal(t, proto.Instance_RAY_RUNNING, e.NewInstanceStatus)
			assert.Equal(t, nodeHex("r-2"), e.GetRayNodeId())
		}
	}

	instances := getInstances(t, e.instanceStorage)
	assert.Len(t, instances, 3)
	statuses := make(map[proto.Instance_InstanceStatus]bool)
	for _, instance := range instances {
		statuses[instance.Status] = true
	}
	assert.Equal(t, map[proto.Instance_InstanceStatus]bool{
		proto.Instance_RAY_RUNNING: true,
		proto.Instance_ALLOCATED:   true,
	}, statuses)
}

// TestCloudInstanceReboot covers a cloud instance reboot (the previous instance
// is TERMINATED).
func TestCloudInstanceReboot(t *testing.T) {
	e := newTestEnv(t)
	addInstances(t, e.instanceStorage, createInstance(instanceSpec{
		id:              "i-1",
		status:          proto.Instance_TERMINATED,
		cloudInstanceId: "c-1",
		rayNodeId:       nodeHex("r-1"),
	}))

	rayNodes := []*proto.NodeState{
		rayNode("r-1", proto.NodeStatus_DEAD, "c-1", "type-1"),
	}

	cloudInstances := map[string]autoscaler.CloudInstance{
		"c-1": cloudInstance("c-1", "type-1", true, proto.NodeKind_WORKER),
	}

	e.subscriber.clear()
	e.reconcile(t, reconcileOpts{rayNodes: rayNodes, cloudInstances: cloudInstances})

	for _, e := range e.subscriber.events {
		assert.Equal(t, proto.Instance_ALLOCATED, e.NewInstanceStatus)
		assert.Equal(t, "c-1", e.GetCloudInstanceId())
	}

	instances := getInstances(t, e.instanceStorage)
	assert.Len(t, instances, 2)
	statuses := make(map[proto.Instance_InstanceStatus]bool)
	for _, instance := range instances {
		statuses[instance.Status] = true
	}
	assert.Equal(t, map[proto.Instance_InstanceStatus]bool{
		proto.Instance_ALLOCATED:  true,
		proto.Instance_TERMINATED: true,
	}, statuses)
}

// TestRayNodeRestartedOnTheSameCloudInstance covers a Ray node restarting on
// the same cloud instance.
func TestRayNodeRestartedOnTheSameCloudInstance(t *testing.T) {
	e := newTestEnv(t)
	addInstances(t, e.instanceStorage, createInstance(instanceSpec{
		id:              "i-1",
		status:          proto.Instance_RAY_RUNNING,
		cloudInstanceId: "c-1",
		rayNodeId:       nodeHex("r-1"),
	}))

	rayNodes := []*proto.NodeState{
		rayNode("r-2", proto.NodeStatus_IDLE, "c-1", "type-1"),
		rayNode("r-1", proto.NodeStatus_DEAD, "c-1", "type-1"),
	}

	cloudInstances := map[string]autoscaler.CloudInstance{
		"c-1": cloudInstance("c-1", "type-1", true, proto.NodeKind_WORKER),
	}

	e.subscriber.clear()
	e.reconcile(t, reconcileOpts{rayNodes: rayNodes, cloudInstances: cloudInstances})

	events := e.subscriber.events
	assert.Len(t, events, 4)
	assert.Equal(t, proto.Instance_ALLOCATED, events[0].NewInstanceStatus)
	assert.Equal(t, "c-1", events[0].GetCloudInstanceId())
	assert.Equal(t, nodeHex("r-2"), events[0].GetRayNodeId())

	assert.Equal(t, proto.Instance_RAY_RUNNING, events[1].NewInstanceStatus)
	assert.Equal(t, events[0].InstanceId, events[1].InstanceId)
	assert.Equal(t, nodeHex("r-2"), events[1].GetRayNodeId())

	assert.Equal(t, proto.Instance_RAY_STOPPED, events[2].NewInstanceStatus)
	assert.Equal(t, "i-1", events[2].InstanceId)
	assert.Equal(t, nodeHex("r-1"), events[2].GetRayNodeId())
	assert.Equal(t, proto.Instance_TERMINATING, events[3].NewInstanceStatus)
	assert.Equal(t, "i-1", events[3].InstanceId)

	instances := getInstances(t, e.instanceStorage)
	assert.Len(t, instances, 2)
	statuses := make(map[proto.Instance_InstanceStatus]bool)
	for _, instance := range instances {
		statuses[instance.Status] = true
	}
	assert.Equal(t, map[proto.Instance_InstanceStatus]bool{
		proto.Instance_RAY_RUNNING: true,
		proto.Instance_TERMINATING: true,
	}, statuses)
}

// TestRayHeadRestartedOnTheSameCloudInstance covers the head node restarting
// on the same cloud instance (the GCS FT scenario).
func TestRayHeadRestartedOnTheSameCloudInstance(t *testing.T) {
	e := newTestEnv(t)

	rayNodes := []*proto.NodeState{
		rayNode("r-2", proto.NodeStatus_IDLE, "c-1", "type-1"),
		rayNode("r-1", proto.NodeStatus_DEAD, "c-1", "type-1"),
	}

	cloudInstances := map[string]autoscaler.CloudInstance{
		"c-1": cloudInstance("c-1", "type-1", true, proto.NodeKind_HEAD),
	}

	e.subscriber.clear()
	e.reconcile(t, reconcileOpts{rayNodes: rayNodes, cloudInstances: cloudInstances})

	events := e.subscriber.events
	assert.Len(t, events, 5)
	assert.Equal(t, proto.Instance_ALLOCATED, events[0].NewInstanceStatus)
	assert.Equal(t, "c-1", events[0].GetCloudInstanceId())
	assert.Equal(t, nodeHex("r-2"), events[0].GetRayNodeId())

	assert.Equal(t, proto.Instance_ALLOCATED, events[1].NewInstanceStatus)
	assert.Equal(t, "c-1", events[1].GetCloudInstanceId())
	assert.Equal(t, nodeHex("r-1"), events[1].GetRayNodeId())

	assert.NotEqual(t, events[1].InstanceId, events[0].InstanceId)

	assert.Equal(t, proto.Instance_RAY_RUNNING, events[2].NewInstanceStatus)
	assert.Equal(t, events[0].InstanceId, events[2].InstanceId)
	assert.Equal(t, nodeHex("r-2"), events[2].GetRayNodeId())

	assert.Equal(t, proto.Instance_RAY_STOPPED, events[3].NewInstanceStatus)
	assert.Equal(t, events[1].InstanceId, events[3].InstanceId)
	assert.Equal(t, nodeHex("r-1"), events[3].GetRayNodeId())

	assert.Equal(t, proto.Instance_TERMINATING, events[4].NewInstanceStatus)
	assert.Equal(t, events[1].InstanceId, events[4].InstanceId)

	instances := getInstances(t, e.instanceStorage)
	assert.Len(t, instances, 2)
	statuses := make(map[proto.Instance_InstanceStatus]bool)
	for _, instance := range instances {
		statuses[instance.Status] = true
	}
	assert.Equal(t, map[proto.Instance_InstanceStatus]bool{
		proto.Instance_RAY_RUNNING: true,
		proto.Instance_TERMINATING: true,
	}, statuses)
}

// TestReconcileMaxWorkerNodesLimitTriggersTermination the max node count
// triggers termination of ALLOCATED instances.
func TestReconcileMaxWorkerNodesLimitTriggersTermination(t *testing.T) {
	e := newTestEnv(t)
	addInstances(t, e.instanceStorage,
		createInstance(instanceSpec{
			id:              "head",
			status:          proto.Instance_RAY_RUNNING,
			nodeKind:        proto.NodeKind_HEAD,
			cloudInstanceId: "c-head",
			rayNodeId:       nodeHex("r-head"),
		}),
		createInstance(instanceSpec{
			id:              "i-0",
			status:          proto.Instance_ALLOCATED,
			instanceType:    "type-1",
			cloudInstanceId: "c-0",
			rayNodeId:       nodeHex("r-0"),
		}),
		createInstance(instanceSpec{
			id:              "i-1",
			status:          proto.Instance_ALLOCATED,
			instanceType:    "type-1",
			cloudInstanceId: "c-1",
			rayNodeId:       nodeHex("r-1"),
		}),
	)

	// The Ray node list is empty: the instances are pending and not running
	// yet.
	cloudInstances := map[string]autoscaler.CloudInstance{
		"c-head": cloudInstance("c-head", "head", true, proto.NodeKind_HEAD),
		"c-0":    cloudInstance("c-0", "type-1", true, proto.NodeKind_WORKER),
		"c-1":    cloudInstance("c-1", "type-1", true, proto.NodeKind_WORKER),
	}

	// The scheduler should terminate both worker instances due to the max node
	// count limit.
	e.scheduler.reply = &scheduler.SchedulingReply{
		ToTerminate: []*proto.TerminationRequest{
			{
				Id:             "t0",
				RayNodeId:      "r-0",
				InstanceId:     "i-0",
				InstanceStatus: proto.Instance_ALLOCATED,
				Cause:          proto.TerminationRequest_MAX_NUM_NODE_PER_TYPE,
			},
			{
				Id:             "t1",
				RayNodeId:      "r-1",
				InstanceId:     "i-1",
				InstanceStatus: proto.Instance_ALLOCATED,
				Cause:          proto.TerminationRequest_MAX_NUM_NODE_PER_TYPE,
			},
		},
	}

	e.reconcile(t, reconcileOpts{
		cloudInstances:          cloudInstances,
		clusterResourceStateVer: 1,
	})

	instances := getInstances(t, e.instanceStorage)
	assert.Equal(t, proto.Instance_TERMINATING, instances["i-0"].Status)
	assert.Equal(t, proto.Instance_TERMINATING, instances["i-1"].Status)
}

// TestTerminateRayInstallingInstancesMaxWorkers the max node count triggers
// termination of RAY_INSTALLING instances.
func TestTerminateRayInstallingInstancesMaxWorkers(t *testing.T) {
	e := newTestEnv(t)
	addInstances(t, e.instanceStorage,
		createInstance(instanceSpec{
			id:              "head",
			status:          proto.Instance_RAY_RUNNING,
			nodeKind:        proto.NodeKind_HEAD,
			cloudInstanceId: "c-head",
			rayNodeId:       nodeHex("r-head"),
		}),
		createInstance(instanceSpec{
			id:              "i-0",
			status:          proto.Instance_RAY_INSTALLING,
			instanceType:    "type-1",
			cloudInstanceId: "c-0",
		}),
		createInstance(instanceSpec{
			id:              "i-1",
			status:          proto.Instance_RAY_INSTALLING,
			instanceType:    "type-1",
			cloudInstanceId: "c-1",
		}),
	)

	// The Ray node list is empty: the RAY_INSTALLING instances have not started
	// Ray yet.
	cloudInstances := map[string]autoscaler.CloudInstance{
		"c-head": cloudInstance("c-head", "head", true, proto.NodeKind_HEAD),
		"c-0":    cloudInstance("c-0", "type-1", true, proto.NodeKind_WORKER),
		"c-1":    cloudInstance("c-1", "type-1", true, proto.NodeKind_WORKER),
	}

	e.scheduler.reply = &scheduler.SchedulingReply{
		ToTerminate: []*proto.TerminationRequest{
			{
				Id:             "t0",
				InstanceId:     "i-0",
				InstanceStatus: proto.Instance_RAY_INSTALLING,
				Cause:          proto.TerminationRequest_MAX_NUM_NODE_PER_TYPE,
			},
			{
				Id:             "t1",
				InstanceId:     "i-1",
				InstanceStatus: proto.Instance_RAY_INSTALLING,
				Cause:          proto.TerminationRequest_MAX_NUM_NODE_PER_TYPE,
			},
		},
	}

	e.reconcile(t, reconcileOpts{
		cloudInstances:          cloudInstances,
		clusterResourceStateVer: 1,
	})

	instances := getInstances(t, e.instanceStorage)
	assert.Equal(t, proto.Instance_TERMINATING, instances["i-0"].Status)
	assert.Equal(t, proto.Instance_TERMINATING, instances["i-1"].Status)
}

// uint64ptr returns a uint64 pointer.
func uint64ptr(v uint64) *uint64 {
	return &v
}
