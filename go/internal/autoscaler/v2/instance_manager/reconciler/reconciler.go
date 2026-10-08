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

// Package reconciler implements the instance state reconciler.
// It corresponds to Python
// python/ray/autoscaler/v2/instance_manager/reconciler.py.
//
// Each round the reconciler does three things:
//  1. syncFrom: passively syncs the instance statuses with the external state
//     (the results of the cloud provider/Ray cluster/installer/stopper), see
//     passive_sync.go;
//  2. stepNext: proactively advances the instance statuses (stuck handling,
//     scale up/down, launch requests, termination, installing Ray), see
//     active_scaling.go;
//  3. reportMetrics: reports the instance metrics.
//
// Dependency direction: this package depends on
// instance_manager/scheduler/autoscaler/subscribers, none of which depend back
// on it; it must not depend on the v2 package (the v2 assembly point imports
// this package, which would create a cycle); the metrics reporter is injected
// via the InstanceMetricsReporter duck-typed interface.
package reconciler

import (
	"fmt"

	"github.com/ray-project/ray/go/internal/autoscaler"
	"github.com/ray-project/ray/go/internal/autoscaler/v2/instance_manager"
	"github.com/ray-project/ray/go/internal/autoscaler/v2/instance_manager/subscribers"
	"github.com/ray-project/ray/go/internal/autoscaler/v2/scheduler"
	"github.com/ray-project/ray/go/pkg/log"
	"github.com/ray-project/ray/go/proto"
)

// InstanceMetricsReporter is the instance metrics reporting interface.
// It corresponds to Python AutoscalerMetricsReporter's
// report_instances/report_resources, satisfied by the v2 package's
// AutoscalerMetricsReporter; injecting via an interface avoids depending on v2
// and creating a cycle.
type InstanceMetricsReporter interface {
	// ReportInstances reports the instance counts per node type per status.
	ReportInstances(instances []proto.Instance, nodeTypeConfigs map[string]instance_manager.NodeTypeConfig) error
	// ReportResources reports the pending resources and the cluster resource
	// metrics.
	ReportResources(instances []proto.Instance, nodeTypeConfigs map[string]instance_manager.NodeTypeConfig)
}

// Reconciler is the instance state reconciler.
// Corresponds to Python reconciler.Reconciler (a singleton with static
// methods); the Go port injects the stable components (the instance
// manager/scheduler/cloud provider/resource monitor/metrics reporter) through
// the constructor, and each round's inputs come in via ReconcileArgs.
type Reconciler struct {
	instanceManager      *instance_manager.InstanceManager
	scheduler            scheduler.IResourceScheduler
	cloudProvider        autoscaler.ICloudInstanceProvider
	cloudResourceMonitor *subscribers.CloudResourceMonitor
	metricsReporter      InstanceMetricsReporter
}

// NewReconciler creates the reconciler.
// cloudResourceMonitor/metricsReporter may be nil: a nil monitor makes the
// scheduler treat every node type's resource availability as normal, and a nil
// reporter skips the metrics reporting.
func NewReconciler(
	instanceManager *instance_manager.InstanceManager,
	scheduler scheduler.IResourceScheduler,
	cloudProvider autoscaler.ICloudInstanceProvider,
	cloudResourceMonitor *subscribers.CloudResourceMonitor,
	metricsReporter InstanceMetricsReporter,
) *Reconciler {
	return &Reconciler{
		instanceManager:      instanceManager,
		scheduler:            scheduler,
		cloudProvider:        cloudProvider,
		cloudResourceMonitor: cloudResourceMonitor,
		metricsReporter:      metricsReporter,
	}
}

// Reconcile runs one reconcile round and returns the autoscaling state of this
// round.
// Corresponds to Python Reconciler.reconcile.
func (r *Reconciler) Reconcile(args *autoscaler.ReconcileArgs) (*proto.AutoscalingState, error) {
	if args == nil || args.RayClusterResourceState == nil {
		return nil, fmt.Errorf("reconcile args or ray cluster resource state is nil")
	}
	asConfig, ok := args.AutoscalingConfig.(*instance_manager.AutoscalingConfig)
	if !ok {
		return nil, fmt.Errorf("invalid autoscaling config type %T, expect *instance_manager.AutoscalingConfig",
			args.AutoscalingConfig)
	}

	rayClusterResourceState := args.RayClusterResourceState
	autoscalingState := &proto.AutoscalingState{
		LastSeenClusterResourceStateVersion: rayClusterResourceState.ClusterResourceStateVersion,
	}

	// Passive sync: align the instance statuses with the external reality of
	// the cloud provider and the Ray cluster.
	r.syncFrom(
		rayClusterResourceState.NodeStates,
		args.NonTerminatedCloudInstances,
		args.CloudProviderErrors,
		args.RayInstallErrors,
		args.RayStopErrors,
		asConfig,
	)

	// Proactive stepping: compute and apply the status transitions needed this
	// round.
	r.stepNext(
		autoscalingState,
		rayClusterResourceState,
		args.NonTerminatedCloudInstances,
		asConfig,
	)

	r.reportMetrics(asConfig)

	return autoscalingState, nil
}

// syncFrom passively syncs the instance statuses with the external state.
// Corresponds to Python Reconciler._sync_from; see passive_sync.go for the
// sub-methods.
func (r *Reconciler) syncFrom(
	rayNodes []*proto.NodeState,
	nonTerminatedCloudInstances map[string]autoscaler.CloudInstance,
	cloudProviderErrors []error,
	rayInstallErrors []error,
	rayStopErrors []error,
	asConfig *instance_manager.AutoscalingConfig,
) {
	// Handle cloud instance allocation: REQUESTED -> ALLOCATED /
	// ALLOCATION_FAILED.
	r.handleCloudInstanceAllocation(nonTerminatedCloudInstances, cloudProviderErrors)
	// Handle terminated cloud instances: * -> TERMINATED.
	r.handleCloudInstanceTerminated(nonTerminatedCloudInstances)
	// Handle cloud instance termination failures: TERMINATING ->
	// TERMINATION_FAILED.
	r.handleCloudInstanceTerminationErrors(cloudProviderErrors)
	// Handle unmanaged extra cloud instances: upsert ALLOCATED.
	r.handleExtraCloudInstances(nonTerminatedCloudInstances, rayNodes)
	// Handle Ray node status changes: * -> RAY_RUNNING/RAY_STOPPED/RAY_STOPPING.
	r.handleRayStatusTransition(rayNodes, asConfig)
	// Handle Ray installation failures: RAY_INSTALLING -> RAY_INSTALL_FAILED.
	r.handleRayInstallFailed(rayInstallErrors)
	// Handle Ray stop failures: RAY_STOP_REQUESTED -> RAY_RUNNING.
	r.handleRayStopFailed(rayStopErrors, rayNodes)
}

// stepNext proactively advances the instance statuses to the next step.
// Corresponds to Python Reconciler._step_next; see active_scaling.go for the
// sub-methods.
func (r *Reconciler) stepNext(
	autoscalingState *proto.AutoscalingState,
	rayClusterResourceState *proto.ClusterResourceState,
	nonTerminatedCloudInstances map[string]autoscaler.CloudInstance,
	asConfig *instance_manager.AutoscalingConfig,
) {
	// handleStuckInstances handles the stuck instances.
	r.handleStuckInstances(asConfig.GetInstanceReconcileConfig())
	// scaleCluster scales the cluster per the scheduler decisions.
	r.scaleCluster(autoscalingState, rayClusterResourceState, asConfig)

	r.handleInstancesLaunch(asConfig)

	r.terminateInstances()

	if !asConfig.DisableNodeUpdaters() {
		r.installRay(nonTerminatedCloudInstances)
	}

	r.fillAutoscalingState(autoscalingState)
}

// getImInstances gets all the instance manager instances and the storage
// version.
// Corresponds to Python Reconciler._get_im_instances.
func (r *Reconciler) getImInstances() ([]*proto.Instance, int64, error) {
	reply := r.instanceManager.GetInstanceManagerState(&proto.GetInstanceManagerStateRequest{})
	if reply.GetStatus().GetCode() != proto.StatusCode_OK {
		return nil, 0, fmt.Errorf("failed to get instance manager state: %s", reply.GetStatus().GetMessage())
	}
	return reply.GetState().GetInstances(), reply.GetState().GetVersion(), nil
}

// updateInstanceManager writes a batch of instance update events to the
// instance manager.
// Corresponds to Python Reconciler._update_instance_manager:
// in the single-writer (reconciler) scenario a version mismatch should not
// happen; failed updates are retried by the next reconcile round, so only the
// error is logged here.
func (r *Reconciler) updateInstanceManager(version int64, updates []*proto.InstanceUpdateEvent) {
	if len(updates) == 0 {
		return
	}

	reply := r.instanceManager.UpdateInstanceManagerState(&proto.UpdateInstanceManagerStateRequest{
		ExpectedVersion: version,
		Updates:         updates,
	})
	if reply.GetStatus().GetCode() != proto.StatusCode_OK {
		log.Log.Error(fmt.Errorf("failed to update instance manager: code=%s, message=%s",
			reply.GetStatus().GetCode().String(), reply.GetStatus().GetMessage()), "")
	}
}

// isHeadNodeRunning reports whether the head node is running and ready.
// Corresponds to Python Reconciler._is_head_node_running:
// scaling up before the head node is ready breaks the worker start, so wait.
func (r *Reconciler) isHeadNodeRunning() bool {
	imInstances, _, err := r.getImInstances()
	if err != nil {
		return false
	}

	for _, instance := range imInstances {
		if instance.NodeKind == proto.NodeKind_HEAD && instance.Status == proto.Instance_RAY_RUNNING {
			return true
		}
	}
	return false
}

// reportMetrics reports the instance metrics.
// Corresponds to Python Reconciler._report_metrics.
func (r *Reconciler) reportMetrics(asConfig *instance_manager.AutoscalingConfig) {
	if r.metricsReporter == nil {
		return
	}

	instances, _, err := r.getImInstances()
	if err != nil {
		return
	}
	nodeTypeConfigs := asConfig.GetNodeTypeConfigs()

	// Convert to the value types and the instance value slice the metrics
	// reporter needs.
	configs := make(map[string]instance_manager.NodeTypeConfig, len(nodeTypeConfigs))
	for nodeType, config := range nodeTypeConfigs {
		configs[nodeType] = *config
	}
	instanceValues := make([]proto.Instance, 0, len(instances))
	for _, instance := range instances {
		instanceValues = append(instanceValues, *instance)
	}

	if err := r.metricsReporter.ReportInstances(instanceValues, configs); err != nil {
		log.Log.Error(err, "Failed to report instances metrics.")
	}
	r.metricsReporter.ReportResources(instanceValues, configs)
}
