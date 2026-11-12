import logging
import math
import time
import uuid
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set, Tuple

from ray._common.utils import binary_to_hex
from ray.autoscaler.v2.instance_manager.cloud_providers.kuberay.cloud_provider import (
    KubeRayProvider,
)
from ray.autoscaler.v2.instance_manager.common import InstanceUtil
from ray.autoscaler.v2.instance_manager.config import (
    AutoscalingConfig,
    InstanceReconcileConfig,
    Provider,
    WorkerReplicaConfig,
)
from ray.autoscaler.v2.instance_manager.instance_manager import InstanceManager
from ray.autoscaler.v2.instance_manager.node_provider import (
    CloudInstance,
    CloudInstanceId,
    CloudInstanceProviderError,
    ICloudInstanceProvider,
    LaunchNodeError,
    TerminateNodeError,
)
from ray.autoscaler.v2.instance_manager.subscribers.cloud_resource_monitor import (
    CloudResourceMonitor,
)
from ray.autoscaler.v2.instance_manager.subscribers.ray_stopper import RayStopError
from ray.autoscaler.v2.instance_manager.subscribers.threaded_ray_installer import (
    RayInstallError,
)
from ray.autoscaler.v2.metrics_reporter import AutoscalerMetricsReporter
from ray.autoscaler.v2.scheduler import IResourceScheduler, SchedulingRequest
from ray.autoscaler.v2.schema import AutoscalerInstance, NodeType
from ray.autoscaler.v2.utils import is_head_node
from ray.core.generated.autoscaler_pb2 import (
    AutoscalingState,
    ClusterResourceState,
    FailedInstanceRequest,
    NodeState,
    NodeStatus,
    PendingInstance,
    PendingInstanceRequest,
)
from ray.core.generated.instance_manager_pb2 import (
    GetInstanceManagerStateRequest,
    Instance as IMInstance,
    InstanceUpdateEvent as IMInstanceUpdateEvent,
    NodeKind,
    StatusCode,
    TerminationRequest,
    UpdateInstanceManagerStateRequest,
)

logger = logging.getLogger(__name__)

# Exhaustive partition of every non-UNKNOWN IMInstance.InstanceStatus used by
# multi-host replica reconciliation. Every state belongs to exactly one bucket:
#   TERMINAL | UNALLOCATED | BOOTING | {RAY_RUNNING} | DRAINING | TEARDOWN.
_TERMINAL_INSTANCE_STATUSES = frozenset(
    {
        IMInstance.TERMINATED,
        IMInstance.ALLOCATION_FAILED,
    }
)
_UNALLOCATED_INSTANCE_STATUSES = frozenset(
    {
        IMInstance.QUEUED,
        IMInstance.REQUESTED,
    }
)
_BOOTING_INSTANCE_STATUSES = frozenset(
    {
        IMInstance.ALLOCATED,
        IMInstance.RAY_INSTALLING,
    }
)
_DRAINING_INSTANCE_STATUSES = frozenset(
    {
        IMInstance.RAY_STOP_REQUESTED,
        IMInstance.RAY_STOPPING,
    }
)
_TEARDOWN_INSTANCE_STATUSES = frozenset(
    {
        IMInstance.ALLOCATION_TIMEOUT,
        IMInstance.RAY_INSTALL_FAILED,
        IMInstance.RAY_STOPPED,
        IMInstance.TERMINATING,
        IMInstance.TERMINATION_FAILED,
    }
)
_ACTIVE_CID_STATUSES = (
    _BOOTING_INSTANCE_STATUSES
    | {IMInstance.RAY_RUNNING}
    | _DRAINING_INSTANCE_STATUSES
    | {IMInstance.TERMINATING}
)
_LIVE_CID_STATUSES = _BOOTING_INSTANCE_STATUSES | {IMInstance.RAY_RUNNING}
_OCCUPYING_CLOUD_STATUSES = _LIVE_CID_STATUSES | _DRAINING_INSTANCE_STATUSES


@dataclass
class _MultiHostReplicaView:
    """Canonical multi-host replica state projection shared across allocation,
    scaling clamps, partial-replica drain filtering, and instance termination."""

    active_cids: Set[CloudInstanceId] = field(default_factory=set)
    booting_cids: Set[CloudInstanceId] = field(default_factory=set)
    live_cids: Set[CloudInstanceId] = field(default_factory=set)
    assigned_reps: Set[Tuple[NodeType, str]] = field(default_factory=set)
    rep_assigned_cids: Dict[Tuple[NodeType, str], Set[str]] = field(
        default_factory=lambda: defaultdict(set)
    )
    rep_lids: Dict[Tuple[NodeType, str], Set[str]] = field(
        default_factory=lambda: defaultdict(set)
    )
    rep_bound_cids_by_lid: Dict[Tuple[Tuple[NodeType, str], str], Set[str]] = field(
        default_factory=lambda: defaultdict(set)
    )
    non_fillable_reps: Set[Tuple[NodeType, str]] = field(default_factory=set)
    rep_booting: Set[Tuple[NodeType, str]] = field(default_factory=set)
    rep_occupying: Dict[Tuple[NodeType, str], int] = field(
        default_factory=lambda: defaultdict(int)
    )
    draining_reps: Set[Tuple[NodeType, str]] = field(default_factory=set)
    ray_running_reps: Set[Tuple[NodeType, str]] = field(default_factory=set)
    claimed_reps_by_lid: Dict[Tuple[NodeType, str], Set[str]] = field(
        default_factory=lambda: defaultdict(set)
    )
    requested_by_lid: Dict[Tuple[NodeType, str], int] = field(
        default_factory=lambda: defaultdict(int)
    )
    in_flight_by_lid: Dict[Tuple[NodeType, str], int] = field(
        default_factory=lambda: defaultdict(int)
    )

    def owed_batch_slots(self, nt: NodeType, rid: str, lid: str, size: int) -> int:
        """Unallocated pods that batch `lid` still owes live replica `(nt, rid)`."""
        rk = (nt, rid)
        if not rid or not lid or rk in self.non_fillable_reps:
            return 0
        bound = self.rep_bound_cids_by_lid.get((rk, lid))
        if not bound:
            return 0
        return min(
            max(0, size - len(self.rep_assigned_cids[rk])),
            max(0, size - len(bound)),
        )


def _build_multihost_replica_view(
    im_instances: List[IMInstance],
    cloud_instances: Dict[CloudInstanceId, CloudInstance],
    replica_sizes: Dict[NodeType, int],
    status_overrides: Optional[Dict[str, "IMInstance.InstanceStatus"]] = None,
) -> _MultiHostReplicaView:
    """Project per-instance IM rows and cloud replica IDs into one canonical view."""
    view = _MultiHostReplicaView()
    overrides = status_overrides or {}

    for inst in im_instances:
        st = overrides.get(inst.instance_id, inst.status)
        if not inst.cloud_instance_id:
            continue
        if st in _ACTIVE_CID_STATUSES:
            view.active_cids.add(inst.cloud_instance_id)
        if st in _BOOTING_INSTANCE_STATUSES:
            view.booting_cids.add(inst.cloud_instance_id)
        if st in _LIVE_CID_STATUSES:
            view.live_cids.add(inst.cloud_instance_id)

    if not replica_sizes:
        return view

    for inst in im_instances:
        if inst.node_kind == NodeKind.HEAD or inst.instance_type not in replica_sizes:
            continue
        nt = inst.instance_type
        st = overrides.get(inst.instance_id, inst.status)
        lid = inst.launch_request_id

        if inst.cloud_instance_id:
            c_inst = cloud_instances.get(inst.cloud_instance_id)
            if c_inst is not None and c_inst.node_kind == NodeKind.HEAD:
                continue
            rid = (
                c_inst.replica_id if c_inst is not None and c_inst.replica_id else ""
            ) or inst.cloud_instance_replica_id
            if not rid:
                continue
            rk = (nt, rid)
            # Track every pod ever bound to `(rk, lid)` (even if later preempted
            # or raylet-restarted) so per-replica owed counts stay exact.
            view.assigned_reps.add(rk)
            if lid:
                view.claimed_reps_by_lid[(nt, lid)].add(rid)
                view.rep_bound_cids_by_lid[(rk, lid)].add(inst.cloud_instance_id)
                view.rep_lids[rk].add(lid)

            # Ignore stale RAY_STOPPED rows shadowed by an active IM row on the
            # same cloud instance (e.g. after a raylet restart).
            if (
                st == IMInstance.RAY_STOPPED
                and inst.cloud_instance_id in view.active_cids
            ):
                continue

            if st in _TERMINAL_INSTANCE_STATUSES:
                if any(
                    h.instance_status
                    in (
                        IMInstance.TERMINATING,
                        IMInstance.ALLOCATION_TIMEOUT,
                        IMInstance.RAY_INSTALL_FAILED,
                    )
                    for h in inst.status_history
                ):
                    view.non_fillable_reps.add(rk)
                continue

            if st in _DRAINING_INSTANCE_STATUSES | _TEARDOWN_INSTANCE_STATUSES:
                view.non_fillable_reps.add(rk)

            if c_inst is None:
                continue
            view.rep_assigned_cids[rk].add(inst.cloud_instance_id)
            if st in _BOOTING_INSTANCE_STATUSES:
                view.rep_booting.add(rk)
            if st in _OCCUPYING_CLOUD_STATUSES:
                view.rep_occupying[rk] += 1
            if st in _DRAINING_INSTANCE_STATUSES:
                view.draining_reps.add(rk)
                view.ray_running_reps.add(rk)
            elif st == IMInstance.RAY_RUNNING:
                view.ray_running_reps.add(rk)
        elif st == IMInstance.REQUESTED:
            view.requested_by_lid[(nt, lid or "")] += 1
            view.in_flight_by_lid[(nt, lid or "")] += 1
        elif st == IMInstance.QUEUED and lid:
            view.in_flight_by_lid[(nt, lid)] += 1

    return view


class Reconciler:
    """A singleton class that reconciles the instance states of the instance manager
    for autoscaler.

    """

    @staticmethod
    def reconcile(
        instance_manager: InstanceManager,
        scheduler: IResourceScheduler,
        cloud_provider: ICloudInstanceProvider,
        cloud_resource_monitor: CloudResourceMonitor,
        ray_cluster_resource_state: ClusterResourceState,
        non_terminated_cloud_instances: Dict[CloudInstanceId, CloudInstance],
        autoscaling_config: AutoscalingConfig,
        cloud_provider_errors: Optional[List[CloudInstanceProviderError]] = None,
        ray_install_errors: Optional[List[RayInstallError]] = None,
        ray_stop_errors: Optional[List[RayStopError]] = None,
        metrics_reporter: Optional[AutoscalerMetricsReporter] = None,
        _logger: Optional[logging.Logger] = None,
    ) -> AutoscalingState:
        """The reconcile method computes InstanceUpdateEvents for the instance manager
        by:

        1. Reconciling the instance manager's instances with external states like
        the cloud provider's, the ray cluster's states, the ray installer's results.
        It performs "passive" status transitions for the instances (where the status
        transition should only be reflecting the external states of the cloud provider
        and the ray cluster, and should not be actively changing them)

        2. Stepping the instances to the active states by computing instance status
        transitions that are needed and updating the instance manager's state.
        These transitions should be "active" where the transitions have side effects
        (through InstanceStatusSubscriber) to the cloud provider and the ray cluster.

        Args:
            instance_manager: The instance manager to reconcile.
            scheduler: The resource scheduler to make scaling decisions.
            cloud_provider: The cloud instance provider used to launch and
                terminate nodes.
            cloud_resource_monitor: The cloud resource monitor for monitoring
                resource availability of all node types.
            ray_cluster_resource_state: The ray cluster's resource state.
            non_terminated_cloud_instances: The non-terminated cloud instances
                from the cloud provider.
            autoscaling_config: The autoscaling config.
            cloud_provider_errors: The errors from the cloud provider.
            ray_install_errors: The errors from RayInstaller.
            ray_stop_errors: The errors from RayStopper.
            metrics_reporter: The metric reporter to report the autoscaler
                metrics.
            _logger: The logger (for testing).

        Returns:
            The updated autoscaling state after reconciling.
        """
        cloud_provider_errors = cloud_provider_errors or []
        ray_install_errors = ray_install_errors or []
        ray_stop_errors = ray_stop_errors or []

        autoscaling_state = AutoscalingState()
        autoscaling_state.last_seen_cluster_resource_state_version = (
            ray_cluster_resource_state.cluster_resource_state_version
        )

        Reconciler._sync_from(
            instance_manager=instance_manager,
            ray_nodes=ray_cluster_resource_state.node_states,
            non_terminated_cloud_instances=non_terminated_cloud_instances,
            cloud_provider_errors=cloud_provider_errors,
            ray_install_errors=ray_install_errors,
            ray_stop_errors=ray_stop_errors,
            autoscaling_config=autoscaling_config,
        )

        Reconciler._step_next(
            autoscaling_state=autoscaling_state,
            instance_manager=instance_manager,
            scheduler=scheduler,
            cloud_provider=cloud_provider,
            cloud_resource_monitor=cloud_resource_monitor,
            ray_cluster_resource_state=ray_cluster_resource_state,
            non_terminated_cloud_instances=non_terminated_cloud_instances,
            autoscaling_config=autoscaling_config,
            _logger=_logger,
        )

        Reconciler._report_metrics(
            instance_manager=instance_manager,
            autoscaling_config=autoscaling_config,
            metrics_reporter=metrics_reporter,
        )

        return autoscaling_state

    @staticmethod
    def _sync_from(
        instance_manager: InstanceManager,
        ray_nodes: List[NodeState],
        non_terminated_cloud_instances: Dict[CloudInstanceId, CloudInstance],
        cloud_provider_errors: List[CloudInstanceProviderError],
        ray_install_errors: List[RayInstallError],
        ray_stop_errors: List[RayStopError],
        autoscaling_config: AutoscalingConfig,
    ):
        """Reconcile the instance states of the instance manager from external states like
        the cloud provider's, the ray cluster's states, the ray installer's results,
        etc.

        For each instance, we try to figure out if we need to transition the instance
        status to a new status, and if so, what the new status should be.

        These transitions should be purely "passive", meaning they should only be
        reflecting the external states of the cloud provider and the ray cluster,
        and should not be actively changing the states of the cloud provider or the ray
        cluster.

        More specifically, we will reconcile status transitions for:
            1.  QUEUED/REQUESTED -> ALLOCATED:
                When an instance with launch request id (indicating a previous launch
                request was made) could be assigned to an unassigned cloud instance
                of the same instance type.
            2.  REQUESTED -> ALLOCATION_FAILED:
                When there's an error from the cloud provider for launch failure so
                that the instance becomes ALLOCATION_FAILED.
            3. ALLOCATED -> ALLOCATION_TIMEOUT:
                When an instance has been allocated to a cloud instance, but is stuck in
                this state. For example, a kubernetes pod remains pending due to
                insufficient resources.
            4.  * -> RAY_RUNNING:
                When a ray node on a cloud instance joins the ray cluster, we will
                transition the instance to RAY_RUNNING.
            5.  * -> TERMINATED:
                When the cloud instance is already terminated, we will transition the
                instance to TERMINATED.
            6.  TERMINATING -> TERMINATION_FAILED:
                When there's an error from the cloud provider for termination failure.
            7.  * -> RAY_STOPPED:
                When ray was stopped on the cloud instance, we will transition the
                instance to RAY_STOPPED.
            8.  * -> RAY_INSTALL_FAILED:
                When there's an error from RayInstaller.
            9. RAY_STOP_REQUESTED -> RAY_RUNNING:
                When requested to stop ray, but failed to stop/drain the ray node
                (e.g. idle termination drain rejected by the node).

        Args:
            instance_manager: The instance manager to reconcile.
            ray_nodes: The ray cluster's states of ray nodes.
            non_terminated_cloud_instances: The non-terminated cloud instances from
                the cloud provider.
            cloud_provider_errors: The errors from the cloud provider.
            ray_install_errors: The errors from RayInstaller.
            ray_stop_errors: The errors from RayStopper.
            autoscaling_config: The autoscaling config.
        """

        # Handle 1 & 2 for cloud instance allocation.
        Reconciler._handle_cloud_instance_allocation(
            instance_manager,
            non_terminated_cloud_instances,
            cloud_provider_errors,
            autoscaling_config=autoscaling_config,
        )
        Reconciler._handle_cloud_instance_terminated(
            instance_manager, non_terminated_cloud_instances
        )

        Reconciler._handle_cloud_instance_termination_errors(
            instance_manager, cloud_provider_errors
        )

        Reconciler._handle_extra_cloud_instances(
            instance_manager, non_terminated_cloud_instances, ray_nodes
        )

        Reconciler._handle_ray_status_transition(
            instance_manager, ray_nodes, autoscaling_config
        )

        Reconciler._handle_ray_install_failed(instance_manager, ray_install_errors)

        Reconciler._handle_ray_stop_failed(instance_manager, ray_stop_errors, ray_nodes)

    @staticmethod
    def _step_next(
        autoscaling_state: AutoscalingState,
        instance_manager: InstanceManager,
        scheduler: IResourceScheduler,
        cloud_provider: ICloudInstanceProvider,
        cloud_resource_monitor: CloudResourceMonitor,
        ray_cluster_resource_state: ClusterResourceState,
        non_terminated_cloud_instances: Dict[CloudInstanceId, CloudInstance],
        autoscaling_config: AutoscalingConfig,
        _logger: Optional[logging.Logger] = None,
    ):
        """Step the reconciler to the next state by computing instance status transitions
        that are needed and updating the instance manager's state.

        Specifically, we will:
            1. Shut down leak cloud instances
                Leaked cloud instances that are not managed by the instance manager.
            2. Terminating instances with ray stopped or ray install failure.
            3. Scale down the cluster:
              (* -> RAY_STOP_REQUESTED/TERMINATING)
                b. Extra cloud due to max nodes config.
                c. Cloud instances with outdated configs.
            4. Scale up the cluster:
              (new QUEUED)
                Create new instances based on the IResourceScheduler's decision for
                scaling up.
            5. Request cloud provider to launch new instances.
              (QUEUED -> REQUESTED)
            6. Install ray
              (ALLOCATED -> RAY_INSTALLING)
                When ray could be installed and launched.
            7. Reconcile IM instances whose ray nodes are missing from GCS.
            8. Handle any stuck instances with timeouts.

        Args:
            autoscaling_state: The autoscaling state populated by this reconcile loop.
            instance_manager: The instance manager to reconcile.
            scheduler: The resource scheduler to make scaling decisions.
            cloud_provider: The cloud instance provider used to launch and
                terminate nodes.
            cloud_resource_monitor: The cloud resource monitor for monitoring
                resource availability of all node types.
            ray_cluster_resource_state: The ray cluster's resource state.
            non_terminated_cloud_instances: The non-terminated cloud instances
                from the cloud provider.
            autoscaling_config: The autoscaling config.
            _logger: The logger (for testing).
        """

        Reconciler._handle_missing_ray_nodes(
            instance_manager=instance_manager,
            ray_nodes=ray_cluster_resource_state.node_states,
        )

        Reconciler._handle_stuck_instances(
            instance_manager=instance_manager,
            reconcile_config=autoscaling_config.get_instance_reconcile_config(),
            _logger=_logger or logger,
        )

        Reconciler._scale_cluster(
            autoscaling_state=autoscaling_state,
            instance_manager=instance_manager,
            cloud_resource_monitor=cloud_resource_monitor,
            ray_state=ray_cluster_resource_state,
            scheduler=scheduler,
            autoscaling_config=autoscaling_config,
            cloud_provider=cloud_provider,
            non_terminated_cloud_instances=non_terminated_cloud_instances,
            _logger=_logger or logger,
        )

        # Fix: Terminate instances before launching new ones
        # This ensures that when launching new instances, the replica count
        # does not include instances that are about to be terminated,
        # preventing the maxReplicas limit from being incorrectly triggered.
        Reconciler._terminate_instances(
            instance_manager=instance_manager,
            autoscaling_config=autoscaling_config,
            non_terminated_cloud_instances=non_terminated_cloud_instances,
        )

        Reconciler._handle_instances_launch(
            instance_manager=instance_manager, autoscaling_config=autoscaling_config
        )

        if not autoscaling_config.disable_node_updaters():
            Reconciler._install_ray(
                instance_manager=instance_manager,
                non_terminated_cloud_instances=non_terminated_cloud_instances,
            )

        Reconciler._fill_autoscaling_state(
            instance_manager=instance_manager,
            autoscaling_state=autoscaling_state,
            autoscaling_config=autoscaling_config,
        )

        # Reclaim long-TERMINATED instance records last, so this cycle's
        # _fill_autoscaling_state still reports instances terminated earlier in
        # the same cycle. TERMINATED is terminal and nothing else deletes these
        # records, so without this the storage scanned every cycle grows without
        # bound on churning clusters and the autoscaler eventually OOMs.
        Reconciler._gc_terminated_instances(
            instance_manager=instance_manager,
            reconcile_config=autoscaling_config.get_instance_reconcile_config(),
            ray_nodes=ray_cluster_resource_state.node_states,
        )

    #######################################################
    # Utility methods for reconciling instance states.
    #######################################################

    @staticmethod
    def _handle_cloud_instance_allocation(
        instance_manager: InstanceManager,
        non_terminated_cloud_instances: Dict[CloudInstanceId, CloudInstance],
        cloud_provider_errors: List[CloudInstanceProviderError],
        autoscaling_config: Optional[AutoscalingConfig] = None,
    ):
        im_instances, version = Reconciler._get_im_instances(instance_manager)
        updates = {}

        # Compute intermediate states.

        instances_with_launch_requests: List[IMInstance] = []
        for instance in im_instances:
            if instance.status != IMInstance.REQUESTED:
                continue

            assert (
                instance.launch_request_id
            ), "Instance in REQUESTED status should have launch_request_id set."
            instances_with_launch_requests.append(instance)

        assigned_cloud_instance_ids: Set[CloudInstanceId] = {
            instance.cloud_instance_id
            for instance in im_instances
            if instance.cloud_instance_id
            and instance.status
            not in [IMInstance.TERMINATED, IMInstance.ALLOCATION_FAILED]
        }
        replica_sizes = (
            Reconciler._get_multihost_replica_sizes(autoscaling_config)
            if autoscaling_config is not None
            else {}
        )
        # Preserve replica_id <-> launch_request_id affinity across concurrent
        # multi-host replica launches and prevent a batch from claiming a pod
        # belonging to an existing, draining, or tearing-down replica.
        replica_view = (
            _build_multihost_replica_view(
                im_instances, non_terminated_cloud_instances, replica_sizes
            )
            if replica_sizes
            else None
        )
        # A single launch request could launch multiple node types, so a launch
        # request id alone doesn't identify a launch error: key the errors by
        # (request id, node type) so that errors of different node types from the
        # same request don't overwrite each other.
        launch_errors: Dict[Tuple[str, NodeType], LaunchNodeError] = {
            (error.request_id, error.node_type): error
            for error in cloud_provider_errors
            if isinstance(error, LaunchNodeError)
        }
        unassigned_cloud_instances_by_type: Dict[
            str, List[CloudInstance]
        ] = defaultdict(list)

        for cloud_instance_id, cloud_instance in non_terminated_cloud_instances.items():
            if cloud_instance_id not in assigned_cloud_instance_ids:
                unassigned_cloud_instances_by_type[cloud_instance.node_type].append(
                    cloud_instance
                )

        # Sort the request instance by the increasing request time.
        instances_with_launch_requests.sort(
            key=lambda instance: InstanceUtil.get_status_transition_times_ns(
                instance, IMInstance.REQUESTED
            )
        )

        # Fail at most the summed LaunchNodeError.count instances per
        # (request id, type). NodeProviderAdapter can emit several batch
        # failures with the same key in one poll; do not keep only the last.
        remaining_launch_failures: Dict[Tuple[str, NodeType], int] = defaultdict(int)
        for error in cloud_provider_errors:
            if isinstance(error, LaunchNodeError):
                remaining_launch_failures[
                    (error.request_id, error.node_type)
                ] += error.count

        # For each instance, try to allocate or fail the allocation.
        for instance in instances_with_launch_requests:
            # Try allocate or fail with errors.
            update_event = Reconciler._try_resolve_pending_allocation(
                instance,
                unassigned_cloud_instances_by_type,
                launch_errors,
                remaining_launch_failures,
                replica_sizes=replica_sizes,
                replica_view=replica_view,
            )
            if not update_event:
                continue

            updates[instance.instance_id] = update_event

        # Update the instance manager for the events.
        Reconciler._update_instance_manager(instance_manager, version, updates)

    @staticmethod
    def _try_resolve_pending_allocation(
        im_instance: IMInstance,
        unassigned_cloud_instances_by_type: Dict[str, List[CloudInstance]],
        launch_errors: Dict[Tuple[str, NodeType], LaunchNodeError],
        remaining_launch_failures: Dict[Tuple[str, NodeType], int],
        replica_sizes: Optional[Dict[NodeType, int]] = None,
        replica_view: Optional[_MultiHostReplicaView] = None,
    ) -> Optional[IMInstanceUpdateEvent]:
        """Allocate, or fail the cloud instance allocation for the instance.

        Args:
            im_instance: The instance to allocate or fail.
            unassigned_cloud_instances_by_type: The unassigned cloud instances by type.
            launch_errors: The launch errors from the cloud provider, keyed by
                (launch request id, node type).
            remaining_launch_failures: Remaining number of instances to fail for
                each (launch request id, node type), from LaunchNodeError.count.
            replica_sizes: Multi-worker replica sizes by node type.
            replica_view: Canonical multi-host replica view updated in place.

        Returns:
            Instance update to ALLOCATED: if there's a matching unassigned cloud
                instance with the same type.
            Instance update to ALLOCATION_FAILED: if the instance allocation failed
                with errors.
            None: if there's no update.

        """
        unassigned_cloud_instance = None

        # Try to allocate an unassigned cloud instance.
        candidates = unassigned_cloud_instances_by_type.get(
            im_instance.instance_type, []
        )
        nt = im_instance.instance_type
        size = replica_sizes.get(nt, 1) if replica_sizes else 1
        lid = im_instance.launch_request_id
        if candidates:
            if size > 1 and replica_view is not None:
                match_idx = None
                # 1. Finish an incomplete live replica already bound to this batch.
                for idx in range(len(candidates) - 1, -1, -1):
                    rid = candidates[idx].replica_id
                    if rid and replica_view.owed_batch_slots(nt, rid, lid, size) > 0:
                        match_idx = idx
                        break
                # 2. Claim a fresh replica only if this batch still has at least a
                # full replica's worth of unclaimed, non-failing REQUESTED slots.
                if match_idx is None:
                    open_live_slots = sum(
                        replica_view.owed_batch_slots(nt, r, lid, size)
                        for r in replica_view.claimed_reps_by_lid[(nt, lid)]
                    )
                    avail_req = max(
                        0,
                        replica_view.requested_by_lid[(nt, lid)]
                        - remaining_launch_failures.get((lid, nt), 0),
                    )
                    if avail_req - open_live_slots >= size:
                        for idx in range(len(candidates) - 1, -1, -1):
                            rid = candidates[idx].replica_id
                            if rid and (nt, rid) not in replica_view.assigned_reps:
                                match_idx = idx
                                break
                # 3. Fallback for providers without replica_id metadata.
                if match_idx is None:
                    for idx in range(len(candidates) - 1, -1, -1):
                        if not candidates[idx].replica_id:
                            match_idx = idx
                            break
                if match_idx is not None:
                    unassigned_cloud_instance = candidates.pop(match_idx)
                    replica_view.requested_by_lid[(nt, lid)] = max(
                        0, replica_view.requested_by_lid[(nt, lid)] - 1
                    )
                    replica_view.in_flight_by_lid[(nt, lid)] = max(
                        0, replica_view.in_flight_by_lid[(nt, lid)] - 1
                    )
                    if unassigned_cloud_instance.replica_id:
                        rk = (nt, unassigned_cloud_instance.replica_id)
                        replica_view.assigned_reps.add(rk)
                        replica_view.rep_assigned_cids[rk].add(
                            unassigned_cloud_instance.cloud_instance_id
                        )
                        replica_view.rep_booting.add(rk)
                        if lid:
                            replica_view.rep_lids[rk].add(lid)
                            replica_view.rep_bound_cids_by_lid[(rk, lid)].add(
                                unassigned_cloud_instance.cloud_instance_id
                            )
                            replica_view.claimed_reps_by_lid[(nt, lid)].add(
                                unassigned_cloud_instance.replica_id
                            )
            else:
                unassigned_cloud_instance = candidates.pop()

        if unassigned_cloud_instance:
            event_kwargs = {}
            if unassigned_cloud_instance.replica_id:
                event_kwargs[
                    "cloud_instance_replica_id"
                ] = unassigned_cloud_instance.replica_id
            return IMInstanceUpdateEvent(
                instance_id=im_instance.instance_id,
                new_instance_status=IMInstance.ALLOCATED,
                cloud_instance_id=unassigned_cloud_instance.cloud_instance_id,
                node_kind=unassigned_cloud_instance.node_kind,
                instance_type=unassigned_cloud_instance.node_type,
                details=(
                    "allocated unassigned cloud instance "
                    f"{unassigned_cloud_instance.cloud_instance_id}"
                ),
                **event_kwargs,
            )

        # Fail at most `count` instances for this (request id, type). The rest
        # stay REQUESTED so a partial cap can still bind later pods.
        launch_error_key = (im_instance.launch_request_id, im_instance.instance_type)
        if remaining_launch_failures.get(launch_error_key, 0) > 0:
            remaining_launch_failures[launch_error_key] -= 1
            if replica_view is not None and size > 1:
                replica_view.requested_by_lid[(nt, lid)] = max(
                    0, replica_view.requested_by_lid[(nt, lid)] - 1
                )
                replica_view.in_flight_by_lid[(nt, lid)] = max(
                    0, replica_view.in_flight_by_lid[(nt, lid)] - 1
                )
            launch_error = launch_errors[launch_error_key]
            return IMInstanceUpdateEvent(
                instance_id=im_instance.instance_id,
                new_instance_status=IMInstance.ALLOCATION_FAILED,
                details=f"launch failed with {str(launch_error)}",
            )
        # No update.
        return None

    @staticmethod
    def _handle_ray_stop_failed(
        instance_manager: InstanceManager,
        ray_stop_errors: List[RayStopError],
        ray_nodes: List[NodeState],
    ):
        """The instance requested to stop ray, but failed to stop/drain the ray node.
        E.g. connection errors, idle termination drain rejected by the node.

        We will transition the instance back to RAY_RUNNING.

        Args:
            instance_manager: The instance manager to reconcile.
            ray_stop_errors: The errors from RayStopper.
            ray_nodes: The ray cluster's states of ray nodes.
        """
        instances, version = Reconciler._get_im_instances(instance_manager)
        updates = {}

        ray_stop_errors_by_instance_id = {
            error.im_instance_id: error for error in ray_stop_errors
        }

        ray_nodes_by_ray_node_id = {binary_to_hex(n.node_id): n for n in ray_nodes}

        ray_stop_requested_instances = {
            instance.instance_id: instance
            for instance in instances
            if instance.status == IMInstance.RAY_STOP_REQUESTED
        }

        for instance_id, instance in ray_stop_requested_instances.items():
            stop_error = ray_stop_errors_by_instance_id.get(instance_id)
            if not stop_error:
                continue

            assert instance.node_id
            ray_node = ray_nodes_by_ray_node_id.get(instance.node_id)
            assert ray_node is not None and ray_node.status in [
                NodeStatus.RUNNING,
                NodeStatus.IDLE,
            ], (
                "There should be a running ray node for instance with ray stop "
                "requested failed."
            )

            updates[instance_id] = IMInstanceUpdateEvent(
                instance_id=instance_id,
                new_instance_status=IMInstance.RAY_RUNNING,
                details="failed to stop/drain ray",
                ray_node_id=instance.node_id,
            )

        Reconciler._update_instance_manager(instance_manager, version, updates)

    @staticmethod
    def _handle_ray_install_failed(
        instance_manager: InstanceManager, ray_install_errors: List[RayInstallError]
    ):

        instances, version = Reconciler._get_im_instances(instance_manager)
        updates = {}

        # Get all instances with RAY_INSTALLING status.
        instances_with_ray_installing = {
            instance.instance_id: instance
            for instance in instances
            if instance.status == IMInstance.RAY_INSTALLING
        }

        install_errors = {error.im_instance_id: error for error in ray_install_errors}

        # For each instance with RAY_INSTALLING status, check if there's any
        # install error.
        for instance_id, instance in instances_with_ray_installing.items():
            install_error = install_errors.get(instance_id)
            if install_error:
                updates[instance_id] = IMInstanceUpdateEvent(
                    instance_id=instance_id,
                    new_instance_status=IMInstance.RAY_INSTALL_FAILED,
                    details=(
                        f"failed to install ray with errors: {install_error.details}"
                    ),
                )

        # Update the instance manager for the events.
        Reconciler._update_instance_manager(instance_manager, version, updates)

    @staticmethod
    def _handle_cloud_instance_terminated(
        instance_manager: InstanceManager,
        non_terminated_cloud_instances: Dict[CloudInstanceId, CloudInstance],
    ):
        """For any IM (instance manager) instance with a cloud node id, if the mapped
        cloud instance is no longer running, transition the instance to TERMINATED.

        Args:
            instance_manager: The instance manager to reconcile.
            non_terminated_cloud_instances: The non-terminated cloud instances from
                the cloud provider.
        """
        updates = {}
        instances, version = Reconciler._get_im_instances(instance_manager)

        non_terminated_instances_with_cloud_instance_assigned = {
            instance.cloud_instance_id: instance
            for instance in instances
            if instance.cloud_instance_id and instance.status != IMInstance.TERMINATED
        }

        for (
            cloud_instance_id,
            instance,
        ) in non_terminated_instances_with_cloud_instance_assigned.items():
            if cloud_instance_id in non_terminated_cloud_instances.keys():
                # The cloud instance is still running.
                continue

            # The cloud instance is terminated.
            updates[instance.instance_id] = IMInstanceUpdateEvent(
                instance_id=instance.instance_id,
                new_instance_status=IMInstance.TERMINATED,
                cloud_instance_id=cloud_instance_id,
                details=f"cloud instance {cloud_instance_id} no longer found",
            )

        Reconciler._update_instance_manager(instance_manager, version, updates)

    @staticmethod
    def _handle_cloud_instance_termination_errors(
        instance_manager: InstanceManager,
        cloud_provider_errors: List[CloudInstanceProviderError],
    ):
        """If any TERMINATING instances have termination errors, transition the instance to
        TERMINATION_FAILED.

        We will retry the termination for the TERMINATION_FAILED instances in the next
        reconciler step.

        Args:
            instance_manager: The instance manager to reconcile.
            cloud_provider_errors: The errors from the cloud provider.

        """
        instances, version = Reconciler._get_im_instances(instance_manager)
        updates = {}

        termination_errors = {
            error.cloud_instance_id: error
            for error in cloud_provider_errors
            if isinstance(error, TerminateNodeError)
        }

        terminating_instances_by_cloud_instance_id = {
            instance.cloud_instance_id: instance
            for instance in instances
            if instance.status == IMInstance.TERMINATING
        }

        for cloud_instance_id, failure in termination_errors.items():
            instance = terminating_instances_by_cloud_instance_id.get(cloud_instance_id)
            if not instance:
                # The instance is no longer in TERMINATING status.
                continue

            updates[instance.instance_id] = IMInstanceUpdateEvent(
                instance_id=instance.instance_id,
                new_instance_status=IMInstance.TERMINATION_FAILED,
                details=f"termination failed: {str(failure)}",
            )

        Reconciler._update_instance_manager(instance_manager, version, updates)

    @staticmethod
    def _get_im_instances(
        instance_manager: InstanceManager,
    ) -> Tuple[List[IMInstance], int]:
        reply = instance_manager.get_instance_manager_state(
            request=GetInstanceManagerStateRequest()
        )
        assert reply.status.code == StatusCode.OK
        im_state = reply.state
        return im_state.instances, im_state.version

    @staticmethod
    def _update_instance_manager(
        instance_manager: InstanceManager,
        version: int,
        updates: Dict[str, IMInstanceUpdateEvent],
    ) -> None:
        if not updates:
            return

        updates = list(updates.values()) or []

        reply = instance_manager.update_instance_manager_state(
            request=UpdateInstanceManagerStateRequest(
                expected_version=version,
                updates=updates,
            )
        )
        # TODO: While it's possible that a version mismatch
        # happens, or some other failures could happen. But given
        # the current implementation:
        #   1. There's only 1 writer (the reconciler) for updating the instance
        #       manager states, so there shouldn't be version mismatch.
        #   2. Any failures in one reconciler step should be caught at a higher
        #       level and be retried in the next reconciler step. If the IM
        #       fails to be updated, we don't have sufficient info to handle it
        #       here.
        assert (
            reply.status.code == StatusCode.OK
        ), f"Failed to update instance manager: {reply}"

    @staticmethod
    def _gc_terminated_instances(
        instance_manager: InstanceManager,
        reconcile_config: InstanceReconcileConfig,
        ray_nodes: List[NodeState],
    ) -> None:
        """Delete terminal instance records retained past the GC window.

        TERMINATED and ALLOCATION_FAILED are both terminal statuses (no outgoing
        transitions) and no other reconcile step removes these records, so the
        instance storage—read in full every cycle and serialized into
        AutoscalingState—otherwise grows without bound on instance-churning
        clusters until the autoscaler OOMs. Records are kept for
        ``terminated_instance_gc_retention_s`` after they enter their terminal
        status so a just-terminated instance stays reportable for at least one
        more cycle before being reclaimed.

        A record whose ``node_id`` is still present in the GCS node snapshot is
        kept regardless of age: its ``node_id`` is what makes
        ``_handle_extra_cloud_instances_from_ray_nodes`` treat the ray node as
        managed, so deleting it while GCS still references the node would
        resurrect it as a spurious ALLOCATED instance. (Only TERMINATED records
        that once reached ALLOCATED carry a node_id; ALLOCATION_FAILED is
        reached from REQUESTED and never has one.)
        """
        retention_s = reconcile_config.terminated_instance_gc_retention_s
        if retention_s < 0:
            # Negative retention disables GC (escape hatch for debugging).
            return

        terminal_statuses = (IMInstance.TERMINATED, IMInstance.ALLOCATION_FAILED)
        instances, version = Reconciler._get_im_instances(instance_manager)
        live_ray_node_ids = {binary_to_hex(n.node_id) for n in ray_nodes}
        # Retain terminal replica records whose batch still has unallocated
        # instances so multi-host replica accounting preserves prior bindings.
        active_lids = {
            instance.launch_request_id
            for instance in instances
            if instance.launch_request_id
            and instance.status in _UNALLOCATED_INSTANCE_STATUSES
        }
        now_ns = time.time_ns()
        retention_ns = retention_s * 1_000_000_000
        to_delete: List[str] = []
        for instance in instances:
            if instance.status not in terminal_statuses:
                continue
            # Keep records GCS still references, else they get resurrected as
            # ALLOCATED by _handle_extra_cloud_instances_from_ray_nodes.
            if instance.node_id and instance.node_id in live_ray_node_ids:
                continue
            if (
                instance.cloud_instance_replica_id
                and instance.launch_request_id
                and instance.launch_request_id in active_lids
            ):
                continue
            terminal_times = InstanceUtil.get_status_transition_times_ns(
                instance, instance.status
            )
            if not terminal_times:
                continue
            if now_ns - max(terminal_times) >= retention_ns:
                to_delete.append(instance.instance_id)

        if not to_delete:
            return

        success, _ = instance_manager.delete_instances(
            instance_ids=to_delete,
            expected_storage_version=version,
        )
        # A version mismatch means another write landed first; the records are
        # still terminal and will be retried next cycle, so this is not fatal.
        if not success:
            logger.debug(
                "Deferred GC of %d terminal instances due to storage version "
                "mismatch; will retry next reconcile.",
                len(to_delete),
            )

    @staticmethod
    def _handle_ray_status_transition(
        instance_manager: InstanceManager,
        ray_nodes: List[NodeState],
        autoscaling_config: AutoscalingConfig,
    ):
        """Handle the ray status transition for the instance manager.

        If a new ray node running on the instance, transition it to RAY_RUNNING.
        If a ray node stopped, transition it to RAY_STOPPED.
        If a ray node is draining, transition it to RAY_STOPPING.

        Args:
            instance_manager: The instance manager to reconcile.
            ray_nodes: The ray cluster's states of ray nodes.
            autoscaling_config: The autoscaling config.
        """
        instances, version = Reconciler._get_im_instances(instance_manager)
        updates = {}

        im_instances_by_cloud_instance_id = {
            instance.cloud_instance_id: instance
            for instance in instances
            if instance.cloud_instance_id
            and instance.status
            not in [IMInstance.TERMINATED, IMInstance.ALLOCATION_FAILED]
        }
        im_instances_by_ray_node_id = {
            instance.node_id: instance for instance in instances if instance.node_id
        }

        for ray_node in ray_nodes:
            im_instance = None
            ray_node_id = binary_to_hex(ray_node.node_id)
            if ray_node_id in im_instances_by_ray_node_id:
                im_instance = im_instances_by_ray_node_id[ray_node_id]
            else:
                if autoscaling_config.provider == Provider.READ_ONLY:
                    # We will use the node id as the cloud instance id for read-only
                    # provider.
                    im_instance = im_instances_by_cloud_instance_id[ray_node_id]
                elif ray_node.instance_id:
                    im_instance = im_instances_by_cloud_instance_id[
                        ray_node.instance_id
                    ]
                else:
                    # This should only happen to a ray node that's not managed by us.
                    logger.warning(
                        f"Ray node {ray_node_id} has no instance id. "
                        "This only happens to a ray node not managed by autoscaler. "
                        "If not, please file a bug at "
                        "https://github.com/ray-project/ray"
                    )
                    continue

            assert im_instance is not None, (
                f"Ray node {ray_node_id} has no matching "
                f"instance with cloud instance id={ray_node.instance_id}. We should "
                "not see a ray node with cloud instance id not found in IM since "
                "we have reconciled all cloud instances, and ray nodes by now."
            )

            reconciled_im_status = Reconciler._reconciled_im_status_from_ray_status(
                ray_node.status, im_instance.status
            )

            if reconciled_im_status != im_instance.status:
                updates[ray_node_id] = IMInstanceUpdateEvent(
                    instance_id=im_instance.instance_id,
                    new_instance_status=reconciled_im_status,
                    details=(
                        f"ray node {ray_node_id} is "
                        f"{NodeStatus.Name(ray_node.status)}"
                    ),
                    ray_node_id=ray_node_id,
                    instance_type=im_instance.instance_type,
                )

        Reconciler._update_instance_manager(instance_manager, version, updates)

    @staticmethod
    def _reconciled_im_status_from_ray_status(
        ray_status: NodeStatus, cur_im_status: IMInstance.InstanceStatus
    ) -> "IMInstance.InstanceStatus":
        """Reconcile the instance status from the ray node status.
        Args:
            ray_status: the current ray node status.
            cur_im_status: the current IM instance status.
        Returns:
            The reconciled IM instance status

        Raises:
            ValueError: If the ray status is unknown.
        """
        reconciled_im_status = None
        if ray_status in [NodeStatus.RUNNING, NodeStatus.IDLE]:
            reconciled_im_status = IMInstance.RAY_RUNNING
        elif ray_status == NodeStatus.DEAD:
            reconciled_im_status = IMInstance.RAY_STOPPED
        elif ray_status == NodeStatus.DRAINING:
            reconciled_im_status = IMInstance.RAY_STOPPING
        else:
            raise ValueError(f"Unknown ray status: {ray_status}")

        if (
            cur_im_status == reconciled_im_status
            or cur_im_status
            in InstanceUtil.get_reachable_statuses(reconciled_im_status)
        ):
            # No need to reconcile if the instance is already in the reconciled status
            # or has already transitioned beyond it.
            return cur_im_status

        return reconciled_im_status

    @staticmethod
    def _handle_instances_launch(
        instance_manager: InstanceManager, autoscaling_config: AutoscalingConfig
    ):

        instances, version = Reconciler._get_im_instances(instance_manager)

        queued_instances = []
        requested_instances = []
        running_instances = []

        for instance in instances:
            if instance.status == IMInstance.QUEUED:
                queued_instances.append(instance)
            elif instance.status == IMInstance.REQUESTED:
                requested_instances.append(instance)
            elif instance.status == IMInstance.RAY_RUNNING:
                running_instances.append(instance)

        if not queued_instances:
            # No QUEUED instances
            return

        to_launch = Reconciler._compute_to_launch(
            queued_instances,
            requested_instances,
            running_instances,
            autoscaling_config.get_upscaling_speed(),
            autoscaling_config.get_max_concurrent_launches(),
            replica_sizes=Reconciler._get_multihost_replica_sizes(autoscaling_config),
        )

        # Transition the instances to REQUESTED for instance launcher to
        # launch them.
        updates = {}
        new_launch_request_id = str(uuid.uuid4())
        for instance_type, instances in to_launch.items():
            for instance in instances:
                # Reuse launch request id for any QUEUED instances that have been
                # requested before due to retry.
                launch_request_id = (
                    new_launch_request_id
                    if len(instance.launch_request_id) == 0
                    else instance.launch_request_id
                )
                updates[instance.instance_id] = IMInstanceUpdateEvent(
                    instance_id=instance.instance_id,
                    new_instance_status=IMInstance.REQUESTED,
                    launch_request_id=launch_request_id,
                    instance_type=instance_type,
                    details=(
                        f"requested to launch {instance_type} with request id "
                        f"{launch_request_id}"
                    ),
                )

        Reconciler._update_instance_manager(instance_manager, version, updates)

    @staticmethod
    def _compute_to_launch(
        queued_instances: List[IMInstance],
        requested_instances: List[IMInstance],
        running_instances: List[IMInstance],
        upscaling_speed: float,
        max_concurrent_launches: int,
        replica_sizes: Optional[Dict[NodeType, int]] = None,
    ) -> Dict[NodeType, List[IMInstance]]:
        def _group_by_type(instances):
            instances_by_type = defaultdict(list)
            for instance in instances:
                instances_by_type[instance.instance_type].append(instance)
            return instances_by_type

        def _queued_times(instance: IMInstance) -> List[int]:
            return sorted(
                InstanceUtil.get_status_transition_times_ns(instance, IMInstance.QUEUED)
            )

        queued_instances_by_type = {
            nt: sorted(insts, key=lambda i: (_queued_times(i), i.instance_id))
            for nt, insts in _group_by_type(queued_instances).items()
        }
        requested_instances_by_type = _group_by_type(requested_instances)
        running_instances_by_type = _group_by_type(running_instances)

        total_num_requested_to_launch = len(requested_instances)
        all_to_launch: Dict[NodeType, List[IMInstance]] = defaultdict(list)

        for instance_type in sorted(
            queued_instances_by_type,
            key=lambda nt: (
                _queued_times(queued_instances_by_type[nt][0]),
                -(replica_sizes or {}).get(nt, 1),
                nt,
            ),
        ):
            queued_instances_for_type = queued_instances_by_type[instance_type]
            running_instances_for_type = running_instances_by_type.get(
                instance_type, []
            )
            size = (replica_sizes or {}).get(instance_type, 1)

            # Enforce the max allowed pending nodes based on current running nodes
            num_desired_to_upscale = max(
                1,
                math.ceil(upscaling_speed * max(len(running_instances_for_type), 1)),
            )
            # Enforce global limit, at most we can launch `max_concurrent_launches`
            available = max_concurrent_launches - total_num_requested_to_launch
            if size > 1:
                num_desired_to_upscale = max(
                    size, ((num_desired_to_upscale + size - 1) // size) * size
                )
                retried = [i for i in queued_instances_for_type if i.launch_request_id]
                fresh = [
                    i for i in queued_instances_for_type if not i.launch_request_id
                ]
                retried_by_lid: Dict[str, List[IMInstance]] = defaultdict(list)
                for inst in retried:
                    retried_by_lid[inst.launch_request_id].append(inst)
                if available > 0 and not requested_instances_by_type.get(instance_type):
                    # Allow one atomic multi-host replica when launch capacity is open
                    # and this group has no in-flight REQUESTED instances, preventing
                    # smaller groups from starving larger multi-host replicas.
                    first_retried = (
                        len(next(iter(retried_by_lid.values())))
                        if retried_by_lid
                        else 0
                    )
                    available = max(available, first_retried + (size if fresh else 0))
                selected_retried: List[IMInstance] = []
                retried_budget = max(0, min(available, num_desired_to_upscale))
                for group in retried_by_lid.values():
                    if len(group) <= retried_budget - len(selected_retried) or (
                        not selected_retried and len(group) <= available
                    ):
                        selected_retried.extend(group)
                rem_avail = max(0, available - len(selected_retried))
                fresh_to_launch = min(
                    len(fresh), max(0, min(rem_avail, num_desired_to_upscale))
                )
                fresh_to_launch -= fresh_to_launch % size
                selected = selected_retried + fresh[:fresh_to_launch]
                if selected:
                    all_to_launch[instance_type].extend(selected)
                    total_num_requested_to_launch += len(selected)
                continue

            # Cap both ends 0 <= num_to_launch <= num_queued
            num_to_launch = min(
                len(queued_instances_for_type),
                max(0, min(available, num_desired_to_upscale)),
            )
            if num_to_launch > 0:
                all_to_launch[instance_type].extend(
                    queued_instances_for_type[:num_to_launch]
                )
                total_num_requested_to_launch += num_to_launch

        return all_to_launch

    @staticmethod
    def _get_multihost_replica_sizes(
        autoscaling_config: Optional[AutoscalingConfig],
    ) -> Dict[NodeType, int]:
        if autoscaling_config is None:
            return {}
        node_type_configs = autoscaling_config.get_node_type_configs() or {}
        return {
            node_type: cfg.replica_config.workers_per_replica
            for node_type, cfg in node_type_configs.items()
            if isinstance(getattr(cfg, "replica_config", None), WorkerReplicaConfig)
            and cfg.replica_config.workers_per_replica > 1
        }

    @staticmethod
    def _handle_missing_ray_nodes(
        instance_manager: InstanceManager,
        ray_nodes: List[NodeState],
    ) -> None:
        """Reconcile IM instances that still point at ray nodes missing from GCS.

        GCS is the source of truth for Ray node liveness. A worker instance in
        RAY_RUNNING / RAY_STOP_REQUESTED / RAY_STOPPING with a node_id should
        have a corresponding entry in the GCS node snapshot. If the node is
        absent entirely, GCS has either GC'd its DEAD entry or otherwise no
        longer considers the ray node alive. Treat this the same as a stopped ray
        node so the instance can continue through the existing RAY_STOPPED ->
        TERMINATING path instead of lingering as RAY_RUNNING with a stale
        node_id.

        We intentionally skip the head node here. During head startup/restart,
        GCS can be reachable while the head raylet is not present in
        ClusterResourceState yet; head-node recovery is outside this worker
        cleanup path.

        Args:
            instance_manager: The instance manager to reconcile.
            ray_nodes: The ray cluster's view of ray nodes from GCS.
        """
        statuses_requiring_ray_node = {
            IMInstance.RAY_RUNNING,
            IMInstance.RAY_STOP_REQUESTED,
            IMInstance.RAY_STOPPING,
        }
        im_instances, version = Reconciler._get_im_instances(instance_manager)
        candidates = [
            instance
            for instance in im_instances
            if instance.status in statuses_requiring_ray_node
            and instance.node_kind != NodeKind.HEAD
            and instance.node_id
        ]
        if not candidates:
            return

        ray_node_ids = {binary_to_hex(n.node_id) for n in ray_nodes}

        updates = {}
        for instance in candidates:
            if instance.node_id in ray_node_ids:
                continue

            updates[instance.instance_id] = IMInstanceUpdateEvent(
                instance_id=instance.instance_id,
                new_instance_status=IMInstance.RAY_STOPPED,
                details=f"ray node {instance.node_id} no longer found in GCS",
                ray_node_id=instance.node_id,
                instance_type=instance.instance_type,
            )

        Reconciler._update_instance_manager(instance_manager, version, updates)

    @staticmethod
    def _handle_stuck_instances(
        instance_manager: InstanceManager,
        reconcile_config: InstanceReconcileConfig,
        _logger: logging.Logger,
    ):
        """Handle stuck instances with timeouts.

        Instances could be stuck in the following status and needs to be updated:
            - REQUESTED: cloud provider is slow/fails to launch instances.
            - ALLOCATED: ray fails to be started on the instance.
            - RAY_INSTALLING: ray fails to be installed on the instance.
            - TERMINATING: cloud provider is slow/fails to terminate instances.

        Instances could be in the following status which could be unbounded or
        transient, and we don't have a timeout mechanism to handle them. We would
        warn if they are stuck for too long:
            - RAY_STOPPING: ray taking time to drain.
            - QUEUED: cloud provider is slow to launch instances, resulting in long
                queue.

            Reconciler should handle below statuses, if not, could be slow
                reconcilation loop or a bug:
            - RAY_INSTALL_FAILED
            - RAY_STOPPED
            - TERMINATION_FAILED


        Args:
            instance_manager: The instance manager to reconcile.
            reconcile_config: The instance reconcile config.
            _logger: The logger to log the warning messages. It's used for testing.

        """
        instances, version = Reconciler._get_im_instances(instance_manager)

        instances_by_status = defaultdict(list)
        for instance in instances:
            instances_by_status[instance.status].append(instance)

        im_updates = {}

        # Fail or retry the cloud instance allocation if it's stuck
        # in the REQUESTED state.
        for instance in instances_by_status[IMInstance.REQUESTED]:
            update = Reconciler._handle_stuck_requested_instance(
                instance,
                reconcile_config.request_status_timeout_s,
                reconcile_config.max_num_retry_request_to_allocate,
            )
            if update:
                im_updates[instance.instance_id] = update

        # Leaked ALLOCATED instances should be terminated.
        # This usually happens when ray fails to be started on the instance, so
        # it's unable to be RAY_RUNNING after a long time.
        for instance in instances_by_status[IMInstance.ALLOCATED]:
            assert (
                instance.cloud_instance_id
            ), "cloud instance id should be set on ALLOCATED instance"
            update = Reconciler._handle_stuck_instance(
                instance,
                reconcile_config.allocate_status_timeout_s,
                new_status=IMInstance.ALLOCATION_TIMEOUT,
                cloud_instance_id=instance.cloud_instance_id,
                instance_type=instance.instance_type,
            )
            if update:
                im_updates[instance.instance_id] = update

        # Fail the installation if it's stuck in RAY_INSTALLING for too long.
        # If RAY_INSTALLING is stuck for too long, it's likely that the instance
        # is not able to install ray, so we should also fail the installation.
        for instance in instances_by_status[IMInstance.RAY_INSTALLING]:
            update = Reconciler._handle_stuck_instance(
                instance,
                reconcile_config.ray_install_status_timeout_s,
                new_status=IMInstance.RAY_INSTALL_FAILED,
            )
            if update:
                im_updates[instance.instance_id] = update

        # If we tried to terminate the instance, but it doesn't terminate (disappear
        # from the cloud provider) after a long time, we fail the termination.
        # This will trigger another attempt to terminate the instance.
        for instance in instances_by_status[IMInstance.TERMINATING]:
            update = Reconciler._handle_stuck_instance(
                instance,
                reconcile_config.terminating_status_timeout_s,
                new_status=IMInstance.TERMINATION_FAILED,
            )
            if update:
                im_updates[instance.instance_id] = update

        # If we tried to stop ray on the instance, but it doesn't stop after a long
        # time, we will transition it back to RAY_RUNNING as the stop/drain somehow
        # failed. Instances whose ray node has already disappeared from GCS are moved
        # to RAY_STOPPED earlier by _handle_missing_ray_nodes, so anything still in
        # RAY_STOP_REQUESTED here genuinely failed to drain.
        for instance in instances_by_status[IMInstance.RAY_STOP_REQUESTED]:
            update = Reconciler._handle_stuck_instance(
                instance,
                reconcile_config.ray_stop_requested_status_timeout_s,
                new_status=IMInstance.RAY_RUNNING,
                ray_node_id=instance.node_id,
            )
            if update:
                im_updates[instance.instance_id] = update

        # These statues could be unbounded or transient, and we don't have a timeout
        # mechanism to handle them. We only warn if they are stuck for too long.
        for status in [
            # Ray taking time to drain. We could also have a timeout when Drain protocol
            # supports timeout.
            IMInstance.RAY_STOPPING,
            # These should just be transient, we will terminate instances with this
            # status in the next reconciler step.
            IMInstance.RAY_INSTALL_FAILED,
            IMInstance.RAY_STOPPED,
            IMInstance.TERMINATION_FAILED,
            # Instances could be in the QUEUED status for a long time if the cloud
            # provider is slow to launch instances.
            IMInstance.QUEUED,
        ]:
            Reconciler._warn_stuck_instances(
                instances_by_status[status],
                status=status,
                warn_interval_s=reconcile_config.transient_status_warn_interval_s,
                logger=_logger,
            )

        Reconciler._update_instance_manager(instance_manager, version, im_updates)

    @staticmethod
    def _warn_stuck_instances(
        instances: List[IMInstance],
        status: IMInstance.InstanceStatus,
        warn_interval_s: int,
        logger: logging.Logger,
    ):
        """Warn if any instance is stuck in a transient/unbounded status for too
        long.
        """
        for instance in instances:
            status_times_ns = InstanceUtil.get_status_transition_times_ns(
                instance, select_instance_status=status
            )
            assert len(status_times_ns) >= 1
            status_time_ns = sorted(status_times_ns)[-1]

            if time.time_ns() - status_time_ns > warn_interval_s * 1e9:
                logger.warning(
                    "Instance {}({}) is stuck in {} for {} seconds.".format(
                        instance.instance_id,
                        IMInstance.InstanceStatus.Name(instance.status),
                        IMInstance.InstanceStatus.Name(status),
                        (time.time_ns() - status_time_ns) // 1e9,
                    )
                )

    @staticmethod
    def _is_head_node_running(instance_manager: InstanceManager) -> bool:
        """Check if the head node is running and ready.

        If we scale up the cluster before head node is running,
        it would cause issues when launching the worker nodes.

        There are corner cases when the GCS is up (so the ray cluster resource
        state is retrievable from the GCS), but the head node's raylet is not
        running so the head node is missing from the reported nodes. This happens
        when the head node is still starting up, or the raylet is not running
        due to some issues, and this would yield false.

        Args:
            instance_manager: The instance manager to reconcile.

        Returns:
            True if the head node is running and ready, False otherwise.
        """

        im_instances, _ = Reconciler._get_im_instances(instance_manager)

        for instance in im_instances:
            if instance.node_kind == NodeKind.HEAD:
                if instance.status == IMInstance.RAY_RUNNING:
                    return True
        return False

    @staticmethod
    def _scale_cluster(
        autoscaling_state: AutoscalingState,
        instance_manager: InstanceManager,
        cloud_resource_monitor: CloudResourceMonitor,
        ray_state: ClusterResourceState,
        scheduler: IResourceScheduler,
        autoscaling_config: AutoscalingConfig,
        cloud_provider: ICloudInstanceProvider,
        non_terminated_cloud_instances: Optional[
            Dict[CloudInstanceId, CloudInstance]
        ] = None,
        _logger: logging.Logger = logger,
    ) -> None:
        """Scale the cluster based on the resource state and the resource scheduler's
        decision:

        - It launches new instances if needed.
        - It terminates extra ray nodes if they should be shut down (preemption
            or idle termination)

        Args:
            autoscaling_state: The autoscaling state to reconcile.
            instance_manager: The instance manager to reconcile.
            cloud_resource_monitor: The cloud resource monitor for monitoring resource
                availability of all node types.
            ray_state: The ray cluster's resource state.
            scheduler: The resource scheduler to make scaling decisions.
            autoscaling_config: The autoscaling config.
            cloud_provider: The cloud provider interface.
            non_terminated_cloud_instances: Optional non-terminated cloud instances
                providing provider replica group IDs.
            _logger: Logger for scale warnings (injectable for tests).
        """

        # Get the current instance states.
        im_instances, version = Reconciler._get_im_instances(instance_manager)

        im_instances_by_instance_id = {
            i.instance_id: i for i in im_instances if i.instance_id
        }
        autoscaler_instances = []
        ray_nodes_by_id = {
            binary_to_hex(node.node_id): node for node in ray_state.node_states
        }
        cloud_instances = non_terminated_cloud_instances or {}

        for im_instance in im_instances:
            ray_node = ray_nodes_by_id.get(im_instance.node_id)
            cid = im_instance.cloud_instance_id or None
            c_inst = cloud_instances.get(cid) if cid else None
            autoscaler_instances.append(
                AutoscalerInstance(
                    ray_node=ray_node,
                    im_instance=im_instance,
                    cloud_instance_id=cid,
                    cloud_instance_replica_id=(
                        (c_inst.replica_id if c_inst is not None else None)
                        or im_instance.cloud_instance_replica_id
                        or None
                    ),
                )
            )

        # TODO(rickyx): We should probably name it as "Planner" or "Scaler"
        # or "ClusterScaler"
        sched_request = SchedulingRequest(
            node_type_configs=autoscaling_config.get_node_type_configs(),
            max_num_nodes=autoscaling_config.get_max_num_nodes(),
            resource_requests=ray_state.pending_resource_requests,
            gang_resource_requests=ray_state.pending_gang_resource_requests,
            cluster_resource_constraints=ray_state.cluster_resource_constraints,
            current_instances=autoscaler_instances,
            idle_timeout_s=autoscaling_config.get_idle_timeout_s(),
            pending_idle_timeout_s=autoscaling_config.get_pending_idle_timeout_s(),
            disable_launch_config_check=(
                autoscaling_config.disable_launch_config_check()
            ),
            cloud_resource_availabilities=(
                cloud_resource_monitor.get_resource_availabilities()
            ),
            recoverable_resource_availabilities=(
                cloud_resource_monitor.get_recoverable_resource_availabilities()
            ),
        )

        if isinstance(cloud_provider, KubeRayProvider):
            sched_request.ippr_specs = cloud_provider.get_ippr_specs()
            sched_request.ippr_statuses = cloud_provider.get_ippr_statuses()

        # Ask scheduler for updates to the cluster shape.
        reply = scheduler.schedule(sched_request)

        # Populate the autoscaling state.
        autoscaling_state.infeasible_resource_requests.extend(
            reply.infeasible_resource_requests
        )
        autoscaling_state.infeasible_gang_resource_requests.extend(
            reply.infeasible_gang_resource_requests
        )
        autoscaling_state.infeasible_cluster_resource_constraints.extend(
            reply.infeasible_cluster_resource_constraints
        )

        if not Reconciler._is_head_node_running(instance_manager):
            # We shouldn't be scaling the cluster until the head node is ready.
            # This could happen when the head node (i.e. the raylet) is still
            # pending registration even though GCS is up.
            # We will wait until the head node is running and ready to avoid
            # scaling the cluster from min worker nodes constraint.
            return

        if autoscaling_config.provider == Provider.READ_ONLY:
            # We shouldn't be scaling the cluster if the provider is read-only.
            return

        # Scale the clusters if needed.
        to_launch = reply.to_launch
        replica_sizes = Reconciler._get_multihost_replica_sizes(autoscaling_config)
        to_terminate = Reconciler._filter_partial_replica_terminations(
            to_terminate=reply.to_terminate,
            im_instances=im_instances,
            im_instances_by_instance_id=im_instances_by_instance_id,
            cloud_instances=cloud_instances,
            replica_sizes=replica_sizes,
            _logger=_logger,
        )

        def _replica_key(inst: IMInstance) -> Optional[Tuple[NodeType, str]]:
            c_inst = cloud_instances.get(inst.cloud_instance_id)
            if (
                c_inst is not None
                and c_inst.replica_id
                and inst.instance_type in replica_sizes
            ):
                return (inst.instance_type, c_inst.replica_id)
            return None

        # Replicas with a Ray stop pending after this tick. The cloud provider
        # deletes a replica as a unit, so their not-yet-running pods wait for the
        # stop to be accepted instead of deleting the replica under it.
        pre_view = _build_multihost_replica_view(
            im_instances, cloud_instances, replica_sizes
        )
        stopping_reps = set(pre_view.draining_reps) | {
            _replica_key(im_instances_by_instance_id[req.instance_id])
            for req in to_terminate
            if req.instance_status == IMInstance.RAY_RUNNING
        }
        stopping_reps.discard(None)

        updates = {}
        skipped_requested: List[str] = []
        # Add terminating instances.
        for terminate_request in to_terminate:
            instance_id = terminate_request.instance_id
            im_instance_to_terminate = im_instances_by_instance_id[instance_id]
            if terminate_request.instance_status == IMInstance.QUEUED and not (
                im_instance_to_terminate.launch_request_id
                and im_instance_to_terminate.instance_type in replica_sizes
            ):
                # QUEUED instances have no cloud resources allocated yet.
                # Cancel the allocation request by transitioning directly to TERMINATED.
                updates[terminate_request.instance_id] = IMInstanceUpdateEvent(
                    instance_id=instance_id,
                    new_instance_status=IMInstance.TERMINATED,
                    termination_request=terminate_request,
                    details=f"allocation canceled: {terminate_request.details}",
                )
            elif terminate_request.instance_status in (
                IMInstance.REQUESTED,
                IMInstance.QUEUED,
            ):
                skipped_requested.append(instance_id)
            elif terminate_request.instance_status in (
                IMInstance.ALLOCATED,
                IMInstance.RAY_INSTALLING,
            ):
                # The instance is not yet running, so we can't request to stop/drain Ray.
                # Therefore, we can skip the RAY_STOP_REQUESTED state and directly terminate the node.
                if _replica_key(im_instance_to_terminate) in stopping_reps:
                    continue
                updates[terminate_request.instance_id] = IMInstanceUpdateEvent(
                    instance_id=instance_id,
                    new_instance_status=IMInstance.TERMINATING,
                    cloud_instance_id=im_instance_to_terminate.cloud_instance_id,
                    termination_request=terminate_request,
                    details=f"terminating ray: {terminate_request.details}",
                )
            elif terminate_request.instance_status == IMInstance.RAY_STOP_REQUESTED:
                continue
            else:
                updates[terminate_request.instance_id] = IMInstanceUpdateEvent(
                    instance_id=instance_id,
                    new_instance_status=IMInstance.RAY_STOP_REQUESTED,
                    termination_request=terminate_request,
                    details=f"draining ray: {terminate_request.details}",
                )
        if skipped_requested:
            _logger.warning(
                "Deferring termination of in-flight REQUESTED instances %s until "
                "cloud allocation completes.",
                skipped_requested,
            )

        pending_launches = [[req.instance_type, req.count] for req in to_launch]
        if pending_launches:
            # Cap launches by instances still occupying slots after this tick's
            # updates (including draining RAY_STOP_REQUESTED / RAY_STOPPING
            # workers and whole active multi-host replicas) so queued launches
            # stay within configured limits until draining slots actually free.
            status_overrides = {
                iid: ev.new_instance_status for iid, ev in updates.items()
            }
            view = _build_multihost_replica_view(
                im_instances,
                cloud_instances,
                replica_sizes,
                status_overrides=status_overrides,
            )
            active_by_type: Dict[NodeType, int] = defaultdict(int)
            active_total = 0
            for inst in im_instances:
                st = status_overrides.get(inst.instance_id, inst.status)
                if (
                    InstanceUtil.is_ray_running_reachable(st)
                    or st == IMInstance.RAY_STOPPING
                ):
                    active_total += 1
                    if inst.node_kind != NodeKind.HEAD:
                        active_by_type[inst.instance_type] += 1

            in_flight_by_lid: Dict[Tuple[NodeType, str], int] = defaultdict(
                int, view.in_flight_by_lid
            )
            for rk in sorted(
                view.rep_occupying,
                key=lambda r: (
                    0
                    if any(
                        in_flight_by_lid[(r[0], lid)] > 0
                        and view.owed_batch_slots(r[0], r[1], lid, replica_sizes[r[0]])
                        > 0
                        for lid in view.rep_lids[r]
                    )
                    or (r not in view.non_fillable_reps and r in view.rep_booting)
                    else 1,
                    r,
                ),
            ):
                nt, rid = rk
                size = replica_sizes[nt]
                occ = view.rep_occupying[rk]
                if occ >= size:
                    continue
                covered = 0
                if rk not in view.non_fillable_reps:
                    missing_unassigned = max(0, size - len(view.rep_assigned_cids[rk]))
                    for lid in sorted(view.rep_lids[rk]):
                        owed = view.owed_batch_slots(nt, rid, lid, size)
                        take = min(
                            missing_unassigned - covered,
                            owed,
                            in_flight_by_lid[(nt, lid)],
                        )
                        in_flight_by_lid[(nt, lid)] -= take
                        covered += take
                    if covered < missing_unassigned and in_flight_by_lid[(nt, "")] > 0:
                        avail_untagged = (
                            in_flight_by_lid[(nt, "")]
                            if rk in view.rep_booting
                            else in_flight_by_lid[(nt, "")] % size
                        )
                        take = min(missing_unassigned - covered, avail_untagged)
                        in_flight_by_lid[(nt, "")] -= take
                        covered += take
                extra_gap = max(0, size - occ - covered)
                active_by_type[nt] += extra_gap
                active_total += extra_gap

            node_type_configs = autoscaling_config.get_node_type_configs() or {}
            max_num_nodes = autoscaling_config.get_max_num_nodes()
            for item in pending_launches:
                nt, count = item
                if nt in node_type_configs:
                    count = min(
                        count,
                        max(
                            0,
                            node_type_configs[nt].max_worker_nodes - active_by_type[nt],
                        ),
                    )
                if max_num_nodes is not None:
                    count = min(count, max(0, max_num_nodes - active_total))
                count -= count % replica_sizes.get(nt, 1)
                item[1] = count
                active_by_type[nt] += count
                active_total += count

        # Add new instances.
        for nt, count in pending_launches:
            for _ in range(count):
                instance_id = InstanceUtil.random_instance_id()
                updates[instance_id] = IMInstanceUpdateEvent(
                    instance_id=instance_id,
                    new_instance_status=IMInstance.QUEUED,
                    instance_type=nt,
                    upsert=True,
                    details=(f"queuing new instance of {nt} " "from scheduler"),
                )

        if isinstance(cloud_provider, KubeRayProvider):
            cloud_provider.do_ippr_requests(reply.to_ippr)

        Reconciler._update_instance_manager(instance_manager, version, updates)

    @staticmethod
    def _filter_partial_replica_terminations(
        to_terminate: List[TerminationRequest],
        im_instances: List[IMInstance],
        im_instances_by_instance_id: Dict[str, IMInstance],
        cloud_instances: Dict[CloudInstanceId, CloudInstance],
        replica_sizes: Dict[NodeType, int],
        _logger: logging.Logger = logger,
    ) -> List[TerminationRequest]:
        """Keep only whole-replica multi-worker scale-downs, deferring partial or
        mid-materialization replicas."""
        if not (to_terminate and replica_sizes and cloud_instances):
            return list(to_terminate)

        view = _build_multihost_replica_view(
            im_instances, cloud_instances, replica_sizes
        )
        # Mid-materialization guard is bounded by request_status_timeout_s.
        done_cids = {
            i.cloud_instance_id
            for i in im_instances
            if i.cloud_instance_id
            and i.cloud_instance_id not in view.live_cids
            and i.status
            not in _TERMINAL_INSTANCE_STATUSES | _UNALLOCATED_INSTANCE_STATUSES
        }
        present_by_rep: Dict[Tuple[NodeType, str], Set[str]] = defaultdict(set)
        for cid, c_inst in cloud_instances.items():
            if (
                c_inst.node_kind != NodeKind.HEAD
                and c_inst.node_type in replica_sizes
                and c_inst.replica_id
            ):
                present_by_rep[(c_inst.node_type, c_inst.replica_id)].add(cid)
        term_cids = {
            im_instances_by_instance_id[r.instance_id].cloud_instance_id
            for r in to_terminate
            if r.instance_status != IMInstance.QUEUED
            and r.instance_id in im_instances_by_instance_id
            and im_instances_by_instance_id[r.instance_id].cloud_instance_id
        }
        kept_terminate: List[TerminationRequest] = []
        skipped_by_rep: Dict[str, List[str]] = defaultdict(list)
        for req in to_terminate:
            inst = im_instances_by_instance_id.get(req.instance_id)
            c_inst = (
                cloud_instances.get(inst.cloud_instance_id)
                if inst is not None and inst.cloud_instance_id
                else None
            )
            rep_key = (
                (inst.instance_type, c_inst.replica_id)
                if inst is not None and c_inst is not None and c_inst.replica_id
                else None
            )
            rep_cids = present_by_rep[rep_key] if rep_key is not None else None
            if (
                rep_cids is not None
                and req.instance_status != IMInstance.QUEUED
                and inst.instance_type in replica_sizes
                and (
                    not (rep_cids <= (term_cids | done_cids))
                    or (
                        len(rep_cids) < replica_sizes[inst.instance_type]
                        and rep_key not in view.non_fillable_reps
                        and (
                            any(
                                view.in_flight_by_lid[(inst.instance_type, lid)] > 0
                                and view.owed_batch_slots(
                                    inst.instance_type,
                                    c_inst.replica_id,
                                    lid,
                                    replica_sizes[inst.instance_type],
                                )
                                > 0
                                for lid in view.rep_lids[rep_key]
                            )
                            or (
                                view.in_flight_by_lid[(inst.instance_type, "")] > 0
                                and (
                                    bool(rep_cids & view.booting_cids)
                                    or view.in_flight_by_lid[(inst.instance_type, "")]
                                    % replica_sizes[inst.instance_type]
                                    != 0
                                )
                            )
                        )
                    )
                )
            ):
                skipped_by_rep[c_inst.replica_id].append(req.instance_id)
                continue
            kept_terminate.append(req)

        if skipped_by_rep:
            _logger.warning(
                "Deferring multi-worker scale-down of %s: partial replica or "
                "in-flight REQUESTED pods, waiting for remaining replica pods.",
                dict(skipped_by_rep),
            )

        return kept_terminate

    @staticmethod
    def _terminate_instances(
        instance_manager: InstanceManager,
        autoscaling_config: Optional[AutoscalingConfig] = None,
        non_terminated_cloud_instances: Optional[
            Dict[CloudInstanceId, CloudInstance]
        ] = None,
    ):
        """Terminate instances with the below statuses:
            - RAY_STOPPED: ray was stopped on the cloud instance.
            - ALLOCATION_TIMEOUT: cloud provider timed out to allocate a running cloud instance.
            - RAY_INSTALL_FAILED: ray installation failed on the cloud instance,
                we will not retry.
            - TERMINATION_FAILED: cloud provider failed to terminate the instance
                or timeout for termination happened, we will retry again.

        Args:
            instance_manager: The instance manager to reconcile.
            autoscaling_config: The autoscaling config, for multi-worker replicas.
            non_terminated_cloud_instances: The non-terminated cloud instances,
                which carry the provider replica id of each worker.
        """

        im_instances, version = Reconciler._get_im_instances(instance_manager)
        updates = {}
        statuses_to_terminate = _TEARDOWN_INSTANCE_STATUSES - {IMInstance.TERMINATING}
        # The cloud provider deletes a multi-worker replica as a unit. When a
        # requested stop was accepted by some workers of a replica but another still
        # runs Ray (its stop was rejected, is pending, or is draining), the stopped
        # workers wait up to terminating_status_timeout_s so the replica is not
        # deleted under work that is still running. After that timeout, the stopped
        # worker proceeds to TERMINATING, which recycles the replica on
        # replica-atomic providers.
        replica_sizes = Reconciler._get_multihost_replica_sizes(autoscaling_config)
        cloud_instances = non_terminated_cloud_instances or {}
        view = _build_multihost_replica_view(
            im_instances, cloud_instances, replica_sizes
        )
        # A RAY_STOPPED row can be stale after a raylet restart on the same
        # cloud instance. If another active IM row still owns that cloud
        # instance, clean up only the stale row without terminating the cloud.
        active_cloud_instance_ids = view.active_cids
        ray_running_reps = view.ray_running_reps

        def _replica_key(inst: IMInstance) -> Optional[Tuple[NodeType, str]]:
            c_inst = cloud_instances.get(inst.cloud_instance_id)
            if (
                c_inst is not None
                and c_inst.replica_id
                and inst.instance_type in replica_sizes
            ):
                return (inst.instance_type, c_inst.replica_id)
            return None

        hold_timeout_s = (
            autoscaling_config.get_instance_reconcile_config().terminating_status_timeout_s
            if autoscaling_config is not None
            else 0
        )

        drained_terminating_reps = set()
        for instance in im_instances:
            if instance.status not in statuses_to_terminate:
                continue

            if (
                instance.status == IMInstance.RAY_STOPPED
                and instance.cloud_instance_id in active_cloud_instance_ids
            ):
                updates[instance.instance_id] = IMInstanceUpdateEvent(
                    instance_id=instance.instance_id,
                    new_instance_status=IMInstance.TERMINATED,
                    details=(
                        "marking stale instance record as terminated from "
                        f"{IMInstance.InstanceStatus.Name(instance.status)} "
                        "without terminating its cloud instance"
                    ),
                )
                continue

            rep_key = _replica_key(instance)
            solicited_stop = (
                instance.status == IMInstance.RAY_STOPPED
                and len(instance.status_history) > 1
                and instance.status_history[-2].instance_status
                in (IMInstance.RAY_STOP_REQUESTED, IMInstance.RAY_STOPPING)
            )
            if solicited_stop and rep_key in ray_running_reps:
                if not InstanceUtil.has_timeout(instance, hold_timeout_s):
                    continue
            elif solicited_stop and rep_key is not None:
                drained_terminating_reps.add(rep_key)

            # Terminate the instance.
            updates[instance.instance_id] = IMInstanceUpdateEvent(
                instance_id=instance.instance_id,
                new_instance_status=IMInstance.TERMINATING,
                cloud_instance_id=instance.cloud_instance_id,
                details="terminating instance from "
                f"{IMInstance.InstanceStatus.Name(instance.status)}",
            )

        if drained_terminating_reps:
            for instance in im_instances:
                if (
                    instance.instance_id not in updates
                    and instance.status
                    in (IMInstance.ALLOCATED, IMInstance.RAY_INSTALLING)
                    and _replica_key(instance) in drained_terminating_reps
                ):
                    updates[instance.instance_id] = IMInstanceUpdateEvent(
                        instance_id=instance.instance_id,
                        new_instance_status=IMInstance.TERMINATING,
                        cloud_instance_id=instance.cloud_instance_id,
                        details="terminating unready sibling of drained replica from "
                        f"{IMInstance.InstanceStatus.Name(instance.status)}",
                    )

        Reconciler._update_instance_manager(instance_manager, version, updates)

    @staticmethod
    def _install_ray(
        instance_manager: InstanceManager,
        non_terminated_cloud_instances: Dict[CloudInstanceId, CloudInstance],
    ) -> None:
        """Install ray on the allocated instances when it's ready (cloud instance
        should be running)

        This is needed if ray installation needs to be performed by
        the instance manager.

        Args:
            instance_manager: The instance manager to reconcile.
            non_terminated_cloud_instances: The non-terminated cloud instances
                from the cloud provider.
        """
        im_instances, version = Reconciler._get_im_instances(instance_manager)
        updates = {}
        for instance in im_instances:
            if instance.status != IMInstance.ALLOCATED:
                continue

            if instance.node_kind == NodeKind.HEAD:
                # Skip head node.
                continue

            cloud_instance = non_terminated_cloud_instances.get(
                instance.cloud_instance_id
            )

            assert cloud_instance, (
                f"Cloud instance {instance.cloud_instance_id} is not found "
                "in non_terminated_cloud_instances."
            )

            if not cloud_instance.is_running:
                # It might still be pending (e.g. setting up ssh)
                continue

            # Install ray on the running cloud instance
            updates[instance.instance_id] = IMInstanceUpdateEvent(
                instance_id=instance.instance_id,
                new_instance_status=IMInstance.RAY_INSTALLING,
                details="installing ray",
            )

        Reconciler._update_instance_manager(instance_manager, version, updates)

    @staticmethod
    def _fill_autoscaling_state(
        instance_manager: InstanceManager,
        autoscaling_state: AutoscalingState,
        autoscaling_config: AutoscalingConfig,
    ) -> None:
        def get_provider_instance_type(instance_type: str) -> str:
            return autoscaling_config.get_provider_instance_type(instance_type)

        # Use the IM instance version for the autoscaler_state_version
        instances, version = Reconciler._get_im_instances(instance_manager)
        autoscaling_state.autoscaler_state_version = version

        # Group instances by status
        instances_by_status = defaultdict(list)
        for instance in instances:
            instances_by_status[instance.status].append(instance)

        # Pending instance requests
        instances_by_launch_request = defaultdict(list)
        queued_instances = []
        for instance in (
            instances_by_status[IMInstance.REQUESTED]
            + instances_by_status[IMInstance.QUEUED]
        ):
            if instance.launch_request_id:
                instances_by_launch_request[instance.launch_request_id].append(instance)
            else:
                queued_instances.append(instance)

        for _, instances in instances_by_launch_request.items():
            num_instances_by_type = defaultdict(int)
            for instance in instances:
                num_instances_by_type[instance.instance_type] += 1

            # All instances with same request id should have the same
            # request time.
            request_update = InstanceUtil.get_last_status_transition(
                instances[0], IMInstance.REQUESTED
            )
            request_time_ns = request_update.timestamp_ns if request_update else 0

            for instance_type, count in num_instances_by_type.items():
                autoscaling_state.pending_instance_requests.append(
                    PendingInstanceRequest(
                        instance_type_name=get_provider_instance_type(instance_type),
                        ray_node_type_name=instance_type,
                        count=int(count),
                        request_ts=int(request_time_ns // 1e9),
                    )
                )

        # Pending instances
        for instance in (
            instances_by_status[IMInstance.ALLOCATED]
            + instances_by_status[IMInstance.RAY_INSTALLING]
        ):
            status_history = sorted(
                instance.status_history, key=lambda x: x.timestamp_ns, reverse=True
            )
            autoscaling_state.pending_instances.append(
                PendingInstance(
                    instance_id=instance.instance_id,
                    instance_type_name=get_provider_instance_type(
                        instance.instance_type
                    ),
                    ray_node_type_name=instance.instance_type,
                    details=status_history[0].details,
                )
            )

        # Failed instance requests
        for instance in instances_by_status[IMInstance.ALLOCATION_FAILED]:
            request_status_update = InstanceUtil.get_last_status_transition(
                instance, IMInstance.REQUESTED
            )
            failed_status_update = InstanceUtil.get_last_status_transition(
                instance, IMInstance.ALLOCATION_FAILED
            )
            failed_time = (
                failed_status_update.timestamp_ns if failed_status_update else 0
            )
            request_time = (
                request_status_update.timestamp_ns if request_status_update else 0
            )
            autoscaling_state.failed_instance_requests.append(
                FailedInstanceRequest(
                    instance_type_name=get_provider_instance_type(
                        instance.instance_type
                    ),
                    ray_node_type_name=instance.instance_type,
                    start_ts=int(request_time // 1e9),
                    failed_ts=int(
                        failed_time // 1e9,
                    ),
                    reason=failed_status_update.details,
                    count=1,
                )
            )

    @staticmethod
    def _handle_stuck_requested_instance(
        instance: IMInstance, timeout_s: int, max_num_retry_request_to_allocate: int
    ) -> Optional[IMInstanceUpdateEvent]:
        """Fail the cloud instance allocation if it's stuck in the REQUESTED state.

        Args:
            instance: The instance to handle.
            timeout_s: The timeout in seconds.
            max_num_retry_request_to_allocate: The maximum number of times an instance
                could be requested to allocate.

        Returns:
            Instance update to ALLOCATION_FAILED: if the instance allocation failed
                with errors.
            None: if there's no update.

        """
        if not InstanceUtil.has_timeout(instance, timeout_s):
            # Not timeout yet, be patient.
            return None

        all_request_times_ns = sorted(
            InstanceUtil.get_status_transition_times_ns(
                instance, select_instance_status=IMInstance.REQUESTED
            )
        )
        # Fail the allocation if we have tried too many times.
        if len(all_request_times_ns) > max_num_retry_request_to_allocate:
            return IMInstanceUpdateEvent(
                instance_id=instance.instance_id,
                new_instance_status=IMInstance.ALLOCATION_FAILED,
                details=(
                    "failed to allocate cloud instance after "
                    f"{len(all_request_times_ns)} attempts > "
                    f"max_num_retry_request_to_allocate={max_num_retry_request_to_allocate}"  # noqa
                ),
            )

        # Retry the allocation if we could by transitioning to QUEUED again.
        return IMInstanceUpdateEvent(
            instance_id=instance.instance_id,
            new_instance_status=IMInstance.QUEUED,
            details=f"queue again to launch after timeout={timeout_s}s",
        )

    @staticmethod
    def _handle_stuck_instance(
        instance: IMInstance,
        timeout_s: int,
        new_status: IMInstance.InstanceStatus,
        **update_kwargs: Dict,
    ) -> Optional[IMInstanceUpdateEvent]:
        """Fail the instance if it's stuck in the status for too long.

        Args:
            instance: The instance to handle.
            timeout_s: The timeout in seconds.
            new_status: The new status to transition to.
            **update_kwargs: Keyword arguments for InstanceUpdateEvent.

        Returns:
            Instance update to the new status: if the instance is stuck in the status
                for too long.
            None: if there's no update.

        """
        if not InstanceUtil.has_timeout(instance, timeout_s):
            # Not timeout yet, be patient.
            return None

        return IMInstanceUpdateEvent(
            instance_id=instance.instance_id,
            new_instance_status=new_status,
            details=f"timeout={timeout_s}s at status "
            f"{IMInstance.InstanceStatus.Name(instance.status)}",
            **update_kwargs,
        )

    @staticmethod
    def _handle_extra_cloud_instances(
        instance_manager: InstanceManager,
        non_terminated_cloud_instances: Dict[CloudInstanceId, CloudInstance],
        ray_nodes: List[NodeState],
    ):
        """For extra cloud instances (i.e. cloud instances that are non terminated as
        returned by cloud provider, but not managed by the instance manager), we
        will create new IM instances with ALLOCATED status.

        Such instances could either be:
            1. Leaked instances that are incorrectly started by the cloud instance
            provider, and they would be terminated eventually if they fail to
            transition to RAY_RUNNING by stuck instances reconciliation, or they
            would join the  ray cluster and be terminated when the cluster scales down.
            2. Instances that are started by the cloud instance provider intentionally
            but not yet discovered by the instance manager. This could happen for
               a. Head node that's started before the autoscaler.
               b. Worker nodes that's started by the cloud provider upon users'
               actions: i.e. KubeRay scaling up the cluster with ray cluster config
               change.
            3. Ray nodes with cloud instance id not in the cloud provider. This could
            happen if there's delay in the Ray's state (i.e. cloud instance already
            terminated, but the ray node is still not dead yet).

        Args:
            instance_manager: The instance manager to reconcile.
            non_terminated_cloud_instances: The non-terminated cloud instances from
                the cloud provider.
            ray_nodes: The ray cluster's states of ray nodes.
        """
        Reconciler._handle_extra_cloud_instances_from_ray_nodes(
            instance_manager,
            ray_nodes,
            non_terminated_cloud_instances=non_terminated_cloud_instances,
        )
        Reconciler._handle_extra_cloud_instances_from_cloud_provider(
            instance_manager, non_terminated_cloud_instances
        )

    @staticmethod
    def _handle_extra_cloud_instances_from_cloud_provider(
        instance_manager: InstanceManager,
        non_terminated_cloud_instances: Dict[CloudInstanceId, CloudInstance],
    ):
        """For extra cloud instances that are not managed by the instance manager but
        are running in the cloud provider, we will create new IM instances with
        ALLOCATED status.

        Args:
            instance_manager: The instance manager to reconcile.
            non_terminated_cloud_instances: The non-terminated cloud instances from
                the cloud provider.
        """
        updates = {}

        instances, version = Reconciler._get_im_instances(instance_manager)
        cloud_instance_ids_managed_by_im = {
            instance.cloud_instance_id
            for instance in instances
            if instance.cloud_instance_id
            and instance.status
            not in [IMInstance.TERMINATED, IMInstance.ALLOCATION_FAILED]
        }

        # Find the extra cloud instances that are not managed by the instance manager.
        for cloud_instance_id, cloud_instance in non_terminated_cloud_instances.items():
            if cloud_instance_id in cloud_instance_ids_managed_by_im:
                continue
            event_kwargs = {}
            if cloud_instance.replica_id:
                event_kwargs["cloud_instance_replica_id"] = cloud_instance.replica_id
            updates[cloud_instance_id] = IMInstanceUpdateEvent(
                instance_id=InstanceUtil.random_instance_id(),  # Assign a new id.
                cloud_instance_id=cloud_instance_id,
                new_instance_status=IMInstance.ALLOCATED,
                node_kind=cloud_instance.node_kind,
                instance_type=cloud_instance.node_type,
                details=(
                    "allocated unmanaged cloud instance :"
                    f"{cloud_instance.cloud_instance_id} "
                    f"({NodeKind.Name(cloud_instance.node_kind)}) from cloud provider"
                ),
                upsert=True,
                **event_kwargs,
            )
        Reconciler._update_instance_manager(instance_manager, version, updates)

    @staticmethod
    def _handle_extra_cloud_instances_from_ray_nodes(
        instance_manager: InstanceManager,
        ray_nodes: List[NodeState],
        non_terminated_cloud_instances: Optional[
            Dict[CloudInstanceId, CloudInstance]
        ] = None,
    ):
        """For extra cloud instances reported by Ray but not managed by the instance
        manager, we will create new IM instances with ALLOCATED status.

        Args:
            instance_manager: The instance manager to reconcile.
            ray_nodes: The ray cluster's states of ray nodes.
            non_terminated_cloud_instances: Optional non-terminated cloud instances
                used to retain provider replica group IDs on adopted records.
        """
        updates = {}

        instances, version = Reconciler._get_im_instances(instance_manager)
        cloud_instance_ids_managed_by_im = {
            instance.cloud_instance_id
            for instance in instances
            if instance.cloud_instance_id
            and not instance.node_id
            and instance.status
            not in [IMInstance.TERMINATED, IMInstance.ALLOCATION_FAILED]
        }
        ray_node_ids_managed_by_im = {
            instance.node_id for instance in instances if instance.node_id
        }

        for ray_node in ray_nodes:
            if not ray_node.instance_id:
                continue

            ray_node_id = binary_to_hex(ray_node.node_id)
            if ray_node_id in ray_node_ids_managed_by_im:
                continue

            cloud_instance_id = ray_node.instance_id
            if cloud_instance_id in cloud_instance_ids_managed_by_im:
                continue

            is_head = is_head_node(ray_node)
            cloud_inst = (non_terminated_cloud_instances or {}).get(cloud_instance_id)
            event_kwargs = {}
            if cloud_inst is not None and cloud_inst.replica_id:
                event_kwargs["cloud_instance_replica_id"] = cloud_inst.replica_id
            updates[ray_node_id] = IMInstanceUpdateEvent(
                instance_id=InstanceUtil.random_instance_id(),  # Assign a new id.
                cloud_instance_id=cloud_instance_id,
                new_instance_status=IMInstance.ALLOCATED,
                node_kind=NodeKind.HEAD if is_head else NodeKind.WORKER,
                ray_node_id=ray_node_id,
                instance_type=ray_node.ray_node_type_name,
                details=(
                    "allocated unmanaged worker cloud instance from ray node: "
                    f"{ray_node_id}"
                ),
                upsert=True,
                **event_kwargs,
            )

        Reconciler._update_instance_manager(instance_manager, version, updates)

    @staticmethod
    def _report_metrics(
        instance_manager: InstanceManager,
        autoscaling_config: AutoscalingConfig,
        metrics_reporter: Optional[AutoscalerMetricsReporter] = None,
    ):
        if not metrics_reporter:
            return

        instances, _ = Reconciler._get_im_instances(instance_manager)
        node_type_configs = autoscaling_config.get_node_type_configs()

        metrics_reporter.report_instances(instances, node_type_configs)
        metrics_reporter.report_resources(instances, node_type_configs)
