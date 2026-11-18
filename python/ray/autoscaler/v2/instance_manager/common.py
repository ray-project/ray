from __future__ import annotations

import time
import uuid
from collections import defaultdict
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Dict, List, Optional, Set, Tuple

from ray._private.ray_constants import env_integer
from ray.core.generated.instance_manager_pb2 import (
    Instance,
    InstanceUpdateEvent,
    NodeKind,
)

if TYPE_CHECKING:
    from ray.autoscaler.v2.instance_manager.node_provider import (
        CloudInstance,
        CloudInstanceId,
    )
    from ray.autoscaler.v2.schema import NodeType

# Upper bound on retained status-history entries per instance. The history is
# append-only and read only for the most recent transitions (e.g. latest
# details, per-status timestamps), so trimming the oldest entries bounds the
# memory a single long-lived or repeatedly-stuck instance can hold while
# preserving every read done by the reconciler. Floored at 10 so a
# misconfigured env var cannot trim the history to empty, which would trip the
# `len(status_times_ns) >= 1` assertion in `has_timeout`.
MAX_STATUS_HISTORY_LENGTH = max(
    10, env_integer("RAY_AUTOSCALER_INSTANCE_MAX_STATUS_HISTORY_LENGTH", 200)
)


class InstanceUtil:
    """A helper class to group updates and operations on an Instance object defined
    in instance_manager.proto
    """

    # Memoized reachable from sets, where the key is the instance status, and
    # the value is the set of instance status that is reachable from the key
    # instance status.
    _reachable_from: Optional[
        Dict["Instance.InstanceStatus", Set["Instance.InstanceStatus"]]
    ] = None

    @staticmethod
    def new_instance(
        instance_id: str,
        instance_type: str,
        status: Instance.InstanceStatus,
        details: str = "",
    ) -> Instance:
        """Returns a new instance with the given status.

        Args:
            instance_id: The instance id.
            instance_type: The instance type.
            status: The status of the new instance.
            details: The details of the status transition.

        Returns:
            The newly-created instance.
        """
        instance = Instance()
        instance.version = 0  # it will be populated by the underlying storage.
        instance.instance_id = instance_id
        instance.instance_type = instance_type
        instance.status = status
        InstanceUtil._record_status_transition(instance, status, details)
        return instance

    @staticmethod
    def random_instance_id() -> str:
        """Returns a random instance id."""
        return str(uuid.uuid4())

    @staticmethod
    def is_cloud_instance_allocated(instance_status: Instance.InstanceStatus) -> bool:
        """Returns True if the instance is in a status where there could exist
        a cloud instance allocated by the cloud provider.
        """
        assert instance_status != Instance.UNKNOWN
        return instance_status in {
            Instance.ALLOCATED,
            Instance.RAY_INSTALLING,
            Instance.RAY_RUNNING,
            Instance.RAY_STOPPING,
            Instance.RAY_STOP_REQUESTED,
            Instance.RAY_STOPPED,
            Instance.TERMINATING,
            Instance.RAY_INSTALL_FAILED,
            Instance.TERMINATION_FAILED,
            Instance.ALLOCATION_TIMEOUT,
        }

    @staticmethod
    def is_ray_running(instance_status: Instance.InstanceStatus) -> bool:
        """Returns True if the instance is in a status where the ray process is
        running on the cloud instance.
            i.e. RAY_RUNNING, RAY_STOP_REQUESTED, RAY_STOPPING
        """
        assert instance_status != Instance.UNKNOWN

        if instance_status in InstanceUtil.get_reachable_statuses(
            Instance.RAY_STOPPING
        ):
            return False

        if instance_status in InstanceUtil.get_reachable_statuses(Instance.RAY_RUNNING):
            return True

        return False

    @staticmethod
    def is_ray_pending(instance_status: Instance.InstanceStatus) -> bool:
        """Returns True if the instance is in a status where the ray process is
        pending to be started on the cloud instance.

        """
        assert instance_status != Instance.UNKNOWN
        # Not gonna be in a RAY_RUNNING status.
        if Instance.RAY_RUNNING not in InstanceUtil.get_reachable_statuses(
            instance_status
        ):
            return False

        # Already running ray.
        if instance_status in InstanceUtil.get_reachable_statuses(Instance.RAY_RUNNING):
            return False

        return True

    def is_ray_running_reachable(instance_status: Instance.InstanceStatus) -> bool:
        """Returns True if the instance is in a status where it may transition
        to RAY_RUNNING status.
        """
        return Instance.RAY_RUNNING in InstanceUtil.get_reachable_statuses(
            instance_status
        )

    @staticmethod
    def set_status(
        instance: Instance,
        new_instance_status: Instance.InstanceStatus,
        details: str = "",
    ) -> bool:
        """Transitions the instance to the new state.

        Args:
            instance: The instance to update.
            new_instance_status: The new status to transition to.
            details: The details of the transition.

        Returns:
            True if the status transition is successful, False otherwise.
        """
        if (
            new_instance_status
            not in InstanceUtil.get_valid_transitions()[instance.status]
        ):
            return False

        instance.status = new_instance_status
        InstanceUtil._record_status_transition(instance, new_instance_status, details)
        return True

    @staticmethod
    def _record_status_transition(
        instance: Instance, status: Instance.InstanceStatus, details: str
    ):
        """Records the status transition.

        Args:
            instance: The instance to update.
            status: The new status to transition to.
            details: The details of the status transition.
        """
        now_ns = time.time_ns()
        instance.status_history.append(
            Instance.StatusHistory(
                instance_status=status,
                timestamp_ns=now_ns,
                details=details,
            )
        )
        # Bound the append-only history so an instance that is repeatedly stuck
        # or retried before reaching a terminal state cannot grow unboundedly.
        # Only the most recent transitions are ever read, so drop the oldest.
        # Delete one at a time (not a slice) since slice deletion on a protobuf
        # repeated field is not supported across all protobuf versions.
        while len(instance.status_history) > MAX_STATUS_HISTORY_LENGTH:
            del instance.status_history[0]

    @staticmethod
    def has_timeout(instance: Instance, timeout_s: int) -> bool:
        """Returns True if the instance has been in the current status for more
        than the given timeout.

        Args:
            instance: The instance to check.
            timeout_s: The timeout in seconds.

        Returns:
            True if the instance has been in the current status for more than
            the timeout_s seconds.
        """
        cur_status = instance.status

        status_times_ns = InstanceUtil.get_status_transition_times_ns(
            instance, select_instance_status=cur_status
        )
        assert len(status_times_ns) >= 1, (
            f"instance {instance.instance_id} has {len(status_times_ns)} "
            f"{Instance.InstanceStatus.Name(cur_status)} status"
        )
        status_time_ns = sorted(status_times_ns)[-1]
        if time.time_ns() - status_time_ns <= (timeout_s * 1e9):
            return False

        return True

    @staticmethod
    def is_solicited_stop(instance: Instance) -> bool:
        """Returns True if the instance reached RAY_STOPPED via RAY_STOP_REQUESTED."""
        if instance.status != Instance.RAY_STOPPED:
            return False
        hist = instance.status_history
        return (
            len(hist) >= 2 and hist[-2].instance_status == Instance.RAY_STOP_REQUESTED
        ) or (
            len(hist) >= 3
            and hist[-2].instance_status == Instance.RAY_STOPPING
            and hist[-3].instance_status == Instance.RAY_STOP_REQUESTED
        )

    @staticmethod
    def get_valid_transitions() -> Dict[
        "Instance.InstanceStatus", Set["Instance.InstanceStatus"]
    ]:
        return {
            # This is the initial status of a new instance.
            Instance.QUEUED: {
                # Cloud provider requested to launch a node for the instance.
                # This happens when the a launch request is made to the node provider.
                Instance.REQUESTED,
                # Allocation request canceled before being requested.
                # This happens when max_workers config is reduced or other termination
                # triggers occur while the instance is still queued.
                Instance.TERMINATED,
            },
            # When in this status, a launch request to the node provider is made.
            Instance.REQUESTED: {
                # Cloud provider allocated a cloud instance for the instance.
                # This happens when the cloud instance first appears in the list of
                # running cloud instances from the cloud instance provider.
                Instance.ALLOCATED,
                # Retry the allocation, become queueing again.
                Instance.QUEUED,
                # Cloud provider fails to allocate one. Either as a timeout or
                # the launch request fails immediately.
                Instance.ALLOCATION_FAILED,
            },
            # When in this status, the cloud instance is allocated and running. This
            # happens when the cloud instance is present in node provider's list of
            # running cloud instances.
            Instance.ALLOCATED: {
                # Ray needs to be install and launch on the provisioned cloud instance.
                # This happens when the cloud instance is allocated, and the autoscaler
                # is responsible for installing and launching ray on the cloud instance.
                # For node provider that manages the ray installation and launching,
                # this state is skipped.
                Instance.RAY_INSTALLING,
                # Ray is already installed on the provisioned cloud
                # instance. It could be any valid ray status.
                Instance.RAY_RUNNING,
                # The cloud provider timed out for allocating running cloud instance.
                # The CloudResourceMonitor subscriber will lower this node-type's priority
                # in feature schedules.
                Instance.ALLOCATION_TIMEOUT,
                Instance.RAY_STOPPING,
                Instance.RAY_STOPPED,
                # Instance is requested to be stopped, e.g. instance leaked: no matching
                # Instance with the same type is found in the autoscaler's state.
                Instance.TERMINATING,
                # cloud instance somehow failed.
                Instance.TERMINATED,
            },
            # Ray process is being installed and started on the cloud instance.
            # This status is skipped for node provider that manages the ray
            # installation and launching. (e.g. Ray-on-Spark)
            Instance.RAY_INSTALLING: {
                # Ray installed and launched successfully, reported by the ray cluster.
                # Similar to the Instance.ALLOCATED -> Instance.RAY_RUNNING transition,
                # where the ray process is managed by the node provider.
                Instance.RAY_RUNNING,
                # Ray installation failed. This happens when the ray process failed to
                # be installed and started on the cloud instance.
                Instance.RAY_INSTALL_FAILED,
                # Wen the ray node is reported as stopped by the ray cluster.
                # This could happen that the ray process was stopped quickly after start
                # such that a ray running node  wasn't discovered and the RAY_RUNNING
                # transition was skipped.
                Instance.RAY_STOPPED,
                # A cloud instance is being terminated (when the instance itself is no
                # longer needed, e.g. instance is outdated, autoscaler is scaling down)
                Instance.TERMINATING,
                # cloud instance somehow failed during the installation process.
                Instance.TERMINATED,
            },
            # Ray process is installed and running on the cloud instance. When in this
            # status, a ray node must be present in the ray cluster.
            Instance.RAY_RUNNING: {
                # Ray is requested to be stopped.
                Instance.RAY_STOP_REQUESTED,
                # Ray is stopping (currently draining),
                # e.g. idle termination.
                Instance.RAY_STOPPING,
                # Ray is already stopped, as reported by the ray cluster.
                Instance.RAY_STOPPED,
                # A cloud instance is being terminated (when the instance itself is no
                # longer needed, e.g. instance is outdated, autoscaler is scaling down)
                Instance.TERMINATING,
                # cloud instance somehow failed.
                Instance.TERMINATED,
            },
            # Ray process should be stopped on the cloud instance. The RayStopper
            # subscriber will listen to this status and stop the ray process.
            Instance.RAY_STOP_REQUESTED: {
                # Ray is stopping on the cloud instance.
                Instance.RAY_STOPPING,
                # Ray stopped already.
                Instance.RAY_STOPPED,
                # Ray stop request failed (e.g. idle node no longer idle),
                # ray is still running.
                Instance.RAY_RUNNING,
                # A cloud instance is being terminated (e.g. sibling worker in
                # a multi-worker replica entered teardown).
                Instance.TERMINATING,
                # cloud instance somehow failed.
                Instance.TERMINATED,
            },
            # An instance has been allocated to a cloud instance, but the cloud
            # provider timed out for allocating running cloud instance, e.g. the
            # a kubernetes pod remains pending due to insufficient resources.
            Instance.ALLOCATION_TIMEOUT: {
                # Instance is requested to be stopped
                Instance.TERMINATING,
                # Cloud instance already disappeared; skip termination request.
                # This transition is allowed to avoid unnecessary termination attempts
                # when the cloud instance has already disappeared (e.g., manually deleted
                # or terminated by another process). While this helps avoid unnecessary
                # retries, it's important to monitor this transition as it may indicate
                # underlying issues with the allocation or termination process itself.
                Instance.TERMINATED,
            },
            # When in this status, the ray process is requested to be stopped to the
            # ray cluster, but not yet present in the dead ray node list reported by
            # the ray cluster.
            Instance.RAY_STOPPING: {
                # Ray is stopped, and the ray node is present in the dead ray node list
                # reported by the ray cluster.
                Instance.RAY_STOPPED,
                # A cloud instance is being terminated (when the instance itself is no
                # longer needed, e.g. instance is outdated, autoscaler is scaling down)
                Instance.TERMINATING,
                # cloud instance somehow failed.
                Instance.TERMINATED,
            },
            # When in this status, the ray process is stopped, and the ray node is
            # present in the dead ray node list reported by the ray cluster.
            Instance.RAY_STOPPED: {
                # A cloud instance is being terminated (when the instance itself is no
                # longer needed, e.g. instance is outdated, autoscaler is scaling down)
                Instance.TERMINATING,
                # cloud instance somehow failed.
                Instance.TERMINATED,
            },
            # When in this status, the cloud instance is requested to be stopped to
            # the node provider.
            Instance.TERMINATING: {
                # When a cloud instance no longer appears in the list of running cloud
                # instances from the node provider.
                Instance.TERMINATED,
                # When the cloud instance failed to be terminated.
                Instance.TERMINATION_FAILED,
            },
            # When in this status, the cloud instance failed to be terminated by the
            # node provider. We will keep retrying.
            Instance.TERMINATION_FAILED: {
                # Retry the termination, become terminating again.
                Instance.TERMINATING,
                # Cloud instance already disappeared; skip termination request.
                Instance.TERMINATED,
            },
            # An instance is marked as terminated when:
            # 1. A cloud instance disappears from the list of running cloud instances
            #    from the node provider (follows from TERMINATING or other running states).
            # 2. An allocation request is canceled before cloud resources are allocated
            #    (follows from QUEUED).
            # This is a terminal state.
            Instance.TERMINATED: set(),  # Terminal state.
            # When in this status, the cloud instance failed to be allocated by the
            # node provider.
            Instance.ALLOCATION_FAILED: set(),  # Terminal state.
            Instance.RAY_INSTALL_FAILED: {
                # Autoscaler requests to shutdown the instance when ray install failed.
                Instance.TERMINATING,
                # cloud instance somehow failed.
                Instance.TERMINATED,
            },
            # Initial state before the instance is created. Should never be used.
            Instance.UNKNOWN: set(),
        }

    @staticmethod
    def get_status_transitions(
        instance: Instance,
        select_instance_status: Optional["Instance.InstanceStatus"] = None,
    ) -> List["Instance.StatusHistory"]:
        """Returns the status history of the instance.

        Args:
            instance: The instance.
            select_instance_status: The go-to status to search for, i.e. select
                only status history when the instance transitions into the status.
                If None, returns all status updates.

        Returns:
            The list of status updates matching ``select_instance_status``,
            or all status updates when ``select_instance_status`` is None.
        """
        history = []
        for status_update in instance.status_history:
            if (
                select_instance_status
                and status_update.instance_status != select_instance_status
            ):
                continue
            history.append(status_update)
        return history

    @staticmethod
    def get_last_status_transition(
        instance: Instance,
        select_instance_status: Optional["Instance.InstanceStatus"] = None,
    ) -> Optional["Instance.StatusHistory"]:
        """Returns the last status transition of the instance.

        Args:
            instance: The instance.
            select_instance_status: The status to search for. If None, returns
                the last status update.

        Returns:
            The last matching status update, or None if no status updates match.
        """
        history = InstanceUtil.get_status_transitions(instance, select_instance_status)
        history.sort(key=lambda x: x.timestamp_ns)
        if history:
            return history[-1]
        return None

    @staticmethod
    def get_status_transition_times_ns(
        instance: Instance,
        select_instance_status: Optional["Instance.InstanceStatus"] = None,
    ) -> List[int]:
        """Returns a list of timestamps of the instance status update.

        Args:
            instance: The instance.
            select_instance_status: The status to search for. If None, returns
                all status update timestamps.

        Returns:
            The list of timestamps of the instance status updates.
        """
        return [
            e.timestamp_ns
            for e in InstanceUtil.get_status_transitions(
                instance, select_instance_status
            )
        ]

    @classmethod
    def get_reachable_statuses(
        cls,
        instance_status: Instance.InstanceStatus,
    ) -> Set["Instance.InstanceStatus"]:
        """Returns the set of instance status that is reachable from the given
        instance status following the status transitions.
        This method is memoized.
        Args:
            instance_status: The instance status to start from.
        Returns:
            The set of instance status that is reachable from the given instance
            status.
        """
        if cls._reachable_from is None:
            cls._compute_reachable()
        return cls._reachable_from[instance_status]

    @staticmethod
    def get_log_str_for_update(instance: Instance, update: InstanceUpdateEvent) -> str:
        """Returns a log string for the given instance update."""
        if update.upsert:
            return (
                f"New instance "
                f"{Instance.InstanceStatus.Name(update.new_instance_status)} (id="
                f"{instance.instance_id}, type={instance.instance_type}, "
                f"cloud_instance_id={instance.cloud_instance_id}, "
                f"ray_id={instance.node_id}): {update.details}"
            )
        return (
            f"Update instance "
            f"{Instance.InstanceStatus.Name(instance.status)}->"
            f"{Instance.InstanceStatus.Name(update.new_instance_status)} (id="
            f"{instance.instance_id}, type={instance.instance_type}, "
            f"cloud_instance_id={instance.cloud_instance_id}, "
            f"ray_id={instance.node_id}): {update.details}"
        )

    @classmethod
    def _compute_reachable(cls):
        """Computes and memorize the from status sets for each status machine with
        a DFS search.
        """
        valid_transitions = cls.get_valid_transitions()

        def dfs(graph, start, visited):
            """Regular DFS algorithm to find all reachable nodes from a given node."""
            for next_node in graph[start]:
                if next_node not in visited:
                    # We delay adding the visited set here so we could capture
                    # the self loop.
                    visited.add(next_node)
                    dfs(graph, next_node, visited)
            return visited

        # Initialize the graphs
        cls._reachable_from = {}
        for status in Instance.InstanceStatus.values():
            # All nodes reachable from 'start'
            visited = set()
            cls._reachable_from[status] = dfs(valid_transitions, status, visited)


# Exhaustive partition of every non-UNKNOWN Instance.InstanceStatus used by
# multi-host replica reconciliation and scheduling. Every state belongs to
# exactly one bucket:
#   TERMINAL | UNALLOCATED | BOOTING | {RAY_RUNNING} | DRAINING | TEARDOWN.
_TERMINAL_INSTANCE_STATUSES = frozenset(
    {
        Instance.TERMINATED,
        Instance.ALLOCATION_FAILED,
    }
)
_UNALLOCATED_INSTANCE_STATUSES = frozenset(
    {
        Instance.QUEUED,
        Instance.REQUESTED,
    }
)
_BOOTING_INSTANCE_STATUSES = frozenset(
    {
        Instance.ALLOCATED,
        Instance.RAY_INSTALLING,
    }
)
_DRAINING_INSTANCE_STATUSES = frozenset(
    {
        Instance.RAY_STOP_REQUESTED,
        Instance.RAY_STOPPING,
    }
)
_TEARDOWN_INSTANCE_STATUSES = frozenset(
    {
        Instance.ALLOCATION_TIMEOUT,
        Instance.RAY_INSTALL_FAILED,
        Instance.RAY_STOPPED,
        Instance.TERMINATING,
        Instance.TERMINATION_FAILED,
    }
)
_ACTIVE_CID_STATUSES = (
    _BOOTING_INSTANCE_STATUSES
    | {Instance.RAY_RUNNING}
    | _DRAINING_INSTANCE_STATUSES
    | {Instance.TERMINATING}
)
_LIVE_CID_STATUSES = _BOOTING_INSTANCE_STATUSES | {Instance.RAY_RUNNING}
_OCCUPYING_CLOUD_STATUSES = _LIVE_CID_STATUSES | _DRAINING_INSTANCE_STATUSES
_TEARDOWN_HISTORY_STATUSES = frozenset(
    {
        Instance.TERMINATING,
        Instance.TERMINATION_FAILED,
        Instance.ALLOCATION_TIMEOUT,
        Instance.RAY_INSTALL_FAILED,
        Instance.RAY_STOPPING,
    }
)


@dataclass
class _ReplicaViewBuildState:
    """Transient per-build accumulators used while projecting IM rows."""

    active_nt_cids: Set[Tuple[NodeType, CloudInstanceId]] = field(default_factory=set)
    live_term_rows_by_rep_lid: Dict[
        Tuple[Tuple[NodeType, str], str], List[Instance]
    ] = field(default_factory=lambda: defaultdict(list))
    missing_rows_by_lid: Dict[Tuple[NodeType, str], List[Instance]] = field(
        default_factory=lambda: defaultdict(list)
    )
    untagged_active_cids: Dict[Tuple[NodeType, str], Set[str]] = field(
        default_factory=lambda: defaultdict(set)
    )
    teardown_reps: Set[Tuple[NodeType, str]] = field(default_factory=set)
    tagged_rows_by_cid: Dict[Tuple[NodeType, str], List[Tuple[int, str]]] = field(
        default_factory=lambda: defaultdict(list)
    )
    term_untagged_td_by_cid: Dict[Tuple[NodeType, str], List[int]] = field(
        default_factory=lambda: defaultdict(list)
    )
    live_untagged_td_cids: Set[Tuple[NodeType, str]] = field(default_factory=set)
    rep_occupying_cids: Dict[Tuple[NodeType, str], Set[str]] = field(
        default_factory=lambda: defaultdict(set)
    )
    term_tagged_rows: List[Tuple[NodeType, str, str, str, bool, int, Instance]] = field(
        default_factory=list
    )


@dataclass
class _MultiHostReplicaView:
    """Canonical multi-host replica state projection shared across allocation,
    scaling clamps, partial-replica drain filtering, instance termination, and
    V2 resource scheduling."""

    active_cids: Set[CloudInstanceId] = field(default_factory=set)
    live_cids: Set[Tuple[NodeType, CloudInstanceId]] = field(default_factory=set)
    assigned_reps: Set[Tuple[NodeType, str]] = field(default_factory=set)
    rep_assigned_cids: Dict[Tuple[NodeType, str], Set[str]] = field(
        default_factory=dict
    )
    rep_lids: Dict[Tuple[NodeType, str], Set[str]] = field(default_factory=dict)
    rep_bound_cids_by_lid: Dict[Tuple[Tuple[NodeType, str], str], Set[str]] = field(
        default_factory=dict
    )
    non_fillable_reps: Set[Tuple[NodeType, str]] = field(default_factory=set)
    rep_occupying: Dict[Tuple[NodeType, str], int] = field(default_factory=dict)
    draining_reps: Set[Tuple[NodeType, str]] = field(default_factory=set)
    ray_running_reps: Set[Tuple[NodeType, str]] = field(default_factory=set)
    claimed_reps_by_lid: Dict[Tuple[NodeType, str], Set[str]] = field(
        default_factory=dict
    )
    requested_by_lid: Dict[Tuple[NodeType, str], int] = field(default_factory=dict)
    vanished_cids_by_lid: Dict[Tuple[NodeType, str], Set[str]] = field(
        default_factory=dict
    )
    torn_down_lids: Set[Tuple[NodeType, str]] = field(default_factory=set)
    unlabeled_occupying_cids: Dict[NodeType, Set[CloudInstanceId]] = field(
        default_factory=dict
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
            max(0, size - len(self.rep_assigned_cids.get(rk, ()))),
            max(0, size - len(bound)),
        )

    def consume_requested_slot(self, nt: NodeType, lid: str) -> None:
        """Decrement one pending REQUESTED slot for batch `(nt, lid)`."""
        key = (nt, lid)
        self.requested_by_lid[key] = max(0, self.requested_by_lid.get(key, 0) - 1)

    def claim_replica_slot(
        self,
        nt: NodeType,
        rid: str,
        lid: str,
        cid: CloudInstanceId,
    ) -> None:
        """Bind allocated pod `cid` in replica `(nt, rid)` to batch `lid`."""
        rk = (nt, rid)
        self.assigned_reps.add(rk)
        self.rep_assigned_cids.setdefault(rk, set()).add(cid)
        if lid:
            self.rep_lids.setdefault(rk, set()).add(lid)
            vanished = self.vanished_cids_by_lid.pop((nt, lid), None)
            bound = self.rep_bound_cids_by_lid.setdefault((rk, lid), set())
            if vanished:
                bound.update(vanished)
            bound.add(cid)
            self.claimed_reps_by_lid.setdefault((nt, lid), set()).add(rid)

    def is_partial_vanished_batch(self, nt: NodeType, lid: str, size: int) -> bool:
        """Return True if batch `(nt, lid)` lost pods mid-boot and has fewer
        than `size` REQUESTED slots remaining without a live claimed replica."""
        key = (nt, lid)
        return (
            not self.claimed_reps_by_lid.get(key)
            and bool(self.vanished_cids_by_lid.get(key))
            and self.requested_by_lid.get(key, 0) < size
        )

    def is_surplus_batch(self, nt: NodeType, lid: str, size: int) -> bool:
        """Return True if batch `(nt, lid)` only has surplus REQUESTED slots
        leftover after filling its live claimed replica(s)."""
        key = (nt, lid)
        claimed_rids = self.claimed_reps_by_lid.get(key)
        return (
            bool(claimed_rids)
            and self.requested_by_lid.get(key, 0) < size
            and all(
                (nt, r) not in self.non_fillable_reps
                and self.owed_batch_slots(nt, r, lid, size) == 0
                for r in claimed_rids
            )
        )

    def has_pending_backfill_for_replica(
        self, nt: NodeType, rid: str, size: int
    ) -> bool:
        """Return True if any live batch still has REQUESTED pods owed to `(nt, rid)`."""
        rk = (nt, rid)
        if rk in self.non_fillable_reps:
            return False
        return any(
            self.requested_by_lid.get((nt, lid), 0) > 0
            and self.owed_batch_slots(nt, rid, lid, size) > 0
            for lid in self.rep_lids.get(rk, ())
        )

    def unclaimed_replicas_by_lid(
        self, replica_sizes: Dict[NodeType, int]
    ) -> Dict[NodeType, Dict[str, int]]:
        """Count whole unclaimed in-flight replicas per `(node_type, launch_request_id)`."""
        result: Dict[NodeType, Dict[str, int]] = {nt: {} for nt in replica_sizes}
        for (nt, lid), req_cnt in self.requested_by_lid.items():
            if req_cnt <= 0 or not lid:
                continue
            if (nt, lid) in self.torn_down_lids:
                continue
            if self.claimed_reps_by_lid.get((nt, lid)):
                continue
            size = replica_sizes[nt]
            vanished = len(self.vanished_cids_by_lid.get((nt, lid), ()))
            reps = (req_cnt + vanished) // size
            if reps > 0:
                result[nt][lid] = reps
        return result

    def pop_matching_candidate(
        self,
        nt: NodeType,
        lid: str,
        size: int,
        candidates: List[CloudInstance],
        remaining_failures: int = 0,
    ) -> Optional[CloudInstance]:
        """Pop and bind the best unassigned cloud pod for `(nt, lid)`."""
        match_idx = None
        avail_req = max(
            0,
            self.requested_by_lid.get((nt, lid), 0) - remaining_failures,
        )
        # 1. Finish an incomplete live replica already bound to this batch.
        for idx in range(len(candidates) - 1, -1, -1):
            rid = candidates[idx].replica_id
            if rid and self.owed_batch_slots(nt, rid, lid, size) > 0:
                match_idx = idx
                break
        # 2. Claim a fresh replica if this batch's unclaimed, non-failing
        # REQUESTED + mid-boot vanished slots cover a full replica beyond
        # open debt on already-claimed live replicas.
        if match_idx is None:
            claimed_rids = self.claimed_reps_by_lid.get((nt, lid), ())
            open_live_slots = sum(
                self.owed_batch_slots(nt, r, lid, size) for r in claimed_rids
            )
            vanished_slots = (
                len(self.vanished_cids_by_lid.get((nt, lid), ()))
                if not claimed_rids
                else 0
            )
            if avail_req + vanished_slots - open_live_slots >= size:
                cand_counts: Dict[str, int] = defaultdict(int)
                for c in candidates:
                    if c.replica_id and (nt, c.replica_id) not in self.assigned_reps:
                        cand_counts[c.replica_id] += 1
                fallback_idx = None
                for idx in range(len(candidates) - 1, -1, -1):
                    rid = candidates[idx].replica_id
                    if rid and (nt, rid) not in self.assigned_reps:
                        if cand_counts[rid] >= avail_req:
                            match_idx = idx
                            break
                        if fallback_idx is None:
                            fallback_idx = idx
                if match_idx is None:
                    match_idx = fallback_idx
        # 3. Fallback for providers without replica_id metadata.
        if (
            match_idx is None
            and not self.claimed_reps_by_lid.get((nt, lid))
            and avail_req > 0
        ):
            for idx in range(len(candidates) - 1, -1, -1):
                if not candidates[idx].replica_id:
                    match_idx = idx
                    break
        if match_idx is None:
            return None
        matched = candidates.pop(match_idx)
        self.consume_requested_slot(nt, lid)
        if matched.replica_id:
            self.claim_replica_slot(
                nt, matched.replica_id, lid, matched.cloud_instance_id
            )
        return matched

    def uncovered_replica_slots_by_type(
        self, replica_sizes: Dict[NodeType, int]
    ) -> Dict[NodeType, int]:
        """Count phantom replica slots (degraded/draining replica gaps not covered
        by owed REQUESTED backfills, plus mid-boot vanished batch slots) per type."""
        uncovered: Dict[NodeType, int] = defaultdict(int)
        requested_by_lid: Dict[Tuple[NodeType, str], int] = defaultdict(
            int, self.requested_by_lid
        )
        for rk in sorted(
            self.rep_occupying,
            key=lambda r: (
                0
                if self.has_pending_backfill_for_replica(
                    r[0], r[1], replica_sizes[r[0]]
                )
                else 1,
                r,
            ),
        ):
            nt, rid = rk
            size = replica_sizes[nt]
            occ = self.rep_occupying[rk]
            if occ >= size:
                continue
            covered = 0
            if rk not in self.non_fillable_reps:
                missing_unassigned = max(
                    0, size - len(self.rep_assigned_cids.get(rk, ()))
                )
                for lid in sorted(self.rep_lids.get(rk, ())):
                    owed = self.owed_batch_slots(nt, rid, lid, size)
                    take = min(
                        missing_unassigned - covered,
                        owed,
                        requested_by_lid[(nt, lid)],
                    )
                    requested_by_lid[(nt, lid)] -= take
                    covered += take
            extra_gap = max(0, size - occ - covered)
            if extra_gap > 0:
                uncovered[nt] += extra_gap

        for (nt, lid), vanished_cids in self.vanished_cids_by_lid.items():
            if self.requested_by_lid.get((nt, lid), 0) > 0 and vanished_cids:
                uncovered[nt] += len(vanished_cids)

        return dict(uncovered)

    def unclaimed_vanished_by_lid(self, nt: NodeType) -> Dict[str, int]:
        """Return vanished pod counts per unclaimed, non-torn-down active batch of `nt`."""
        return {
            lid: len(vc)
            for (t, lid), vc in self.vanished_cids_by_lid.items()
            if t == nt
            and vc
            and (t, lid) not in self.torn_down_lids
            and self.requested_by_lid.get((t, lid), 0) > 0
            and not self.claimed_reps_by_lid.get((t, lid))
        }

    def unlabeled_backfill_gap(
        self, nt: NodeType, size: int, extra_queued: int = 0
    ) -> int:
        """Missing pods needed to round existing unlabeled pods of `nt` up to `size`."""
        unlabeled = self.unlabeled_occupying_cids.get(nt)
        if not unlabeled or size <= 1:
            return 0
        vanished_by_lid = self.unclaimed_vanished_by_lid(nt)
        unclaimed_req = sum(
            cnt + vanished_by_lid.get(lid, 0)
            for (t, lid), cnt in self.requested_by_lid.items()
            if t == nt
            and cnt > 0
            and (t, lid) not in self.torn_down_lids
            and not self.claimed_reps_by_lid.get((t, lid))
        )
        return (-(len(unlabeled) + unclaimed_req + extra_queued)) % size

    @classmethod
    def build(
        cls,
        im_instances: List[Instance],
        cloud_instances: Dict[CloudInstanceId, CloudInstance],
        replica_sizes: Dict[NodeType, int],
        status_overrides: Optional[Dict[str, "Instance.InstanceStatus"]] = None,
    ) -> "_MultiHostReplicaView":
        """Project per-instance IM rows and cloud replica IDs into one canonical view."""
        view = cls()
        overrides = status_overrides or {}
        state = _ReplicaViewBuildState(
            active_nt_cids=view._index_cloud_ids(im_instances, overrides)
        )
        if not replica_sizes:
            return view

        for inst in im_instances:
            if (
                inst.node_kind == NodeKind.HEAD
                or inst.instance_type not in replica_sizes
            ):
                continue
            st = overrides.get(inst.instance_id, inst.status)
            if not inst.cloud_instance_id:
                if st == Instance.REQUESTED:
                    req_key = (inst.instance_type, inst.launch_request_id or "")
                    view.requested_by_lid[req_key] = (
                        view.requested_by_lid.get(req_key, 0) + 1
                    )
                continue

            c_inst = _match_worker_cloud_instance(
                inst, cloud_instances.get(inst.cloud_instance_id)
            )
            rid = c_inst.replica_id if c_inst is not None and c_inst.replica_id else ""
            if st in _TERMINAL_INSTANCE_STATUSES:
                view._record_terminal_row(inst, rid, c_inst is not None, state)
            else:
                view._record_live_row(inst, st, rid, c_inst, state)

        view._propagate_terminated_untagged_teardowns(state)
        cls._classify_terminated_tagged_rows(state)
        view._reattach_live_terminal_rows(state)
        view._propagate_live_teardowns(state)
        view._reattach_missing_rows(state, replica_sizes)
        return view

    def _index_cloud_ids(
        self,
        im_instances: List[Instance],
        overrides: Dict[str, "Instance.InstanceStatus"],
    ) -> Set[Tuple[NodeType, CloudInstanceId]]:
        active_nt_cids: Set[Tuple[NodeType, CloudInstanceId]] = set()
        for inst in im_instances:
            st = overrides.get(inst.instance_id, inst.status)
            if not inst.cloud_instance_id:
                continue
            if st in _ACTIVE_CID_STATUSES:
                self.active_cids.add(inst.cloud_instance_id)
                active_nt_cids.add((inst.instance_type, inst.cloud_instance_id))
            if st in _LIVE_CID_STATUSES:
                self.live_cids.add((inst.instance_type, inst.cloud_instance_id))
        return active_nt_cids

    def _record_terminal_row(
        self,
        inst: Instance,
        rid: str,
        has_c_inst: bool,
        state: _ReplicaViewBuildState,
    ) -> None:
        nt = inst.instance_type
        lid = inst.launch_request_id
        ck = (nt, inst.cloud_instance_id)
        if not lid:
            if _has_teardown_history(inst):
                end_ns = max((h.timestamp_ns for h in inst.status_history), default=0)
                state.term_untagged_td_by_cid[ck].append(end_ns)
            return
        start_ns = _instance_cloud_start_ns(inst)
        state.tagged_rows_by_cid[ck].append((start_ns, lid))
        if _has_teardown_history(inst):
            self.torn_down_lids.add((nt, lid))
        state.term_tagged_rows.append(
            (nt, inst.cloud_instance_id, rid, lid, has_c_inst, start_ns, inst)
        )

    def _record_live_row(
        self,
        inst: Instance,
        st: "Instance.InstanceStatus",
        rid: str,
        c_inst: Optional[CloudInstance],
        state: _ReplicaViewBuildState,
    ) -> None:
        nt = inst.instance_type
        lid = inst.launch_request_id
        ck = (nt, inst.cloud_instance_id)
        is_shadowed_stopped = _is_shadowed_stopped(
            nt, st, inst.cloud_instance_id, state.active_nt_cids
        )
        in_teardown = not is_shadowed_stopped and st in (
            _TEARDOWN_INSTANCE_STATUSES | {Instance.RAY_STOPPING}
        )
        if lid:
            state.tagged_rows_by_cid[ck].append((_instance_cloud_start_ns(inst), lid))
            if in_teardown:
                self.torn_down_lids.add((nt, lid))
        elif in_teardown:
            state.live_untagged_td_cids.add(ck)

        if c_inst is None:
            if lid and not is_shadowed_stopped:
                state.missing_rows_by_lid[(nt, lid)].append(inst)
            return

        if not rid:
            if not is_shadowed_stopped and st in _OCCUPYING_CLOUD_STATUSES:
                self.unlabeled_occupying_cids.setdefault(nt, set()).add(
                    inst.cloud_instance_id
                )
            return

        rk = (nt, rid)
        self.assigned_reps.add(rk)
        if lid:
            self.claimed_reps_by_lid.setdefault((nt, lid), set()).add(rid)
            self.rep_bound_cids_by_lid.setdefault((rk, lid), set()).add(
                inst.cloud_instance_id
            )
            self.rep_lids.setdefault(rk, set()).add(lid)

        # Ignore stale RAY_STOPPED rows shadowed by an active IM row on the
        # same cloud instance (e.g. after a raylet restart).
        if is_shadowed_stopped:
            return

        if not lid:
            state.untagged_active_cids[rk].add(inst.cloud_instance_id)

        if in_teardown:
            state.teardown_reps.add(rk)
        if st in _DRAINING_INSTANCE_STATUSES | _TEARDOWN_INSTANCE_STATUSES:
            self.non_fillable_reps.add(rk)

        self.rep_assigned_cids.setdefault(rk, set()).add(inst.cloud_instance_id)
        if st in _OCCUPYING_CLOUD_STATUSES:
            state.rep_occupying_cids[rk].add(inst.cloud_instance_id)
            self.rep_occupying[rk] = len(state.rep_occupying_cids[rk])
        if st in _DRAINING_INSTANCE_STATUSES:
            self.draining_reps.add(rk)
            self.ray_running_reps.add(rk)
        elif st == Instance.RAY_RUNNING:
            self.ray_running_reps.add(rk)

    def _propagate_terminated_untagged_teardowns(
        self,
        state: _ReplicaViewBuildState,
    ) -> None:
        # Propagate teardown from already-TERMINATED untagged rows (e.g. after a
        # mid-boot raylet restart on an unlabeled pod or post-delete reconcile)
        # before reattaching live terminal rows, bounded by incarnation timestamps
        # so stale records cannot poison a newer reused cloud_instance_id.
        for ck, td_end_times in state.term_untagged_td_by_cid.items():
            tagged_rows = state.tagged_rows_by_cid.get(ck)
            if not tagged_rows:
                continue
            for td_end_ns in td_end_times:
                _propagate_cid_teardown_lids(
                    ck[0], tagged_rows, self.torn_down_lids, max_start_ns=td_end_ns
                )

    @staticmethod
    def _classify_terminated_tagged_rows(state: _ReplicaViewBuildState) -> None:
        # Classify terminal tagged rows after all incarnations on (nt, cid) are
        # known so a stale row on a reused cloud_instance_id cannot hijack a
        # newer replica or lose its vanished-slot accounting on its original
        # replica.
        latest_start_by_cid = {
            ck: max(start_ns for start_ns, _ in rows)
            for ck, rows in state.tagged_rows_by_cid.items()
        }
        for nt, cid, rid, lid, has_c_inst, start_ns, inst in state.term_tagged_rows:
            if start_ns < latest_start_by_cid[(nt, cid)] or not has_c_inst:
                state.missing_rows_by_lid[(nt, lid)].append(inst)
            elif rid:
                state.live_term_rows_by_rep_lid[((nt, rid), lid)].append(inst)

    def _reattach_live_terminal_rows(self, state: _ReplicaViewBuildState) -> None:
        # Reattach terminal rows whose cloud pod is still alive (e.g. a raylet
        # restart mid-replica-boot where an untagged RAY_RUNNING row adopted
        # the pod).
        for (rk, lid), rows in state.live_term_rows_by_rep_lid.items():
            nt, rid = rk
            term_cids = {r.cloud_instance_id for r in rows}
            is_td = (nt, lid) in self.torn_down_lids
            if rk in self.assigned_reps and (
                lid in self.rep_lids.get(rk, ())
                or (not is_td and bool(term_cids & state.untagged_active_cids[rk]))
            ):
                self.claimed_reps_by_lid.setdefault((nt, lid), set()).add(rid)
                self.rep_bound_cids_by_lid.setdefault((rk, lid), set()).update(
                    term_cids
                )
                self.rep_lids.setdefault(rk, set()).add(lid)
                if is_td:
                    self.non_fillable_reps.add(rk)

    def _propagate_live_teardowns(self, state: _ReplicaViewBuildState) -> None:
        # Propagate teardown of any live pod on a replica (including untagged
        # rows from raylet restarts or ray-node adoption) to all launch batches
        # bound to that replica so sibling REQUESTED slots fail instead of
        # leaking.
        for rk in state.teardown_reps:
            for bound_lid in self.rep_lids.get(rk, ()):
                self.torn_down_lids.add((rk[0], bound_lid))

        # Propagate teardown of live untagged rows by (node_type,
        # cloud_instance_id) so unlabeled legacy pods and pods whose cloud
        # instance already vanished before _handle_cloud_instance_terminated
        # still mark their launch batch.
        for ck in state.live_untagged_td_cids:
            tagged_rows = state.tagged_rows_by_cid.get(ck)
            if tagged_rows:
                _propagate_cid_teardown_lids(ck[0], tagged_rows, self.torn_down_lids)

    def _reattach_missing_rows(
        self,
        state: _ReplicaViewBuildState,
        replica_sizes: Dict[NodeType, int],
    ) -> None:
        # Reattach pods that disappeared from cloud_instances to their
        # per-replica launch batch: onto its live replica if any pod remains
        # alive, or into vanished_cids_by_lid if all bound pods of a
        # non-torn-down batch vanished mid-boot before all `size` slots
        # allocated.
        for (nt, lid), rows in state.missing_rows_by_lid.items():
            missing_cids = {r.cloud_instance_id for r in rows}
            is_td = (nt, lid) in self.torn_down_lids
            claimed_rids = self.claimed_reps_by_lid.get((nt, lid), set())
            if claimed_rids:
                for rid in claimed_rids:
                    rk = (nt, rid)
                    self.rep_bound_cids_by_lid.setdefault((rk, lid), set()).update(
                        missing_cids
                    )
                    if is_td:
                        self.non_fillable_reps.add(rk)
            elif (
                not is_td
                and self.requested_by_lid.get((nt, lid), 0) > 0
                and len(missing_cids) < replica_sizes[nt]
            ):
                self.vanished_cids_by_lid.setdefault((nt, lid), set()).update(
                    missing_cids
                )


def _is_shadowed_stopped(
    nt: NodeType,
    status: "Instance.InstanceStatus",
    cloud_instance_id: Optional[str],
    active_nt_cids: Set[Tuple[NodeType, str]],
) -> bool:
    """Return True if a RAY_STOPPED row is shadowed by an active row on the same pod."""
    return (
        status == Instance.RAY_STOPPED
        and bool(cloud_instance_id)
        and (nt, cloud_instance_id) in active_nt_cids
    )


def _match_worker_cloud_instance(
    im_instance: Optional[Instance],
    cloud_instance: Optional[CloudInstance],
) -> Optional[CloudInstance]:
    """Return `cloud_instance` if it belongs to worker `im_instance`."""
    if (
        im_instance is None
        or cloud_instance is None
        or im_instance.node_kind == NodeKind.HEAD
        or cloud_instance.node_kind == NodeKind.HEAD
    ):
        return None
    if cloud_instance.node_type and (
        cloud_instance.node_type != im_instance.instance_type
    ):
        return None
    return cloud_instance


def _get_worker_replica_id(
    im_instance: Optional[Instance],
    cloud_instance: Optional[CloudInstance],
) -> Optional[str]:
    """Return the cloud replica ID for a matching worker instance, if any."""
    matched = _match_worker_cloud_instance(im_instance, cloud_instance)
    return matched.replica_id if matched is not None and matched.replica_id else None


def _get_replica_key(
    inst: Optional[Instance],
    cloud_instances: Dict[CloudInstanceId, CloudInstance],
    replica_sizes: Dict[NodeType, int],
) -> Optional[Tuple[NodeType, str]]:
    """Return `(instance_type, replica_id)` for a labeled multi-worker instance."""
    if (
        inst is None
        or not inst.cloud_instance_id
        or inst.instance_type not in replica_sizes
    ):
        return None
    rid = _get_worker_replica_id(inst, cloud_instances.get(inst.cloud_instance_id))
    return (inst.instance_type, rid) if rid else None


def _has_teardown_history(inst: Instance) -> bool:
    """Return True if an IM record ever entered a teardown status."""
    return any(
        h.instance_status in _TEARDOWN_HISTORY_STATUSES for h in inst.status_history
    )


def _instance_cloud_start_ns(inst: Instance) -> int:
    """Earliest post-allocation status timestamp for incarnation ordering on a cid.

    Ignores QUEUED/REQUESTED timestamps because a launch batch can be requested
    before an earlier incarnation on the same cloud_instance_id starts.
    """
    times = [
        h.timestamp_ns
        for h in inst.status_history
        if h.instance_status not in _UNALLOCATED_INSTANCE_STATUSES
    ]
    if times:
        return min(times)
    if inst.status_history:
        return min(h.timestamp_ns for h in inst.status_history)
    return 0


def _propagate_cid_teardown_lids(
    nt: NodeType,
    tagged_rows: List[Tuple[int, str]],
    torn_down_lids: Set[Tuple[NodeType, str]],
    max_start_ns: Optional[int] = None,
) -> None:
    """Propagate untagged teardown on `(nt, cid)` to its latest prior tagged batch.

    When `max_start_ns` is given (for a TERMINATED untagged teardown row), only
    tagged incarnations whose cloud start is at or before `max_start_ns` are
    considered so a stale terminal row cannot poison a newer reused pod. When
    `max_start_ns` is None (for a live untagged teardown row), no newer
    incarnation can have allocated the same cloud_instance_id, so the latest
    tagged incarnation on that cid owns the pod.
    """
    candidates = [
        (start_ns, lid)
        for start_ns, lid in tagged_rows
        if max_start_ns is None or start_ns <= max_start_ns
    ]
    if not candidates:
        return
    latest_start_ns = max(start_ns for start_ns, _ in candidates)
    for start_ns, lid in candidates:
        if start_ns == latest_start_ns:
            torn_down_lids.add((nt, lid))
