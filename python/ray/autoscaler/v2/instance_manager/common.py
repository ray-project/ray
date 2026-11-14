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
class _MultiHostReplicaView:
    """Canonical multi-host replica state projection shared across allocation,
    scaling clamps, partial-replica drain filtering, instance termination, and
    V2 resource scheduling."""

    active_cids: Set[CloudInstanceId] = field(default_factory=set)
    live_cids: Set[Tuple[NodeType, CloudInstanceId]] = field(default_factory=set)
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
    vanished_cids_by_lid: Dict[Tuple[NodeType, str], Set[str]] = field(
        default_factory=lambda: defaultdict(set)
    )
    torn_down_lids: Set[Tuple[NodeType, str]] = field(default_factory=set)

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
    c_inst = cloud_instances.get(inst.cloud_instance_id)
    if (
        c_inst is not None
        and c_inst.replica_id
        and c_inst.node_kind != NodeKind.HEAD
        and (not c_inst.node_type or c_inst.node_type == inst.instance_type)
    ):
        return (inst.instance_type, c_inst.replica_id)
    return None


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


def _build_multihost_replica_view(
    im_instances: List[Instance],
    cloud_instances: Dict[CloudInstanceId, CloudInstance],
    replica_sizes: Dict[NodeType, int],
    status_overrides: Optional[Dict[str, "Instance.InstanceStatus"]] = None,
) -> _MultiHostReplicaView:
    """Project per-instance IM rows and cloud replica IDs into one canonical view."""
    view = _MultiHostReplicaView()
    overrides = status_overrides or {}

    active_nt_cids: Set[Tuple[NodeType, CloudInstanceId]] = set()
    for inst in im_instances:
        st = overrides.get(inst.instance_id, inst.status)
        if not inst.cloud_instance_id:
            continue
        if st in _ACTIVE_CID_STATUSES:
            view.active_cids.add(inst.cloud_instance_id)
            active_nt_cids.add((inst.instance_type, inst.cloud_instance_id))
        if st in _LIVE_CID_STATUSES:
            view.live_cids.add((inst.instance_type, inst.cloud_instance_id))

    if not replica_sizes:
        return view

    live_term_rows_by_rep_lid: Dict[
        Tuple[Tuple[NodeType, str], str], List[Instance]
    ] = defaultdict(list)
    missing_rows_by_lid: Dict[Tuple[NodeType, str], List[Instance]] = defaultdict(list)
    untagged_active_cids: Dict[Tuple[NodeType, str], Set[str]] = defaultdict(set)
    teardown_reps: Set[Tuple[NodeType, str]] = set()
    tagged_rows_by_cid: Dict[Tuple[NodeType, str], List[Tuple[int, str]]] = defaultdict(
        list
    )
    term_untagged_td_by_cid: Dict[Tuple[NodeType, str], List[int]] = defaultdict(list)
    live_untagged_td_cids: Set[Tuple[NodeType, str]] = set()
    term_tagged_rows: List[Tuple[NodeType, str, str, str, bool, int, Instance]] = []

    for inst in im_instances:
        if inst.node_kind == NodeKind.HEAD or inst.instance_type not in replica_sizes:
            continue
        nt = inst.instance_type
        st = overrides.get(inst.instance_id, inst.status)
        lid = inst.launch_request_id

        if inst.cloud_instance_id:
            c_inst = cloud_instances.get(inst.cloud_instance_id)
            if c_inst is not None and (
                c_inst.node_kind == NodeKind.HEAD
                or (c_inst.node_type and c_inst.node_type != nt)
            ):
                c_inst = None
            rid = c_inst.replica_id if c_inst is not None and c_inst.replica_id else ""
            ck = (nt, inst.cloud_instance_id)
            if st in _TERMINAL_INSTANCE_STATUSES:
                if not lid:
                    if _has_teardown_history(inst):
                        end_ns = max(
                            (h.timestamp_ns for h in inst.status_history), default=0
                        )
                        term_untagged_td_by_cid[ck].append(end_ns)
                    continue
                start_ns = _instance_cloud_start_ns(inst)
                tagged_rows_by_cid[ck].append((start_ns, lid))
                if _has_teardown_history(inst):
                    view.torn_down_lids.add((nt, lid))
                term_tagged_rows.append(
                    (
                        nt,
                        inst.cloud_instance_id,
                        rid,
                        lid,
                        c_inst is not None,
                        start_ns,
                        inst,
                    )
                )
                continue

            is_shadowed_stopped = (
                st == Instance.RAY_STOPPED
                and (nt, inst.cloud_instance_id) in active_nt_cids
            )
            in_teardown = not is_shadowed_stopped and st in (
                _TEARDOWN_INSTANCE_STATUSES | {Instance.RAY_STOPPING}
            )
            if lid:
                tagged_rows_by_cid[ck].append((_instance_cloud_start_ns(inst), lid))
                if in_teardown:
                    view.torn_down_lids.add((nt, lid))
            elif in_teardown:
                live_untagged_td_cids.add(ck)

            if c_inst is None:
                if lid and not is_shadowed_stopped:
                    missing_rows_by_lid[(nt, lid)].append(inst)
                continue

            if not rid:
                continue

            rk = (nt, rid)
            view.assigned_reps.add(rk)
            if lid:
                view.claimed_reps_by_lid[(nt, lid)].add(rid)
                view.rep_bound_cids_by_lid[(rk, lid)].add(inst.cloud_instance_id)
                view.rep_lids[rk].add(lid)

            # Ignore stale RAY_STOPPED rows shadowed by an active IM row on the
            # same cloud instance (e.g. after a raylet restart).
            if is_shadowed_stopped:
                continue

            if not lid:
                untagged_active_cids[rk].add(inst.cloud_instance_id)

            if in_teardown:
                teardown_reps.add(rk)
            if st in _DRAINING_INSTANCE_STATUSES | _TEARDOWN_INSTANCE_STATUSES:
                view.non_fillable_reps.add(rk)

            view.rep_assigned_cids[rk].add(inst.cloud_instance_id)
            if st in _OCCUPYING_CLOUD_STATUSES:
                view.rep_occupying[rk] += 1
            if st in _DRAINING_INSTANCE_STATUSES:
                view.draining_reps.add(rk)
                view.ray_running_reps.add(rk)
            elif st == Instance.RAY_RUNNING:
                view.ray_running_reps.add(rk)
        elif st == Instance.REQUESTED:
            view.requested_by_lid[(nt, lid or "")] += 1

    # Propagate teardown from already-TERMINATED untagged rows (e.g. after a
    # mid-boot raylet restart on an unlabeled pod or post-delete reconcile)
    # before reattaching live terminal rows, bounded by incarnation timestamps
    # so stale records cannot poison a newer reused cloud_instance_id.
    for ck, td_end_times in term_untagged_td_by_cid.items():
        tagged_rows = tagged_rows_by_cid.get(ck)
        if not tagged_rows:
            continue
        for td_end_ns in td_end_times:
            _propagate_cid_teardown_lids(
                ck[0], tagged_rows, view.torn_down_lids, max_start_ns=td_end_ns
            )

    # Classify terminal tagged rows after all incarnations on (nt, cid) are known
    # so a stale row on a reused cloud_instance_id cannot hijack a newer replica
    # or lose its vanished-slot accounting on its original replica.
    latest_start_by_cid = {
        ck: max(start_ns for start_ns, _ in rows)
        for ck, rows in tagged_rows_by_cid.items()
    }
    for nt, cid, rid, lid, has_c_inst, start_ns, inst in term_tagged_rows:
        if start_ns < latest_start_by_cid[(nt, cid)] or not has_c_inst:
            missing_rows_by_lid[(nt, lid)].append(inst)
        elif rid:
            live_term_rows_by_rep_lid[((nt, rid), lid)].append(inst)

    # Reattach terminal rows whose cloud pod is still alive (e.g. a raylet
    # restart mid-replica-boot where an untagged RAY_RUNNING row adopted the pod).
    for (rk, lid), rows in live_term_rows_by_rep_lid.items():
        nt, rid = rk
        term_cids = {r.cloud_instance_id for r in rows}
        is_td = (nt, lid) in view.torn_down_lids
        if rk in view.assigned_reps and (
            lid in view.rep_lids[rk]
            or (not is_td and bool(term_cids & untagged_active_cids[rk]))
        ):
            view.claimed_reps_by_lid[(nt, lid)].add(rid)
            view.rep_bound_cids_by_lid[(rk, lid)].update(term_cids)
            view.rep_lids[rk].add(lid)
            if is_td:
                view.non_fillable_reps.add(rk)

    # Propagate teardown of any live pod on a replica (including untagged rows
    # from raylet restarts or ray-node adoption) to all launch batches bound to
    # that replica so sibling REQUESTED slots fail instead of leaking.
    for rk in teardown_reps:
        for bound_lid in view.rep_lids[rk]:
            view.torn_down_lids.add((rk[0], bound_lid))

    # Propagate teardown of live untagged rows by (node_type, cloud_instance_id)
    # so unlabeled legacy pods and pods whose cloud instance already vanished
    # before _handle_cloud_instance_terminated still mark their launch batch.
    for ck in live_untagged_td_cids:
        tagged_rows = tagged_rows_by_cid.get(ck)
        if tagged_rows:
            _propagate_cid_teardown_lids(ck[0], tagged_rows, view.torn_down_lids)

    # Reattach pods that disappeared from cloud_instances to their per-replica
    # launch batch: onto its live replica if any pod remains alive, or into
    # vanished_cids_by_lid if all bound pods of a non-torn-down batch vanished
    # mid-boot before all `size` slots allocated.
    for (nt, lid), rows in missing_rows_by_lid.items():
        missing_cids = {r.cloud_instance_id for r in rows}
        is_td = (nt, lid) in view.torn_down_lids
        claimed_rids = view.claimed_reps_by_lid.get((nt, lid), set())
        if claimed_rids:
            for rid in claimed_rids:
                rk = (nt, rid)
                view.rep_bound_cids_by_lid[(rk, lid)].update(missing_cids)
                if is_td:
                    view.non_fillable_reps.add(rk)
        elif (
            not is_td
            and view.requested_by_lid[(nt, lid)] > 0
            and len(missing_cids) < replica_sizes[nt]
        ):
            view.vanished_cids_by_lid[(nt, lid)].update(missing_cids)

    return view
