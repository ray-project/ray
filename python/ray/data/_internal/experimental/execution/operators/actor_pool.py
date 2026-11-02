"""The actor-only backend's actor pool.

``_ExperimentalActorPool`` tracks per-actor scheduling state for
``ExperimentalActorPoolMapOperator`` and owns:

- **Actor selection**: prefers an actor on the bundle's producing node
  (per-node locality heaps) and falls back to the globally least-loaded actor
  (``_ExperimentalActorRank``: fewest tasks in flight, earliest last
  submission).

- **Per-actor budgets**: on the input side, an actor accepts tasks while it
  has fewer than ``max_tasks_in_flight`` in flight
  (``_is_available_for_scheduling``). On the output side each actor carries
  bytes/blocks budgets that bound how much of its generator output may be
  pulled; the bytes budget is derived per node from object-store capacity,
  clamped into
  [``PER_ACTOR_OBJECT_STORE_MIN_BYTES``, ``PER_ACTOR_OBJECT_STORE_MAX_BYTES``],
  and can be raised at runtime by ``upgrade_output_limit`` -- the deadlock
  hatch when every task is blocked on output budget.

  the resource bank is in charge of tracking per-actor object store budgets, but
  the actor pool is in charge of enforcing limits on those budgets + adding additional budgets

- **Draining**: a downscaled actor is marked ``DRAINING``: it takes no new
  tasks but keeps its node resources until it has no in-flight tasks AND the
  ResourceBank no longer counts any output blocks against it
  (``_is_fully_drained``), at which point ``process_draining_actors`` tears it
  down. A drain can end early (the actor died), or be cancelled (revived by a
  reclaiming upscale, or by a Ray-core restart of an actor that still owes
  work). ``draining_generation`` lets the operator cheaply detect changes to
  the draining set.
"""

from __future__ import annotations

import logging
import math
import time
import uuid
from collections import Counter, defaultdict
from typing import (
    Callable,
    DefaultDict,
    Dict,
    List,
    Optional,
    Set,
    Tuple,
)

from typing_extensions import override

import ray
from ray import ObjectRef
from ray.actor import ActorHandle
from ray.data._internal.actor_autoscaler import AutoscalingActorPool
from ray.data._internal.actor_autoscaler.autoscaling_actor_pool import (
    ActorPoolScalingRequest,
    AutoscalingActorConfig,
)
from ray.data._internal.execution.execution_flags import (
    BLOCK_SIZE_TO_OUTPUT_BYTES_RATIO,
    DRAIN_STALL_WARN_S,
    OBJECT_STORE_OUTPUT_FRACTION,
    PENDING_ACTOR_EXPIRY_S,
    PER_ACTOR_OBJECT_STORE_MAX_BYTES,
    PER_ACTOR_OBJECT_STORE_MIN_BYTES,
    REFRESH_ACTOR_STATE,
)
from ray.data._internal.execution.interfaces import (
    ExecutionResources,
    NodeIdStr,
    RefBundle,
)
from ray.data._internal.execution.interfaces.common import ActorStatus, LogicalActorId
from ray.data._internal.execution.interfaces.execution_options import safe_or
from ray.data._internal.execution.resource_bank import (
    ResourceBankBase,
    is_cross_node_transfer,
)
from ray.data._internal.execution.util import memory_string
from ray.data._internal.experimental.execution.actor_only_op_metrics import (
    _ActorOnlyOpMetrics,
)
from ray.data._internal.experimental.execution.operators.actor_pool_models import (
    _ACTOR_STATE_ALIVE,
    _ACTOR_STATE_DEAD,
    _ACTOR_STATE_RESTARTING,
    DrainReason,
    ExperimentalActorPoolScalingRequest,
    _DrainingActorInfo,
    _ExperimentalActorRank,
    _ExperimentalActorState,
    _PendingActorInfo,
)
from ray.data._internal.util import MiB
from ray.data._internal.utils.cached_ray_internals import (
    get_actor_locations,
    get_draining_nodes,
)
from ray.data._internal.utils.heapdict import heapdict
from ray.util.debug import log_once
from ray.util.scheduling_strategies import (
    NodeAffinitySchedulingStrategy,
)

logger = logging.getLogger(__name__)


class _ExperimentalActorPool(AutoscalingActorPool):
    """A pool of actors for map task execution.

    This class is in charge of tracking the number of in-flight tasks per
    actor, providing the least heavily loaded actor to the operator, and
    draining actors when the pool scales down (see the module docstring for
    the drain lifecycle).
    """

    _ACTOR_POOL_SCALE_DOWN_DEBOUNCE_PERIOD_S = 3

    def __init__(
        self,
        create_actor_fn: "Callable[..., Tuple[ActorHandle, ObjectRef, ExecutionResources]]",
        config: AutoscalingActorConfig,
        map_worker_cls_name: str = "MapWorker",
        debounce_period_s: int = _ACTOR_POOL_SCALE_DOWN_DEBOUNCE_PERIOD_S,
    ):
        """Initialize the actor pool.

        Args:
            create_actor_fn: Callable that takes key-value labels, a logical actor
                ID, and an optional scheduling strategy, then creates an actor.
                Returns the actor handle, a reference to the actor's node ID, and
                the actor's resource usage.
            config: Configuration for the autoscaling actor pool, including
                min/max/initial pool sizes, concurrency, and resource usage.
            map_worker_cls_name: Name of the map worker class for logging
                purposes.
            debounce_period_s: Debounce period for scaling down after scaling
                up.
        """
        super().__init__(config=config)
        # Actor-only metrics the pool records drain outcomes on, injected by
        # the operator via ``set_metrics`` (see ``_ActorOnlyOpMetrics``).
        self._metrics: Optional[_ActorOnlyOpMetrics] = None
        self._metrics_op_tag: Optional[str] = None
        self._resource_bank: Optional[ResourceBankBase] = None
        self._create_actor_fn = create_actor_fn
        self._map_worker_cls_name = map_worker_cls_name
        self._debounce_period_s = debounce_period_s
        # Timestamp of the last scale up action; drives the downscale debounce.
        self._last_upscaled_at: Optional[float] = None
        self._last_downscaling_debounce_warning_ts: Optional[float] = None
        # Actors that have started running, including alive and restarting actors.
        self._running_actors: Dict[ActorHandle, _ExperimentalActorState] = {}
        # Actors that are not yet ready (still pending creation).
        self._pending_actors: Dict[ObjectRef, ActorHandle] = {}
        # Placement bookkeeping for pending actors, parallel to
        # ``_pending_actors``.
        self._pending_actor_info: Dict[ObjectRef, _PendingActorInfo] = {}
        # Draining state machines, keyed by actor. Entries also
        # remain in ``_running_actors`` (with ``status == ActorStatus.
        # DRAINING``) until the actor is observed dead.
        self._draining_actors: Dict[ActorHandle, _DrainingActorInfo] = {}
        # Bumped on every change to the set of draining actors (add on
        # draining start, remove on reclaim/release). A monotonic Python int
        # (unbounded, never wraps): readers keep their last-seen value and skip
        # work while it is unchanged -- e.g. the operator's output-queue
        # priority sync only reconciles when this moves.
        self.draining_generation: int = 0
        # Debounce clock for the stalled-drain WARNING (see
        # ``process_draining_actors``): at most one line per
        # ``DRAIN_STALL_WARN_S`` for the whole pool.
        self._last_drain_stall_warn_ts: float = 0.0
        # Map from actor handle to its logical ID.
        self._actor_to_logical_id: Dict[ActorHandle, LogicalActorId] = {}
        # Per-actor resource usage, needed because ray_remote_args_fn can
        # produce different resources for each actor.
        self._actor_resource_usage: Dict[ActorHandle, ExecutionResources] = {}
        # Cached aggregate resource counters.
        self._total_usage = ExecutionResources.zero()
        self._pending_or_restarting_usage = ExecutionResources.zero()
        # Resources this pool occupies (or has promised) per node. Running,
        # pending and restarting actors all count: they hold their resources
        # until they are actually dead. Maintained incrementally at the
        # create/ready/death sites (``_add_committed_usage`` /
        # ``_remove_committed_usage``) so readers pay nothing per tick. Running
        # actors are counted at their actual location; pending actors at their
        # ``target_node_id`` (only when one was set -- untargeted pendings
        # contribute nothing until they become running). Public: the operator's
        # ``committed_usage_by_node()`` hands it out.
        self.committed_usage_by_node: Dict[NodeIdStr, ExecutionResources] = {}
        # Running actors by status (IDLE / ACTIVE / RESTARTING / DRAINING).
        # ``_set_status`` is the only writer of a running actor's status, and
        # ``_register_running_actor`` / ``_release_running_actor`` are the only
        # places an actor enters or leaves the histogram, so the
        # ``num_*_actors()`` views cannot drift from ``_running_actors``.
        self._status_counts: Counter[ActorStatus] = Counter()
        self._total_num_tasks_in_flight: int = 0

        # Global fallback heap of all alive actors that may accept another
        # task (see ``_is_available_for_scheduling``: under the per-actor
        # tasks-in-flight cap).
        self._available_actors_to_tasks_fallback_heap = heapdict[
            ActorHandle, _ExperimentalActorRank
        ]()

        # ALIVE node -> per-node heap of ALIVE actors that may accept another
        # task. Same membership gate as the fallback heap; this one indexes by
        # node for locality-preferring selection.
        self._alive_node_to_available_actor_heap: DefaultDict[
            NodeIdStr, heapdict[ActorHandle, _ExperimentalActorRank]
        ] = defaultdict(heapdict[ActorHandle, _ExperimentalActorRank])

        # NOTE: Only contains ALIVE actors.
        self._logical_id_to_actor: Dict[LogicalActorId, ActorHandle] = {}

        # NOTE: Only contains ALIVE actors.
        self._node_to_actor_states: Dict[
            NodeIdStr, Dict[LogicalActorId, _ExperimentalActorState]
        ] = {}
        # Nodes hosting at least one actor with ``num_tasks_in_flight > 0``.
        self._node_to_active_actor_count: Dict[NodeIdStr, int] = {}

    def set_resource_bank(self, resource_bank: ResourceBankBase) -> None:
        self._resource_bank = resource_bank

    def set_metrics(self, metrics: _ActorOnlyOpMetrics, op_tag: str) -> None:
        """Inject the actor-only metrics the pool records drain outcomes on."""
        self._metrics = metrics
        self._metrics_op_tag = op_tag

    @property
    def _op_tag(self) -> str:
        assert self._metrics_op_tag is not None, "set_metrics() provides the op tag"
        return self._metrics_op_tag

    @property
    def resource_bank(self) -> ResourceBankBase:
        assert self._resource_bank is not None
        return self._resource_bank

    @property
    def map_worker_cls_name(self) -> str:
        return self._map_worker_cls_name

    def _set_status(self, state: _ExperimentalActorState, status: ActorStatus) -> bool:
        """Move a running actor to ``status``, keeping ``_status_counts`` in
        step; returns whether the status changed."""
        if state.status is status:
            return False
        self._status_counts[state.status] -= 1
        self._status_counts[status] += 1
        state.status = status
        return True

    def _is_available_for_scheduling(self, state: _ExperimentalActorState) -> bool:
        """Whether this actor may accept another task. Used by both scheduling
        heaps (per-node locality and global fallback).

        A DRAINING actor never may (the draining gate).
        Otherwise the actor is admitted while it has fewer than
        ``max_tasks_in_flight`` tasks in flight.
        """
        if state.status == ActorStatus.DRAINING:
            return False
        return state.num_tasks_in_flight < state.max_tasks_in_flight

    def _remove_from_heaps(self, actor: ActorHandle, node_id: NodeIdStr) -> None:
        """Drop the actor from both scheduling heaps if present."""
        if actor in self._available_actors_to_tasks_fallback_heap:
            del self._available_actors_to_tasks_fallback_heap[actor]
        node_heap = self._alive_node_to_available_actor_heap.get(node_id)
        if node_heap is not None and actor in node_heap:
            del node_heap[actor]

    def _sync_actor_to_heaps(
        self, actor: ActorHandle, state: _ExperimentalActorState
    ) -> None:
        """Reconcile ``actor``'s membership in both scheduling heaps.

        Both heaps share the ``_is_available_for_scheduling`` gate, so their
        membership is identical; they differ only in what they index (per-node
        for locality vs. global for fallback).

        Restarting actors stay out of both heaps regardless of capacity;
        ``refresh_actor_state`` re-syncs them once they are ALIVE again (see
        ``_update_rank``).
        """
        if state.status == ActorStatus.RESTARTING:
            self._remove_from_heaps(actor, state.actor_location)
            return
        if self._is_available_for_scheduling(state):
            rank = _ExperimentalActorRank(
                num_tasks_in_flight=state.num_tasks_in_flight,
                latest_task_submission_ts=state.latest_task_submission_ts,
            )
            self._alive_node_to_available_actor_heap[state.actor_location][actor] = rank
            self._available_actors_to_tasks_fallback_heap[actor] = rank
        else:
            node_heap = self._alive_node_to_available_actor_heap.get(
                state.actor_location
            )
            if node_heap is not None and actor in node_heap:
                del node_heap[actor]
            if actor in self._available_actors_to_tasks_fallback_heap:
                del self._available_actors_to_tasks_fallback_heap[actor]

    def _mark_node_actor_active(self, state: _ExperimentalActorState) -> None:
        """Record that one more actor on ``state.actor_location`` has in-flight
        tasks.

        Invariant: an actor is counted at its ``actor_location`` iff
        ``num_tasks_in_flight > 0``. ``_relocate_actor`` is the only writer of
        ``actor_location`` and re-marks the actor across the move, so the
        paired decrement can always read the location.
        """
        node_id = state.actor_location
        self._node_to_active_actor_count[node_id] = (
            self._node_to_active_actor_count.get(node_id, 0) + 1
        )

    def _mark_node_actor_inactive(self, state: _ExperimentalActorState) -> None:
        """Record that one actor on ``state.actor_location`` no longer has
        in-flight tasks."""
        node_id = state.actor_location
        count = self._node_to_active_actor_count[node_id] - 1
        if count == 0:
            del self._node_to_active_actor_count[node_id]
        else:
            self._node_to_active_actor_count[node_id] = count

    def _relocate_actor(
        self, actor: ActorHandle, state: _ExperimentalActorState, node_id: NodeIdStr
    ) -> None:
        """Move an actor's node-keyed bookkeeping after it landed on a new node.

        Ray core restarts a lost actor wherever it fits, so ``actor_location``
        changes mid-run. Several structures are keyed by it but are only written
        when an actor becomes ready or is released, which leaves them pointing at
        the node the actor has left:

          - the committed-usage ledger. Release debits the node the actor
            lives on *now*, so a stale entry leaves the old node with a charge
            nothing can clear: capacity accounting reads a permanent overdraft
            on a node that may not even exist.
          - ``_node_to_actor_states``, which backs ``nodes()`` and per-node
            victim selection.
          - the per-node count of actors with in-flight tasks.

        The scheduling heaps need no help here: ``refresh_actor_state`` clears
        and rebuilds them from ``actor_location`` on every tick.

        A no-op when the location is unchanged, so callers can invoke it on every
        refresh.
        """
        old_node_id = state.actor_location
        if node_id == old_node_id:
            return

        usage = self._actor_resource_usage.get(actor)
        if usage is not None:
            self._remove_committed_usage(old_node_id, usage)
            self._add_committed_usage(node_id, usage)

        logical_id = state.logical_id
        old_states = self._node_to_actor_states.get(old_node_id)
        if old_states is not None:
            old_states.pop(logical_id, None)
            if not old_states:
                del self._node_to_actor_states[old_node_id]
        self._node_to_actor_states.setdefault(node_id, {})[logical_id] = state

        # Re-mark rather than edit the counter directly, so the pairing
        # invariant holds: the decrement reads the old location, the increment
        # the new one.
        was_active = state.num_tasks_in_flight > 0
        if was_active:
            self._mark_node_actor_inactive(state)
        state.actor_location = node_id
        if was_active:
            self._mark_node_actor_active(state)

    def nodes(self) -> Set[NodeIdStr]:
        """Nodes hosting any alive actor in this pool (idle or active)."""
        return set(self._node_to_actor_states.keys())

    def active_nodes(self) -> Set[NodeIdStr]:
        """Nodes hosting at least one actor with in-flight tasks."""
        return set(self._node_to_active_actor_count.keys())

    # === Overriding methods of AutoscalingActorPool ===

    @override
    def num_running_actors(self) -> int:
        # NOTE: Draining actors are excluded -- they no longer accept work
        # and are counted separately via ``num_draining_actors`` so sizing
        # decisions see them as "going away but still holding resources".
        return len(self._running_actors) - self.num_draining_actors()

    @override
    def num_draining_actors(self) -> int:
        """Actors draining prior to release."""
        return self._status_counts[ActorStatus.DRAINING]

    @override
    def num_restarting_actors(self) -> int:
        """Running actors Ray core is currently restarting."""
        return self._status_counts[ActorStatus.RESTARTING]

    @override
    def num_active_actors(self) -> int:
        """Running actors with tasks in flight, DRAINING ones included (they
        still hold work even though they take no new tasks). Read off the
        per-node active map, whose invariant is "counted iff
        ``num_tasks_in_flight > 0``"."""
        return sum(self._node_to_active_actor_count.values())

    @override
    def num_idle_actors(self) -> int:
        # IDLE = schedulable with nothing in flight. DRAINING and RESTARTING
        # actors are not schedulable, so they are not idle.
        return self._status_counts[ActorStatus.IDLE]

    @override
    def num_pending_actors(self) -> int:
        return len(self._pending_actors)

    @override
    def num_tasks_in_flight(self) -> int:
        return self._total_num_tasks_in_flight

    @override
    def scale(self, req: ActorPoolScalingRequest) -> Optional[int]:
        """Apply a scaling request; returns the *applied* delta.

        The return value reports what actually happened (created + reclaimed on
        upscale, released on downscale, negative), which can be smaller in
        magnitude than ``req.delta`` -- e.g. a reclaim shortfall, or victims
        that disappeared between planning and apply. Callers keeping a ledger
        must record the returned value, not the request.
        """
        # Normalize legacy requests (DefaultActorAutoscaler / start() without
        # placement-resolved requests). Empty targets mean untargeted behavior.
        req = ExperimentalActorPoolScalingRequest.from_base(req)

        # Verify request could be applied
        if not self._can_apply_request(req):
            return 0

        if req.delta > 0:
            return self._scale_up(req)
        elif req.delta < 0:
            return self._scale_down(req)
        return None

    @override
    def refresh_actor_state(self):
        if not REFRESH_ACTOR_STATE:
            # Even with the (expensive) full refresh disabled, draining actors
            # must still be observed: one killed before its drain completes
            # (node loss) must be seen as DEAD so the pool releases it.
            # Otherwise it stays in the draining set forever and the operator
            # never sees its resources freed.
            for actor in list(self._draining_actors):
                self._update_running_actor_state(actor)
            return
        self._alive_node_to_available_actor_heap.clear()
        self._available_actors_to_tasks_fallback_heap.clear()
        # NOTE: Copy the keys -- observing a draining actor as DEAD
        # releases it from ``_running_actors`` mid-iteration.
        for actor in list(self._running_actors):
            self._update_running_actor_state(actor)

    @override
    def on_task_submitted(self, actor: ActorHandle, input_bundle: RefBundle):
        state = self._running_actors[actor]
        state.num_tasks_in_flight += 1
        state.num_input_bytes_in_flight += input_bundle.size_bytes()
        state.latest_task_submission_ts = time.perf_counter()
        self._total_num_tasks_in_flight += 1

        # Transfer ownership of primary (local) inputs to this actor by
        # withdrawing them from the ResourceBank now: the producing actor's
        # outstanding output stats drop immediately rather than waiting for
        # the bundle to be fully consumed when the downstream task is done.
        # The withdraw at task-done is idempotent, so it'll be a no-op
        # for these refs.
        target_node_id = state.actor_location
        for entry in input_bundle.blocks:
            if not is_cross_node_transfer(
                bm=entry.metadata, target_node_id=target_node_id
            ):
                self.resource_bank.on_block_consumed(input_ref=entry.ref)

        if state.num_tasks_in_flight == 1:
            self._mark_node_actor_active(state)
            # 0 -> 1 in flight is IDLE -> ACTIVE: only IDLE actors are
            # selectable (DRAINING and RESTARTING stay out of the heaps).
            assert state.status is ActorStatus.IDLE, state.status
            self._set_status(state, ActorStatus.ACTIVE)

        # Selected by `select_actors`, so it must have been available and thus
        # present in the scheduling heaps (both share the same membership).
        assert actor in self._alive_node_to_available_actor_heap[state.actor_location]
        self._sync_actor_to_heaps(actor=actor, state=state)

    @override
    def get_actor_location(self, actor: ActorHandle) -> NodeIdStr:
        return self._running_actors[actor].actor_location

    @override
    def shutdown(self, force: bool = False):
        """Kills all actors, including running/active actors.

        This is called once the operator is shutting down.
        """
        self._release_pending_actors(force=force)
        self._release_running_actors(force=force)

    @override
    def pending_to_running(self, ready_ref: ray.ObjectRef) -> Optional[ActorHandle]:
        """Mark the actor corresponding to the provided ready future as running,
        making the actor pickable.

        Args:
            ready_ref: The ready future for the actor that we wish to mark as
                running.

        Returns:
            The actor handle of the pending/now ready actor. Otherwise, returns
            `None` if actor has already been killed.

        Raises:
            RayError: If the actor initialization failed. The actor is cleaned up
                from internal tracking before re-raising.
        """
        if ready_ref not in self._pending_actors:
            # The actor has been removed from the pool before becoming running.
            return None
        actor = self._pending_actors.pop(ready_ref)
        pending_info = self._pending_actor_info.pop(ready_ref, None)
        # Target node this pending actor was committed to (None if untargeted,
        # in which case it contributed nothing to the committed-usage map).
        pending_target_node = (
            pending_info.target_node_id if pending_info is not None else None
        )
        try:
            actor_location: NodeIdStr = ray.get(ready_ref)
            assert actor_location is not None
            logical_id = self.get_actor_logical_id(actor)
            # Building the state also derives the actor's per-actor
            # output-bytes budget from its landing node's capacity. Done inside
            # this try so a failure there (e.g. the actor landed off-target on
            # a node the bank cannot resolve) is cleaned up exactly like an
            # init failure, rather than leaking pending/committed usage.
            state = self._make_running_state(logical_id, actor_location)
        except Exception:
            self._rollback_failed_pending(actor, pending_target_node)
            raise
        self._register_running_actor(actor, state, pending_target_node)
        return actor

    @override
    def get_pending_actor_refs(self) -> List[ObjectRef]:
        return list(self._pending_actors.keys())

    @override
    def select_actors(
        self,
        bundle: Optional[RefBundle] = None,
        actor_locality_enabled: bool = False,
    ) -> Optional[ActorHandle]:
        """Select an actor to process the given bundle.

        When ``bundle`` is ``None``, returns any available actor with spare
        capacity (used by ``can_schedule_task`` to probe schedulability).
        When ``bundle`` is provided, returns the best actor for that bundle
        (considering locality when ``actor_locality_enabled`` is True).

        This method peeks (does not pop) from the heap, so
        ``on_task_submitted()`` must be called for the returned actor before
        the next ``select_actors()`` call. Otherwise the same actor will be
        selected repeatedly. The caller in
        ``ExperimentalActorPoolMapOperator.launch_task()`` upholds this
        contract.

        Args:
            bundle: The bundle to schedule. When ``None``, returns any
                available actor with spare capacity.
            actor_locality_enabled: Whether to consider data locality
                when selecting an actor.

        Returns:
            An actor handle if an actor with capacity is available, otherwise
            ``None``.
        """

        # Locality path first: prefer an actor on a node that already holds
        # the bundle's blocks.
        if bundle is not None and actor_locality_enabled:
            target_actor = self._find_actor_with_locality(bundle)
            if target_actor is not None:
                return target_actor

        # Fallback (non-local) path: pick the actor with the lowest rank.
        # Both heaps share the ``_is_available_for_scheduling`` membership
        # gate, so an empty fallback heap means no actor anywhere has capacity.
        if not self._available_actors_to_tasks_fallback_heap:
            return None
        (
            fallback_actor,
            _fallback_rank,
        ) = self._available_actors_to_tasks_fallback_heap.peekitem()
        return fallback_actor

    @override
    def on_task_completed(self, actor: ActorHandle, input_bundle: RefBundle):
        """Called when a task completes. Returns the provided actor to the pool."""
        if actor not in self._running_actors:
            # The actor was already released while this task was still in
            # flight (e.g. an external death -- OOM / preemption / lost node).
            # Releasing the actor already reversed its in-flight accounting,
            # so there is nothing more to do here.
            return
        state = self._running_actors[actor]
        assert state.num_tasks_in_flight > 0
        state.num_tasks_in_flight -= 1
        state.num_input_bytes_in_flight -= input_bundle.size_bytes()
        assert state.num_input_bytes_in_flight >= 0, (
            f"num_input_bytes_in_flight underflow on actor={actor}: "
            f"{state.num_input_bytes_in_flight}"
        )

        self._total_num_tasks_in_flight -= 1
        if state.num_tasks_in_flight == 0:
            self._mark_node_actor_inactive(state)
            # 1 -> 0 in flight: ACTIVE -> IDLE. A DRAINING or RESTARTING actor
            # finishing its last task keeps its status; neither is schedulable.
            if state.status is ActorStatus.ACTIVE:
                self._set_status(state, ActorStatus.IDLE)

        # A task can complete (as a failure) while its actor is RESTARTING --
        # the accounting above must still drain so the ALIVE transition derives
        # the right status from num_tasks_in_flight. The heap sync below keeps
        # restarting actors out of the scheduling heaps.

        # Reconcile both heaps: the completed task decreased
        # ``num_tasks_in_flight``, which may make the actor available for
        # scheduling again.
        self._sync_actor_to_heaps(actor, state)

    @override
    def get_actor_logical_id(self, actor: ActorHandle) -> LogicalActorId:
        return self._actor_to_logical_id[actor]

    @override
    def get_logical_ids(self) -> List[LogicalActorId]:
        """Get the logical IDs for pending and running actors in the actor pool.

        We can't use Ray Core actor IDs because we need to identify actors by labels,
        but labels must be set before creation, and actor IDs aren't available until
        after.
        """
        return list(self._actor_to_logical_id.values())

    @override
    def current_logical_usage(self) -> ExecutionResources:
        return self._total_usage

    @override
    def pending_logical_usage(self) -> ExecutionResources:
        return self._pending_or_restarting_usage

    # === End of overriding methods of AutoscalingActorPool ===

    def _scale_up(self, req: ExperimentalActorPoolScalingRequest) -> int:
        """Revive reclaimable draining actors first, then create new ones.

        Reviving is free capacity-wise (a draining actor already holds its
        node's resources) and skips actor startup. Only the genuinely-new
        actors carry target nodes.
        """
        reclaimed = self.reclaim_draining_actors(req.num_to_reclaim)
        target_nodes = req.target_nodes_to_scale_actors_on
        # One node id per NEW actor; ids may repeat to place several actors
        # on the same node (e.g. ("n1", "n1", "n1") -> 3 actors on n1).
        # Empty means untargeted (Ray-core-scheduled) creation. A reclaim
        # shortfall is NOT topped up on the targeted path: the shortfall
        # actors were planned onto specific nodes we cannot invent
        # placements for -- the returned count reports what happened.
        num_new = len(target_nodes) if target_nodes else req.delta - reclaimed
        assert (
            not target_nodes or len(target_nodes) == req.delta - req.num_to_reclaim
        ), (
            f"Expected len(target_nodes) == delta - num_to_reclaim (one node "
            f"id per genuinely-new actor; repeats allowed): delta={req.delta}, "
            f"num_to_reclaim={req.num_to_reclaim}, target_nodes={target_nodes}"
        )

        logger.debug(
            f"Scaling up {self._map_worker_cls_name} actor pool by {req.delta} "
            f"(reclaimed={reclaimed}, new={num_new}, reason={req.reason}, "
            f"{self.get_actor_info()})"
        )

        for i in range(num_new):
            target_node_id = target_nodes[i] if target_nodes else None
            # Untargeted creation (no target node): default to Ray-core
            # scheduling. NodeAffinity is only used when the caller resolved
            # a specific target node.
            scheduling_strategy = (
                NodeAffinitySchedulingStrategy(target_node_id, soft=True)
                if target_node_id is not None
                else None
            )
            actor, ready_ref, resource_usage = self._create_actor(
                scheduling_strategy=scheduling_strategy
            )
            self._add_pending_actor(
                actor, ready_ref, resource_usage, target_node_id=target_node_id
            )

        # Capture last scale up timestamp
        self._last_upscaled_at = time.time()

        applied = reclaimed + num_new
        if applied != req.delta:
            logger.warning(
                "Upscale applied partially: requested=%d reclaimed=%d created=%d",
                req.delta,
                reclaimed,
                num_new,
            )
        return applied

    def _scale_down(self, req: ExperimentalActorPoolScalingRequest) -> int:
        """Downscale victims; returns the (negative) applied delta.

        A pending victim is cancelled (and released) immediately; a running
        victim only BEGINS draining here -- it is released later by
        ``process_draining_actors`` once fully drained. The returned delta
        counts victims acted on, not actors released.
        """
        target_num_actors = abs(req.delta)

        if req.actor_ids_to_downscale:
            assert len(req.actor_ids_to_downscale) == target_num_actors, (
                f"Expected one victim per downscaled actor "
                f"(delta={req.delta}, victims={req.actor_ids_to_downscale})"
            )
            num_downscaled = self._downscale_exact_victims(req)
        elif req.victim_node is not None:
            num_downscaled = self._downscale_on_node(req)
        else:
            num_downscaled = self._downscale_inactive(target_num_actors)

        if num_downscaled > 0:
            logger.debug(
                f"Scaled down {self._map_worker_cls_name} actor pool by "
                f"{num_downscaled} (reason={req.reason}; {self.get_actor_info()})"
            )

        return -num_downscaled

    def _downscale_exact_victims(self, req: ExperimentalActorPoolScalingRequest) -> int:
        """Downscale the exact victims the caller chose."""
        num_downscaled = 0
        for logical_id in req.actor_ids_to_downscale:
            if self._downscale_actor_by_id(logical_id, reclaimable=req.reclaimable):
                num_downscaled += 1
        return num_downscaled

    def _downscale_on_node(self, req: ExperimentalActorPoolScalingRequest) -> int:
        """Pick victims on ``req.victim_node`` from live state, cheapest drain
        first. May act on fewer than asked if the node's pool shrank since the
        request was planned."""
        assert req.victim_node is not None
        states = self._node_to_actor_states.get(req.victim_node, {})
        # Cheapest drain first: in-flight tasks dominate drain time (each must
        # run to completion and typically adds more outstanding outputs when it
        # finishes), so rank by fewest tasks, then fewest output blocks the
        # ResourceBank still counts against the actor (already-queued blocks
        # drain fast -- the output queue serves draining actors first). Idle
        # actors sort first automatically (0 tasks).
        ranked = sorted(
            (
                (
                    state.num_tasks_in_flight,
                    self._num_outstanding_outputs(state),
                    logical_id,
                )
                for logical_id, state in states.items()
                if state.status != ActorStatus.DRAINING
            ),
        )
        num_downscaled = 0
        for _, _, logical_id in ranked[: abs(req.delta)]:
            if self._downscale_actor_by_id(logical_id, reclaimable=req.reclaimable):
                num_downscaled += 1
        return num_downscaled

    def _downscale_inactive(self, count: int) -> int:
        """Untargeted downscale: drop pendings first, then drain idle actors."""
        num_downscaled = 0
        for _ in range(count):
            if self._remove_inactive_actor():
                num_downscaled += 1
        return num_downscaled

    def _make_running_state(
        self, logical_id: LogicalActorId, actor_location: NodeIdStr
    ) -> _ExperimentalActorState:
        """Build the initial state for an actor that just became ready."""
        default_max_num_output_bytes = self._derive_default_max_num_output_bytes(
            actor_location
        )
        # From here on the per-actor caps are concrete: the bytes budget was
        # configured or derived from the node's object store, and the block cap
        # is configured (unbounded only when the pool is built without one).
        assert (
            math.isfinite(default_max_num_output_bytes)
            and default_max_num_output_bytes > 0
        ), f"invalid per-actor output-bytes budget {default_max_num_output_bytes}"
        max_num_outputs = safe_or(self._config.max_num_outputs_per_actor, float("inf"))
        assert max_num_outputs > 0, f"invalid per-actor output cap {max_num_outputs}"
        return _ExperimentalActorState(
            logical_id=logical_id,
            num_tasks_in_flight=0,
            num_input_bytes_in_flight=0,
            actor_location=actor_location,
            max_num_outputs=max_num_outputs,
            max_num_output_bytes=default_max_num_output_bytes,
            default_max_num_output_bytes=default_max_num_output_bytes,
            max_tasks_in_flight=self.max_tasks_in_flight_per_actor(),
            # NOTE: We assume any actor that goes from pending to running is IDLE
            status=ActorStatus.IDLE,
            latest_task_submission_ts=0,
        )

    def _rollback_failed_pending(
        self, actor: ActorHandle, pending_target_node: Optional[NodeIdStr]
    ) -> None:
        """Undo a failed pending actor's bookkeeping.

        Actor init failed, or its per-actor output budget couldn't be derived.
        Clean up the actor from ``_actor_to_logical_id`` (for all exceptions,
        not just RayError) to prevent memory leaks where dead actor handles
        remain, and roll back its pending/committed usage.
        """
        usage = self._actor_resource_usage.pop(actor)
        self._total_usage = self._total_usage.subtract(usage)
        self._pending_or_restarting_usage = self._pending_or_restarting_usage.subtract(
            usage
        )
        if pending_target_node is not None:
            self._remove_committed_usage(pending_target_node, usage)
        self._actor_to_logical_id.pop(actor, None)

    def _register_running_actor(
        self,
        actor: ActorHandle,
        state: _ExperimentalActorState,
        pending_target_node: Optional[NodeIdStr],
    ) -> None:
        """Install a just-ready actor into the pool's running-actor indexes."""
        self._running_actors[actor] = state
        # New running actors join the pool IDLE with nothing in flight.
        assert state.status is ActorStatus.IDLE and state.num_tasks_in_flight == 0
        self._status_counts[state.status] += 1
        # Mirror the new actor into the logical-id and node indexes. These
        # two maps are the source of truth for "which actors / nodes does
        # the pool currently keep alive" -- maintained explicitly here
        # (not via defaultdict-on-access) so cluster autoscaling and the
        # output-limit upgrader can trust their keysets.
        self._logical_id_to_actor[state.logical_id] = actor
        self._node_to_actor_states.setdefault(state.actor_location, {})[
            state.logical_id
        ] = state
        # Actor is no longer pending -- subtract from pending usage.
        usage = self._actor_resource_usage[actor]
        self._pending_or_restarting_usage = self._pending_or_restarting_usage.subtract(
            usage
        )
        # Re-attribute committed usage from the (optional) pending target node to
        # the actual landing node now that placement is resolved.
        if pending_target_node is not None:
            self._remove_committed_usage(pending_target_node, usage)
        self._add_committed_usage(state.actor_location, usage)
        self._sync_actor_to_heaps(actor, state)

    def _can_apply_request(self, req: ActorPoolScalingRequest) -> bool:
        """Returns whether Actor Pool is able to execute scaling request"""

        if req.delta < 0:
            # To prevent bouncing back and forth, we disallow scale down for
            # a "cool-off" period after the most recent scaling up, with an intention
            # to allow application to actually utilize newly provisioned resources
            # before making decisions on subsequent actions.
            #
            # Note that this action is unidirectional and doesn't apply to
            # scaling up, ie if actor pool just scaled down, it'd still be able
            # to scale back up immediately.
            if (
                not req.force
                and self._last_upscaled_at is not None
                and (time.time() <= self._last_upscaled_at + self._debounce_period_s)
            ):
                # NOTE: To avoid spamming logs unnecessarily, debounce log is
                #       produced once per upscaling event
                if self._last_upscaled_at != self._last_downscaling_debounce_warning_ts:
                    logger.debug(
                        f"Ignoring scaling down request (request={req}; "
                        f"reason=debounced from scaling up at {self._last_upscaled_at})"
                    )
                    self._last_downscaling_debounce_warning_ts = self._last_upscaled_at

                return False

        return True

    def _create_actor(
        self,
        scheduling_strategy: NodeAffinitySchedulingStrategy | str | None = None,
    ) -> Tuple[ActorHandle, ObjectRef, ExecutionResources]:
        logical_actor_id = str(uuid.uuid4())
        labels = {self.get_logical_id_label_key(): logical_actor_id}
        actor, ready_ref, resource_usage = self._create_actor_fn(
            labels, logical_actor_id, scheduling_strategy=scheduling_strategy
        )
        self._actor_to_logical_id[actor] = logical_actor_id
        return actor, ready_ref, resource_usage

    def _update_running_actor_state(self, actor: ActorHandle):
        """Refresh one running actor from its GCS-reported state.

        Dispatches on ALIVE / RESTARTING / DEAD; each observer returns whether
        the actor was released, in which case there is no rank to update.
        """
        actor_state = actor._get_local_state()
        running_actor_state = self._running_actors[actor]
        died = False

        if actor_state in (None, _ACTOR_STATE_DEAD):
            # actor._get_local_state can return None if the state is Unknown.
            died = True
            released = self._observe_dead(actor, running_actor_state)
        elif actor_state != _ACTOR_STATE_ALIVE:
            assert actor_state == _ACTOR_STATE_RESTARTING, actor_state
            released = self._observe_restarting(actor, running_actor_state)
        else:
            released = self._observe_alive(actor, running_actor_state)

        if not released:
            self._update_rank(actor=actor, state=running_actor_state, died=died)

    def _observe_dead(self, actor: ActorHandle, state: _ExperimentalActorState) -> bool:
        """Handle an actor observed DEAD; returns whether it was released."""
        if state.status == ActorStatus.DRAINING:
            self._record_drain_outcome_on_death(actor, state)
            self._release_running_actor(actor)
            return True
        return False

    def _observe_restarting(
        self, actor: ActorHandle, state: _ExperimentalActorState
    ) -> bool:
        """Handle an actor observed RESTARTING; returns whether it was released.

        The subtle case is a draining actor whose process is gone. Left
        DRAINING it could never finish draining: with ``max_restarts=-1``
        (what map actors use) a lost actor is reported RESTARTING and never
        DEAD, so the retire-on-death path can't fire; the status is sticky, so
        it never rejoins the restarting accounting either; and its in-flight
        tasks retry against the restarted process, so ``num_tasks_in_flight``
        never reaches 0 and ``process_draining_actors`` can never tear it
        down. A sticky DRAINING status would pin its node's capacity for
        the rest of the run.
        """
        if state.status == ActorStatus.DRAINING:
            if self._is_fully_drained(state):
                # Nothing left to serve, so this is the teardown
                # ``process_draining_actors`` would have done anyway. Honor
                # the decision to shed it.
                self._teardown_drained_actor(actor)
                return True
            # It still owes work, and Ray core is bringing it back precisely
            # to serve that -- retried tasks, and reconstruction of outputs we
            # haven't consumed. Killing it would abandon that. Stop draining
            # it instead and let it rejoin the pool; a later downscale picks a
            # new victim if the pool should still shrink. (The drain's
            # stickiness exists to stop an evicted actor being revived on a
            # node the pool no longer owns, which cannot apply here: the actor
            # only restarts once that node is gone, so core brings it back
            # somewhere else.)
            self._cancel_draining(actor, cause="core_restart")

        if self._set_status(state, ActorStatus.RESTARTING):
            self._pending_or_restarting_usage = self._pending_or_restarting_usage.add(
                self._actor_resource_usage[actor]
            )
        return False

    def _observe_alive(
        self, actor: ActorHandle, state: _ExperimentalActorState
    ) -> bool:
        """Handle an actor observed ALIVE; returns whether it was released."""
        if state.status is ActorStatus.RESTARTING:
            self._pending_or_restarting_usage = (
                self._pending_or_restarting_usage.subtract(
                    self._actor_resource_usage[actor]
                )
            )
            self._set_status(
                state,
                ActorStatus.IDLE
                if state.num_tasks_in_flight == 0
                else ActorStatus.ACTIVE,
            )
        return False

    def _update_rank(
        self, actor: ActorHandle, state: _ExperimentalActorState, died: bool
    ):
        """Update the scheduling rank for an actor after a state refresh.

        Heap membership requires the actor to be alive and available for
        scheduling (see ``_is_available_for_scheduling``). Restarting/dead
        actors are removed from both heaps; the per-node heap is cleared by
        ``refresh_actor_state`` before this method runs, so the explicit
        removal there is defensive.
        """
        if state.status == ActorStatus.RESTARTING or died:
            self._remove_from_heaps(actor, state.actor_location)
            return
        self._sync_actor_to_heaps(actor, state)

    def _add_pending_actor(
        self,
        actor: ActorHandle,
        ready_ref: ObjectRef,
        resource_usage: ExecutionResources,
        target_node_id: Optional[NodeIdStr] = None,
    ):
        """Adds a pending actor to the pool.

        This actor won't be pickable until it is marked as running via a
        pending_to_running() call.

        Args:
            actor: The not-yet-ready actor to add as pending to the pool.
            ready_ref: The ready future for the actor.
            resource_usage: The actual resource usage for this actor.
            target_node_id: Node the actor was pinned to via
                NodeAffinitySchedulingStrategy, or None if core-scheduled.
        """
        self._pending_actors[ready_ref] = actor
        self._pending_actor_info[ready_ref] = _PendingActorInfo(
            logical_id=self._actor_to_logical_id[actor],
            target_node_id=target_node_id,
        )
        self._actor_resource_usage[actor] = resource_usage
        self._total_usage = self._total_usage.add(resource_usage)
        self._pending_or_restarting_usage = self._pending_or_restarting_usage.add(
            resource_usage
        )
        # A pending actor is committed to its target node (if one was set);
        # untargeted/core-scheduled pendings contribute nothing until ready.
        if target_node_id is not None:
            self._add_committed_usage(target_node_id, resource_usage)

    def _remove_inactive_actor(self) -> bool:
        """Removes a single pending or idle actor, if any actors are pending/idle.

        Returns whether an inactive actor was actually released.
        """
        # We prioritize dropping pending actors over idle actors to reduce actor
        # starting churn.
        released = self._try_remove_pending_actor()
        if not released:
            released = self._try_remove_idle_actor()
        return released

    def _try_remove_pending_actor(self) -> bool:
        if self._pending_actors:
            # At least one pending actor, so drop the first one.
            ready_ref = next(iter(self._pending_actors.keys()))
            self._remove_pending_actor_by_ref(ready_ref)
            return True
        # No pending actors, so indicate to the caller that no actors were
        # released.
        return False

    def _remove_pending_actor_by_ref(self, ready_ref: ObjectRef) -> None:
        """Drop a specific pending actor and its bookkeeping.

        The pool's references are dropped and Ray core garbage-collects the
        actor; an explicit kill would make its outputs unreconstructable.
        """
        actor = self._pending_actors.pop(ready_ref)
        info = self._pending_actor_info.pop(ready_ref, None)
        usage = self._actor_resource_usage.pop(actor)
        self._total_usage = self._total_usage.subtract(usage)
        self._pending_or_restarting_usage = self._pending_or_restarting_usage.subtract(
            usage
        )
        if info is not None and info.target_node_id is not None:
            self._remove_committed_usage(info.target_node_id, usage)
        del self._actor_to_logical_id[actor]

    def _try_remove_idle_actor(self) -> bool:
        for actor, state in self._running_actors.items():
            if state.num_tasks_in_flight == 0 and state.status != ActorStatus.DRAINING:
                # At least one idle actor: mark it draining (stop new
                # submissions now; ``process_draining_actors`` releases it once
                # it is fully drained -- no in-flight tasks, no outstanding
                # outputs).
                self.begin_draining(actor, reason=DrainReason.RESIZE)
                return True
        # No idle actors, so indicate to the caller that no actors were
        # released.
        return False

    def release_expired_pending_actors(
        self, expiry_s: float = PENDING_ACTOR_EXPIRY_S, now: Optional[float] = None
    ) -> int:
        """Release pending actors older than ``expiry_s``, returning the count.

        A pending whose ready-ref never resolves -- an actor wedged in failed
        restarts after losing its constructor args (ray#53727), a stuck
        runtime-env install, or a ghost -- otherwise holds the pool's pending
        count until the job dies. The handle is force-killed: a never-ready
        actor cannot be gracefully released, and killing it also breaks a
        failed-restart loop. The freed claim lets the caller re-place the
        actor with current cluster information.
        """
        now = time.time() if now is None else now
        expired = [
            ready_ref
            for ready_ref, info in self._pending_actor_info.items()
            if now - info.created_at > expiry_s
        ]
        for ready_ref in expired:
            actor = self._pending_actors.pop(ready_ref)
            info = self._pending_actor_info.pop(ready_ref)
            usage = self._actor_resource_usage.pop(actor)
            self._total_usage = self._total_usage.subtract(usage)
            self._pending_or_restarting_usage = (
                self._pending_or_restarting_usage.subtract(usage)
            )
            if info.target_node_id is not None:
                self._remove_committed_usage(info.target_node_id, usage)
            self._actor_to_logical_id.pop(actor, None)
            logger.warning(
                "Releasing pending actor %s: not ready after %.0fs "
                "(target_node=%s). Killing the handle and freeing its claim so "
                "it can be re-placed.",
                info.logical_id,
                now - info.created_at,
                info.target_node_id,
            )
            ray.kill(actor)
        return len(expired)

    def _release_pending_actors(self, force: bool):
        # Release pending actors from the set of pending ones
        pending = dict(self._pending_actors)
        # Capture target-node bookkeeping before clearing so we can release each
        # pending actor's committed-usage contribution.
        pending_info = dict(self._pending_actor_info)
        self._pending_actors.clear()
        self._pending_actor_info.clear()
        for ready_ref, actor in pending.items():
            usage = self._actor_resource_usage.pop(actor)
            self._total_usage = self._total_usage.subtract(usage)
            self._pending_or_restarting_usage = (
                self._pending_or_restarting_usage.subtract(usage)
            )
            info = pending_info.get(ready_ref)
            if info is not None and info.target_node_id is not None:
                self._remove_committed_usage(info.target_node_id, usage)
            self._actor_to_logical_id.pop(actor, None)

        if force:
            for actor in pending.values():
                # NOTE: Actors can't be brought back after being ``ray.kill``-ed,
                #       hence we're only doing that if this is a forced release
                ray.kill(actor)

    def _release_running_actors(self, force: bool):
        running = list(self._running_actors.keys())

        for actor in running:
            self._release_running_actor(actor)

        # NOTE: Actors can't be brought back after being ``ray.kill``-ed,
        #       hence we're only doing that if this is a forced release
        if force:
            for actor in running:
                ray.kill(actor)

    def _release_running_actor(self, actor: ActorHandle):
        """Remove the given actor from the pool by dropping all pool references."""
        # NOTE: By default, we remove references to the actor and let ref counting
        # garbage collect the actor, instead of using ray.kill.
        #
        # Otherwise, actor cannot be reconstructed for the purposes of produced
        # object's lineage reconstruction.
        if actor not in self._running_actors:
            return

        # Update cached statistics before removing the actor
        actor_state = self._running_actors[actor]

        # Update total tasks in flight
        self._total_num_tasks_in_flight -= actor_state.num_tasks_in_flight

        # The per-node active map counts any running actor (incl. draining)
        # with in-flight tasks, so unmark whenever the released actor still
        # had tasks in flight (e.g. forced shutdown).
        if actor_state.num_tasks_in_flight > 0:
            self._mark_node_actor_inactive(actor_state)
        # Leave the status histogram.
        self._status_counts[actor_state.status] -= 1

        # Retire the draining state machine, if any.
        if actor_state.status is ActorStatus.DRAINING:
            self._draining_actors.pop(actor, None)
            self.draining_generation += 1

        self._remove_from_heaps(actor, actor_state.actor_location)

        # Drop logical-id and node-keyed views in lockstep with
        # ``_running_actors`` so the two maps never reference a freed
        # actor (or a node that no longer hosts any). Drop the node
        # entry entirely once its last actor is gone -- the keyset of
        # ``_node_to_actor_states`` is meant to reflect "nodes the pool
        # is currently keeping alive".
        node_id = actor_state.actor_location
        logical_id = actor_state.logical_id
        self._logical_id_to_actor.pop(logical_id, None)
        node_states = self._node_to_actor_states.get(node_id)
        if node_states is not None:
            node_states.pop(logical_id, None)
            if not node_states:
                del self._node_to_actor_states[node_id]

        del self._running_actors[actor]
        del self._actor_to_logical_id[actor]

        # Tell ResourceBank to drop this actor from the live-actor
        # indexes. Outputs the actor produced may still be in flight
        # downstream
        self.resource_bank.deregister_actor(actor_id=logical_id)

        usage = self._actor_resource_usage.pop(actor)
        self._total_usage = self._total_usage.subtract(usage)
        # Release this running actor's committed usage at its node.
        self._remove_committed_usage(node_id, usage)
        if actor_state.status == ActorStatus.RESTARTING:
            self._pending_or_restarting_usage = (
                self._pending_or_restarting_usage.subtract(usage)
            )

    def _teardown_drained_actor(self, actor: ActorHandle) -> None:
        """Tear down a fully-drained actor (no in-flight tasks, no outstanding
        outputs). The pool's references are dropped and Ray core reclaims the
        actor; an explicit kill would make its produced objects
        unreconstructable."""
        if self._metrics is not None:
            info = self._draining_actors[actor]
            self._metrics.record_drain_completed(
                self._op_tag, info.reason.value, time.time() - info.since
            )
        self._release_running_actor(actor)

    def begin_draining(
        self,
        actor: ActorHandle,
        reclaimable: bool = True,
        reason: DrainReason = DrainReason.UNKNOWN,
    ) -> None:
        """Mark an actor for draining. While draining, no new tasks can be
        submitted to it.

        The actor is removed from both scheduling heaps immediately (no new
        task submissions; ``_is_available_for_scheduling`` keeps it out) but
        stays in
        ``_running_actors`` and the capacity maps as it occupies its node's
        resources until it is actually dead. ``process_draining_actors``
        releases it once it is fully drained (no in-flight tasks AND no
        outstanding outputs).

        ``reclaimable=False`` marks an eviction (our allocation shrank): the
        actor drains and dies like any other, but ``reclaim_draining_actors``
        will never revive it, so a later upscale can't resurrect an actor on a
        node we no longer own."""
        state = self._running_actors[actor]
        assert (
            state.status != ActorStatus.DRAINING
        ), f"Actor {state.logical_id} already draining"
        # A RESTARTING victim leaves the restarting usage accounting here: it
        # is otherwise only released on an ALIVE transition, and DRAINING
        # supersedes that.
        if state.status is ActorStatus.RESTARTING:
            self._pending_or_restarting_usage = (
                self._pending_or_restarting_usage.subtract(
                    self._actor_resource_usage[actor]
                )
            )
        self._set_status(state, ActorStatus.DRAINING)
        self._remove_from_heaps(actor, state.actor_location)
        self._draining_actors[actor] = _DrainingActorInfo(
            since=time.time(), reclaimable=reclaimable, reason=reason
        )
        self.draining_generation += 1
        if self._metrics is not None:
            self._metrics.record_drain_started(self._op_tag, reason.value)

    def _record_drain_outcome_on_death(
        self, actor: ActorHandle, state: _ExperimentalActorState
    ) -> None:
        """Record how a draining actor's death ends its drain.

        A fully drained one just finished early. One that still owed work is
        recorded as interrupted, with how long it had been draining so a
        stalled drain is distinguishable from plain bad luck (e.g. preemption).
        Note the drain age runs until the death was *detected*, so under
        abrupt node loss it includes the failure-detection latency.
        """
        info = self._draining_actors[actor]
        drain_age_s = time.time() - info.since
        reason = info.reason.value
        if self._is_fully_drained(state):
            if self._metrics is not None:
                self._metrics.record_drain_completed(self._op_tag, reason, drain_age_s)
            return
        if self._metrics is not None:
            self._metrics.record_drain_interrupted(self._op_tag, reason, drain_age_s)
        # No per-drain "outputs lost" accounting here: the output queue keeps
        # draining (driver-side) between the node failing and its death being
        # detected, so outstanding counts are ~always 0 by now. The real losses
        # -- block data on the dead node -- surface downstream as
        # ObjectLostError/RayActorError and are counted where they are
        # detected.
        logger.warning(
            "Draining %s actor %s (drain reason: %s) died after %.0fs with %d "
            "outstanding output block(s); they are discarded and recomputed "
            "where lineage allows.",
            self._map_worker_cls_name,
            actor,
            reason,
            drain_age_s,
            self._num_outstanding_outputs(state),
        )

    def _is_fully_drained(self, state: _ExperimentalActorState) -> bool:
        """Whether a draining actor has nothing left to serve: no in-flight
        tasks, and no output blocks the ResourceBank still counts against it
        (pulled but not yet consumed downstream, or prebuffered in its
        generators).

        Both must hold before it is safe to tear the actor down -- releasing it
        with outstanding outputs can strand blocks that are no longer
        reconstructable. The in-flight check is the pool's own: the bank's
        prebuffered estimate reads zero when the generator buffer is configured
        to zero, so it cannot stand in for it.
        """
        if state.num_tasks_in_flight > 0:
            return False
        live = self.resource_bank.live_object_store(actor_id=state.logical_id)
        return live.all_outputs_consumed()

    def _num_outstanding_outputs(self, state: _ExperimentalActorState) -> int:
        """Output blocks the ResourceBank still counts against this actor."""
        live = self.resource_bank.live_object_store(actor_id=state.logical_id)
        return live.get_output_blocks()

    def _cancel_draining(self, actor: ActorHandle, cause: str) -> None:
        """Return a draining actor to the normal lifecycle.

        Inverse of ``begin_draining``'s bookkeeping. The caller
        decides what the actor becomes next (ALIVE-ish here, RESTARTING at the
        restart site) and re-syncs the scheduling heaps.

        ``cause`` says why the drain was cancelled ("reclaimed" for an upscale
        revival, "core_restart" for a Ray-core restart of an actor that still
        owes work); the drain reason only records why it started.
        """
        state = self._running_actors[actor]
        assert (
            state.status == ActorStatus.DRAINING
        ), f"Actor {state.logical_id} is not draining"
        drain_reason = self._draining_actors[actor].reason
        del self._draining_actors[actor]
        self.draining_generation += 1
        if self._metrics is not None:
            self._metrics.record_drain_cancelled(
                self._op_tag, drain_reason.value, cause
            )
        # Leaving DRAINING: back to IDLE (nothing in flight) or ACTIVE.
        self._set_status(
            state,
            ActorStatus.IDLE if state.num_tasks_in_flight == 0 else ActorStatus.ACTIVE,
        )

    def num_reclaimable_draining_actors(self) -> int:
        """Draining actors that can still be revived -- excludes evicted
        (``reclaimable=False``) actors, which must not be resurrected."""
        return sum(1 for info in self._draining_actors.values() if info.reclaimable)

    def draining_actor_ids(self) -> Set[LogicalActorId]:
        """Logical ids of actors currently draining (marked draining, not yet
        released). Used to keep the output queue's prioritized set in sync."""
        return {
            self._running_actors[actor].logical_id for actor in self._draining_actors
        }

    def reclaim_draining_actors(self, count: int) -> int:
        """Revive up to ``count`` draining actors back into the running pool
        (inverse of ``begin_draining``). Returns the number revived.
        Saves actor startup + capacity churn vs killing and recreating."""
        if count <= 0:
            return 0
        reclaimed = 0
        for actor, info in list(self._draining_actors.items()):
            if reclaimed >= count:
                break
            if not info.reclaimable:
                continue
            state = self._running_actors[actor]
            self._cancel_draining(actor, cause="reclaimed")
            # Re-admit to the scheduling heaps (it now passes the draining
            # gate in _is_available_for_scheduling again).
            self._sync_actor_to_heaps(actor, state)
            reclaimed += 1
        if reclaimed:
            logger.debug(
                "Reclaimed %d draining %s actor(s) back into the running pool",
                reclaimed,
                self._map_worker_cls_name,
            )
        return reclaimed

    def process_draining_actors(self) -> int:
        """Release every draining actor that is now fully drained.

        Called once per scheduling tick. An actor is "fully drained" once it has
        no in-flight tasks AND no output blocks the ResourceBank still counts
        against it.

        Returns:
            The number of actors released this call.
        """
        if not self._draining_actors:
            return 0

        now = time.time()
        num_finalized = 0
        stalled = []
        for actor in list(self._draining_actors):
            state = self._running_actors[actor]
            if self._is_fully_drained(state):
                self._teardown_drained_actor(actor)
                num_finalized += 1
            elif now - self._draining_actors[actor].since >= DRAIN_STALL_WARN_S:
                stalled.append(
                    (state.num_tasks_in_flight, self._num_outstanding_outputs(state))
                )

        if stalled and now - self._last_drain_stall_warn_ts >= DRAIN_STALL_WARN_S:
            max_age = max(now - info.since for info in self._draining_actors.values())
            total_in_flight = sum(in_flight for in_flight, _ in stalled)
            total_outstanding = sum(unconsumed for _, unconsumed in stalled)
            logger.warning(
                f"{len(stalled)} draining {self._map_worker_cls_name} actor(s) have not "
                f"finished draining after {DRAIN_STALL_WARN_S}s "
                f"(longest {max_age:.0f}s; {total_in_flight} task(s) still in flight, "
                f"{total_outstanding} outstanding output block(s)). They hold their node "
                "capacity until their tasks finish and their outputs are consumed "
                "downstream."
            )
            self._last_drain_stall_warn_ts = now

        return num_finalized

    def _downscale_actor_by_id(
        self, logical_id: LogicalActorId, reclaimable: bool = True
    ) -> bool:
        """Downscale one exact victim chosen by the caller.

        ``reclaimable=False`` evicts (allocation shrank): the actor drains but is
        never revived. A pending victim is simply cancelled either way.
        """
        for ready_ref, info in self._pending_actor_info.items():
            if info.logical_id == logical_id:
                self._remove_pending_actor_by_ref(ready_ref)
                return True

        actor = self._logical_id_to_actor.get(logical_id)
        if actor is None or self._running_actors[actor].status == ActorStatus.DRAINING:
            logger.debug(
                f"Skipping downscale of actor {logical_id}: not found or "
                "already draining."
            )
            return False
        # reclaimable=False here means exactly one thing (see the docstring):
        # the allocation shrank and the caller is evicting.
        self.begin_draining(
            actor,
            reclaimable=reclaimable,
            reason=DrainReason.RESIZE if reclaimable else DrainReason.ALLOC_SHRINK,
        )
        return True

    def has_actor(self, actor_id: LogicalActorId) -> bool:
        """Whether the pool still holds a running actor with this logical id.

        False once the actor has been released (downscaled or reaped), even if
        in-flight task refs for it are still resolving.
        """
        return actor_id in self._logical_id_to_actor

    def is_actor_draining(self, actor: ActorHandle) -> bool:
        """Whether this (still-running) actor is draining. False if released."""
        state = self._running_actors.get(actor)
        return state is not None and state.status == ActorStatus.DRAINING

    def pending_ids_by_target_node(self) -> Dict[NodeIdStr, List[LogicalActorId]]:
        """Logical ids of pending actors, grouped by their target node."""
        result: DefaultDict[NodeIdStr, List[LogicalActorId]] = defaultdict(list)
        for info in self._pending_actor_info.values():
            if info.target_node_id is not None:
                result[info.target_node_id].append(info.logical_id)
        return dict(result)

    def running_actor_status_counts_by_node(
        self,
    ) -> Dict[NodeIdStr, "Counter[ActorStatus]"]:
        """Per-node counts of running actors by status (pendings excluded --
        see ``pending_ids_by_target_node``)."""
        counts: DefaultDict[NodeIdStr, Counter[ActorStatus]] = defaultdict(Counter)
        for node_id, states in self._node_to_actor_states.items():
            for state in states.values():
                counts[node_id][state.status] += 1
        return dict(counts)

    def _add_committed_usage(
        self, node_id: NodeIdStr, usage: ExecutionResources
    ) -> None:
        """Credit ``usage`` to ``node_id`` in the committed-usage map."""
        self.committed_usage_by_node[node_id] = self.committed_usage_by_node.get(
            node_id, ExecutionResources.zero()
        ).add(usage)

    def _remove_committed_usage(
        self, node_id: NodeIdStr, usage: ExecutionResources
    ) -> None:
        """Debit ``usage`` from ``node_id``, dropping the entry once it hits zero
        so the map never accumulates stale zero-valued nodes (matching the
        empty-node-omitting semantics callers expect)."""
        # Every debit follows a credit for the same node (pending target,
        # ready location, or relocation), so the entry must exist.
        assert (
            node_id in self.committed_usage_by_node
        ), f"no committed usage recorded for node={node_id}"
        remaining = self.committed_usage_by_node[node_id].subtract(usage)
        assert remaining.is_non_negative(), (
            f"committed usage for node={node_id} went negative: {remaining} "
            f"(debited {usage})"
        )
        if remaining.is_zero():
            self.committed_usage_by_node.pop(node_id, None)
        else:
            self.committed_usage_by_node[node_id] = remaining

    def _find_actor_with_locality(self, bundle: RefBundle) -> Optional[ActorHandle]:
        """Find an alive actor on the most-preferred node for the bundle.

        Preferred nodes are visited in descending order of on-node bytes; for
        each node, the per-node heap's peek gives the locality-best actor
        (see ``_ExperimentalActorRank``: ascending ``num_tasks_in_flight``).

        Args:
            bundle: The bundle to find an actor for.

        Returns:
            The locality-preferred actor, or None if no preferred node has any
            alive actor with capacity.
        """
        preferred_locs = bundle.get_preferred_object_locations_from_metadata()
        if not preferred_locs:
            return None

        for node_id, _ in sorted(
            preferred_locs.items(), key=lambda item: (-item[1], item[0])
        ):
            node_heap = self._alive_node_to_available_actor_heap.get(node_id)
            if not node_heap:
                continue
            actor, _ = node_heap.peekitem()
            return actor

        return None

    def _derive_default_max_num_output_bytes(self, actor_location: NodeIdStr) -> int:
        """Derive the per-actor output-bytes budget for an actor on
        ``actor_location``.

        Uses an explicit ``max_num_output_bytes_per_actor`` if configured;
        otherwise derives a CPU-proportional share of the node's usable object
        store and clamps it into the system [MIN, MAX] bounds.
        """
        if self._config.max_num_output_bytes_per_actor is not None:
            return self._config.max_num_output_bytes_per_actor

        res = self.resource_bank.node_capacity(node_id=actor_location)
        per_actor_usage = self._config.per_actor_resource_usage
        usable_object_store_for_outputs_only = (
            res.object_store_memory * OBJECT_STORE_OUTPUT_FRACTION
        )
        # NOTE: Normalize the object-store budget on CPU alone. We deliberately
        # do NOT also normalize on memory/GPU since normalizing across multiple
        # dimensions makes the budget hard to reason about (e.g. a num_cpus=0,
        # num_gpus=1 actor on a single-GPU node would take the entire object
        # store).
        node_cpu = res.cpu
        actor_cpu = per_actor_usage.cpu
        if node_cpu > 0 and actor_cpu > 0:
            obj_store_per_actor = (
                int(usable_object_store_for_outputs_only * (actor_cpu / node_cpu) / MiB)
                * MiB
            )
        else:
            # Default to 0 when node_cpu == 0. This can happen with
            # custom compute configs.
            obj_store_per_actor = 0

        # Clamp into the system [MIN, MAX] so high-cpu / low-cpu actors don't
        # get unbounded or tiny budgets. Clamping up to MIN can cause spilling
        # if the node's object store cannot hold the floor.
        default_max_num_output_bytes = min(
            PER_ACTOR_OBJECT_STORE_MAX_BYTES,
            max(PER_ACTOR_OBJECT_STORE_MIN_BYTES, obj_store_per_actor),
        )
        if default_max_num_output_bytes != obj_store_per_actor and log_once(
            f"actor_only_output_bytes_budget_clamped_{id(self)}"
        ):
            logger.warning(
                f"{self._map_worker_cls_name}: derived per-actor "
                f"output-bytes budget {memory_string(obj_store_per_actor)} "
                f"is outside the system range "
                f"[{memory_string(PER_ACTOR_OBJECT_STORE_MIN_BYTES)}, "
                f"{memory_string(PER_ACTOR_OBJECT_STORE_MAX_BYTES)}]; "
                f"clamping to {memory_string(default_max_num_output_bytes)}. "
                "Clamping up may cause spilling if the node's object store "
                "cannot hold the floor. Override with "
                "RAY_DATA_PER_ACTOR_OBJECT_STORE_MIN_BYTES / "
                "RAY_DATA_PER_ACTOR_OBJECT_STORE_MAX_BYTES or set "
                "max_num_output_bytes_per_actor explicitly. Logged once "
                "per actor pool."
            )
        return default_max_num_output_bytes

    def reset_output_limit(self):
        default_max_outputs = safe_or(
            self._config.max_num_outputs_per_actor, float("inf")
        )
        for state in self._running_actors.values():
            # Restore each actor's derived (or configured) output-bytes budget,
            # not an unbounded default -- otherwise the block-size sizing in
            # _create_task_context sees an infinite limit.
            state.max_num_output_bytes = state.default_max_num_output_bytes
            state.max_num_outputs = default_max_outputs

    def upgrade_output_limit(
        self, node_id: NodeIdStr, delta_bytes: int, delta_outputs: int = 1
    ):
        """Raise output bp caps on the most-constrained actor on ``node_id``.

        Picks the actor on the given node with the lowest current
        ``(max_num_output_bytes, max_num_outputs)`` tuple and bumps both
        of its caps in place on the ``_ExperimentalActorState`` (which is the
        same object held in ``_running_actors``, so no second write is
        needed). The caller has already chosen ``node_id`` based on
        cluster-wide signals; we only need to fan that decision out to a
        specific actor.
        """
        node_states = self._node_to_actor_states.get(node_id)
        assert node_states, (
            f"upgrade_output_limit called for node={node_id} with no "
            f"running actors; pool has nodes={list(self._node_to_actor_states)}"
        )

        candidate_states = [
            s for s in node_states.values() if s.status != ActorStatus.DRAINING
        ] or list(node_states.values())
        best_state = min(
            candidate_states,
            key=lambda s: (s.max_num_output_bytes, s.max_num_outputs),
        )
        best_state.max_num_output_bytes += delta_bytes
        best_state.max_num_outputs += delta_outputs
        # NOTE: ``inf + delta`` is still ``inf`` -- the unset dimension
        # stays unset after the bump, which is fine: the tuple key above
        # will still discriminate via the finite dimension on the next call.

        actor = self._logical_id_to_actor[best_state.logical_id]
        logger.info(
            f"\nname={self._map_worker_cls_name} ({self.get_actor_info()})\n"
            f"Increasing the capacity for actor={actor} "
            f"located on {node_id} by {memory_string(delta_bytes)} / "
            f"{delta_outputs} output,\n"
            f"new limit is {memory_string(best_state.max_num_output_bytes)} / "
            f"{best_state.max_num_outputs} outputs\n"
        )

    def output_bytes_limit(self, actor_id: LogicalActorId) -> float:
        actor = self._logical_id_to_actor.get(actor_id)
        if actor is None:
            # The actor was released (killed / died) while one of its data
            # tasks lingers; callers like the output-backpressure path may still
            # ask for its limit -- impose no constraint rather than KeyError.
            return float("inf")
        return self._running_actors[actor].max_num_output_bytes

    def estimate_bytes_per_output(self, node_id: NodeIdStr) -> float | None:
        """Estimated bytes per output block for the actors on ``node_id``, or
        None if none of them has a finite output-bytes budget.

        This is a prediction, not a measurement: the mean per-actor
        ``max_num_output_bytes`` scaled by ``BLOCK_SIZE_TO_OUTPUT_BYTES_RATIO``
        (the per-actor target block size), matching how ``_create_task_context``
        sizes output blocks. The budget is derived per node (a CPU share of
        that node's object store), so the estimate is scoped to one node.
        """
        total: float = 0.0
        count: int = 0
        for state in self._node_to_actor_states.get(node_id, {}).values():
            limit = state.max_num_output_bytes
            if not math.isfinite(limit):
                # Unbounded budget (explicit override, or a raised cap):
                # carries no block-size information, so skip it rather than
                # poison the mean.
                continue
            total += limit
            count += 1
        if count == 0:
            return None
        return total * BLOCK_SIZE_TO_OUTPUT_BYTES_RATIO / count

    def output_count_limit(self, actor_id: LogicalActorId) -> float:
        actor = self._logical_id_to_actor.get(actor_id)
        if actor is None:
            return float("inf")
        return self._running_actors[actor].max_num_outputs


class _NodeAwareExperimentalActorPool(_ExperimentalActorPool):
    """An actor pool that drains actors off nodes that are going away. Ray
    reports a node as DRAINING once it is scheduled for removal -- for a spot
    instance, when the cloud provider issues the preemption notice. Actors
    there are put on the draining path (see
    ``_drain_actors_on_draining_nodes``), which both stops new task submissions
    to them and hands their work back before the node is reclaimed.
    Furthermore, unlike the parent implementation, this actor pool updates
    actor locations upon restart.

    The actor-only counterpart of ``_NodeAwareActorPool``.
    """

    # Snapshots refreshed at the top of ``refresh_actor_state``. Class-level
    # defaults so ``_update_rank`` (reachable through the parent's
    # draining-actor observation even with RAY_DATA_REFRESH_ACTOR_STATE off)
    # never reads unset attributes.
    _actor_node_id_map_snapshot: Dict[LogicalActorId, NodeIdStr] = {}
    _draining_nodes_snapshot: Dict[NodeIdStr, int] = {}

    def __init__(
        self,
        *args,
        get_draining_nodes: Callable[[], Dict[NodeIdStr, int]] = get_draining_nodes,
        get_actor_locations: Callable[
            [Tuple[LogicalActorId, ...]], Dict[LogicalActorId, NodeIdStr]
        ] = get_actor_locations,
        **kwargs,
    ):
        """``get_draining_nodes`` / ``get_actor_locations`` default to the
        cached Ray internals; tests inject fakes instead of patching module
        globals."""
        super().__init__(*args, **kwargs)
        self._get_draining_nodes = get_draining_nodes
        self._get_actor_locations = get_actor_locations

    @override
    def refresh_actor_state(self):
        if not REFRESH_ACTOR_STATE:
            # The parent still observes draining actors on this path.
            super().refresh_actor_state()
            return

        # Snapshots state that _update_running_actor_state() depends on.
        # Must be captured before the parent pass runs (see _update_rank).
        self._actor_node_id_map_snapshot = self._get_actor_locations(
            tuple(self.get_logical_ids())
        )
        self._draining_nodes_snapshot = self._get_draining_nodes()

        super().refresh_actor_state()

        # Runs after the parent pass so it reads each actor's just-refreshed
        # location (see _update_rank).
        self._drain_actors_on_draining_nodes()

    @override
    def pending_to_running(self, ready_ref: ray.ObjectRef) -> Optional[ActorHandle]:
        actor = super().pending_to_running(ready_ref)
        if actor is None or not REFRESH_ACTOR_STATE:
            return actor

        # The base class makes every newly ready actor schedulable. Actor-ready
        # callbacks are processed before dispatch, so without this an actor that
        # came up on a draining node takes tasks until the next refresh.
        state = self._running_actors[actor]
        if state.actor_location in self._get_draining_nodes():
            self.begin_draining(actor, reclaimable=False, reason=DrainReason.PREEMPTION)

        return actor

    def _drain_actors_on_draining_nodes(self) -> None:
        """Begin draining every actor on a draining node.

        Victims are marked non-reclaimable: their node is going away, so a later
        upscale must never revive them. Draining is one-way for the same reason
        -- if a drain were ever cancelled, the actor still dies and gets
        replaced elsewhere.

        NOTE: tearing a fully-drained actor down is driven by
        ``process_draining_actors``, which the operator runs each scheduling
        tick. Failing that, the actor simply stops taking work and is retired
        when the node takes it down.
        """
        if not self._draining_nodes_snapshot:
            return

        drained: Counter[NodeIdStr] = Counter()
        # Snapshot the items: begin_draining mutates pool state
        # (heaps, counters, actor status) as we go.
        for actor, state in list(self._running_actors.items()):
            if state.actor_location not in self._draining_nodes_snapshot:
                continue
            if state.status == ActorStatus.DRAINING:
                # Already draining (e.g. an ordinary downscale): its node is
                # going away, so mark it non-reclaimable so a later upscale
                # never revives it here.
                self._draining_actors[actor].reclaimable = False
                continue
            if state.status == ActorStatus.RESTARTING:
                # A restarting actor's recorded location is stale (Ray core may
                # bring it back on another node), so wait until it is ALIVE
                # again before judging where it lives.
                continue
            self.begin_draining(actor, reclaimable=False, reason=DrainReason.PREEMPTION)
            drained[state.actor_location] += 1

        if drained:
            logger.info(
                "Draining %d %s actor(s) off %d node(s) being taken down "
                "(e.g. spot preemption): %s",
                sum(drained.values()),
                self.map_worker_cls_name,
                len(drained),
                dict(drained),
            )

    @override
    def _update_rank(
        self, actor: ActorHandle, state: _ExperimentalActorState, died: bool
    ):
        """Refresh the actor's node location, then rank it as the parent does."""
        # The ActorLocationTracker may return stale information when
        #   - the actor is recently created
        #   - get_actor_locations() returns *cached* information
        # NOTE:
        #   - We fall back to the previous location if information is stale
        #   - Actor locations are not updated in the base implementation
        logical_id = self.get_actor_logical_id(actor)
        node_id = (
            self._actor_node_id_map_snapshot.get(logical_id, None)
            or state.actor_location
        )
        assert node_id is not None, f"{state.actor_location}"
        # Not a plain assignment: several pool structures are keyed by the
        # actor's node and have to move with it (see _relocate_actor). No-op
        # when the location is unchanged.
        self._relocate_actor(actor, state, node_id)

        super()._update_rank(actor, state, died)
