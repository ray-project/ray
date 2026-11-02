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
  down. A drain can end early (the actor died) or be cancelled by an upscale
  reclaim. ``draining_generation`` lets the operator cheaply detect changes to
  the draining set.

- **Death**: actors are never restarted by Ray core (``max_restarts=0``). A
  dead actor is released like any other; the sizer places its replacement and
  Ray Data lineage recovery redoes its lost work.
"""

from __future__ import annotations

import logging
import math
import time
from collections import Counter, defaultdict
from typing import (
    Callable,
    DefaultDict,
    Dict,
    List,
    Mapping,
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
)
from ray.data._internal.execution.interfaces import (
    ExecutionResources,
    NodeIdStr,
    RefBundle,
)
from ray.data._internal.execution.interfaces.common import ActorId, ActorStatus
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
    _ACTOR_STATE_DEAD,
    DrainReason,
    ScaleDownRequest,
    ScaleUpRequest,
    _DrainingActorInfo,
    _ExperimentalActorRank,
    _ExperimentalActorState,
    _PendingActorInfo,
)
from ray.data._internal.util import MiB
from ray.data._internal.utils.cached_ray_internals import get_draining_nodes
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
        # Actors that have started running (draining ones included).
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
        # Map from actor handle to its Ray actor id (hex).
        self._actor_to_id: Dict[ActorHandle, ActorId] = {}
        # Per-actor resource usage, needed because ray_remote_args_fn can
        # produce different resources for each actor.
        self._actor_resource_usage: Dict[ActorHandle, ExecutionResources] = {}
        # Cached aggregate resource counters.
        self._total_usage = ExecutionResources.zero()
        self._pending_usage = ExecutionResources.zero()
        # Resources this pool occupies (or has promised) per node. Running and
        # pending actors both count: they hold their resources until they are
        # actually dead. Maintained incrementally at the
        # create/ready/death sites (``_add_committed_usage`` /
        # ``_remove_committed_usage``) so readers pay nothing per tick. Running
        # actors are counted at their actual location; pending actors at their
        # ``target_node_id`` (only when one was set -- untargeted pendings
        # contribute nothing until they become running). Public: the sizer reads
        # it directly.
        self.committed_usage_by_node: Dict[NodeIdStr, ExecutionResources] = {}
        # Running actors by status (IDLE / ACTIVE / DRAINING).
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
        self._id_to_actor: Dict[ActorId, ActorHandle] = {}

        # NOTE: Only contains ALIVE actors.
        self._node_to_actor_states: Dict[
            NodeIdStr, Dict[ActorId, _ExperimentalActorState]
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
        """
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
        ``num_tasks_in_flight > 0``. ``actor_location`` never changes (actors
        are not restarted), so the paired decrement reads the same node.
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
        """Always 0: actors are never restarted (``max_restarts=0``)."""
        return 0

    @override
    def num_active_actors(self) -> int:
        """Running actors with tasks in flight, DRAINING ones included (they
        still hold work even though they take no new tasks). Read off the
        per-node active map, whose invariant is "counted iff
        ``num_tasks_in_flight > 0``"."""
        return sum(self._node_to_active_actor_count.values())

    @override
    def num_idle_actors(self) -> int:
        # IDLE = schedulable with nothing in flight. DRAINING actors are not
        # schedulable, so they are not idle.
        return self._status_counts[ActorStatus.IDLE]

    @override
    def num_pending_actors(self) -> int:
        return len(self._pending_actors)

    @override
    def num_tasks_in_flight(self) -> int:
        return self._total_num_tasks_in_flight

    @override
    def scale(self, req: ActorPoolScalingRequest) -> int:
        """Apply a ``ScaleUpRequest`` / ``ScaleDownRequest``; returns the
        *applied* delta, which can fall short of ``req.delta``."""
        if not self._can_apply_request(req):
            return 0
        if isinstance(req, ScaleUpRequest):
            return self._scale_up(req)
        assert isinstance(req, ScaleDownRequest), (
            f"Unexpected scaling request {req!r}: the actor-only backend only "
            "sends ScaleUpRequest / ScaleDownRequest"
        )
        return self._scale_down(req)

    @override
    def refresh_actor_state(self):
        """Release dead actors, rebuild the scheduling heaps, and tear down
        draining actors that have finished draining. Runs once per scheduling
        step."""
        self._alive_node_to_available_actor_heap.clear()
        self._available_actors_to_tasks_fallback_heap.clear()
        # NOTE: Copy the keys -- releasing a dead actor removes it from
        # ``_running_actors`` mid-iteration.
        for actor in list(self._running_actors):
            self._update_running_actor_state(actor)
        self.process_draining_actors()

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
            # selectable (DRAINING actors stay out of the heaps).
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
            actor_id = self.get_actor_logical_id(actor)
            # Building the state also derives the actor's per-actor
            # output-bytes budget from its landing node's capacity. Done inside
            # this try so a failure there (e.g. the actor landed off-target on
            # a node the bank cannot resolve) is cleaned up exactly like an
            # init failure, rather than leaking pending/committed usage.
            state = self._make_running_state(actor_id, actor_location)
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
            # 1 -> 0 in flight: ACTIVE -> IDLE. A DRAINING actor finishing its
            # last task keeps its status; it is not schedulable.
            if state.status is ActorStatus.ACTIVE:
                self._set_status(state, ActorStatus.IDLE)

        # Reconcile both heaps: the completed task decreased
        # ``num_tasks_in_flight``, which may make the actor available for
        # scheduling again.
        self._sync_actor_to_heaps(actor, state)

    @override
    def get_actor_logical_id(self, actor: ActorHandle) -> ActorId:
        """The actor's Ray actor id (hex). The base-class name is kept for the
        shared ``AutoscalingActorPool`` interface."""
        return self._actor_to_id[actor]

    @override
    def get_logical_ids(self) -> List[ActorId]:
        """Ray actor ids (hex) of the pending and running actors."""
        return list(self._actor_to_id.values())

    @override
    def current_logical_usage(self) -> ExecutionResources:
        return self._total_usage

    @override
    def pending_logical_usage(self) -> ExecutionResources:
        return self._pending_usage

    # === End of overriding methods of AutoscalingActorPool ===

    def _scale_up(self, req: ScaleUpRequest) -> int:
        """Reclaim draining actors first, then create the rest.

        Reclaiming is free capacity-wise (a draining actor already holds its
        node's resources) and skips startup.
        """
        reclaimed = self.reclaim_draining_actors(limit=req.num_to_reclaim)
        num_new = len(req.nodes) if req.nodes else req.delta - reclaimed

        logger.debug(
            f"Scaling up {self._map_worker_cls_name} actor pool by {req.delta} "
            f"(reclaimed={reclaimed}, new={num_new}, reason={req.reason}, "
            f"{self.get_actor_info()})"
        )

        for i in range(num_new):
            target_node_id = req.nodes[i] if req.nodes else None
            # Hard affinity: the bank and the sizer account for the actor on
            # its target node, and actors never move afterwards. If the node
            # goes away first, ``release_expired_pending_actors`` frees the
            # claim and the sizer re-places it.
            scheduling_strategy = (
                NodeAffinitySchedulingStrategy(target_node_id, soft=False)
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
            logger.debug(
                "Upscale applied partially: requested=%d reclaimed=%d created=%d",
                req.delta,
                reclaimed,
                num_new,
            )
        return applied

    def _scale_down(self, req: ScaleDownRequest) -> int:
        """Shed victims; returns the (negative) applied delta.

        A pending victim is cancelled (and released) immediately; a running
        victim only BEGINS draining here -- it is released later by
        ``process_draining_actors`` once fully drained. The returned delta
        counts victims acted on, not actors released.
        """
        num_downscaled = sum(
            1
            for actor_id in req.pending_ids
            if self._downscale_actor_by_id(actor_id, req.drain_reason)
        )
        num_downscaled += sum(
            self._shed_on_node(node_id, count, req.drain_reason)
            for node_id, count in Counter(req.nodes).items()
        )

        if num_downscaled > 0:
            logger.debug(
                f"Scaled down {self._map_worker_cls_name} actor pool by "
                f"{num_downscaled} (reason={req.reason}; {self.get_actor_info()})"
            )

        return -num_downscaled

    def _shed_on_node(self, node_id: NodeIdStr, count: int, reason: DrainReason) -> int:
        """Shed up to ``count`` actors on ``node_id``, picked from live state.

        Pendings targeted at the node go first (cancelled outright), then
        running actors cheapest drain first: fewest tasks in flight (each must
        run to completion and tends to add outputs), then fewest output blocks
        the ResourceBank still counts against the actor. Sheds fewer if the
        node holds fewer eligible actors than asked.
        """
        victims: List[ActorId] = [
            info.actor_id
            for info in self._pending_actor_info.values()
            if info.target_node_id == node_id
        ]
        ranked = sorted(
            (
                state.num_tasks_in_flight,
                self._num_outstanding_outputs(state),
                actor_id,
            )
            for actor_id, state in self._node_to_actor_states.get(node_id, {}).items()
            if state.status != ActorStatus.DRAINING
        )
        victims.extend(actor_id for _, _, actor_id in ranked)

        num_downscaled = 0
        for actor_id in victims[:count]:
            if self._downscale_actor_by_id(actor_id, reason):
                num_downscaled += 1
        return num_downscaled

    def _make_running_state(
        self, actor_id: ActorId, actor_location: NodeIdStr
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
            actor_id=actor_id,
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
        Clean up the actor from ``_actor_to_id`` (for all exceptions,
        not just RayError) to prevent memory leaks where dead actor handles
        remain, and roll back its pending/committed usage.
        """
        usage = self._actor_resource_usage.pop(actor)
        self._total_usage = self._total_usage.subtract(usage)
        self._pending_usage = self._pending_usage.subtract(usage)
        if pending_target_node is not None:
            self._remove_committed_usage(pending_target_node, usage)
        self._actor_to_id.pop(actor, None)

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
        self._id_to_actor[state.actor_id] = actor
        self._node_to_actor_states.setdefault(state.actor_location, {})[
            state.actor_id
        ] = state
        # Actor is no longer pending -- subtract from pending usage.
        usage = self._actor_resource_usage[actor]
        self._pending_usage = self._pending_usage.subtract(usage)
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
        actor, ready_ref, resource_usage = self._create_actor_fn(
            {}, None, scheduling_strategy=scheduling_strategy
        )
        self._actor_to_id[actor] = actor._actor_id.hex()
        return actor, ready_ref, resource_usage

    def _update_running_actor_state(self, actor: ActorHandle):
        """Refresh one running actor from its GCS-reported state: release it
        if dead, else re-admit it to the scheduling heaps."""
        # actor._get_local_state can return None if the state is Unknown.
        if actor._get_local_state() in (None, _ACTOR_STATE_DEAD):
            self.release_dead_actor(actor)
        else:
            self._sync_actor_to_heaps(actor, self._running_actors[actor])

    def release_dead_actor(self, actor: ActorHandle) -> None:
        """Release an actor that died (any status). Its in-flight tasks fail
        and are handed to Ray Data's recovery; their completion callbacks find
        the actor gone and skip the pool bookkeeping. No-op if already
        released."""
        state = self._running_actors.get(actor)
        if state is None:
            return
        if state.status == ActorStatus.DRAINING:
            self._record_drain_outcome_on_death(actor, state)
        else:
            logger.warning(
                "%s actor %s on node %s died with %d task(s) in flight; "
                "releasing it.",
                self._map_worker_cls_name,
                state.actor_id,
                state.actor_location,
                state.num_tasks_in_flight,
            )
        self._release_running_actor(actor)

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
            actor_id=self._actor_to_id[actor],
            target_node_id=target_node_id,
        )
        self._actor_resource_usage[actor] = resource_usage
        self._total_usage = self._total_usage.add(resource_usage)
        self._pending_usage = self._pending_usage.add(resource_usage)
        # A pending actor is committed to its target node (if one was set);
        # untargeted/core-scheduled pendings contribute nothing until ready.
        if target_node_id is not None:
            self._add_committed_usage(target_node_id, resource_usage)

    def _remove_pending_actor_by_ref(self, ready_ref: ObjectRef) -> None:
        """Drop a specific pending actor and its bookkeeping.

        The pool's references are dropped and Ray core garbage-collects the
        actor; an explicit kill would make its outputs unreconstructable.
        """
        actor = self._pending_actors.pop(ready_ref)
        info = self._pending_actor_info.pop(ready_ref, None)
        usage = self._actor_resource_usage.pop(actor)
        self._total_usage = self._total_usage.subtract(usage)
        self._pending_usage = self._pending_usage.subtract(usage)
        if info is not None and info.target_node_id is not None:
            self._remove_committed_usage(info.target_node_id, usage)
        del self._actor_to_id[actor]

    def release_expired_pending_actors(
        self, expiry_s: float = PENDING_ACTOR_EXPIRY_S, now: Optional[float] = None
    ) -> int:
        """Release pending actors older than ``expiry_s``, returning the count.

        A pending whose ready-ref never resolves -- its target node went away
        (placement is hard node affinity), a stuck runtime-env install, or a
        ghost -- otherwise holds the pool's pending count until the job dies.
        The handle is force-killed: a never-ready actor cannot be gracefully
        released. The freed claim lets the caller re-place the actor with
        current cluster information.
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
            self._pending_usage = self._pending_usage.subtract(usage)
            if info.target_node_id is not None:
                self._remove_committed_usage(info.target_node_id, usage)
            self._actor_to_id.pop(actor, None)
            logger.warning(
                "Releasing pending actor %s: not ready after %.0fs "
                "(target_node=%s). Killing the handle and freeing its claim so "
                "it can be re-placed.",
                info.actor_id,
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
            self._pending_usage = self._pending_usage.subtract(usage)
            info = pending_info.get(ready_ref)
            if info is not None and info.target_node_id is not None:
                self._remove_committed_usage(info.target_node_id, usage)
            self._actor_to_id.pop(actor, None)

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
        """Remove the given actor from the pool by dropping all pool references.
        Ref counting then garbage collects the actor."""
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
        actor_id = actor_state.actor_id
        self._id_to_actor.pop(actor_id, None)
        node_states = self._node_to_actor_states.get(node_id)
        if node_states is not None:
            node_states.pop(actor_id, None)
            if not node_states:
                del self._node_to_actor_states[node_id]

        del self._running_actors[actor]
        del self._actor_to_id[actor]

        # Tell ResourceBank to drop this actor from the live-actor
        # indexes. Outputs the actor produced may still be in flight
        # downstream
        self.resource_bank.deregister_actor(actor_id=actor_id)

        usage = self._actor_resource_usage.pop(actor)
        self._total_usage = self._total_usage.subtract(usage)
        # Release this running actor's committed usage at its node.
        self._remove_committed_usage(node_id, usage)

    def _teardown_drained_actor(self, actor: ActorHandle) -> None:
        """Tear down a fully-drained actor (no in-flight tasks, no outstanding
        outputs). The pool's references are dropped and Ray core reclaims the
        actor."""
        if self._metrics is not None:
            info = self._draining_actors[actor]
            self._metrics.record_drain_completed(
                self._op_tag, info.reason.value, time.time() - info.since
            )
        self._release_running_actor(actor)

    def begin_draining(self, actor: ActorHandle, *, reason: DrainReason) -> None:
        """Mark an actor for draining. While draining, no new tasks can be
        submitted to it.

        The actor is removed from both scheduling heaps immediately (no new
        task submissions; ``_is_available_for_scheduling`` keeps it out) but
        stays in ``_running_actors`` and the capacity maps as it occupies its
        node's resources until it is actually dead. ``process_draining_actors``
        releases it once it is fully drained (no in-flight tasks AND no
        outstanding outputs).

        ``reason`` tags the drain metrics and decides whether a later upscale
        may reclaim the actor (``DrainReason.reclaimable``): an actor losing
        its node (preemption, allocation shrink) drains and dies like any
        other but is never resurrected."""
        state = self._running_actors[actor]
        assert (
            state.status != ActorStatus.DRAINING
        ), f"Actor {state.actor_id} already draining"
        self._set_status(state, ActorStatus.DRAINING)
        self._remove_from_heaps(actor, state.actor_location)
        self._draining_actors[actor] = _DrainingActorInfo(
            since=time.time(), reason=reason
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

        Both must hold before it is safe to tear the actor down. The in-flight
        check is the pool's own: the bank's
        prebuffered estimate reads zero when the generator buffer is configured
        to zero, so it cannot stand in for it.
        """
        if state.num_tasks_in_flight > 0:
            return False
        live = self.resource_bank.live_object_store(actor_id=state.actor_id)
        return live.all_outputs_consumed()

    def _num_outstanding_outputs(self, state: _ExperimentalActorState) -> int:
        """Output blocks the ResourceBank still counts against this actor."""
        live = self.resource_bank.live_object_store(actor_id=state.actor_id)
        return live.get_output_blocks()

    def _cancel_draining(self, actor: ActorHandle, cause: str) -> None:
        """Return a draining actor to the normal lifecycle.

        Inverse of ``begin_draining``'s bookkeeping; the caller re-syncs the
        scheduling heaps.

        ``cause`` says why the drain was cancelled ("reclaimed" for an upscale
        reclaim); the drain reason only records why it started.
        """
        state = self._running_actors[actor]
        assert (
            state.status == ActorStatus.DRAINING
        ), f"Actor {state.actor_id} is not draining"
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

    def _reclaimable_draining_actors(self) -> List[ActorHandle]:
        """Draining actors a reclaim may take, in reclaim order: busiest first
        (most tasks in flight, then most outstanding outputs), since reclaiming
        that actor rescues the most work from a slow drain. Ties keep
        drain-start order. Actors losing their node are excluded."""
        candidates = [
            actor for actor, info in self._draining_actors.items() if info.reclaimable
        ]
        candidates.sort(
            key=lambda actor: (
                -self._running_actors[actor].num_tasks_in_flight,
                -self._num_outstanding_outputs(self._running_actors[actor]),
            )
        )
        return candidates

    def num_reclaimable_draining_actors(self) -> int:
        """Draining actors a later upscale may still reclaim."""
        return sum(1 for info in self._draining_actors.values() if info.reclaimable)

    def reclaimable_node_ids(self, *, limit: int) -> List[NodeIdStr]:
        """Node id of each of the first ``limit`` draining actors a reclaim
        would take, in reclaim order (one entry per actor, so a node repeats
        once per actor on it). Placement counts these back onto their nodes
        before deciding where new actors go."""
        return [
            self._running_actors[actor].actor_location
            for actor in self._reclaimable_draining_actors()[:limit]
        ]

    def alive_actor_states_by_node(
        self,
    ) -> Mapping[NodeIdStr, Mapping[ActorId, _ExperimentalActorState]]:
        """Per-node states of the alive actors (draining ones included, with
        ``status == DRAINING``). Read-only: for placement snapshots."""
        return self._node_to_actor_states

    def draining_actor_ids(self) -> Set[ActorId]:
        """Logical ids of actors currently draining (marked draining, not yet
        released). Used to keep the output queue's prioritized set in sync."""
        return {self._running_actors[actor].actor_id for actor in self._draining_actors}

    def reclaim_draining_actors(self, *, limit: int) -> int:
        """Reclaim up to ``limit`` draining actors back into the running pool
        (inverse of ``begin_draining``), busiest first. Returns the number
        reclaimed. Saves actor startup + capacity churn vs killing and
        recreating."""
        if limit <= 0:
            return 0
        reclaimed = 0
        for actor in self._reclaimable_draining_actors()[:limit]:
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

        Called from ``refresh_actor_state`` once per scheduling step. An actor is "fully drained" once it has
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

    def _downscale_actor_by_id(self, actor_id: ActorId, reason: DrainReason) -> bool:
        """Shed one actor: a pending one is cancelled, a running one begins
        draining with ``reason``. False if it is gone or already draining."""
        for ready_ref, info in self._pending_actor_info.items():
            if info.actor_id == actor_id:
                self._remove_pending_actor_by_ref(ready_ref)
                return True

        actor = self._id_to_actor.get(actor_id)
        if actor is None or self._running_actors[actor].status == ActorStatus.DRAINING:
            logger.debug(
                f"Skipping {reason.value} downscale of actor {actor_id}: "
                f"{'not found' if actor is None else 'already draining'}."
            )
            return False
        self.begin_draining(actor, reason=reason)
        return True

    def has_actor(self, actor_id: ActorId) -> bool:
        """Whether the pool still holds a running actor with this logical id.

        False once the actor has been released (downscaled or reaped), even if
        in-flight task refs for it are still resolving.
        """
        return actor_id in self._id_to_actor

    def is_actor_draining(self, actor: ActorHandle) -> bool:
        """Whether this (still-running) actor is draining. False if released."""
        state = self._running_actors.get(actor)
        return state is not None and state.status == ActorStatus.DRAINING

    def pending_ids_by_target_node(self) -> Dict[NodeIdStr, List[ActorId]]:
        """Logical ids of pending actors, grouped by their target node."""
        result: DefaultDict[NodeIdStr, List[ActorId]] = defaultdict(list)
        for info in self._pending_actor_info.values():
            if info.target_node_id is not None:
                result[info.target_node_id].append(info.actor_id)
        return dict(result)

    def untargeted_pending_ids(self) -> List[ActorId]:
        """Logical ids of pending actors with no target node (placement-group
        or initial-size actors); ``pending_ids_by_target_node`` omits them."""
        return [
            info.actor_id
            for info in self._pending_actor_info.values()
            if info.target_node_id is None
        ]

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

    def draining_usage_by_node(self) -> Dict[NodeIdStr, ExecutionResources]:
        """Resources draining actors still hold, per node: the part of
        ``committed_usage_by_node`` that is already on its way out."""
        usage_by_node: Dict[NodeIdStr, ExecutionResources] = {}
        for actor in self._draining_actors:
            usage = self._actor_resource_usage.get(actor)
            if usage is None:
                continue
            node_id = self._running_actors[actor].actor_location
            usage_by_node[node_id] = usage_by_node.get(
                node_id, ExecutionResources.zero()
            ).add(usage)
        return usage_by_node

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
        # or ready location), so the entry must exist.
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

        alive = [
            s for s in node_states.values() if s.status != ActorStatus.DRAINING
        ] or list(node_states.values())
        # Prefer actors with tasks in flight: they hold the stalled outputs, and
        # raising an idle actor's caps relieves nothing.
        candidate_states = [s for s in alive if s.num_tasks_in_flight > 0] or alive
        best_state = min(
            candidate_states,
            key=lambda s: (s.max_num_output_bytes, s.max_num_outputs),
        )
        best_state.max_num_output_bytes += delta_bytes
        best_state.max_num_outputs += delta_outputs
        # NOTE: ``inf + delta`` is still ``inf`` -- the unset dimension
        # stays unset after the bump, which is fine: the tuple key above
        # will still discriminate via the finite dimension on the next call.

        actor = self._id_to_actor[best_state.actor_id]
        logger.debug(
            f"\nname={self._map_worker_cls_name} ({self.get_actor_info()})\n"
            f"Increasing the capacity for actor={actor} "
            f"located on {node_id} by {memory_string(delta_bytes)} / "
            f"{delta_outputs} output,\n"
            f"new limit is {memory_string(best_state.max_num_output_bytes)} / "
            f"{best_state.max_num_outputs} outputs\n"
        )

    def output_bytes_limit(self, actor_id: ActorId) -> float:
        actor = self._id_to_actor.get(actor_id)
        if actor is None:
            # The actor was released (killed / died) while one of its data
            # tasks lingers; callers like the output-backpressure path may still
            # ask for its limit -- impose no constraint rather than KeyError.
            return float("inf")
        return self._running_actors[actor].max_num_output_bytes

    def running_actor_ids(self) -> List[ActorId]:
        """Ray actor ids of running actors, draining ones included."""
        return [state.actor_id for state in self._running_actors.values()]

    def total_output_bytes_limit(self) -> float:
        """Sum of ``output_bytes_limit`` over running actors. Draining ones
        count: they hold their outputs until those are consumed."""
        return sum(
            state.max_num_output_bytes for state in self._running_actors.values()
        )

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

    def output_count_limit(self, actor_id: ActorId) -> float:
        actor = self._id_to_actor.get(actor_id)
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

    The actor-only counterpart of ``_NodeAwareActorPool``.
    """

    _draining_nodes_snapshot: Dict[NodeIdStr, int] = {}

    def __init__(
        self,
        *args,
        get_draining_nodes: Callable[[], Dict[NodeIdStr, int]] = get_draining_nodes,
        **kwargs,
    ):
        """``get_draining_nodes`` defaults to the cached Ray internal; tests
        inject a fake instead of patching module globals."""
        super().__init__(*args, **kwargs)
        self._get_draining_nodes = get_draining_nodes

    @override
    def refresh_actor_state(self):
        self._draining_nodes_snapshot = self._get_draining_nodes()
        super().refresh_actor_state()
        # Runs after the parent pass so dead actors are already released.
        self._drain_actors_on_draining_nodes()

    @override
    def pending_to_running(self, ready_ref: ray.ObjectRef) -> Optional[ActorHandle]:
        actor = super().pending_to_running(ready_ref)
        if actor is None:
            return actor

        # The base class makes every newly ready actor schedulable. Actor-ready
        # callbacks are processed before dispatch, so without this an actor that
        # came up on a draining node takes tasks until the next refresh.
        state = self._running_actors[actor]
        if state.actor_location in self._get_draining_nodes():
            self.begin_draining(actor, reason=DrainReason.PREEMPTION)

        return actor

    def _drain_actors_on_draining_nodes(self) -> None:
        """Begin draining every actor on a draining node.

        Victims are marked non-reclaimable: their node is going away, so a later
        upscale must never reclaim them. Draining is one-way for the same reason
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
                # Already draining (e.g. an ordinary downscale): the node is
                # going away, so a later upscale must never reclaim it here,
                # whatever started the drain.
                self._draining_actors[actor].reclaimable = False
                continue
            self.begin_draining(actor, reason=DrainReason.PREEMPTION)
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
