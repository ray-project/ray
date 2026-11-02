"""The actor-only backend's ``ActorPoolMapOperator``.

``ExperimentalActorPoolMapOperator`` subclasses the base ``ActorPoolMapOperator``
through explicit seams and swaps its push-based dispatch for a pull-based
protocol:

- **Dispatch**: ``_maybe_dispatch_next_bundle`` only enqueues full bundles onto
  the operator's internal task-submission queue. The streaming executor drives
  submission through ``can_submit_task()`` / ``launch_task()``. Actor selection
  and per-actor input/output budgets live in ``_ExperimentalActorPool`` (see
  ``actor_pool.py``); the operator turns the output budgets into
  ``task_pull_request``s that bound how much generator output the executor may
  pull per actor.

- **Draining**: the pool owns the drain lifecycle; the operator keeps the
  output queue in sync (``_sync_draining_priorities`` prioritizes draining
  actors' blocks so the outputs the ResourceBank still counts against them are
  consumed sooner) and tags each queued output with its producing actor.

- **ResourceBank**: the operator reports task/output/consumption events into
  the per-execution ``ResourceBank`` and reads back live per-actor/per-node
  object-store usage to compute pull budgets. Operator and pool satisfy the
  bank's structural ``ActorPoolOperator`` / ``ActorPoolAccounting`` protocols.
"""

from __future__ import annotations

import functools
import logging
import math
import time
import types
from collections import Counter, defaultdict
from typing import (
    TYPE_CHECKING,
    Any,
    DefaultDict,
    Dict,
    List,
    NoReturn,
    Optional,
    Tuple,
)

from typing_extensions import override

import ray
from ray import ObjectRef
from ray.actor import ActorHandle
from ray.data._internal.actor_autoscaler import AutoscalingActorPool
from ray.data._internal.actor_autoscaler.autoscaling_actor_pool import (
    AutoscalingActorConfig,
)
from ray.data._internal.compute import ActorPoolStrategy
from ray.data._internal.execution.block_ref_counter import BlockRefCounter
from ray.data._internal.execution.bundle_queue import BaseBundleQueue
from ray.data._internal.execution.execution_flags import (
    BLOCK_SIZE_TO_OUTPUT_BYTES_RATIO,
    CORE_ACTOR_BACKPRESSURE_NUM_OBJECTS_PER_YIELD,
    PER_ACTOR_INPUT_BACKPRESSURE_LIMIT,
    PER_ACTOR_OUTPUT_BACKPRESSURE_LIMIT,
    PER_ACTOR_OUTPUT_BYTES_BACKPRESSURE_LIMIT,
)
from ray.data._internal.execution.interfaces import (
    ExecutionOptions,
    ExecutionResources,
    NodeIdStr,
    PhysicalOperator,
    RefBundle,
    TaskContext,
)
from ray.data._internal.execution.interfaces.common import ActorId, ActorStatus
from ray.data._internal.execution.interfaces.physical_operator import TaskPullRequest
from ray.data._internal.execution.operators.actor_pool_map_operator import (
    ActorPoolMapOperator,
)
from ray.data._internal.execution.operators.map_operator import MapOperator
from ray.data._internal.execution.resource_bank import (
    DEFAULT_BYTES_PER_OUTPUT,
    ResourceBankBase,
)
from ray.data._internal.execution.util import memory_string, merge_label_selector
from ray.data._internal.experimental.execution.actor_only_op_metrics import (
    _ActorOnlyOpMetrics,
)
from ray.data._internal.experimental.execution.bundle_queue import (
    DrainingActorPriorityBundleQueue,
)
from ray.data._internal.experimental.execution.operators.actor_pool import (
    _ExperimentalActorPool,
    _NodeAwareExperimentalActorPool,
)
from ray.data._internal.experimental.execution.operators.placement_constraints import (
    PlacementConstraint,
)
from ray.data._internal.util import MiB
from ray.data.context import DataContext
from ray.exceptions import ActorUnavailableError, GetTimeoutError, RayActorError
from ray.util.debug import log_once
from ray.util.scheduling_strategies import (
    NodeAffinitySchedulingStrategy,
    PlacementGroupSchedulingStrategy,
    SchedulingStrategyT,
)

if TYPE_CHECKING:
    from ray.data._internal.execution.lineage_tracker import LineageTracker

logger = logging.getLogger(__name__)

# How stale the write-through (blocked, running) output-backpressure snapshot
# may get before the progress-bar path recomputes it itself (i.e. when no
# regular caller -- the executor or sizer -- is keeping it fresh).
_OUTPUT_BACKPRESSURE_CACHE_TTL_S = 1.0

# Floor on how much ``upgrade_output_limit`` raises an actor's output-bytes cap
# per call, so a tiny mean block size still makes progress.
_MIN_OUTPUT_LIMIT_UPGRADE_BYTES = MiB

# Wait budget for a pool's initial actors when ``wait_for_min_actors_s`` is unset:
#   budget = min(BASE + PER_ACTOR * num_actors, MAX)
# aborting early once no actor has started for STALL seconds. A single fixed
# deadline fits pool sizes badly (5 min is generous for 2 actors, too tight for
# 100 actors spread over several autoscaled nodes), so the budget scales with
# the pool and progress -- not wall clock -- decides whether to keep waiting.
# Deliberately not tunable: RAY_DATA_DEFAULT_WAIT_FOR_MIN_ACTORS_S is the one
# override and replaces all of this with one explicit deadline.
WAIT_FOR_MIN_ACTORS_BASE_S = 300
WAIT_FOR_MIN_ACTORS_PER_ACTOR_S = 15
WAIT_FOR_MIN_ACTORS_MAX_S = 1800
WAIT_FOR_MIN_ACTORS_STALL_S = 300

# Scheduling strategies Ray Data sets on its own behalf, not placement the user
# asked for: ``DataContext.scheduling_strategy`` defaults to "SPREAD" (and is
# deprecated in the new actor-only backend -- Data manages scheduling
# internally), and ``read_api`` stamps it on every distributed read. They are
# hints rather than pins, so ``_apply_default_remote_args`` drops them and the
# sizer places actors itself.
_IGNORED_SCHEDULING_STRATEGIES = frozenset({"DEFAULT", "SPREAD"})

# The node-id label Ray core stamps on every node; a label_selector on it is a
# node pin.
# pyrefly: ignore[missing-attribute]  # Cython module attribute.
_NODE_ID_LABEL_KEY: str = ray._raylet.RAY_NODE_ID_KEY


def wait_for_min_actors_budget_s(num_actors: int) -> float:
    """Overall wait budget for ``num_actors`` initial actors to come up.

    Grows linearly with the pool's minimum size (a bigger minimum may need
    several nodes autoscaled in) and saturates at ``WAIT_FOR_MIN_ACTORS_MAX_S``:
    1 actor -> ~5min, 20 -> 10min, 100 -> 30min (capped). This is only the
    ceiling; the wait normally ends far sooner, and a stalled pool fails after
    ``WAIT_FOR_MIN_ACTORS_STALL_S`` without waiting out the budget.
    """
    assert num_actors >= 0, f"num_actors must be non-negative, got {num_actors}"
    budget = WAIT_FOR_MIN_ACTORS_BASE_S + WAIT_FOR_MIN_ACTORS_PER_ACTOR_S * num_actors
    return float(min(budget, WAIT_FOR_MIN_ACTORS_MAX_S))


def _configure_map_worker_for_core_backpressure(
    map_worker_cls: type, data_context: DataContext
) -> type:
    """Configure a map-worker class for the grouped yields the actor-only
    backend's actor-wide generator cap counts against.

    Wraps ``submit`` with ``_num_objects_per_yield`` so the block and its
    metadata are reserved atomically against the actor-wide generator cap, and
    flips the context flag that makes ``yield_block_with_stats`` produce the
    matching grouped tuples on the worker.
    """
    if getattr(map_worker_cls.submit, "__ray_num_objects_per_yield__", None) == (
        CORE_ACTOR_BACKPRESSURE_NUM_OBJECTS_PER_YIELD
    ):
        data_context._use_grouped_streaming_generator_yields = True
        return map_worker_cls
    # Decorate a per-class copy of ``submit``: ``ray.method`` annotates the
    # function object in place, and the inherited attribute is the function
    # shared with ``_MapWorker`` -- mutating it would flip grouped yields on
    # for every operator in the process, including ones created with the
    # backend disabled.
    base_submit = map_worker_cls.submit
    submit_copy = types.FunctionType(
        base_submit.__code__,
        base_submit.__globals__,
        base_submit.__name__,
        base_submit.__defaults__,
        base_submit.__closure__,
    )
    functools.update_wrapper(submit_copy, base_submit)
    # ``update_wrapper`` records the original under ``__wrapped__``, and Ray's
    # actor-class introspection follows it (``inspect.unwrap``) when it reads
    # a method's ``@ray.method`` options -- it would find the base ``submit``
    # with no grouped-yield annotation, and the worker's grouped tuples would
    # then arrive as single objects. This copy IS the method; nothing to unwrap.
    del submit_copy.__wrapped__  # pyrefly: ignore[missing-attribute]
    submit_copy.__kwdefaults__ = base_submit.__kwdefaults__
    map_worker_cls.submit = ray.method(
        _num_objects_per_yield=CORE_ACTOR_BACKPRESSURE_NUM_OBJECTS_PER_YIELD,
    )(submit_copy)
    data_context._use_grouped_streaming_generator_yields = True
    return map_worker_cls


class ExperimentalActorPoolMapOperator(ActorPoolMapOperator):
    """Actor-only-backend ``ActorPoolMapOperator``. See the module docstring
    for the dispatch model, per-actor budgets, and the drain lifecycle."""

    # Cadence of the ``wait_for_min_actors`` poll loop, and how often it logs
    # progress while actors are still coming up.
    MIN_ACTORS_POLL_INTERVAL_S: float = 1.0
    MIN_ACTORS_LOG_INTERVAL_S: float = 30.0

    def __init__(self, *args, **kwargs):
        self._user_set_ray_remote_args: Dict[str, Any] = dict(
            kwargs.get("ray_remote_args") or {}
        )
        self._user_set_ray_remote_args_fn = kwargs.get("ray_remote_args_fn") is not None

        super().__init__(*args, **kwargs)
        self._input_queue_bytes_by_node: DefaultDict[NodeIdStr, int] = defaultdict(int)
        # Pool draining-generation last reconciled into the output queue's
        # prioritized set. -1 forces the first sync. See _sync_draining_priorities.
        self._last_synced_draining_generation: int = -1
        # Ray task ids by task index, for ResourceBank attribution.
        self._task_ray_id: Dict[int, ray.TaskID] = {}
        # Actor-only metrics, injected by the streaming executor (see
        # ``set_actor_only_metrics``); the operator runs without them.
        self._actor_only_metrics: Optional[_ActorOnlyOpMetrics] = None
        self._metrics_op_tag: Optional[str] = None
        # Write-through snapshot of ``output_backpressured_actors()`` for the
        # progress-bar path (see ``cached_output_backpressured_actors``).
        self._output_backpressured_actors_cache: Tuple[int, int] = (0, 0)
        self._output_backpressured_actors_cache_ts: float = float("-inf")
        # Parsed once on first use: constraints are fixed for the op's lifetime.
        self._placement_constraint: Optional[PlacementConstraint] = None

    def set_actor_only_metrics(self, metrics: _ActorOnlyOpMetrics, op_tag: str) -> None:
        self._actor_only_metrics = metrics
        self._metrics_op_tag = op_tag
        self.actor_pool.set_metrics(metrics, op_tag)

    @property
    def actor_pool(self) -> _ExperimentalActorPool:
        assert isinstance(self._actor_pool, _ExperimentalActorPool)
        return self._actor_pool

    @property
    def resource_bank(self) -> ResourceBankBase:
        assert (
            self._resource_bank is not None
        ), "The streaming executor provides the ResourceBank via start()"
        return self._resource_bank

    def validate_ray_remote_args(
        self, fn_resolved_args: Optional[Dict[str, Any]] = None
    ) -> None:
        """Reject ``ray_remote_args`` the sizer cannot reason about.

        Checks the user's ``ray_remote_args`` by default, or ``fn_resolved_args``
        if given (the per-actor args a ``ray_remote_args_fn`` resolved to).

        The sizer owns actor placement and models capacity in cpu/gpu/memory
        only. ``num_cpus`` / ``num_gpus`` / ``memory`` and non-placement knobs
        (``max_task_retries``, ``runtime_env``, ...) are fine. Everything else
        is rejected up front, when the ``OperatorSizer`` is built, rather than
        silently mis-placed:

        - a non-zero ``max_restarts``: a dead actor is replaced by the sizer and
          its work is redone by Ray Data lineage recovery, never by a Ray core
          restart;

        - a ``scheduling_strategy`` other than ``NodeAffinitySchedulingStrategy``
          (how local-scheme reads pin to the driver; honored as a node pin) or
          ``PlacementGroupSchedulingStrategy`` (honored by capping the pool at
          the PG's bundle capacity and letting Ray core place into it);
        - ``placement_group_capture_child_tasks``: Data's internal tasks must
          not be captured into the user's placement group;
        - custom ``resources``: the capacity model does not track them;
        - ``max_concurrency`` below 1: Ray accepts 0, but sizing divides by it.

        A ``ray_remote_args_fn`` is allowed only if it assigns a placement
        group and nothing else (the vLLM "ray" backend does exactly this): a fn
        that pins placement or changes the resource footprint would break
        capacity accounting. Calling the fn here would mint a real placement
        group, so ``_merge_ray_remote_args`` validates its output per actor.
        """
        if fn_resolved_args is None:
            args = self._user_set_ray_remote_args
        else:
            args = fn_resolved_args
            strategy = args.get("scheduling_strategy")
            if not isinstance(strategy, PlacementGroupSchedulingStrategy):
                self._raise_unsupported(
                    "a ray_remote_args_fn that does not assign a placement group "
                    f"(resolved scheduling_strategy={strategy!r}); only a fn that "
                    "assigns a PlacementGroupSchedulingStrategy is supported"
                )
            for key in ("num_cpus", "num_gpus", "memory", "resources"):
                if args.get(key) != self._ray_remote_args.get(key):
                    self._raise_unsupported(
                        f"a ray_remote_args_fn that overrides {key!r} "
                        f"({self._ray_remote_args.get(key)!r} -> {args.get(key)!r}); "
                        "the fn may only assign a placement group, not change the "
                        "actor's resource footprint"
                    )
        strategy = args.get("scheduling_strategy")
        allowed_strategies = (
            NodeAffinitySchedulingStrategy,
            PlacementGroupSchedulingStrategy,
        )
        if isinstance(strategy, str):
            # ``read_datasource`` (read_api.py) stamps
            # DataContext.scheduling_strategy ("SPREAD") on its read op, so a
            # string here is the user's unless this is a read.
            # Either way it never reaches an actor: _apply_default_remote_args
            # drops it and the sizer places instead.
            if not self.is_read_op:
                self._raise_unsupported(
                    f"scheduling_strategy={strategy!r}; this backend supports "
                    f"only a label_selector, a placement group, and "
                    f"num_cpus / num_gpus / memory"
                )
        elif strategy is not None and not isinstance(strategy, allowed_strategies):
            self._raise_unsupported(
                "a custom scheduling_strategy (the sizer owns actor placement)"
            )
        if args.get("max_restarts", 0) != 0:
            self._raise_unsupported(
                f"max_restarts={args['max_restarts']!r} (actors are never "
                "restarted by Ray core: a dead actor is replaced and its work is "
                "redone by Ray Data lineage recovery)"
            )
        if args.get("placement_group_capture_child_tasks"):
            self._raise_unsupported(
                "placement_group_capture_child_tasks (Data's internal tasks must "
                "not be captured into the user's placement group)"
            )
        if args.get("resources"):
            self._raise_unsupported(
                "custom resources (the capacity model only tracks cpu/gpu/memory)"
            )
        # The merged args: max_concurrency can also come from
        # ActorPoolStrategy(max_concurrent_calls_per_actor=...).
        max_concurrency = self._ray_remote_args.get("max_concurrency")
        if max_concurrency is not None and max_concurrency < 1:
            raise ValueError(
                f"max_concurrency must be >= 1, got {max_concurrency} "
                f"(operator {self.name})"
            )

    @override
    def _on_actor_init_death(self, error: Exception) -> None:
        """Charge ``max_consecutive_actor_init_deaths`` only for deaths where
        the actor's ``__init__`` raised: a broken UDF fails every replacement,
        so it must exhaust the budget and fail the dataset. A process or node
        loss while starting (OOM, preemption, chaos) is not the UDF's fault;
        the pool already dropped the pending actor and the sizer re-places it.
        """
        if isinstance(error, RayActorError) and error.actor_init_failed:
            super()._on_actor_init_death(error)
            return
        logger.debug(
            f"{self.name}: an actor was lost while starting; the sizer replaces it.",
            exc_info=error,
        )

    @override
    def extra_resource_usage(self) -> ExecutionResources:
        """Always zero: Ray core never recreates these actors for lineage
        reconstruction (``max_restarts=0``)."""
        return ExecutionResources.zero()

    @override
    def _create_actor_pool(
        self, compute_strategy: ActorPoolStrategy
    ) -> "AutoscalingActorPool":
        config = self._create_actor_pool_config(compute_strategy)
        return _NodeAwareExperimentalActorPool(
            # The base operator annotates the readiness ref with the
            # ``ray.types`` alias, the pool interface with ``ray.ObjectRef``;
            # the same class at runtime.
            # pyrefly: ignore[bad-argument-type]
            create_actor_fn=self._start_actor,
            config=config,
            map_worker_cls_name=self._map_worker_cls_name,
        )

    @override
    def start(
        self,
        options: ExecutionOptions,
        block_ref_counter: BlockRefCounter,
        lineage_tracker: Optional["LineageTracker"] = None,
        resource_bank: Optional[ResourceBankBase] = None,
    ) -> None:
        assert (
            resource_bank is not None
        ), "The streaming executor provides the ResourceBank via start()"
        self.actor_pool.set_resource_bank(resource_bank)
        super().start(
            options, block_ref_counter, lineage_tracker, resource_bank=resource_bank
        )

    @override
    def _scale_to_initial_size(self) -> None:
        """No-op: initial sizing is the sizer's decision, made with
        constraint-aware placement rather than untargeted creation."""
        pass

    @override
    def _create_map_worker_cls(self, map_worker_cls_name: str) -> type:
        # Grouped block+metadata yields, reserved atomically against the
        # actor-wide generator cap this backend bounds output with.
        return _configure_map_worker_for_core_backpressure(
            super()._create_map_worker_cls(map_worker_cls_name), self.data_context
        )

    @override
    def _apply_default_actor_task_remote_args(
        self,
        ray_actor_task_remote_args: Optional[Dict[str, Any]],
        data_context: DataContext,
    ) -> Dict[str, Any]:
        ray_actor_task_remote_args = dict(ray_actor_task_remote_args or {})
        if not self._generator_backpressure_disabled:
            # The actor-wide cap (``_actor_generator_backpressure_num_objects``
            # on the actor) owns generator backpressure; the per-task budget is
            # disabled (-1) so it does not compete with the actor-wide waiter.
            # Set before the base defaults so they leave it alone.
            ray_actor_task_remote_args.setdefault(
                "_generator_backpressure_num_objects", -1
            )
        return super()._apply_default_actor_task_remote_args(
            ray_actor_task_remote_args, data_context
        )

    @override
    def wait_for_min_actors(self) -> None:
        """Block until the pool's initial actors are up.

        Always on for this backend. An explicit ``wait_for_min_actors_s`` (> 0)
        is a hard deadline; otherwise the deadline scales with the number of
        actors waited on (see ``wait_for_min_actors_budget_s``) and the wait
        aborts as soon as actor startup stalls, so a wedged pool fails fast no
        matter how large its budget is.
        """
        refs = self._actor_pool.get_pending_actor_refs()
        if not refs:
            return
        configured = self.data_context.wait_for_min_actors_s
        if configured > 0:
            timeout, stall_timeout = float(configured), None
            stall_note = ""
        else:
            timeout = wait_for_min_actors_budget_s(len(refs))
            stall_timeout = float(WAIT_FOR_MIN_ACTORS_STALL_S)
            stall_note = f", aborting early if no actor starts for {stall_timeout:.0f}s"
        logger.info(
            f"{self._name}: waiting for {len(refs)} minimum actors to start "
            f"before execution begins (up to {timeout:.0f}s{stall_note})."
        )
        self._wait_for_pending_actors(refs, timeout, stall_timeout)

    def _wait_for_pending_actors(
        self,
        refs: List[ObjectRef],
        timeout_s: float,
        stall_timeout_s: Optional[float],
    ) -> None:
        """Wait for ``refs`` to resolve, tracking startup progress.

        Args:
            refs: Pending actor readiness refs.
            timeout_s: Overall budget for the whole set.
            stall_timeout_s: If set, also fail when no ref resolves within this
                many seconds of the last one that did.
        """
        total = len(refs)
        pending = list(refs)
        start_t = time.perf_counter()
        last_progress_t = start_t
        last_log_t = start_t

        while pending:
            now = time.perf_counter()
            budget = start_t + timeout_s - now
            stalled = stall_timeout_s is not None and (
                now - last_progress_t >= stall_timeout_s
            )
            if budget <= 0 or stalled:
                self._raise_min_actors_timeout(
                    num_started=total - len(pending),
                    total=total,
                    elapsed_s=now - start_t,
                    stalled_for_s=(now - last_progress_t) if stalled else None,
                )

            # Poll at most every MIN_ACTORS_POLL_INTERVAL_S; the stall check
            # above runs on every iteration, so no tighter bound is needed.
            ready, pending = ray.wait(
                pending,
                num_returns=len(pending),
                timeout=min(budget, self.MIN_ACTORS_POLL_INTERVAL_S),
            )
            if ready:
                # Surfaces actor construction failures (e.g. a raising
                # __init__) immediately instead of at first task submission.
                ray.get(ready)
                last_progress_t = time.perf_counter()

            now = time.perf_counter()
            if pending and now - last_log_t >= self.MIN_ACTORS_LOG_INTERVAL_S:
                last_log_t = now
                logger.info(
                    f"{self._name}: {total - len(pending)}/{total} minimum "
                    f"actors started after {now - start_t:.0f}s."
                )

    def _raise_min_actors_timeout(
        self,
        *,
        num_started: int,
        total: int,
        elapsed_s: float,
        stalled_for_s: Optional[float],
    ) -> NoReturn:
        if stalled_for_s is not None:
            cause = (
                f"no additional actor started in the last {stalled_for_s:.0f}s "
                f"({num_started}/{total} up after {elapsed_s:.0f}s)"
            )
        else:
            cause = (
                f"only {num_started}/{total} actors started within the "
                f"{elapsed_s:.0f}s budget"
            )
        message = (
            f"{self._name}: timed out waiting for the operator's minimum "
            f"actors to start -- {cause}. This usually means the cluster "
            "cannot fit every operator's minimum actors at once -- tune the "
            "DAG's per-operator minimum concurrency so all minimums fit, or "
            "increase RAY_DATA_DEFAULT_WAIT_FOR_MIN_ACTORS_S if your "
            "infrastructure needs longer to provision nodes."
        )
        logger.error(message)
        raise GetTimeoutError(message)

    @override
    def _merge_ray_remote_args(
        self, scheduling_strategy: SchedulingStrategyT = None
    ) -> Dict[str, Any]:
        remote_args = super()._merge_ray_remote_args()
        if self._user_set_ray_remote_args_fn:
            self.validate_ray_remote_args(fn_resolved_args=remote_args)
        if scheduling_strategy is not None:
            # Placement-resolved creation supersedes the op's default
            # scheduling_strategy (a string). Self-placed (PG /
            # ray_remote_args_fn) ops are created untargeted, so the strategy
            # is None for them -- this never silently overrides genuine user
            # placement intent.
            remote_args["scheduling_strategy"] = scheduling_strategy
        return remote_args

    def _raise_unsupported(self, reason: str) -> NoReturn:
        """Reject an operator configuration this backend cannot honor. Shared by
        the ray_remote_args_fn check here and the placement-constraint checks
        layered on in later PRs."""
        raise NotImplementedError(
            f"The Ray Data actor-only backend does not support {self.name}: "
            f"it specifies {reason}. Set only num_cpus / num_gpus / memory in "
            f"ray_remote_args, or disable the actor-only backend "
            f"(RAY_DATA_ACTOR_ONLY_BACKEND=0). "
            f"(ray_remote_args={self._user_set_ray_remote_args}, "
            f"ray_remote_args_fn={self._user_set_ray_remote_args_fn})"
        )

    # --- sizer surface --------------------------------------------------------
    # The OperatorSizer sizes this operator through the methods below; the
    # operator never resizes itself (see ``_scale_to_initial_size``).

    def _sync_draining_priorities(self) -> None:
        """Sync the output queue's prioritized set with the pool's draining
        set, so a draining actor's already-queued blocks are served first and
        it reaches "fully drained" (and is released) sooner.

        Newly-draining actors are prioritized (their queued blocks jump the
        line); reclaimed/released actors are deprioritized. The sync is skipped
        entirely unless the pool's draining set changed since the last call.
        """
        queue = self._output_queue
        generation = self.actor_pool.draining_generation
        if (
            isinstance(queue, DrainingActorPriorityBundleQueue)
            and generation != self._last_synced_draining_generation
        ):
            self._last_synced_draining_generation = generation
            draining = self.actor_pool.draining_actor_ids()
            prioritized = queue.priority_actors()
            # Only newly-draining actors trigger the O(queue) prioritize scan;
            # reclaimed/gone actors are dropped in O(1).
            for actor_id in draining - prioritized:
                queue.prioritize_actor(actor_id)
            for actor_id in prioritized - draining:
                queue.deprioritize_actor(actor_id)

    def placement_constraint(self) -> PlacementConstraint:
        """This op's scheduling constraints as a label_selector (+ optional
        placement group). A ``NodeAffinitySchedulingStrategy`` pin becomes a
        ``ray.io/node-id`` selector entry, so every consumer sees one shape.

        A static placement group reaches us either as the legacy
        ``placement_group`` remote arg or inside a
        ``PlacementGroupSchedulingStrategy``; both normalize to the same
        fields here.
        """
        if self._placement_constraint is not None:
            return self._placement_constraint
        merged = merge_label_selector(
            self._ray_remote_args, self.data_context.execution_options.label_selector
        )
        selector = dict(merged.get("label_selector") or {})
        strategy = self._ray_remote_args.get("scheduling_strategy")
        if isinstance(strategy, NodeAffinitySchedulingStrategy):
            selector[_NODE_ID_LABEL_KEY] = strategy.node_id

        pg = self._user_set_ray_remote_args.get("placement_group")
        bundle_index = self._user_set_ray_remote_args.get(
            "placement_group_bundle_index"
        )
        pg_strategy = self._user_set_ray_remote_args.get("scheduling_strategy")
        if pg is None and isinstance(pg_strategy, PlacementGroupSchedulingStrategy):
            pg = pg_strategy.placement_group
            bundle_index = pg_strategy.placement_group_bundle_index

        self._placement_constraint = PlacementConstraint(
            label_selector=selector or None,
            placement_group=pg,
            placement_group_bundle_index=bundle_index,
            mints_pg_per_actor=self._user_set_ray_remote_args_fn,
        )
        return self._placement_constraint

    @override
    def _create_output_queue(self, preserve_order: bool) -> BaseBundleQueue:
        # Serve a draining actor's outputs first so it can be released sooner.
        # ``preserve_order`` is not honored: the executor's topology validation
        # (``topology_validation``) rejects it right after the operators start,
        # i.e. after this call, so asserting on it here would pre-empt that
        # error with an AssertionError.
        return DrainingActorPriorityBundleQueue()

    @override
    def _output_add_kwargs(self, task_index: int) -> Dict[str, Any]:
        # Tag the bundle with its producing actor so the queue can serve
        # draining actors' outputs first.
        return {"key": self._actor_id_for_task(task_index)}

    def _actor_id_for_task(self, task_index: int) -> ActorId:
        """The actor running ``task_index``, from the ResourceBank's task ->
        actor binding. Valid while the task is live: outputs are enqueued before
        the task's completion is processed, so the binding is always present
        here."""
        actor_id = self.resource_bank.get_actor_id_from_task_id(
            op=self, task_id=self._task_ray_id[task_index]
        )
        assert actor_id is not None, f"task {task_index} has no actor in the bank"
        return actor_id

    def input_queue_bytes_by_node(self) -> Dict[NodeIdStr, int]:
        """Bytes queued for dispatch, by the node that produced them. Nodes
        drop out once their queued bytes reach zero."""
        return dict(self._input_queue_bytes_by_node)

    def actor_counts_by_node(self) -> Dict[NodeIdStr, "Counter[ActorStatus]"]:
        """Breakdown of actor counts per node, pendings included (at their
        target node). Part of the ResourceBank's ``ActorPoolOperator``
        protocol."""
        counts = {
            node_id: Counter(status_counts)
            for node_id, status_counts in (
                self.actor_pool.running_actor_status_counts_by_node().items()
            )
        }
        for (
            node_id,
            pending_ids,
        ) in self.actor_pool.pending_ids_by_target_node().items():
            counts.setdefault(node_id, Counter())[ActorStatus.PENDING] += len(
                pending_ids
            )
        return counts

    @override
    def _maybe_dispatch_next_bundle(self) -> None:
        # Pull-based input protocol: instead of self-dispatching a task when a
        # full bundle is ready (the base push model), enqueue the bundle onto the
        # operator's internal task-submission queue. The scheduling loop later
        # pulls work from this queue via can_submit_task() / launch_task().
        if self._block_ref_bundler.peek_next() is not None:
            self._add_to_inner_bundle_queue()

    @override
    def has_next(self) -> bool:
        # Skip the base ActorPoolMapOperator layer (its has_next dispatches
        # queued bundles as a side effect -- the push model); go straight to
        # the generic operator implementation.
        return super(ActorPoolMapOperator, self).has_next()

    @override
    def all_inputs_done(self):
        self._block_ref_bundler.finalize()

        # Drain any remaining bundles into the internal task-submission queue.
        while self._block_ref_bundler.has_next():
            self._add_to_inner_bundle_queue()

        assert (
            self._block_ref_bundler.estimate_size_bytes() == 0
        ), f"Bundler in {self} must be empty (got {self._block_ref_bundler.num_blocks()} blocks)"

        # Skip the base ActorPoolMapOperator/MapOperator layers (min-actors
        # warning + bundler drain-dispatch -- the push model) and go straight
        # to the generic operator finalization.
        super(MapOperator, self).all_inputs_done()

    @override
    def _apply_default_remote_args(
        self, ray_remote_args: Dict[str, Any], data_context: DataContext
    ) -> Dict[str, Any]:
        strategy = (ray_remote_args or {}).get("scheduling_strategy")
        user_set_strategy = (
            strategy is not None and strategy not in _IGNORED_SCHEDULING_STRATEGIES
        )
        ray_remote_args = super()._apply_default_remote_args(
            ray_remote_args, data_context
        )
        # Ray core never restarts these actors: a dead actor is released, the
        # sizer places a replacement, and Ray Data lineage recovery redoes its
        # work. Core still retries tasks on a live actor (``retry_exceptions``),
        # which the base only enables when restarts are on.
        ray_remote_args["max_restarts"] = 0
        ray_remote_args.setdefault("max_task_retries", -1)
        if not user_set_strategy:
            # Placement is resolved per actor at creation time (NodeAffinity,
            # or an untargeted placement honoring a label_selector); drop the
            # base default strategy string.
            ray_remote_args.pop("scheduling_strategy", None)
        if (
            not self._generator_backpressure_disabled
            and "_actor_generator_backpressure_num_objects" not in ray_remote_args
        ):
            # The actor-wide generator cap is what bounds output on this
            # backend, so the buffer that sizes it must be a real block count.
            buffer = data_context._max_num_blocks_in_streaming_gen_buffer
            assert buffer is not None and buffer > 0, (
                "_max_num_blocks_in_streaming_gen_buffer must be a positive block "
                f"count on the actor-only backend, got {buffer!r}"
            )
            # Object count: one grouped yield emits block + metadata.
            ray_remote_args["_actor_generator_backpressure_num_objects"] = (
                CORE_ACTOR_BACKPRESSURE_NUM_OBJECTS_PER_YIELD * buffer
            )
        return ray_remote_args

    @override
    def _create_actor_pool_config(
        self, compute_strategy: ActorPoolStrategy
    ) -> "AutoscalingActorConfig":
        max_actor_concurrency = self._ray_remote_args.get("max_concurrency", 1)

        # Input admission is a per-actor cap on tasks in flight: the user's
        # strategy or DataContext value, else a small multiple of the actor's
        # concurrency so every slot stays pipelined.
        max_tasks_in_flight_per_actor: int = (
            compute_strategy.max_tasks_in_flight_per_actor
            or self.data_context.max_tasks_in_flight_per_actor
            or max_actor_concurrency * PER_ACTOR_INPUT_BACKPRESSURE_LIMIT
        )

        max_num_output_bytes_per_actor: Optional[int] = (
            compute_strategy.max_num_output_bytes_per_actor
            or PER_ACTOR_OUTPUT_BYTES_BACKPRESSURE_LIMIT
        )

        max_num_outputs_per_actor: Optional[int] = (
            compute_strategy.max_num_outputs_per_actor
            or PER_ACTOR_OUTPUT_BACKPRESSURE_LIMIT
        )
        # The block cap is always resolved (strategy value, else the flag
        # default): this backend bounds per-actor outstanding blocks to cap the
        # number of object refs on the head node. The bytes cap may stay None,
        # which the pool resolves per actor from its node's object store.
        assert max_num_outputs_per_actor is not None

        per_actor_resource_usage = ExecutionResources(
            cpu=self._ray_remote_args.get("num_cpus"),
            gpu=self._ray_remote_args.get("num_gpus"),
            memory=self._ray_remote_args.get("memory"),
        )

        logger.info(
            "Actor pool backpressure limits for %s: max_actor_concurrency=%s "
            "max_tasks_in_flight_per_actor=%s "
            "max_num_output_bytes_per_actor=%s max_num_outputs_per_actor=%s",
            self.name,
            max_actor_concurrency,
            max_tasks_in_flight_per_actor,
            max_num_output_bytes_per_actor,
            max_num_outputs_per_actor,
        )

        config = AutoscalingActorConfig(
            min_size=compute_strategy.min_size,
            max_size=compute_strategy.max_size,
            initial_size=compute_strategy.initial_size,
            max_tasks_in_flight_per_actor=max_tasks_in_flight_per_actor,
            max_num_output_bytes_per_actor=max_num_output_bytes_per_actor,
            max_num_outputs_per_actor=max_num_outputs_per_actor,
            max_actor_concurrency=max_actor_concurrency,
            per_actor_resource_usage=per_actor_resource_usage,
        )
        return config

    def _add_to_inner_bundle_queue(self):
        # The ref bundler combines one or more `RefBundle`s into a new
        # `RefBundle`. To update metrics appropriately, we need to deque
        # original input bundles.
        (
            bundled_input,
            input_refs,
        ) = self._block_ref_bundler.get_next_with_original()

        for input_ref in input_refs:
            self._metrics.on_input_dequeued(input_ref, input_index=0)
        self._bundle_queue.add(bundled_input)
        for (
            node_id,
            num_bytes,
        ) in bundled_input.get_preferred_object_locations_from_metadata().items():
            self._input_queue_bytes_by_node[node_id] += num_bytes
        self._metrics.on_input_queued(bundled_input, input_index=0)
        # Notify first input for deferred initialization (e.g., Iceberg schema
        # evolution).
        self._notify_first_input(bundled_input)

    def has_queued_bundle(self) -> bool:
        """Whether a bundle is ready to dispatch (it may still wait on an
        actor)."""
        return self._bundle_queue.peek_next() is not None

    def _pick_next_actor(self) -> Tuple[ActorHandle, RefBundle] | None:
        bundle = self._bundle_queue.peek_next()
        if bundle is None:
            return None

        if self._actor_locality_enabled is None:
            actor_locality_enabled = True
        else:
            actor_locality_enabled = self._actor_locality_enabled

        next_actor = self.actor_pool.select_actors(
            bundle=bundle, actor_locality_enabled=actor_locality_enabled
        )
        if next_actor is None:
            return None

        return next_actor, bundle

    def can_submit_task(self) -> bool:
        """Whether a queued bundle can be dispatched right now.

        Contract with the scheduling loop: returns True only when a subsequent
        ``launch_task()`` is guaranteed to submit a task.
        """
        return self._pick_next_actor() is not None

    @override
    def _create_task_context(self, actor: ActorHandle) -> TaskContext:
        """Build TaskContext, sizing blocks from this actor's output budget.

        Skips the bytes-based override when the map transformer sizes by row
        count or has block shaping disabled (e.g. StreamingRepartition) -- those
        transforms reject ``target_max_block_size`` overrides.
        """
        # Ops that size by rows / disable shaping cannot accept a bytes override.
        last_transform = self.get_map_transformer().get_transform_fns()[-1]
        block_opt = last_transform.output_block_size_option
        can_override_block_size = block_opt is None or (
            not block_opt.disable_block_shaping
            and block_opt.target_num_rows_per_block is None
        )

        if not can_override_block_size:
            target_max_block_size = self.target_max_block_size_override
        else:
            target_max_block_size = (
                self.target_max_block_size_override
                or self.data_context.target_max_block_size
            )
            # None means the user disabled chopping -- leave blocks whole.
            if target_max_block_size is not None:
                output_limit = self.actor_pool.output_bytes_limit(
                    self.actor_pool.get_actor_logical_id(actor)
                )
                assert math.isfinite(output_limit) and output_limit > 0
                target_max_block_size = min(
                    int(output_limit * BLOCK_SIZE_TO_OUTPUT_BYTES_RATIO),
                    target_max_block_size,
                )

        return TaskContext(
            task_idx=self._next_data_task_idx,
            op_name=self.name,
            target_max_block_size_override=target_max_block_size,
        )

    @override
    def launch_task(self):
        """Dispatch one task from the internal queue to a selected actor.

        The scheduling loop only calls this after ``can_submit_task()``
        returned True, so actor selection cannot come up empty here.
        """
        actor_bundle = self._pick_next_actor()
        assert actor_bundle is not None, "launch_task requires can_submit_task()"
        actor, bundle = actor_bundle
        actor_node_id = self.actor_pool.get_actor_location(actor)
        actor_logical_id = self.actor_pool.get_actor_logical_id(actor)

        self.resource_bank.maybe_register_actor(
            op=self,
            actor_id=actor_logical_id,
            node_id=actor_node_id,
        )

        self._dequeue_bundle_for_dispatch(bundle)
        self.actor_pool.on_task_submitted(actor, input_bundle=bundle)
        # _submit_data_task below assigns this task index, then increments.
        task_index = self._next_data_task_idx

        ctx = self._create_task_context(actor)
        gen = self._submit_to_actor(actor, bundle, ctx)

        self._submit_data_task(
            gen=gen,
            inputs=bundle,
            task_output_ready_callback=functools.partial(
                self._on_task_output_ready,
                task_index,
                ctx,
                actor,
                actor_logical_id,
            ),
            task_done_callback=functools.partial(
                self._on_task_done, actor_to_return=actor
            ),
        )
        data_task = self._data_tasks[task_index]
        ray_task_id = data_task.get_task_id()
        self._task_ray_id[task_index] = ray_task_id
        self.resource_bank.on_task_submitted(
            actor_id=actor_logical_id,
            task_id=ray_task_id,
            input_bms=list(bundle.metadata),
        )

        self._record_locality_metrics(
            bundle,
            actor_node_id,
            bundle.get_preferred_object_locations_from_metadata(),
        )

    def _dequeue_bundle_for_dispatch(self, bundle: RefBundle) -> None:
        """Remove a bundle from the internal queue and its per-node byte
        accounting, recording the dequeue in metrics."""
        self._bundle_queue.remove(bundle)
        for (
            node_id,
            num_bytes,
        ) in bundle.get_preferred_object_locations_from_metadata().items():
            remaining = self._input_queue_bytes_by_node[node_id] - num_bytes
            assert remaining >= 0, f"queued bytes for node={node_id} underflow"
            if remaining == 0:
                del self._input_queue_bytes_by_node[node_id]
            else:
                self._input_queue_bytes_by_node[node_id] = remaining
        self._metrics.on_input_dequeued(bundle, input_index=0)

    def _submit_to_actor(self, actor: ActorHandle, bundle: RefBundle, ctx: TaskContext):
        """Submit the map task for ``bundle`` to ``actor``; returns the
        streaming generator."""
        input_blocks = [entry.ref for entry in bundle.blocks]
        actor_task_args = dict(self._ray_actor_task_remote_args)
        extra_labels = actor_task_args.pop("_labels", None) or {}
        return actor.submit.options(
            num_returns="streaming",
            _labels={self._OPERATOR_ID_LABEL_KEY: self.id, **extra_labels},
            **actor_task_args,
        ).remote(
            self._data_context_ref,
            ctx,
            *input_blocks,
            slices=bundle.slices,
            **self.get_map_task_kwargs(),
        )

    def _on_task_output_ready(
        self,
        task_index: int,
        ctx: TaskContext,
        actor: ActorHandle,
        actor_logical_id: ActorId,
        output: RefBundle,
    ) -> None:
        """Per-output bookkeeping: bank registration and overshoot detection."""
        ray_task_id = self._task_ray_id.get(task_index)
        target_block_size = ctx.target_max_block_size_override
        output_bytes_limit = self.actor_pool.output_bytes_limit(
            actor_id=actor_logical_id
        )
        for entry in output.blocks:
            size_bytes = entry.metadata.size_bytes or 0
            if (
                target_block_size is not None
                and target_block_size > 0
                and size_bytes > 1.5 * target_block_size
            ):
                self._warn_block_size_overshoot(actor, size_bytes, target_block_size)
            self.resource_bank.on_new_output(
                op=self,
                ref=entry.ref,
                bm=entry.metadata,
                task_id=ray_task_id,
            )
            actor_live = self.resource_bank.live_object_store(actor_id=actor_logical_id)
            total_output_bytes = (
                actor_live.num_pulled_output_bytes
                + actor_live.num_prebuffered_output_bytes
            )
            if (
                math.isfinite(output_bytes_limit)
                and output_bytes_limit > 0
                and total_output_bytes > 1.5 * output_bytes_limit
            ):
                self._warn_output_limit_overshoot(
                    actor, total_output_bytes, output_bytes_limit
                )

    def _warn_block_size_overshoot(
        self, actor: ActorHandle, size_bytes: int, target: int
    ) -> None:
        if (metrics := self._actor_only_metrics) is not None:
            assert self._metrics_op_tag is not None
            metrics.record_block_size_overshoot(self._metrics_op_tag)
        if log_once(f"actor_only_block_size_overshoot_{self.id}"):
            logger.warning(
                f"{self.name}: actor={actor} produced an "
                f"output of {memory_string(size_bytes)}, which is "
                f"more than 150% of its target block size "
                f"{memory_string(target)}. This warning "
                "is logged once per operator; see the "
                "data_actor_only_operator_output_overshoots metric "
                "for the full count."
            )

    def _warn_output_limit_overshoot(
        self, actor: ActorHandle, total_bytes: int, limit: float
    ) -> None:
        if (metrics := self._actor_only_metrics) is not None:
            assert self._metrics_op_tag is not None
            metrics.record_output_limit_overshoot(self._metrics_op_tag)
        if log_once(f"actor_only_output_bytes_overshoot_{self.id}"):
            logger.warning(
                f"{self.name}: actor={actor} has "
                f"{memory_string(total_bytes)} of outstanding "
                f"output, which is more than 150% of its output-bytes "
                f"limit {memory_string(limit)}. This "
                "warning is logged once per operator; see the "
                "data_actor_only_operator_output_overshoots metric "
                "for the full count."
            )

    def _on_task_done(
        self,
        inputs: RefBundle,
        task_index: int,
        exception: Optional[Exception],
        actor_to_return: ActorHandle,
    ) -> None:
        """Reverse the in-flight accounting a finished task held, and release
        the actor right away if the task failed because it died, so nothing
        else is dispatched to it before the next state refresh.

        The bank and pool calls are tolerant of an already-released actor (a
        forced shutdown, or an earlier failed task on the same dead actor,
        releases it before its other in-flight tasks finish)."""
        ray_task_id = self._task_ray_id.pop(task_index)

        self.resource_bank.on_task_completed(op=self, task_id=ray_task_id)
        self.actor_pool.on_task_completed(actor_to_return, input_bundle=inputs)
        for entry in inputs.blocks:
            self.resource_bank.on_block_consumed(input_ref=entry.ref)
        if isinstance(exception, RayActorError) and not isinstance(
            exception, ActorUnavailableError
        ):
            self.actor_pool.release_dead_actor(actor_to_return)

    def max_output_bytes_for_actor(self, actor_id: ActorId) -> float:
        """The (soft) maximum number of bytes that we can pull for this actor.
        Actors can handle multiple tasks, so we apply the limit at a per-actor
        level. It's a soft maximum because we cannot be precise in how many
        bytes an actor can produce."""
        return self.actor_pool.output_bytes_limit(actor_id=actor_id)

    def max_outputs_for_actor(self, actor_id: ActorId) -> float:
        """The (hard) maximum number of outputs that we can pull for this actor.
        Actors can handle multiple tasks, so we apply the limit at a per-actor
        level. It's a hard maximum because ray core provides a way to pull by #
        of blocks."""
        return self.actor_pool.output_count_limit(actor_id=actor_id)

    @override
    def refresh_state(self):
        """Updates internal state. The pool refresh releases dead actors and
        tears down fully-drained ones; priorities are synced first so the
        draining set it reads is current."""
        info = self.get_actor_info()
        if info.pending > 0:
            self.actor_pool.reset_output_limit()
        self._sync_draining_priorities()
        return super().refresh_state()

    def upgrade_output_limit(self) -> bool:
        """In the scenario that
            - the upstream operator cannot produce more outputs (output
              backpressured), and
            - the downstream operator cannot launch a task due to some
              requirement (like batch_size),
        we increase the output limit for an actor whose node has the most
        available object store. We increase it by one block.
        """

        # Prefer nodes that currently host in-flight work: upgrading an idle
        # node's caps does not relieve the backpressured producer.
        pool_nodes = self.actor_pool.active_nodes()
        if not pool_nodes:
            return False

        most_free_obj_store: float = float("-inf")
        best_node: NodeIdStr | None = None

        for node_id in pool_nodes:
            obj_store_usage = self.resource_bank.live_object_store(node_id=node_id)
            obj_store_limit = self.resource_bank.node_capacity(
                node_id=node_id
            ).object_store_memory
            obj_store_free = (
                obj_store_limit
                - obj_store_usage.num_pulled_output_bytes
                - obj_store_usage.num_prebuffered_output_bytes
                - obj_store_usage.num_input_bytes
            )

            if obj_store_free > most_free_obj_store:
                most_free_obj_store = obj_store_free
                best_node = node_id

        assert best_node is not None
        # Block size is derived per node (a CPU share of that node's object
        # store), so size the extra block from the chosen node only: its
        # observed mean output first, then the pool's per-node estimate.
        mean_output_bytes = (
            self.resource_bank.cumulative_object_store(
                op=self, node_id=best_node
            ).output_bytes.mean
            or self.actor_pool.estimate_bytes_per_output(node_id=best_node)
            or DEFAULT_BYTES_PER_OUTPUT
        )
        delta_bytes = max(
            _MIN_OUTPUT_LIMIT_UPGRADE_BYTES, int(math.ceil(mean_output_bytes))
        )
        self.actor_pool.upgrade_output_limit(
            node_id=best_node,
            delta_bytes=delta_bytes,
            delta_outputs=1,
        )
        return True

    def task_pull_request(self, task_idx: int) -> TaskPullRequest:
        """Return how many bytes to pull from streaming generators."""
        ray_task_id = self._task_ray_id.get(task_idx)
        actor_id = self.resource_bank.get_actor_id_from_task_id(
            op=self, task_id=ray_task_id
        )
        assert actor_id is not None
        # NOTE: the bytes_to_read are a soft upper bound, because we can't
        # read the exact bytes from a streaming generator.
        return self._actor_pull_budget(actor_id)

    def remaining_pull_bytes(self) -> float:
        """Bytes still pullable across this op's actors: the sum of their
        remaining per-actor pull budgets."""
        return sum(
            self._actor_pull_budget(actor_id).bytes_to_read
            for actor_id in self.actor_pool.running_actor_ids()
        )

    def _actor_pull_budget(self, actor_id: ActorId) -> TaskPullRequest:
        """Remaining per-actor pull budget: limit minus pulled and prebuffered
        outputs, clamped at zero. All tasks on an actor share it."""
        actor_object_store = self.resource_bank.live_object_store(actor_id=actor_id)
        max_output_bytes_per_actor = self.max_output_bytes_for_actor(actor_id=actor_id)
        max_bytes_to_read = max(
            0.0,
            max_output_bytes_per_actor
            - actor_object_store.num_pulled_output_bytes
            - actor_object_store.num_prebuffered_output_bytes,
        )

        max_outputs_per_actor = self.max_outputs_for_actor(actor_id=actor_id)
        # NOTE: This can go below 0 when the user specified a max_output_per_actor
        # less than _generator_backpressure_num_objects
        max_blocks_to_read = max(
            0.0,
            max_outputs_per_actor
            - actor_object_store.num_pulled_output_blocks
            - actor_object_store.num_prebuffered_output_blocks,
        )
        return TaskPullRequest(
            bytes_to_read=max_bytes_to_read,
            blocks_to_read=max_blocks_to_read,
        )

    @override
    def backpressure_progress_str(self) -> str:
        """Report the share of actors actually blocked on output.

        The base class reports output backpressure as the policy name that
        tripped, which is a boolean: it says a pull budget somewhere is
        exhausted, not how much of the pool that costs. For an actor pool the
        actionable quantity is what fraction of serving actors had a ready
        output they could not pull, so that replaces the ``outputs(...)`` tag
        here.

        This is the raw per-iteration observation, so expect it to move between
        progress-bar refreshes.
        """
        parts = []
        blocked, _ = self.cached_output_backpressured_actors()
        if blocked > 0:
            parts.append(f"blocked={blocked}/{self.actor_pool.total_size()}")
        if not parts:
            return ""
        return f"[{', '.join(parts)}]"

    def is_output_backpressured(self) -> bool:
        """Output backpressured means that the actor has the means
        to physically produce more outputs, but because Ray Data wants to avoid
        spilling, we intentionally do not pull outputs from the actor's
        streaming generators."""
        info = self.get_actor_info()
        if info.running == 0 or info.pending > 0:
            return False
        if not self._data_tasks:
            return False
        # TASK denomination, deliberately: this is the deadlock hatch's trigger,
        # which asks "is every ACTIVE task blocked on output budget?". At the
        # pipeline tail that is a couple of blocked tasks on a mostly-idle
        # pool; the actor-denominated pair reads blocked < running there and
        # would disable the hatch at exactly the state it exists for.
        blocked, active = self.output_backpressured_fraction()
        return blocked == active

    def output_backpressured_fraction(self) -> Tuple[int, int]:
        """(blocked, active) data-task counts for output backpressure.

        A task is blocked when any field of its pull budget is exhausted
        (bytes or block count). ``is_output_backpressured`` is the
        blocked == active special case; the fraction serves as an
        operator-level output-queue-full signal.
        """
        zeros = TaskPullRequest.zero()
        blocked = 0
        active = 0
        for task in self._data_tasks.values():
            actor_id = self.resource_bank.get_actor_id_from_task_id(
                op=self, task_id=task.get_task_id()
            )
            if actor_id is None or not self.actor_pool.has_actor(actor_id):
                # Skip tasks whose actor has been released (killed / died): the
                # ref is a lingering orphan that task processing will reap and
                # it carries no meaningful backpressure signal.
                continue
            active += 1
            spec = self.task_pull_request(task_idx=task.task_index())
            if not spec.all_fields_gt(zeros):
                blocked += 1
        return blocked, active

    def output_backpressured_actors(self) -> Tuple[int, int]:
        """(blocked, running) ACTOR counts for output backpressure.

        An actor is blocked when its remaining pull budget is exhausted in any
        field. Unlike ``output_backpressured_fraction`` the denominator is the
        whole running pool, so idle actors count as unblocked capacity rather
        than vanishing from the ratio. An idle actor with an exhausted budget
        (consumed by prebuffered / still-unconsumed outputs) counts as blocked:
        dispatching to it produces blocks it cannot emit.

        Both counts cover the same population, the serving (IDLE / ACTIVE)
        actors, so ``blocked == running`` means every serving actor is stuck.
        Draining and restarting actors are excluded from both: they take no
        new work, and a stuck drain shows up in the drain metrics instead.

        The scan is O(actors) with ResourceBank lookups, so every call also
        write-throughs its result for the progress-bar path (see
        ``cached_output_backpressured_actors``): callers that need exact
        values (idle detection, the sizer) call this method and thereby keep
        the cache fresh.
        """
        info = self.get_actor_info()
        if info.running == 0 or info.pending > 0:
            result = (0, 0)
        else:
            zeros = TaskPullRequest.zero()
            blocked = 0
            for states in self.actor_pool.alive_actor_states_by_node().values():
                for actor_id, state in states.items():
                    if state.status not in (ActorStatus.IDLE, ActorStatus.ACTIVE):
                        continue
                    if not self._actor_pull_budget(actor_id).all_fields_gt(zeros):
                        blocked += 1
            result = (blocked, info.running)
        self._output_backpressured_actors_cache = result
        self._output_backpressured_actors_cache_ts = time.monotonic()
        return result

    def cached_output_backpressured_actors(self) -> Tuple[int, int]:
        """The last write-through result of ``output_backpressured_actors``,
        recomputed only when stale (no regular caller has refreshed it within
        the TTL). The progress bar reads this several times per refresh and
        must never pay for the full scan itself."""
        # TODO(sampan): Replace the scan + TTL cache with incremental tracking.
        age = time.monotonic() - self._output_backpressured_actors_cache_ts
        if age > _OUTPUT_BACKPRESSURE_CACHE_TTL_S:
            return self.output_backpressured_actors()
        return self._output_backpressured_actors_cache

    def has_enough_budget_for_prebuffered_outputs(self) -> bool:
        """With low object store, the operator may not have enough budget
        for prebuffered outputs. Returns True if at least one actor with tasks
        in flight has more budget than its prebuffered outputs; False if every
        working actor is stalled on them.

        Only working actors count: an idle actor (say, one the sizer just
        added) has a full budget and no prebuffered outputs, but its spare
        budget relieves nothing -- the stalled outputs sit on the actors that
        hold the tasks.
        """
        working = False
        for states in self.actor_pool.alive_actor_states_by_node().values():
            for actor_id, state in states.items():
                if state.num_tasks_in_flight == 0:
                    continue
                working = True
                actor_object_store = self.resource_bank.live_object_store(
                    actor_id=actor_id
                )
                limit = TaskPullRequest(
                    bytes_to_read=self.max_output_bytes_for_actor(actor_id=actor_id),
                    blocks_to_read=self.max_outputs_for_actor(actor_id=actor_id),
                )
                usage = TaskPullRequest(
                    bytes_to_read=actor_object_store.num_prebuffered_output_bytes,
                    blocks_to_read=actor_object_store.num_prebuffered_output_blocks,
                )
                if limit.all_fields_gt(usage):
                    return True
        # Nothing is working, so nothing is stalled.
        return not working


def next_actor_pool_ops(
    op: PhysicalOperator,
) -> Tuple[ExperimentalActorPoolMapOperator, ...]:
    """The nearest downstream actor-pool ops, looking through ops without an
    actor pool: for ``read -> limit -> write``, write is next for read."""
    found: List[ExperimentalActorPoolMapOperator] = []
    for downstream in op.output_dependencies:
        if isinstance(downstream, ExperimentalActorPoolMapOperator):
            found.append(downstream)
        else:
            found.extend(next_actor_pool_ops(downstream))
    return tuple(dict.fromkeys(found))


def prev_actor_pool_ops(
    op: PhysicalOperator,
) -> Tuple[ExperimentalActorPoolMapOperator, ...]:
    """The nearest upstream actor-pool ops, looking through ops without an
    actor pool: for ``read -> limit -> map``, read is previous for map."""
    found: List[ExperimentalActorPoolMapOperator] = []
    for upstream in op.input_dependencies:
        if isinstance(upstream, ExperimentalActorPoolMapOperator):
            found.append(upstream)
        else:
            found.extend(prev_actor_pool_ops(upstream))
    return tuple(dict.fromkeys(found))
