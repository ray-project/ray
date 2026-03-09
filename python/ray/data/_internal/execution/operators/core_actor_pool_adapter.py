"""Adapter to integrate ray.experimental.actor_pool.ActorPool with Ray Data.

This module provides ClassBasedActorPoolAdapter, which implements the
AutoscalingActorPool interface expected by Ray Data's ActorPoolMapOperator,
using the C++-backed ActorPool with actor_cls directly.

The class-based adapter enables Ray Data to benefit from:
- C++-backed pool management
- Cross-actor retry on failures
- Unified ActorPool API across Ray
"""

import logging
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Type

import ray
from ray.actor import ActorHandle
from ray.data._internal.actor_autoscaler import AutoscalingActorPool
from ray.data._internal.actor_autoscaler.autoscaling_actor_pool import (
    ActorPoolScalingRequest,
)
from ray.data._internal.execution.interfaces import ExecutionResources
from ray.data._internal.execution.interfaces.physical_operator import _ActorPoolInfo
from ray.types import ObjectRef

logger = logging.getLogger(__name__)


@dataclass
class _ActorState:
    """Per-actor state for Ray Data compatibility."""

    num_tasks_in_flight: int
    actor_location: str
    is_restarting: bool


class ClassBasedActorPoolAdapter(AutoscalingActorPool):
    """Class-based adapter using ray.experimental.actor_pool.ActorPool.

    This adapter implements the AutoscalingActorPool interface expected by
    ActorPoolMapOperator, using the new C++-backed ActorPool with actor_cls
    directly (no callback).

    Key features:
    - Creates ActorPool internally which handles actor lifecycle
    - ActorPool generates logical IDs and sets labels automatically
    - Maintains Python-side state tracking for Ray Data compatibility
    """

    _ACTOR_POOL_SCALE_DOWN_DEBOUNCE_PERIOD_S = 10
    _ACTOR_POOL_GRACEFUL_SHUTDOWN_TIMEOUT_S = 30
    _LOGICAL_ACTOR_ID_LABEL_KEY = "__ray_data_logical_actor_id"

    def __init__(
        self,
        actor_cls: Type,
        per_actor_resource_usage: ExecutionResources,
        *,
        min_size: int,
        max_size: int,
        initial_size: int,
        max_actor_concurrency: int,
        max_tasks_in_flight_per_actor: int,
        actor_kwargs: Optional[Dict[str, Any]] = None,
        actor_options: Optional[Dict[str, Any]] = None,
        operator_id: Optional[str] = None,
        _enable_actor_pool_on_exit_hook: bool = False,
    ):
        """Initialize the adapter.

        Args:
            actor_cls: The actor class (e.g., _MapWorker) to instantiate.
            per_actor_resource_usage: Resource usage per actor.
            min_size: Minimum pool size.
            max_size: Maximum pool size.
            initial_size: Initial pool size.
            max_actor_concurrency: Max concurrent tasks per actor.
            max_tasks_in_flight_per_actor: Max tasks that can be queued per actor.
            actor_kwargs: Keyword arguments for actor constructor.
            actor_options: Options for ray.remote() (num_cpus, num_gpus, etc.).
            operator_id: Operator ID for labeling actors.
            _enable_actor_pool_on_exit_hook: Enable actor cleanup hook.
        """
        from ray.experimental.actor_pool import ActorPool

        self._min_size = min_size
        self._max_size = max_size
        self._initial_size = initial_size
        self._max_actor_concurrency = max_actor_concurrency
        self._max_tasks_in_flight = max_tasks_in_flight_per_actor
        self._per_actor_resource_usage = per_actor_resource_usage
        self._enable_actor_pool_on_exit_hook = _enable_actor_pool_on_exit_hook

        assert self._min_size >= 1
        assert self._max_size >= self._min_size
        assert self._initial_size <= self._max_size
        assert self._initial_size >= self._min_size
        assert self._max_tasks_in_flight >= 1

        # Build static labels for operator tracking
        static_labels = {}
        if operator_id:
            static_labels["__ray_data_operator_id"] = operator_id

        # Create the ActorPool (this creates initial_size=0 actors initially)
        # We set initial_size=0 because Ray Data's ActorPoolMapOperator calls
        # scale() explicitly after start() to create actors
        self._pool = ActorPool(
            actor_cls=actor_cls,
            min_size=min_size,
            max_size=max_size,
            initial_size=0,  # Don't create actors yet
            actor_kwargs=actor_kwargs or {},
            actor_options=actor_options or {},
            max_tasks_in_flight_per_actor=self._max_tasks_in_flight,
            logical_id_label_key=self._LOGICAL_ACTOR_ID_LABEL_KEY,
            logical_id_kwarg_name="logical_actor_id",
            static_labels=static_labels,
        )

        # Scale down debouncing
        self._last_upscaled_at: Optional[float] = None
        self._last_downscaling_debounce_warning_ts: Optional[float] = None

        # Actor tracking (Python-side for Ray Data compatibility)
        self._running_actors: Dict[ActorHandle, _ActorState] = {}
        self._pending_actors: Dict[ObjectRef, ActorHandle] = {}

        # Cached counts
        self._num_restarting_actors: int = 0
        self._num_active_actors: int = 0
        self._total_num_tasks_in_flight: int = 0

    # =========================================================================
    # AutoscalingActorPool Interface Implementation
    # =========================================================================

    def min_size(self) -> int:
        return self._min_size

    def max_size(self) -> int:
        return self._max_size

    def current_size(self) -> int:
        return self.num_pending_actors() + self.num_running_actors()

    def num_running_actors(self) -> int:
        return len(self._running_actors)

    def num_active_actors(self) -> int:
        return self._num_active_actors

    def num_pending_actors(self) -> int:
        return len(self._pending_actors)

    def num_restarting_actors(self) -> int:
        return self._num_restarting_actors

    def num_alive_actors(self) -> int:
        return len(self._running_actors) - self._num_restarting_actors

    def max_tasks_in_flight_per_actor(self) -> int:
        return self._max_tasks_in_flight

    def max_actor_concurrency(self) -> int:
        return self._max_actor_concurrency

    def num_tasks_in_flight(self) -> int:
        return self._total_num_tasks_in_flight

    def initial_size(self) -> int:
        return self._initial_size

    def per_actor_resource_usage(self) -> ExecutionResources:
        return self._per_actor_resource_usage

    def get_pool_util(self) -> float:
        if self.num_running_actors() == 0:
            return 0.0
        return self.num_tasks_in_flight() / (
            self._max_actor_concurrency * self.num_running_actors()
        )

    def num_free_task_slots(self) -> int:
        """Number of free task slots across all running actors."""
        return max(
            0,
            self._max_tasks_in_flight * self.num_running_actors()
            - self._total_num_tasks_in_flight,
        )

    def scale(self, req: ActorPoolScalingRequest) -> Optional[int]:
        """Apply scaling request."""
        if not self._can_apply(req):
            return 0

        if req.delta > 0:
            target_num_actors = req.delta
            logger.debug(
                f"Scaling up actor pool by {target_num_actors} "
                f"(reason={req.reason}, {self.get_actor_info()})"
            )

            self._pool.scale(target_num_actors)

            # Get the newly created actors and track them as pending
            # ActorPool creates actors synchronously, but we need to track
            # their readiness via get_location.remote()
            for actor in self._pool.actors[-target_num_actors:]:
                ready_ref = actor.get_location.remote()
                self._pending_actors[ready_ref] = actor

            self._last_upscaled_at = time.time()
            return target_num_actors

        elif req.delta < 0:
            num_released = 0
            target_num_actors = abs(req.delta)

            for _ in range(target_num_actors):
                if self._remove_inactive_actor():
                    num_released += 1

            if num_released > 0:
                logger.debug(
                    f"Scaled down actor pool by {num_released} "
                    f"(reason={req.reason}; {self.get_actor_info()})"
                )

            return -num_released

        return None

    def _can_apply(self, config: ActorPoolScalingRequest) -> bool:
        """Check if scaling request can be applied (with debouncing)."""
        if config.delta < 0:
            if (
                not config.force
                and self._last_upscaled_at is not None
                and (
                    time.time()
                    <= self._last_upscaled_at
                    + self._ACTOR_POOL_SCALE_DOWN_DEBOUNCE_PERIOD_S
                )
            ):
                if self._last_upscaled_at != self._last_downscaling_debounce_warning_ts:
                    logger.debug(
                        f"Ignoring scaling down request (request={config}; "
                        f"reason=debounced from scaling up at {self._last_upscaled_at})"
                    )
                    self._last_downscaling_debounce_warning_ts = self._last_upscaled_at
                return False
        return True

    # =========================================================================
    # Actor Lifecycle Management
    # =========================================================================

    def pending_to_running(self, ready_ref: ObjectRef) -> bool:
        """Move actor from pending to running state.

        Args:
            ready_ref: The ObjectRef that signals the actor is ready.

        Returns:
            True if actor was moved, False if it was already removed.
        """
        if ready_ref not in self._pending_actors:
            return False

        actor = self._pending_actors.pop(ready_ref)
        try:
            actor_location = ray.get(ready_ref)
        except Exception:
            raise

        self._running_actors[actor] = _ActorState(
            num_tasks_in_flight=0,
            actor_location=actor_location,
            is_restarting=False,
        )

        # Update the C++ pool with the actor's node location for
        # locality-aware scheduling. The actor was already added during
        # scale() without a location; this call updates it in-place.
        if actor_location:
            from ray._raylet import NodeID

            node_id = NodeID.from_hex(actor_location)
            import ray._private.worker as worker_module

            worker = worker_module.global_worker
            worker.core_worker.add_actor_to_pool(
                self._pool.pool_id,
                actor._actor_id,
                location=node_id,
            )

        return True

    def running_actors(self) -> Dict[ActorHandle, _ActorState]:
        """Get running actors with their state."""
        return self._running_actors

    def get_pending_actor_refs(self) -> List[ObjectRef]:
        """Get refs for pending actors."""
        return list(self._pending_actors.keys())

    def get_running_actor_refs(self) -> List[ActorHandle]:
        """Get refs for running actors."""
        return list(self._running_actors.keys())

    def get_logical_ids(self) -> List[str]:
        """Get logical IDs for all actors."""
        return self._pool.get_logical_ids()

    def get_logical_id_label_key(self) -> str:
        """Get the label key for logical actor ID."""
        return self._LOGICAL_ACTOR_ID_LABEL_KEY

    # =========================================================================
    # C++ Pool Task Submission
    # =========================================================================

    def submit_task(
        self,
        method_name: str,
        args: tuple,
        kwargs: dict,
        num_returns,
        remote_args: Optional[Dict[str, Any]] = None,
    ):
        """Submit a task via the C++ ActorPoolManager.

        The C++ pool handles actor selection, load balancing, and fault tolerance.
        Returns (ObjectRefGenerator or ObjectRef list, None) — the actor is unknown
        to Python since C++ chose it.
        """
        import ray._private.worker as worker_module
        from ray._common.signature import flatten_args
        from ray._raylet import STREAMING_GENERATOR_RETURN, ObjectRefGenerator

        worker = worker_module.global_worker
        remote_args = remote_args or {}

        actor = next(iter(self._running_actors))
        language = actor._ray_actor_language
        fn_signature = actor._ray_method_signatures[method_name]
        fn_descriptor = actor._ray_function_descriptor[method_name]
        list_args = flatten_args(fn_signature, args, kwargs)

        actual_num_returns = num_returns
        if num_returns == "streaming":
            actual_num_returns = STREAMING_GENERATOR_RETURN
        elif num_returns == "dynamic":
            actual_num_returns = -1

        num_cpus = remote_args.get("num_cpus", 0)
        concurrency_group = remote_args.get("concurrency_group_name", b"")
        if isinstance(concurrency_group, str):
            concurrency_group = concurrency_group.encode()
        backpressure = remote_args.get("_generator_backpressure_num_objects", -1)

        object_refs = worker.core_worker.submit_task_to_pool(
            self._pool.pool_id,
            language,
            fn_descriptor,
            list_args,
            name=fn_descriptor.function_name.encode(),
            num_returns=actual_num_returns,
            num_method_cpus=num_cpus,
            concurrency_group_name=concurrency_group,
            generator_backpressure_num_objects=backpressure,
            enable_task_events=True,
        )

        if not object_refs:
            self._total_num_tasks_in_flight -= 0  # nothing submitted
            raise RuntimeError(
                "C++ ActorPoolManager returned no refs — pool has no actors "
                "with available capacity. This indicates a mismatch between "
                "Python num_free_task_slots() and C++ max_tasks_in_flight_per_actor."
            )

        self._total_num_tasks_in_flight += 1

        if actual_num_returns == STREAMING_GENERATOR_RETURN:
            return ObjectRefGenerator(object_refs[0], worker), None

        if len(object_refs) == 1:
            return object_refs[0], None
        return object_refs, None

    @property
    def supports_pool_submission(self) -> bool:
        return True

    # =========================================================================
    # Task Tracking (Python-side for Ray Data compatibility)
    # =========================================================================

    def on_task_submitted(self, actor: ActorHandle):
        """Called when a task is submitted to an actor."""
        self._running_actors[actor].num_tasks_in_flight += 1
        self._total_num_tasks_in_flight += 1

        if self._running_actors[actor].num_tasks_in_flight == 1:
            self._num_active_actors += 1

    def on_task_completed(self, actor: Optional[ActorHandle] = None):
        """Called when a task completes.

        Args:
            actor: The actor that ran the task. None for pool-submitted tasks
                   where C++ selected the actor.
        """
        if actor is not None and actor in self._running_actors:
            assert self._running_actors[actor].num_tasks_in_flight > 0
            self._running_actors[actor].num_tasks_in_flight -= 1
            if self._running_actors[actor].num_tasks_in_flight == 0:
                self._num_active_actors -= 1

        self._total_num_tasks_in_flight -= 1

    def refresh_actor_state(self):
        """Refresh actor states from GCS (alive vs restarting)."""
        from ray.core.generated import gcs_pb2

        for actor in self.get_running_actor_refs():
            actor_state = actor._get_local_state()
            if actor_state in (None, gcs_pb2.ActorTableData.ActorState.DEAD):
                continue
            elif actor_state != gcs_pb2.ActorTableData.ActorState.ALIVE:
                self.update_running_actor_state(actor, is_restarting=True)
            else:
                self.update_running_actor_state(actor, is_restarting=False)

    def update_running_actor_state(self, actor: ActorHandle, is_restarting: bool):
        """Update actor's restarting state."""
        if actor not in self._running_actors:
            return
        if self._running_actors[actor].is_restarting == is_restarting:
            return

        self._running_actors[actor].is_restarting = is_restarting
        if is_restarting:
            self._num_restarting_actors += 1
        else:
            self._num_restarting_actors -= 1

    # =========================================================================
    # Pool Management
    # =========================================================================

    def num_idle_actors(self) -> int:
        """Number of idle actors (no tasks in flight)."""
        return len(self._running_actors) - self._num_active_actors

    def _remove_inactive_actor(self) -> bool:
        """Remove a pending or idle actor."""
        released = self._try_remove_pending_actor()
        if not released:
            released = self._try_remove_idle_actor()
        return released

    def _try_remove_pending_actor(self) -> bool:
        """Try to remove a pending actor."""
        if self._pending_actors:
            ready_ref = next(iter(self._pending_actors.keys()))
            self._pending_actors.pop(ready_ref)
            # Scale down the underlying pool
            self._pool.scale(-1)
            return True
        return False

    def _try_remove_idle_actor(self) -> bool:
        """Try to remove an idle running actor."""
        for actor, state in list(self._running_actors.items()):
            if state.num_tasks_in_flight == 0:
                self._release_running_actor(actor)
                return True
        return False

    def shutdown(self, force: bool = False):
        """Shutdown the pool."""
        self._release_pending_actors(force=force)
        self._release_running_actors(force=force)
        # Shutdown the underlying ActorPool
        self._pool.shutdown(force=force)

    def _release_pending_actors(self, force: bool):
        """Release all pending actors."""
        self._pending_actors.clear()

    def _release_running_actors(self, force: bool):
        """Release all running actors."""
        running = list(self._running_actors.keys())
        on_exit_refs = []

        for actor in running:
            ref = self._release_running_actor(actor)
            if ref:
                on_exit_refs.append(ref)

        if on_exit_refs:
            ray.wait(on_exit_refs, timeout=self._ACTOR_POOL_GRACEFUL_SHUTDOWN_TIMEOUT_S)

    def _release_running_actor(self, actor: ActorHandle) -> Optional[ObjectRef]:
        """Release a single running actor."""
        if actor not in self._running_actors:
            return None

        actor_state = self._running_actors[actor]
        self._total_num_tasks_in_flight -= actor_state.num_tasks_in_flight

        if actor_state.num_tasks_in_flight > 0:
            self._num_active_actors -= 1

        if actor_state.is_restarting:
            self._num_restarting_actors -= 1

        ref = None
        if self._enable_actor_pool_on_exit_hook:
            try:
                ref = actor.on_exit.remote()
            except Exception:
                pass

        del self._running_actors[actor]
        return ref

    def get_actor_info(self) -> _ActorPoolInfo:
        """Get actor pool info for metrics."""
        return _ActorPoolInfo(
            running=self.num_alive_actors(),
            pending=self.num_pending_actors(),
            restarting=self.num_restarting_actors(),
        )
