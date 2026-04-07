"""Ray Core Actor Pool API.

This module provides an actor pool that enables cross-actor retries, load balancing,
and efficient task scheduling across a pool of equivalent actors.

Example usage::

    import ray
    from ray.experimental.actor_pool import ActorPool, RetryPolicy

    @ray.remote
    class Worker:
        def process(self, data):
            return data * 2

    pool = ActorPool(
        actor_cls=Worker,
        size=4,
        retry=RetryPolicy(max_attempts=3),
    )

    # Submit tasks to the pool
    refs = [pool.submit("process", i) for i in range(10)]
    results = ray.get(refs)
    pool.shutdown()
"""

import logging
import uuid
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple, Type

import ray
from ray._raylet import ActorPoolID
from ray.actor import ActorClass

logger = logging.getLogger(__name__)


@dataclass
class RetryPolicy:
    """Retry policy for actor pool tasks.

    Attributes:
        max_attempts: Maximum number of retry attempts (-1 for infinite).
        backoff_ms: Initial backoff in milliseconds between retries.
        backoff_multiplier: Multiplier for exponential backoff.
        max_backoff_ms: Maximum backoff in milliseconds.
        retry_on_system_errors: Whether to retry on system errors (actor death, etc).
    """

    max_attempts: int = 3
    backoff_ms: int = 1000
    backoff_multiplier: float = 2.0
    max_backoff_ms: int = 60000
    retry_on_system_errors: bool = True


class ActorPool:
    """Ray Core Actor Pool.

    A pool of equivalent actors that tasks can run on. The pool handles:
    - Load balancing across actors
    - Cross-actor retries (if an actor fails, retry on a different actor)
    - Task queueing when all actors are busy

    This is the Phase 1 implementation targeting Ray Data integration.

    Args:
        actor_cls: The actor class to instantiate.
        size: Fixed pool size (mutually exclusive with min_size/max_size).
        min_size: Minimum pool size for autoscaling.
        max_size: Maximum pool size (-1 for unbounded).
        initial_size: Initial pool size (defaults to min_size or size).
        actor_args: Positional arguments for actor constructor.
        actor_kwargs: Keyword arguments for actor constructor.
        actor_options: Options to pass to ray.remote() for actor creation.
        retry: Retry policy for failed tasks.
        max_tasks_in_flight_per_actor: Max concurrent tasks per actor in C++ pool.
        logical_id_label_key: Label key for logical actor IDs.
        logical_id_kwarg_name: Kwarg name to inject logical_id into actor constructor.
        static_labels: Static labels to apply to all actors in the pool.

    Example::

        pool = ActorPool(
            actor_cls=MyWorker,
            size=4,
            actor_options={"num_gpus": 1},
            retry=RetryPolicy(max_attempts=5),
        )
        ref = pool.submit("process", data)
        result = ray.get(ref)
        pool.shutdown()
    """

    def __init__(
        self,
        actor_cls: Type,
        size: Optional[int] = None,
        min_size: Optional[int] = None,
        max_size: Optional[int] = None,
        initial_size: Optional[int] = None,
        actor_args: Tuple = (),
        actor_kwargs: Optional[Dict[str, Any]] = None,
        actor_options: Optional[Dict[str, Any]] = None,
        retry: Optional[RetryPolicy] = None,
        max_tasks_in_flight_per_actor: int = 1,
        # Label support for Ray Data integration
        logical_id_label_key: Optional[str] = None,
        logical_id_kwarg_name: Optional[str] = None,
        static_labels: Optional[Dict[str, str]] = None,
    ):
        if actor_kwargs is None:
            actor_kwargs = {}
        if actor_options is None:
            actor_options = {}
        if retry is None:
            retry = RetryPolicy()

        # Validate size arguments
        if size is not None:
            if min_size is not None or max_size is not None:
                raise ValueError("Cannot specify both 'size' and 'min_size'/'max_size'")
            # 'size' sets the initial actor count. It does NOT constrain
            # min_size, so the pool can be scaled freely afterward.
            # Use min_size/max_size explicitly for autoscaling bounds.
            min_size = 0
            max_size = -1
            initial_size = size
        else:
            min_size = min_size or 1
            max_size = max_size or -1
            initial_size = initial_size if initial_size is not None else min_size

        self._actor_cls = actor_cls
        self._actor_args = actor_args
        self._actor_kwargs = actor_kwargs
        self._actor_options = actor_options
        self._retry = retry
        self._min_size = min_size
        self._max_size = max_size
        self._initial_size = initial_size

        # Label support for Ray Data integration
        self._logical_id_label_key = logical_id_label_key
        self._logical_id_kwarg_name = logical_id_kwarg_name
        self._static_labels = static_labels or {}
        self._actor_to_logical_id: Dict[ray.actor.ActorHandle, str] = {}

        if isinstance(actor_cls, ActorClass):
            self._remote_cls = actor_cls
        else:
            self._remote_cls = ray.remote(actor_cls)

        # Get the core worker
        self._core_worker = ray._private.worker.global_worker.core_worker

        # Register the pool
        self._pool_id: ActorPoolID = self._core_worker.register_actor_pool(
            max_retry_attempts=retry.max_attempts,
            retry_backoff_ms=retry.backoff_ms,
            retry_backoff_multiplier=retry.backoff_multiplier,
            max_retry_backoff_ms=retry.max_backoff_ms,
            retry_on_system_errors=retry.retry_on_system_errors,
            max_tasks_in_flight_per_actor=max_tasks_in_flight_per_actor,
            min_size=min_size,
            max_size=max_size,
            initial_size=initial_size,
        )

        # Track actor handles for direct access
        self._actor_handles: List[ray.actor.ActorHandle] = []

        # Track tasks submitted (Python-side counter, shadows C++ stats
        # because the adapter path calls submit_task directly on actors,
        # not through C++ SubmitTaskToPool).
        self._tasks_submitted = 0

        # Track whether shutdown() has been called to prevent double-unregister.
        self._is_shutdown = False

        # Create initial actors
        for _ in range(initial_size):
            self._create_and_add_actor()

        logger.info(
            f"Created ActorPool {self._pool_id.hex()} with {initial_size} actors"
        )
        self._log_debug("created", initial_size=initial_size)

    def _log_debug(self, event: str, **fields: Any) -> None:
        stats = None
        try:
            if hasattr(self, "_core_worker") and hasattr(self, "_pool_id"):
                stats = self._core_worker.get_pool_stats(self._pool_id)
        except Exception:
            stats = None

        logger.warning(
            "ActorPoolDebugPy %s",
            {
                "event": event,
                "pool_id": self._pool_id.hex() if hasattr(self, "_pool_id") else None,
                "size": len(getattr(self, "_actor_handles", [])),
                "logical_ids": len(getattr(self, "_actor_to_logical_id", {})),
                "is_shutdown": getattr(self, "_is_shutdown", None),
                "stats": stats,
                **fields,
            },
        )

    def _create_and_add_actor(self) -> ray.actor.ActorHandle:
        """Create a new actor and add it to the pool."""
        # Generate logical ID if configured
        logical_id = None
        if self._logical_id_label_key or self._logical_id_kwarg_name:
            logical_id = str(uuid.uuid4())

        # Build labels
        labels = dict(self._static_labels)
        if logical_id and self._logical_id_label_key:
            labels[self._logical_id_label_key] = logical_id

        # Build actor kwargs (may include logical_id injection)
        actor_kwargs = dict(self._actor_kwargs)
        if logical_id and self._logical_id_kwarg_name:
            actor_kwargs[self._logical_id_kwarg_name] = logical_id

        # Merge user-provided actor_options with per-actor options in one call.
        # max_restarts=-1 makes actors restartable so that when an actor dies,
        # GCS marks it RESTARTING instead of permanently DEAD.  This avoids the
        # DisconnectActor(dead=true) path which calls MarkTaskNoRetry and
        # interferes with in-flight task reference counting.  Cross-actor retry
        # (via InternalHeartbeat) redirects the task to a healthy pool actor,
        # so the restarted actor is not actually needed for task completion.
        options = dict(self._actor_options)
        options["max_restarts"] = -1
        if labels:
            options["_labels"] = labels
        actor = self._remote_cls.options(**options).remote(
            *self._actor_args, **actor_kwargs
        )

        self._actor_handles.append(actor)

        # Track logical ID mapping
        if logical_id:
            self._actor_to_logical_id[actor] = logical_id

        actor_id = actor._actor_id
        # TODO: Get actual node location for locality-aware scheduling
        self._core_worker.add_actor_to_pool(self._pool_id, actor_id)
        self._log_debug(
            "create_and_add_actor",
            actor_id=actor_id.hex(),
            logical_id=logical_id,
        )

        return actor

    @property
    def pool_id(self) -> ActorPoolID:
        """Get the pool ID."""
        return self._pool_id

    @property
    def actors(self) -> List[ray.actor.ActorHandle]:
        """Get direct access to actor handles."""
        return list(self._actor_handles)

    @property
    def size(self) -> int:
        """Get current pool size."""
        return len(self._actor_handles)

    @property
    def min_size(self) -> int:
        """Get minimum pool size."""
        return self._min_size

    @property
    def max_size(self) -> int:
        """Get maximum pool size."""
        return self._max_size

    @property
    def initial_size(self) -> int:
        """Get initial pool size."""
        return self._initial_size

    def get_logical_ids(self) -> List[str]:
        """Get logical IDs for all actors in the pool.

        Only returns IDs if logical_id_label_key was configured.
        """
        return list(self._actor_to_logical_id.values())

    def get_logical_id(self, actor: ray.actor.ActorHandle) -> Optional[str]:
        """Get logical ID for a specific actor."""
        return self._actor_to_logical_id.get(actor)

    def get_logical_id_label_key(self) -> Optional[str]:
        """Get the label key used for logical actor IDs."""
        return self._logical_id_label_key

    def submit(
        self,
        method_name: str,
        *args,
        num_returns: int = 1,
        **kwargs,
    ) -> ray.ObjectRef:
        """Submit a task to the pool.

        The pool will select an appropriate actor based on load and locality.
        If the task fails, it will be automatically retried on a different actor
        (up to max_retry_attempts).

        Args:
            method_name: Name of the actor method to call.
            *args: Positional arguments for the method.
            num_returns: Number of return values (-2 for streaming generator).
            **kwargs: Keyword arguments for the method.

        Returns:
            ObjectRef for the task result.
        """
        if not self._actor_handles:
            raise RuntimeError("No actors in pool")

        actor = self._actor_handles[0]
        language = actor._ray_actor_language
        fn_signature = actor._ray_method_signatures[method_name]
        fn_descriptor = actor._ray_function_descriptor[method_name]

        from ray._common.signature import flatten_args

        list_args = flatten_args(fn_signature, args, kwargs)

        self._tasks_submitted += 1

        object_refs = self._core_worker.submit_task_to_pool(
            self._pool_id,
            language,
            fn_descriptor,
            list_args,
            name=fn_descriptor.function_name.encode(),
            num_returns=num_returns,
            num_method_cpus=0,
            concurrency_group_name=b"",
            generator_backpressure_num_objects=-1,
            enable_task_events=True,
        )

        if not object_refs:
            raise RuntimeError(
                "Failed to submit task to pool: no alive actors with capacity"
            )

        if len(object_refs) == 1:
            return object_refs[0]
        return object_refs

    def map(
        self,
        method_name: str,
        items: List[Any],
    ) -> List[ray.ObjectRef]:
        """Map a method over a list of items.

        Args:
            method_name: Name of the actor method to call.
            items: List of items to process.

        Returns:
            List of ObjectRefs for the results.
        """
        refs = []
        for item in items:
            ref = self.submit(method_name, item)
            refs.append(ref)
        return refs

    def stats(self) -> Dict[str, Any]:
        """Get pool statistics.

        Returns:
            Dictionary with:
            - total_tasks_submitted: Total tasks submitted to the pool
            - total_tasks_failed: Total tasks that failed
            - total_tasks_retried: Total tasks that were retried
            - num_actors: Current number of actors
            - backlog_size: Number of queued tasks
            - total_in_flight: Total in-flight tasks
            - waiting_for_actor_retries: Retried tasks blocked on an empty pool
        """
        # Get C++ stats but override total_tasks_submitted with the Python-side
        # counter, since tasks submitted via the adapter's direct actor calls
        # are not tracked by C++ SubmitTaskToPool.
        stats = self._core_worker.get_pool_stats(self._pool_id)
        stats["total_tasks_submitted"] = self._tasks_submitted
        return stats

    def scale(self, delta: int) -> int:
        """Scale the pool by delta actors.

        Args:
            delta: Number of actors to add (positive) or remove (negative).

        Returns:
            Actual number of actors added (positive) or removed (negative).
        """
        if delta > 0:
            for _ in range(delta):
                self._create_and_add_actor()
            logger.info(f"Scaled pool {self._pool_id.hex()} up by {delta} actors")
            self._log_debug("scale_up", delta=delta)
            return delta
        elif delta < 0:
            # Remove actors from the end, but never go below min_size.
            removable = max(0, len(self._actor_handles) - self._min_size)
            num_to_remove = min(abs(delta), removable)
            for _ in range(num_to_remove):
                actor = self._actor_handles.pop()
                self._actor_to_logical_id.pop(actor, None)
                actor_id = actor._actor_id
                self._core_worker.remove_actor_from_pool(self._pool_id, actor_id)
                ray.kill(actor)
            logger.info(
                f"Scaled pool {self._pool_id.hex()} down by {num_to_remove} actors"
            )
            self._log_debug("scale_down", delta=-num_to_remove)
            return -num_to_remove
        return 0

    def remove_actor(self, actor: ray.actor.ActorHandle, kill: bool = True) -> bool:
        """Remove a specific actor from the pool.

        Unregisters the actor from Python and C++ tracking. Does NOT
        enforce min_size — callers are responsible for pool sizing.

        Args:
            actor: The actor handle to remove.
            kill: Whether to kill the actor process (default True).

        Returns:
            True if the actor was found and removed, False otherwise.
        """
        if actor not in self._actor_handles:
            return False

        self._actor_handles.remove(actor)
        self._actor_to_logical_id.pop(actor, None)
        self._core_worker.remove_actor_from_pool(self._pool_id, actor._actor_id)
        if kill:
            ray.kill(actor)
        self._log_debug(
            "remove_actor",
            actor_id=actor._actor_id.hex(),
            kill=kill,
        )
        return True

    def shutdown(self, force: bool = False, grace_period_s: float = 30.0) -> None:
        """Shutdown the pool and kill all actors.

        Args:
            force: If True, forcefully kill actors without waiting.
            grace_period_s: Time to wait for actors to finish.
                TODO(P7): Currently unused — implement drain-before-kill.
        """
        if self._is_shutdown:
            return

        logger.info(f"Shutting down pool {self._pool_id.hex()}")
        self._log_debug("shutdown_begin", force=force, grace_period_s=grace_period_s)

        # Kill all actors
        for actor in self._actor_handles:
            try:
                ray.kill(actor, no_restart=True)
            except Exception as e:
                logger.warning(f"Error killing actor: {e}")

        self._actor_handles.clear()
        self._actor_to_logical_id.clear()

        # Unregister the pool
        self._core_worker.unregister_actor_pool(self._pool_id)
        self._is_shutdown = True
        self._log_debug("shutdown_end", force=force)

    def __del__(self):
        """Cleanup when the pool is garbage collected."""
        try:
            if (
                not getattr(self, "_is_shutdown", True)
                and hasattr(self, "_pool_id")
                and hasattr(self, "_core_worker")
            ):
                self._log_debug("gc_unregister")
                self._core_worker.unregister_actor_pool(self._pool_id)
        except Exception:
            # Ignore errors during cleanup (worker may have shut down)
            pass

    def __repr__(self) -> str:
        return f"ActorPool(id={self._pool_id.hex()[:8]}..., " f"size={self.size})"


__all__ = ["ActorPool", "ActorPoolID", "RetryPolicy"]
