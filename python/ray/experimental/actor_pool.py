"""Ray Core Actor Pool API (C++-backed).

This module provides a C++-backed actor pool that enables cross-actor retries,
load balancing, and efficient task scheduling across a pool of equivalent actors.

Example usage:
    >>> import ray
    >>> from ray.experimental.actor_pool import ActorPool, RetryPolicy, OrderingMode
    >>>
    >>> @ray.remote
    ... class Worker:
    ...     def process(self, data):
    ...         return data * 2
    >>>
    >>> pool = ActorPool(
    ...     actor_cls=Worker,
    ...     size=4,
    ...     retry=RetryPolicy(max_attempts=3),
    ... )
    >>>
    >>> # Submit tasks to the pool (C++ picks the actor)
    >>> refs = [pool.submit("process", i) for i in range(10)]
    >>> results = ray.get(refs)
    >>> pool.shutdown()
"""

import logging
from dataclasses import dataclass
from enum import IntEnum
from typing import Any, Callable, Dict, List, Optional, Tuple, Type, Union

import ray
from ray._raylet import ActorPoolID

logger = logging.getLogger(__name__)


class OrderingMode(IntEnum):
    """Task ordering mode for actor pools."""

    # Tasks execute in any order (highest throughput)
    UNORDERED = 0
    # Tasks with same key execute in FIFO order (per-key serialization)
    PER_KEY_FIFO = 1
    # All tasks execute in strict FIFO order (lowest throughput)
    GLOBAL_FIFO = 2


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
    """Ray Core Actor Pool (C++-backed).

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
        ordering: Task ordering mode.

    Example:
        >>> pool = ActorPool(
        ...     actor_cls=MyWorker,
        ...     size=4,
        ...     actor_options={"num_gpus": 1},
        ...     retry=RetryPolicy(max_attempts=5),
        ... )
        >>> ref = pool.submit("process", data)
        >>> result = ray.get(ref)
        >>> pool.shutdown()
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
        ordering: OrderingMode = OrderingMode.UNORDERED,
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
                raise ValueError(
                    "Cannot specify both 'size' and 'min_size'/'max_size'"
                )
            min_size = size
            max_size = size
            initial_size = size
        else:
            min_size = min_size or 1
            max_size = max_size or -1
            initial_size = initial_size or min_size

        self._actor_cls = actor_cls
        self._actor_args = actor_args
        self._actor_kwargs = actor_kwargs
        self._actor_options = actor_options
        self._retry = retry
        self._ordering = ordering
        self._min_size = min_size
        self._max_size = max_size
        self._initial_size = initial_size

        # Create the remote actor class
        self._remote_cls = ray.remote(**actor_options)(actor_cls)

        # Get the core worker
        self._core_worker = ray._private.worker.global_worker.core_worker

        # Register the pool in C++
        self._pool_id: ActorPoolID = self._core_worker.register_actor_pool(
            max_retry_attempts=retry.max_attempts,
            retry_backoff_ms=retry.backoff_ms,
            retry_backoff_multiplier=retry.backoff_multiplier,
            max_retry_backoff_ms=retry.max_backoff_ms,
            retry_on_system_errors=retry.retry_on_system_errors,
            ordering_mode=int(ordering),
            min_size=min_size,
            max_size=max_size,
            initial_size=initial_size,
        )

        # Track actor handles for direct access
        self._actor_handles: List[ray.actor.ActorHandle] = []

        # Create initial actors
        for _ in range(initial_size):
            self._create_and_add_actor()

        logger.info(
            f"Created ActorPool {self._pool_id.hex()} with {initial_size} actors"
        )

    def _create_and_add_actor(self) -> ray.actor.ActorHandle:
        """Create a new actor and add it to the pool."""
        actor = self._remote_cls.remote(*self._actor_args, **self._actor_kwargs)
        self._actor_handles.append(actor)

        # Get actor ID and add to C++ pool
        actor_id = actor._actor_id
        # TODO: Get actual node location for locality-aware scheduling
        self._core_worker.add_actor_to_pool(self._pool_id, actor_id)

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

    def submit(
        self,
        method_name: str,
        *args,
        key: Optional[str] = None,
        **kwargs,
    ) -> ray.ObjectRef:
        """Submit a task to the pool.

        The pool will select an appropriate actor based on load and locality.
        If the task fails, it will be automatically retried on a different actor
        (up to max_retry_attempts).

        Note: In Phase 1, this uses a simple round-robin approach via direct
        actor method calls. Full C++ task submission will be implemented in
        a future phase.

        Args:
            method_name: Name of the actor method to call.
            *args: Positional arguments for the method.
            key: Optional key for per-key ordering (only used with PER_KEY_FIFO).
            **kwargs: Keyword arguments for the method.

        Returns:
            ObjectRef for the task result.
        """
        # Phase 1: Simple round-robin via direct actor calls
        # Full C++ SubmitTaskToPool integration will come in a later phase
        if not self._actor_handles:
            raise RuntimeError("No actors in pool")

        # Get stats to find least loaded actor
        stats = self.stats()
        # For now, use simple round-robin based on task count
        actor_idx = stats["total_tasks_submitted"] % len(self._actor_handles)
        actor = self._actor_handles[actor_idx]

        # Call the method on the selected actor
        method = getattr(actor, method_name)
        return method.remote(*args, **kwargs)

    def map(
        self,
        method_name: str,
        items: List[Any],
        key_fn: Optional[Callable[[Any], str]] = None,
    ) -> List[ray.ObjectRef]:
        """Map a method over a list of items.

        Args:
            method_name: Name of the actor method to call.
            items: List of items to process.
            key_fn: Optional function to extract a key from each item
                   (for per-key ordering).

        Returns:
            List of ObjectRefs for the results.
        """
        refs = []
        for item in items:
            key = key_fn(item) if key_fn else None
            ref = self.submit(method_name, item, key=key)
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
        """
        return self._core_worker.get_pool_stats(self._pool_id)

    def scale(self, delta: int) -> None:
        """Scale the pool by delta actors.

        Args:
            delta: Number of actors to add (positive) or remove (negative).
        """
        if delta > 0:
            for _ in range(delta):
                self._create_and_add_actor()
            logger.info(f"Scaled pool {self._pool_id.hex()} up by {delta} actors")
        elif delta < 0:
            # Remove actors from the end
            num_to_remove = min(abs(delta), len(self._actor_handles))
            for _ in range(num_to_remove):
                actor = self._actor_handles.pop()
                actor_id = actor._actor_id
                self._core_worker.remove_actor_from_pool(self._pool_id, actor_id)
                ray.kill(actor)
            logger.info(
                f"Scaled pool {self._pool_id.hex()} down by {num_to_remove} actors"
            )

    def shutdown(self, force: bool = False, grace_period_s: float = 30.0) -> None:
        """Shutdown the pool and kill all actors.

        Args:
            force: If True, forcefully kill actors without waiting.
            grace_period_s: Time to wait for actors to finish (not used in Phase 1).
        """
        logger.info(f"Shutting down pool {self._pool_id.hex()}")

        # Kill all actors
        for actor in self._actor_handles:
            try:
                ray.kill(actor, no_restart=True)
            except Exception as e:
                logger.warning(f"Error killing actor: {e}")

        self._actor_handles.clear()

        # Unregister the pool in C++
        self._core_worker.unregister_actor_pool(self._pool_id)

    def __del__(self):
        """Cleanup when the pool is garbage collected."""
        try:
            if hasattr(self, "_pool_id") and hasattr(self, "_core_worker"):
                self._core_worker.unregister_actor_pool(self._pool_id)
        except Exception:
            # Ignore errors during cleanup (worker may have shut down)
            pass

    def __repr__(self) -> str:
        return (
            f"ActorPool(id={self._pool_id.hex()[:8]}..., "
            f"size={self.size}, "
            f"ordering={self._ordering.name})"
        )


__all__ = ["ActorPool", "ActorPoolID", "OrderingMode", "RetryPolicy"]

