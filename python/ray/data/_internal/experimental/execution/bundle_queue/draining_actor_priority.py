from __future__ import annotations

from collections import deque
from typing import TYPE_CHECKING, Any, Deque, Optional, Tuple

from typing_extensions import override

from ray.data._internal.execution.bundle_queue.base import BaseBundleQueue

if TYPE_CHECKING:
    from ray.data._internal.execution.interfaces import RefBundle

# Producing-actor identifier carried with each queued bundle. Typed loosely
# (``Any``) so this queue doesn't depend on the actor-only backend's
# ``LogicalActorId`` (a str); callers pass whatever id they key actors by.
ActorId = Any


class DrainingActorPriorityBundleQueue(BaseBundleQueue):
    """A non-order-preserving output queue that consumes a *draining* actor's
    output blocks before everyone else's.

    Experimental: the output queue of ``ExperimentalActorPoolMapOperator`` (the
    actor-only backend, which rejects ``preserve_order``). Each bundle is added
    with ``key=<producing actor id>``. Actors marked via :meth:`prioritize_actor`
    (the operator does this when the pool starts draining an actor) have their
    bundles served first, so the blocks the ResourceBank still counts against
    the actor are consumed sooner and the actor can be released. Within a lane,
    order is FIFO.

    NOT order-preserving. NOT thread-safe.
    """

    def __init__(self):
        super().__init__()
        # Bundles from draining (prioritized) actors, served first; then the rest.
        # Each entry is ``(bundle, actor_id)``.
        self._prioritized: Deque[Tuple[RefBundle, ActorId]] = deque()
        self._regular: Deque[Tuple[RefBundle, ActorId]] = deque()
        self._priority_actors: set = set()

    @override
    # pyrefly: ignore[bad-override]  # narrows **kwargs to the one key it takes
    def _add_inner(self, bundle: RefBundle, key: ActorId = None) -> None:
        """``key`` is the producing actor's id (same parameter name as
        ``ReorderingBundleQueue``, so the operator can swap queues)."""
        entry = (bundle, key)
        if key is not None and key in self._priority_actors:
            self._prioritized.append(entry)
        else:
            self._regular.append(entry)

    def _pop(self) -> Tuple[RefBundle, ActorId]:
        if self._prioritized:
            return self._prioritized.popleft()
        if self._regular:
            return self._regular.popleft()
        raise ValueError(f"Popping from empty {self.__class__.__name__} is prohibited")

    @override
    def _get_next_inner(self) -> RefBundle:
        # Base ``get_next`` wraps this with ``_on_dequeue_bundle`` for metrics.
        return self._pop()[0]

    def prioritize_actor(self, actor_id: ActorId) -> None:
        """Serve ``actor_id``'s output blocks before others'. Moves the actor's
        already-queued bundles into the priority lane (preserving their relative
        order), so blocks queued before it began draining are prioritized too."""
        if actor_id in self._priority_actors:
            return
        # Always record membership, even with nothing queued yet: the actor's
        # FUTURE outputs must route to the priority lane (see ``_add_inner``).
        self._priority_actors.add(actor_id)
        if not self._regular:
            return
        remaining: Deque[Tuple[RefBundle, ActorId]] = deque()
        for entry in self._regular:
            if entry[1] == actor_id:
                self._prioritized.append(entry)
            else:
                remaining.append(entry)
        self._regular = remaining

    def deprioritize_actor(self, actor_id: ActorId) -> None:
        """Stop prioritizing ``actor_id`` (e.g. it was reclaimed back into the
        running pool). Already-prioritized bundles are left where they are --
        serving them first is harmless; new bundles go to the regular lane."""
        self._priority_actors.discard(actor_id)

    def priority_actors(self) -> frozenset:
        """The set of actor ids currently prioritized (read-only snapshot)."""
        return frozenset(self._priority_actors)

    @override
    def peek_next(self) -> Optional[RefBundle]:
        if self._prioritized:
            return self._prioritized[0][0]
        if self._regular:
            return self._regular[0][0]
        return None

    @override
    def has_next(self) -> bool:
        return bool(self._prioritized or self._regular)

    @override
    def clear(self):
        self._reset_metrics()
        self._prioritized.clear()
        self._regular.clear()
        self._priority_actors.clear()

    @override
    def finalize(self, **kwargs: Any):
        # Order across actors is not preserved, so there is nothing to finalize.
        return None
