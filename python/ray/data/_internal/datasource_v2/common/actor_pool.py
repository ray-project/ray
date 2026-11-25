"""The actor-pool protocol shared by the footer and header reader pools.

Parquet footer reads and CSV header reads both run in a pool of Ray actors
for one ``list_files`` call: several SPREAD-scheduled actors, batches handed
out round-robin with a bounded in-flight window, results delivered in
dispatch order, then every actor killed. The pool is made on first use, so
a listing that needs no actor work (CSV files all smaller than one chunk)
starts none.
"""

from __future__ import annotations

import logging
from collections import deque
from contextlib import contextmanager
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Deque,
    Iterable,
    Iterator,
    List,
    Optional,
    Tuple,
)

import ray

if TYPE_CHECKING:
    from ray.actor import ActorProxy
    from ray.data._internal.datasource_v2.interfaces.file_indexer import FileInfo

logger = logging.getLogger(__name__)


class LazyActorPool:
    """A round-robin pool of reader actors, made on first use.

    ``len`` is the configured size without provisioning, so an in-flight
    window can be sized before the first actor exists; the actors are
    built by ``make_actors`` on the first :meth:`next_actor` call.
    """

    def __init__(
        self,
        num_actors: int,
        make_actors: Callable[[], List["ActorProxy[Any]"]],
    ):
        self._num_actors = num_actors
        self._make_actors = make_actors
        self._actors: List["ActorProxy[Any]"] = []
        self._dispatched = 0

    def __len__(self) -> int:
        return self._num_actors

    def next_actor(self) -> "ActorProxy[Any]":
        """Return the next actor, round-robin; build the pool on first use."""
        if not self._actors:
            self._actors = self._make_actors()
            logger.debug("Provisioned %d reader actors", len(self._actors))
        actor = self._actors[self._dispatched % len(self._actors)]
        self._dispatched += 1
        return actor

    def kill(self) -> None:
        """Kill every actor, a no-op until the pool has been made."""
        for actor in self._actors:
            # ``ActorProxy`` is ``ActorHandle | type[T]``; kill wants a handle.
            # pyrefly: ignore[bad-argument-type]
            ray.kill(actor)


@contextmanager
def reader_actor_pool(
    actor_cls: Any,
    *,
    num_actors: int,
    options: Optional[dict] = None,
    **ctor_kwargs: Any,
) -> Iterator[LazyActorPool]:
    """Yield a :class:`LazyActorPool` of ``num_actors`` actors, killed on exit.

    ``options`` go to ``.options()`` (e.g. ``max_restarts``) and the rest to
    the constructor. SPREAD keeps the reads' IO from bottlenecking on one
    node; killing every actor even on an early exit cancels their in-flight
    reads.
    """
    pool = LazyActorPool(
        num_actors,
        lambda: [
            actor_cls.options(scheduling_strategy="SPREAD", **(options or {})).remote(
                **ctor_kwargs
            )
            for _ in range(num_actors)
        ],
    )
    try:
        yield pool
    finally:
        pool.kill()


def dispatch_batches(
    pool: LazyActorPool,
    batches: Iterable[List["FileInfo"]],
    submit: Callable[[LazyActorPool, List["FileInfo"]], Any],
    *,
    window: int = 0,
) -> Iterator[Tuple[List["FileInfo"], Any]]:
    """Yield each batch with what ``submit`` returned for it, at most ``window`` ahead.

    ``submit`` draws actors from the pool itself, so a batch that turns out
    to need no work (CSV files with no header to read) draws none and the
    pool stays unprovisioned. Batches come back in dispatch order, so a
    caller rebuilding a listing in order never waits unless every batch in
    the window is still being read. ``window`` (``0`` -> two per actor)
    bounds the in-flight batches, keeping listing roughly demand-driven and
    memory flat. A caller that abandons this iterator early stops
    dispatch; the in-flight reads are cancelled when its actor pool is
    killed.
    """
    batches = iter(batches)
    pending: Deque[Tuple[List["FileInfo"], Any]] = deque()
    window = window or len(pool) * 2

    def dispatch_next() -> bool:
        batch = next(batches, None)
        if batch is None:
            return False
        pending.append((batch, submit(pool, batch)))
        return True

    for _ in range(window):
        if not dispatch_next():
            break
    while pending:
        yield pending.popleft()
        dispatch_next()
