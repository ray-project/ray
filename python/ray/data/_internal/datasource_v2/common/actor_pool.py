"""The actor-pool protocol shared by the footer and header reader pools.

Parquet footer reads and CSV header reads both run in a pool of Ray actors
provisioned for one ``list_files`` call: several SPREAD-scheduled actors,
batches dispatched round-robin with a bounded in-flight window, results
delivered in dispatch order, then every actor killed.
"""

from __future__ import annotations

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


@contextmanager
def reader_actor_pool(
    actor_cls: Any,
    *,
    num_actors: int,
    options: Optional[dict] = None,
    **ctor_kwargs: Any,
) -> Iterator[List["ActorProxy[Any]"]]:
    """Yield ``num_actors`` SPREAD actors built from ``actor_cls``, killed on exit.

    ``options`` go to ``.options()`` (e.g. ``max_restarts``) and the rest to
    the constructor. SPREAD keeps the reads' IO from bottlenecking on one
    node; killing every actor even on an early exit cancels their in-flight
    reads.
    """
    actors = [
        actor_cls.options(scheduling_strategy="SPREAD", **(options or {})).remote(
            **ctor_kwargs
        )
        for _ in range(num_actors)
    ]
    try:
        yield actors
    finally:
        for actor in actors:
            # ``ActorProxy`` is ``ActorHandle | type[T]``; kill wants a handle.
            # pyrefly: ignore[bad-argument-type]
            ray.kill(actor)


def dispatch_batches(
    actors: List["ActorProxy[Any]"],
    batches: Iterable[List["FileInfo"]],
    submit: Callable[["ActorProxy[Any]", List["FileInfo"]], Any],
    *,
    window: int = 0,
) -> Iterator[Tuple[List["FileInfo"], Any]]:
    """Yield each batch with what ``submit`` returned for it, at most ``window`` ahead.

    Batches go to the actors round-robin and come back in dispatch order, so a
    caller rebuilding a listing in order never waits unless every batch in the
    window is still being read. ``window`` (``0`` -> two per actor) bounds the
    in-flight batches, keeping listing roughly demand-driven and memory flat.
    A caller that abandons this iterator early stops dispatch; the in-flight
    reads are cancelled when its actor pool is killed.
    """
    batches = iter(batches)
    pending: Deque[Tuple[List["FileInfo"], Any]] = deque()
    window = window or len(actors) * 2
    batch_no = 0

    def dispatch_next() -> bool:
        nonlocal batch_no
        batch = next(batches, None)
        if batch is None:
            return False
        pending.append((batch, submit(actors[batch_no % len(actors)], batch)))
        batch_no += 1
        return True

    for _ in range(window):
        if not dispatch_next():
            break
    while pending:
        yield pending.popleft()
        dispatch_next()
