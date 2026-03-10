"""Collective communication for the Rust Ray backend.

Provides ``create_collective_group()`` which initialises NCCL across a
set of GPU actors so that RDT can use point-to-point send/recv.
"""

from __future__ import annotations

import pickle
import socket
import threading
from typing import Dict, List, Optional

# Global registry: actor_id (bytes-like) → (rank, group_name)
_actor_rank_map: Dict[str, int] = {}
_group_actors: Dict[str, list] = {}
_lock = threading.Lock()


def _find_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("", 0))
        return s.getsockname()[1]


def get_actor_rank(actor_handle) -> Optional[int]:
    """Look up the NCCL rank for an actor handle."""
    key = _actor_key(actor_handle)
    with _lock:
        return _actor_rank_map.get(key)


def get_group_actors() -> Dict[str, list]:
    """Return the global group→actors mapping."""
    with _lock:
        return dict(_group_actors)


def _actor_key(actor_handle) -> str:
    """Return a hashable key for an actor handle."""
    return str(actor_handle._actor_id)


def create_collective_group(
    actors: list,
    backend: str = "nccl",
    name: Optional[str] = None,
):
    """Initialise an NCCL collective group across *actors*.

    Each actor must be a GPU actor running in a separate process (i.e.
    created with ``num_gpus=1``). This function submits
    ``__init_nccl__(rank, world_size, init_method)`` to every actor
    concurrently and blocks until all have joined.

    The actor→rank mapping is stored globally so that
    ``_ActorMethodHandle.remote()`` can look up ranks for RDT transfers.
    """
    import ray  # local import to avoid circular dependency

    if name is None:
        import uuid
        name = str(uuid.uuid4())

    world_size = len(actors)
    port = _find_free_port()
    init_method = f"tcp://127.0.0.1:{port}"

    # Submit __init_nccl__ to all actors concurrently via threads because
    # NCCL init_process_group blocks until all ranks join.
    errors = [None] * world_size
    results = [None] * world_size

    def _init_rank(rank):
        try:
            actor = actors[rank]
            driver = ray._runtime.driver
            serialized = [
                pickle.dumps(rank),
                pickle.dumps(world_size),
                pickle.dumps(init_method),
            ]
            oid = driver.submit_actor_method(
                actor._actor_id, "__init_nccl__", serialized
            )
            data = driver.get([oid.binary()], 60000)  # 60s timeout
            if data and data[0] is not None:
                results[rank] = pickle.loads(bytes(data[0][0]))
            else:
                errors[rank] = "timeout"
        except Exception as e:
            errors[rank] = str(e)

    threads = []
    for r in range(world_size):
        t = threading.Thread(target=_init_rank, args=(r,))
        t.start()
        threads.append(t)
    for t in threads:
        t.join(timeout=120)

    # Check for errors
    for i, err in enumerate(errors):
        if err is not None:
            raise RuntimeError(
                f"Failed to init NCCL on actor rank {i}: {err}"
            )

    # Register actor→rank mapping
    with _lock:
        for rank, actor in enumerate(actors):
            _actor_rank_map[_actor_key(actor)] = rank
        _group_actors[name] = list(actors)

    return name
