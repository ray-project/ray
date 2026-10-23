"""Placing sandboxes on the node that caches their image."""

import hashlib
import logging
import os
import threading
import time
from typing import Any, Dict, List, Optional

import ray
from ray.experimental.sandbox._internal.image_utils import parse_image_ref
from ray.util.annotations import PublicAPI
from ray.util.placement_group import get_current_placement_group
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

logger = logging.getLogger(__name__)

# How long a process reuses the cluster's node list, so a burst of creates
# doesn't ask the GCS for it each time.
_NODES_TTL_SECONDS = 5.0
_nodes_lock = threading.Lock()
_nodes: List[Dict[str, Any]] = []
_nodes_fetched_at = float("-inf")


def _alive_nodes() -> List[Dict[str, Any]]:
    global _nodes, _nodes_fetched_at
    with _nodes_lock:
        if time.monotonic() - _nodes_fetched_at >= _NODES_TTL_SECONDS:
            try:
                _nodes = [node for node in ray.nodes() if node.get("Alive")]
            except Exception as err:
                # A soft preference mustn't fail the create: keep the last
                # list, and don't retry for every create of a burst.
                logger.warning("Couldn't list the cluster's nodes: %s", err)
            _nodes_fetched_at = time.monotonic()
        return _nodes


def _is_local_archive(image: str) -> bool:
    """Whether ``image`` names a tar archive on the local filesystem."""
    return (
        image.endswith(".tar")
        or image.startswith(("/", "./", "../"))
        or os.path.isfile(image)
    )


def _image_key(image: str) -> str:
    """The image's identity, the same however its reference is spelled."""
    registry, repo, reference = parse_image_ref(image)
    return f"{registry}/{repo}:{reference}"


def _score(node_id: str, image_key: str) -> bytes:
    return hashlib.blake2b(f"{node_id}\0{image_key}".encode(), digest_size=8).digest()


@PublicAPI(stability="alpha")
def image_scheduling_strategy(
    image: str,
    num_cpus: Optional[float] = None,
    num_gpus: Optional[float] = None,
    memory: Optional[float] = None,
    resources: Optional[Dict[str, float]] = None,
) -> Optional[NodeAffinitySchedulingStrategy]:
    """Prefer the node that an image maps to, so its sandboxes share one cache.

    Each node caches the images its sandboxes run, and a cold pull of a large
    image takes a minute or more. This maps each image to one of the live
    nodes whose total resources fit the actor, by rendezvous hashing: every
    caller picks the same node for an image, however its reference is
    spelled, with no coordination, so even a burst of sandboxes started
    before any pull finishes lands on one node and pulls the image once. A
    node joining or leaving the cluster moves only the images it wins or
    held. The preference is soft: when that node lacks the resources right
    now, or is gone, the actor runs wherever Ray would have put it.

    :func:`~ray.experimental.sandbox.create` and the sandbox HTTP and gRPC
    APIs apply it themselves. Pass it as the ``scheduling_strategy`` of an
    actor that creates sandboxes with
    :class:`~ray.experimental.sandbox.SandboxRuntime`.

    Args:
        image: The image the actor's sandboxes run.
        num_cpus: The actor's CPU request; unset counts as 1, as Ray
            schedules an actor.
        num_gpus: The actor's GPU request.
        memory: The actor's memory request in bytes.
        resources: The actor's custom resource requests.

    Returns:
        A soft node affinity. None when Ray isn't initialized, when ``image`` is
        a local archive, which is on one node's filesystem only, when no live
        node fits the actor, or when the caller runs in a placement group,
        whose child actors a strategy would take out of it.
    """
    if (
        not ray.is_initialized()
        or _is_local_archive(image)
        or get_current_placement_group() is not None
    ):
        return None
    demand = {
        "CPU": 1 if num_cpus is None else num_cpus,
        "GPU": num_gpus or 0,
        "memory": memory or 0,
        **(resources or {}),
    }
    candidates = [
        node["NodeID"]
        for node in _alive_nodes()
        if all(
            (node.get("Resources") or {}).get(name, 0) >= amount
            for name, amount in demand.items()
            if amount
        )
    ]
    if not candidates:
        return None
    key = _image_key(image)
    return NodeAffinitySchedulingStrategy(
        node_id=max(candidates, key=lambda node_id: _score(node_id, key)),
        soft=True,
        _spill_on_unavailable=True,
    )
