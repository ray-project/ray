"""Application-level exclusive NIC assignment for RDT/NIXL transfers.

Without pinning, UCX chooses network devices on its own, so multiple actors
on the same node can silently contend on one NIC while others sit idle
(https://github.com/ray-project/ray/issues/64426). This module provides a
cluster-wide registry (a detached actor) that hands each RDT actor an
exclusive RDMA NIC, which is then exported via ``UCX_NET_DEVICES`` before the
NIXL agent is created.

The feature is opt-in via ``RAY_RDT_NIC_PINNING=1`` and fails open: any
discovery or allocator failure falls back to UCX's default device selection.
"""

import glob
import logging
import os
from typing import Dict, List, Optional

import ray

logger = logging.getLogger(__name__)

NIC_ALLOCATOR_NAME = "_ray_rdt_nic_allocator"
NIC_ALLOCATOR_NAMESPACE = "_ray_rdt"

RDT_NIC_PINNING_ENV_VAR = "RAY_RDT_NIC_PINNING"

# Overridable for testing.
_INFINIBAND_SYSFS_ROOT = "/sys/class/infiniband"


def discover_rdma_nics() -> List[str]:
    """Enumerate port-qualified RDMA device names (e.g. ``mlx5_0:1``).

    Scans ``/sys/class/infiniband``, which remains mounted inside
    containers/pods where regular netdevs are network-namespaced away (the
    same surface ``_is_efa_available`` relies on). Returns a deterministic
    sorted list so a device index always maps to the same physical NIC on a
    node; empty on hosts without RDMA devices (including non-Linux).
    """
    nics = []
    for dev_path in sorted(glob.glob(os.path.join(_INFINIBAND_SYSFS_ROOT, "*"))):
        dev = os.path.basename(dev_path)
        ports = sorted(glob.glob(os.path.join(dev_path, "ports", "*")))
        for port_path in ports:
            nics.append(f"{dev}:{os.path.basename(port_path)}")
    return nics


def _nic_pinning_enabled() -> bool:
    return os.environ.get(RDT_NIC_PINNING_ENV_VAR, "0") == "1"


@ray.remote(num_cpus=0)
class NICAllocator:
    """Cluster-wide registry mapping (node, NIC) -> owning actor.

    A single detached instance serves the whole cluster. All methods run in
    the actor's single-threaded event loop, so no additional locking is
    needed.
    """

    def __init__(self):
        # node_id -> {nic_name -> owning actor_id or None}
        self._nics: Dict[str, Dict[str, Optional[str]]] = {}

    def register_node(self, node_id: str, nic_names: List[str]) -> None:
        """Record the NICs available on a node.

        Idempotent: the first registration wins so that repeat calls from
        later actors on the same node don't wipe live assignments.
        """
        if node_id not in self._nics:
            self._nics[node_id] = dict.fromkeys(nic_names)

    def acquire(self, node_id: str, actor_id: str) -> Optional[str]:
        """Return an exclusive NIC for ``actor_id`` on ``node_id``.

        Re-entrant: an actor that already holds a NIC (e.g. its NIXL agent
        was rebuilt) gets the same NIC back rather than leaking a second
        one. Returns None when the node is unknown or all NICs are taken;
        callers must treat None as "fall back to unpinned".
        """
        node_nics = self._nics.get(node_id)
        if not node_nics:
            return None
        for nic, owner in node_nics.items():
            if owner == actor_id:
                return nic
        for nic, owner in node_nics.items():
            if owner is None:
                node_nics[nic] = actor_id
                return nic
        return None

    def release(self, node_id: str, actor_id: str) -> None:
        """Free every NIC held by ``actor_id`` on ``node_id``."""
        for nic, owner in self._nics.get(node_id, {}).items():
            if owner == actor_id:
                self._nics[node_id][nic] = None

    def release_all_for_node(self, node_id: str) -> None:
        """Reclaim all NICs on a node, e.g. after the node died."""
        for nic in self._nics.get(node_id, {}):
            self._nics[node_id][nic] = None

    def snapshot(self) -> Dict[str, Dict[str, Optional[str]]]:
        """Full view of current assignments, for debugging/observability."""
        return self._nics


def _get_or_create_allocator() -> "ray.actor.ActorHandle":
    try:
        return ray.get_actor(NIC_ALLOCATOR_NAME, namespace=NIC_ALLOCATOR_NAMESPACE)
    except ValueError:
        # get_if_exists resolves concurrent creation races atomically.
        return NICAllocator.options(
            name=NIC_ALLOCATOR_NAME,
            namespace=NIC_ALLOCATOR_NAMESPACE,
            lifetime="detached",
            get_if_exists=True,
        ).remote()


def acquire_nic_for_current_actor(timeout_s: float = 10.0) -> Optional[str]:
    """Best-effort: register this node's NICs and acquire one exclusively.

    Returns a port-qualified device name suitable for ``UCX_NET_DEVICES``,
    or None when pinning is disabled, the caller is a driver, no NIC is
    available, or anything fails. NIC pinning is a performance
    optimization, so every failure path degrades to UCX's own device
    selection rather than raising.
    """
    if not _nic_pinning_enabled():
        return None
    nics = discover_rdma_nics()
    if not nics:
        logger.info(
            "%s=1 but no RDMA NICs were discovered under %s; "
            "UCX will select network devices itself.",
            RDT_NIC_PINNING_ENV_VAR,
            _INFINIBAND_SYSFS_ROOT,
        )
        return None
    try:
        ctx = ray.get_runtime_context()
        actor_id = ctx.get_actor_id()
        if actor_id is None:
            # Drivers are not pinned; only long-lived actors contend on NICs.
            return None
        node_id = ctx.get_node_id()
        allocator = _get_or_create_allocator()
        ray.get(allocator.register_node.remote(node_id, nics), timeout=timeout_s)
        nic = ray.get(allocator.acquire.remote(node_id, actor_id), timeout=timeout_s)
        if nic is None:
            logger.warning(
                "All %d RDMA NICs on node %s are already assigned; "
                "falling back to unpinned UCX device selection.",
                len(nics),
                node_id,
            )
        return nic
    except Exception:
        logger.warning(
            "RDT NIC acquisition failed; falling back to unpinned UCX "
            "device selection.",
            exc_info=True,
        )
        return None


def release_nic_for_current_actor(timeout_s: float = 5.0) -> None:
    """Best-effort release of this actor's NIC on shutdown."""
    if not _nic_pinning_enabled():
        return
    try:
        ctx = ray.get_runtime_context()
        actor_id = ctx.get_actor_id()
        if actor_id is None:
            return
        allocator = ray.get_actor(NIC_ALLOCATOR_NAME, namespace=NIC_ALLOCATOR_NAMESPACE)
        ray.get(
            allocator.release.remote(ctx.get_node_id(), actor_id),
            timeout=timeout_s,
        )
    except Exception:
        logger.debug("RDT NIC release skipped.", exc_info=True)
