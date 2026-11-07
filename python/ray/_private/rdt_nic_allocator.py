"""Application-level exclusive NIC assignment for RDT/NIXL transfers.

Without pinning, UCX chooses network devices on its own, so multiple actors
on the same node can silently contend on one NIC while others sit idle
(https://github.com/ray-project/ray/issues/64426). This module provides a
cluster-wide registry (a detached actor) that hands each RDT actor an
exclusive RDMA NIC, which is then exported via ``UCX_NET_DEVICES`` before the
NIXL agent is created.

The feature is opt-in via ``RAY_RDT_NIC_PINNING=1`` and fails open: any
discovery or allocator failure falls back to UCX's default device selection.

Deliberately kept outside ``ray.experimental.rdt``: that package's
``__init__.py`` eagerly imports the NIXL, CUDA IPC, and collective (NCCL/
GLOO) tensor transport modules, so importing anything from within it -- even
this file alone -- pulls all of that in. Living under ``ray._private``
instead lets callers that only need the tiny bookkeeping in this module
(e.g. ``Worker.shutdown_rdt_manager``, on every actor's shutdown path) avoid
paying that cost when RDT/NIXL was never used.
"""

import glob
import json
import logging
import os
from typing import Dict, List, Optional

import ray

logger = logging.getLogger(__name__)

NIC_ALLOCATOR_NAME = "_ray_rdt_nic_allocator"
NIC_ALLOCATOR_NAMESPACE = "_ray_rdt"

RDT_NIC_PINNING_ENV_VAR = "RAY_RDT_NIC_PINNING"

# Internal KV namespace/key the allocator persists its assignments under, so
# a restarted (or freshly re-created, after get_if_exists) actor recovers
# the same state instead of silently starting from an empty registry while
# surviving actors still hold NICs the new instance has never heard of.
_KV_NAMESPACE = b"rdt_nic_allocator"
_KV_STATE_KEY = "state"

# Overridable for testing.
_INFINIBAND_SYSFS_ROOT = "/sys/class/infiniband"

# Process-local record of the NIC (if any) this process successfully
# acquired. Set only on a confirmed successful acquire, so a subsequent
# release can check this and skip the GCS/allocator round trip entirely for
# processes that never held a NIC (pinning disabled, no NICs, or the pool
# was exhausted).
_acquired_nic: Optional[str] = None


def _port_is_active(port_path: str) -> bool:
    """Whether the RDMA port at ``port_path`` is link-active.

    Reads the sysfs ``state`` file (e.g. ``"4: ACTIVE"`` or ``"1: DOWN"``).
    A port that's cabled-but-down, administratively disabled, or otherwise
    unusable must never be handed out for exclusive pinning: doing so would
    steer UCX at a device that can't move data, which is strictly worse
    than the unpinned default this feature is meant to never regress below.
    Any I/O error (missing/unreadable file, unexpected format) is treated
    as inactive -- consistent with failing open by excluding a possibly-
    fine port, rather than risking pinning to a possibly-dead one.
    """
    try:
        with open(os.path.join(port_path, "state")) as f:
            return "ACTIVE" in f.read()
    except OSError:
        return False


def discover_rdma_nics() -> List[str]:
    """Enumerate port-qualified, link-active RDMA device names (e.g.
    ``mlx5_0:1``).

    Scans ``/sys/class/infiniband``, which remains mounted inside
    containers/pods where regular netdevs are network-namespaced away (the
    same surface ``_is_efa_available`` relies on). Returns a deterministic
    sorted list so a device index always maps to the same physical NIC on a
    node; empty on hosts without RDMA devices (including non-Linux). Ports
    that aren't link-active (see ``_port_is_active``) are excluded.
    """
    nics = []
    for dev_path in sorted(glob.glob(os.path.join(_INFINIBAND_SYSFS_ROOT, "*"))):
        dev = os.path.basename(dev_path)
        ports = sorted(glob.glob(os.path.join(dev_path, "ports", "*")))
        for port_path in ports:
            if not _port_is_active(port_path):
                continue
            nics.append(f"{dev}:{os.path.basename(port_path)}")
    return nics


def _nic_pinning_enabled() -> bool:
    return os.environ.get(RDT_NIC_PINNING_ENV_VAR, "0") == "1"


class _NICAllocatorImpl:
    """Cluster-wide registry mapping (node, NIC) -> owning actor.

    A single detached instance serves the whole cluster. All methods run in
    the actor's single-threaded event loop, so no additional locking is
    needed.

    Left undecorated (not ``@ray.remote`` directly) and wrapped lazily by
    ``_get_or_create_allocator`` below, so simply importing this module
    (e.g. from a shutdown hook, to check ``_acquired_nic``) never pays the
    cost of Ray actor-class registration when NIC pinning was never used.

    Assignments are persisted to Ray's internal KV store (GCS-backed) after
    every mutation and reloaded on ``__init__``. Without this, a restarted
    -- or freshly re-created via ``get_if_exists`` -- allocator would start
    with an empty registry while actors elsewhere in the cluster keep
    running with ``UCX_NET_DEVICES`` pointing at NICs the new instance has
    no memory of, letting it hand the same NIC out twice.
    """

    def __init__(self):
        # node_id -> {nic_name -> owning actor_id or None}
        self._nics: Dict[str, Dict[str, Optional[str]]] = self._load_persisted_state()

    def _load_persisted_state(self) -> Dict[str, Dict[str, Optional[str]]]:
        """Best-effort rehydration from the KV store.

        Falls back to an empty registry -- never raises -- so that (a) a
        plain unit-test instantiation with no live cluster still works, and
        (b) a missing/corrupt KV entry degrades to "start fresh" rather
        than crashing allocator startup.
        """
        from ray.experimental.internal_kv import (
            _internal_kv_get,
            _internal_kv_initialized,
        )

        if not _internal_kv_initialized():
            return {}
        try:
            raw = _internal_kv_get(_KV_STATE_KEY, namespace=_KV_NAMESPACE)
            return json.loads(raw) if raw else {}
        except Exception:
            logger.warning(
                "Failed to load persisted RDT NIC allocator state; "
                "starting with an empty registry.",
                exc_info=True,
            )
            return {}

    def _persist_state(self) -> None:
        """Best-effort write-through of the full registry to the KV store.

        Called synchronously at the end of every mutating method, so a
        caller that receives a successful response is guaranteed the
        assignment was already durable before the method returned -- not
        just updated in this process's memory.
        """
        from ray.experimental.internal_kv import (
            _internal_kv_initialized,
            _internal_kv_put,
        )

        if not _internal_kv_initialized():
            return
        try:
            _internal_kv_put(
                _KV_STATE_KEY, json.dumps(self._nics), namespace=_KV_NAMESPACE
            )
        except Exception:
            logger.warning(
                "Failed to persist RDT NIC allocator state; a crash before "
                "the next successful persist could lose this update.",
                exc_info=True,
            )

    def register_node(self, node_id: str, nic_names: List[str]) -> None:
        """Record the NICs available on a node.

        Idempotent: the first registration wins so that repeat calls from
        later actors on the same node don't wipe live assignments.
        """
        if node_id not in self._nics:
            self._nics[node_id] = dict.fromkeys(nic_names)
            self._persist_state()

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
                self._persist_state()
                return nic
        return None

    def release(self, node_id: str, actor_id: str) -> None:
        """Free every NIC held by ``actor_id`` on ``node_id``."""
        changed = False
        for nic, owner in self._nics.get(node_id, {}).items():
            if owner == actor_id:
                self._nics[node_id][nic] = None
                changed = True
        if changed:
            self._persist_state()

    def release_all_for_node(self, node_id: str) -> None:
        """Reclaim all NICs on a node, e.g. after the node died."""
        node_nics = self._nics.get(node_id)
        if not node_nics:
            return
        for nic in node_nics:
            node_nics[nic] = None
        self._persist_state()

    def snapshot(self) -> Dict[str, Dict[str, Optional[str]]]:
        """Full view of current assignments, for debugging/observability."""
        return self._nics


# The @ray.remote-wrapped actor class, built lazily on first use (see
# _get_nic_allocator_actor_cls) rather than at module import time.
_NICAllocatorActorCls = None


def _get_nic_allocator_actor_cls():
    global _NICAllocatorActorCls
    if _NICAllocatorActorCls is None:
        _NICAllocatorActorCls = ray.remote(num_cpus=0)(_NICAllocatorImpl)
    return _NICAllocatorActorCls


def _get_head_node_id() -> Optional[str]:
    """Best-effort hex node ID of the cluster's head node, or None.

    Used only to bias allocator placement (see _get_or_create_allocator);
    any failure here just means the allocator gets scheduled normally.
    """
    from ray._common.constants import HEAD_NODE_RESOURCE_NAME

    try:
        for node in ray.nodes():
            if HEAD_NODE_RESOURCE_NAME in node.get("Resources", {}):
                return node["NodeID"]
    except Exception:
        logger.debug("Failed to look up head node id.", exc_info=True)
    return None


def _get_or_create_allocator() -> "ray.actor.ActorHandle":
    try:
        return ray.get_actor(NIC_ALLOCATOR_NAME, namespace=NIC_ALLOCATOR_NAMESPACE)
    except ValueError:
        # get_if_exists resolves concurrent creation races atomically. Even
        # if this races with another caller and a different one "wins" the
        # creation, both end up pointed at the same persisted KV state, so
        # there's no correctness gap from the race itself.
        options = dict(
            name=NIC_ALLOCATOR_NAME,
            namespace=NIC_ALLOCATOR_NAMESPACE,
            lifetime="detached",
            get_if_exists=True,
            # Restarts alone would be pointless without KV persistence
            # above (a restarted actor would just start empty again); with
            # it, this lets Ray recover the allocator in place instead of
            # always falling through to creating a brand new actor here.
            max_restarts=-1,
        )
        head_node_id = _get_head_node_id()
        if head_node_id is not None:
            from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

            # Correlates the allocator's failure domain with "the whole
            # cluster is going down" rather than an arbitrary worker node
            # dying independently. soft=True: if the head node is
            # unschedulable for some reason, fall back to normal placement
            # rather than failing allocator creation outright -- KV
            # persistence means correctness doesn't depend on where this
            # actually lands.
            options["scheduling_strategy"] = NodeAffinitySchedulingStrategy(
                head_node_id, soft=True
            )
        return _get_nic_allocator_actor_cls().options(**options).remote()


def acquire_nic_for_current_actor(timeout_s: float = 10.0) -> Optional[str]:
    """Best-effort: register this node's NICs and acquire one exclusively.

    Returns a port-qualified device name suitable for ``UCX_NET_DEVICES``,
    or None when pinning is disabled, the caller is a driver, no NIC is
    available, or anything fails. NIC pinning is a performance
    optimization, so every failure path degrades to UCX's own device
    selection rather than raising.

    On a confirmed successful acquire, records the NIC in the process-local
    ``_acquired_nic`` so a later ``release_nic_for_current_actor`` call can
    skip the allocator round trip entirely if this process never held one.
    """
    global _acquired_nic

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

    ctx = ray.get_runtime_context()
    actor_id = ctx.get_actor_id()
    if actor_id is None:
        # Drivers are not pinned; only long-lived actors contend on NICs.
        return None
    node_id = ctx.get_node_id()

    try:
        allocator = _get_or_create_allocator()
        ray.get(allocator.register_node.remote(node_id, nics), timeout=timeout_s)
        acquire_ref = allocator.acquire.remote(node_id, actor_id)
        nic = ray.get(acquire_ref, timeout=timeout_s)
    except ray.exceptions.GetTimeoutError:
        # The client wait timed out, but the single-threaded allocator may
        # have already committed this NIC to actor_id. We don't know either
        # way, so fire a best-effort, non-blocking release to undo a
        # possible orphaned reservation rather than leaking it until the
        # node dies. This is safe even if nothing was actually committed
        # (release on an unheld NIC is a no-op).
        logger.warning(
            "Timed out waiting for NIC allocator on node %s; falling back "
            "to unpinned UCX device selection and releasing any possible "
            "orphaned reservation.",
            node_id,
        )
        try:
            allocator.release.remote(node_id, actor_id)
        except Exception:
            logger.debug("Best-effort orphan release failed.", exc_info=True)
        return None
    except Exception:
        logger.warning(
            "RDT NIC acquisition failed; falling back to unpinned UCX "
            "device selection.",
            exc_info=True,
        )
        return None

    if nic is None:
        logger.warning(
            "All %d RDMA NICs on node %s are already assigned; "
            "falling back to unpinned UCX device selection.",
            len(nics),
            node_id,
        )
        return None

    _acquired_nic = nic
    return nic


def release_nic_for_current_actor(timeout_s: float = 5.0) -> None:
    """Best-effort release of this actor's NIC on shutdown.

    Returns immediately, without any GCS/allocator call, if this process
    never successfully acquired a NIC (pinning disabled, no NICs found, or
    the pool was exhausted) -- the common case during actor teardown.

    ``_acquired_nic`` is only cleared once the release RPC is confirmed to
    succeed. If it fails (allocator unreachable, timeout, etc.), the local
    record is left as-is: the allocator may still show this actor as the
    owner, so clearing our own note here would make the leak permanent by
    silencing any future retry.
    """
    global _acquired_nic

    if _acquired_nic is None:
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
        logger.debug(
            "RDT NIC release failed; keeping local record so a later "
            "retry can still attempt it.",
            exc_info=True,
        )
        return
    _acquired_nic = None
