"""Preemption signal plumbing.

Two pieces live in this module:

  * :class:`PreemptionWatcher` — a plain Python class that polls a drain
    source (Ray Core's ``get_draining_nodes()`` by default) and, on each
    change, fans out a :class:`PreemptionInfo` to all worker actors and
    notifies the controller. Designed to run as a Ray actor (see
    :func:`make_preemption_watcher_actor`), but the class itself is plain
    Python so unit tests can exercise it without spawning an actor.

  * :class:`PreemptionCallback` — a thin :class:`WorkerGroupCallback` that
    spawns one :class:`PreemptionWatcher` actor per worker-group instance
    and kills it on shutdown. Builds the failure-domain map once, when the
    worker group starts, and passes it to the watcher.

Why a dedicated actor (vs. an asyncio task in the controller):

  * Process isolation: a buggy / stuck poll loop doesn't affect the
    controller's main control loop.
  * Lifecycle decoupling: poll cadence is independent of controller state.
  * No reliance on ``asyncio.get_event_loop()`` inside a callback hook.
  * Naturally extensible to push-based signal sources later (k8s informer,
    SIGTERM handler, cloud event queues) without disturbing the controller.
"""

import asyncio
import logging
import os
import time
from collections import defaultdict
from typing import TYPE_CHECKING, Callable, Dict, List, Optional, Set

import ray
from ray.train.v2._internal.execution.callback import WorkerGroupCallback
from ray.train.v2._internal.execution.preemption import PreemptionInfo

# Default bounded-wait deadline (seconds) when Ray Core reports a drain
# but no deadline (deadline_ms=0). Set to "inf" / "none" to disable the
# bounded wait and revert to the unbounded behavior.
_DEFAULT_DEADLINE_ENV = "RAY_TRAIN_PREEMPTION_DEFAULT_DEADLINE_S"
_DEFAULT_DEADLINE_FALLBACK_S = 60.0

# Test-only env var: comma-separated "rank:fake_node_id" entries that override
# the per-worker node_id lookup when building the failure-domain map. Useful
# for single-node unit tests that need to simulate a multi-node failure
# domain. For integration tests, prefer a real multi-node cluster via
# ``ray.cluster_utils.Cluster``. NEVER set this in production.
_FAKE_NODE_MAP_ENV = "RAY_TRAIN_PREEMPTION_FAKE_NODE_MAP"

if TYPE_CHECKING:
    from ray.actor import ActorHandle
    from ray.train.v2._internal.execution.worker_group.worker_group import WorkerGroup


logger = logging.getLogger(__name__)


# Type alias: () -> {node_id_hex: deadline_ms}. deadline_ms=0 means
# "no deadline reported by Core."
DrainSource = Callable[[], Dict[str, int]]


def _default_drain_source() -> Dict[str, int]:
    """Default drain source: Ray Core's ``get_draining_nodes()`` RPC.

    Returns ``{node_id_hex: deadline_ms}`` for every node the GCS currently
    has marked as draining. ``deadline_ms == 0`` means "no deadline reported"
    — the bounded-wait default kicks in elsewhere (see ``_resolve_deadline``).

    For integration tests that need to simulate a drain, use the real Ray
    Core drain RPC via ``GcsClient.drain_node`` against a multi-node local
    cluster (see ``preemption_integration_demo.py``). For unit tests, inject
    a custom ``drain_source`` callable when constructing ``PreemptionWatcher``.
    """
    import ray._private.state as state

    try:
        return state.state.get_draining_nodes()
    except Exception as e:  # pragma: no cover -- defensive
        logger.warning("get_draining_nodes() failed: %s", e)
        return {}


# ─── PreemptionWatcher ──────────────────────────────────────────────────────


class PreemptionWatcher:
    """Polls a drain source and routes :class:`PreemptionInfo` updates.

    Designed to be wrapped as a Ray actor (see
    :func:`make_preemption_watcher_actor`) but the class itself is plain
    Python so unit tests can drive ``_on_drain_change`` directly without
    spawning an actor.

    Lifecycle:
      * ``watch()`` (async) runs the poll loop until ``stop()`` is called or
        the actor is killed.
      * ``stop()`` sets a flag that makes the next iteration of ``watch()``
        return cleanly.

    Args:
        controller_actor: TrainController actor handle. ``notify_preempting``
            is called when a new drain is observed.
        worker_actors_by_rank: ``{rank: worker_actor_handle}`` for every
            worker in the worker group. Built once by the callback when the
            WG starts.
        node_to_domain_ranks: ``{node_id: [ranks_in_failure_domain]}``.
            For TPU multislice, every host in a slice maps to the same set
            of ranks (the whole slice). For non-TPU, each node maps to just
            its own ranks.
        poll_interval_s: Seconds between drain-source polls.
        drain_source: Override the default drain source. Used in unit tests.
    """

    def __init__(
        self,
        controller_actor: "ActorHandle",
        worker_actors_by_rank: Dict[int, "ActorHandle"],
        node_to_domain_ranks: Dict[str, List[int]],
        poll_interval_s: float = 5.0,
        drain_source: Optional[DrainSource] = None,
    ):
        self._controller = controller_actor
        self._workers_by_rank = dict(worker_actors_by_rank)
        self._node_to_domain_ranks = dict(node_to_domain_ranks)
        self._poll_interval_s = poll_interval_s
        self._drain_source: DrainSource = drain_source or _default_drain_source

        self._last_drained: Dict[str, int] = {}
        self._stopped = False

    async def watch(self) -> None:
        """Main poll loop. Runs until ``stop()`` or the actor is killed."""
        n_workers = len(self._workers_by_rank)
        n_nodes = len(set(self._node_to_domain_ranks.keys()))
        fake_map_env = os.environ.get(_FAKE_NODE_MAP_ENV)
        logger.info(
            "PreemptionWatcher started: poll_interval=%.1fs, "
            "nodes=%d, workers=%d, fake_node_map=%s",
            self._poll_interval_s,
            n_nodes,
            n_workers,
            fake_map_env if fake_map_env else "(none)",
        )
        logger.info(
            "PreemptionWatcher domain map: %s",
            dict(sorted(self._node_to_domain_ranks.items())),
        )

        while not self._stopped:
            try:
                drained = self._drain_source()
                if drained != self._last_drained:
                    logger.info(
                        "Drained nodes changed: %s -> %s",
                        self._last_drained,
                        drained,
                    )
                    self._on_drain_change(drained)
                    self._last_drained = dict(drained)
            except Exception as e:
                logger.warning("Preemption poll failed: %s", e)
            await asyncio.sleep(self._poll_interval_s)

        logger.info("PreemptionWatcher stopped.")

    def stop(self) -> None:
        """Signal the poll loop to exit on its next iteration."""
        self._stopped = True

    # ─── Logic ────────────────────────────────────────────────────────────

    def _on_drain_change(self, drained: Dict[str, int]) -> None:
        """Compute affected ranks + deadline, notify controller, fan out
        ``mark_preempt`` to all worker actors."""
        affected_ranks: Set[int] = set()
        affected_nodes: Set[str] = set()
        deadlines_ms: List[int] = []

        for node_id, deadline_ms in drained.items():
            ranks_in_domain = self._node_to_domain_ranks.get(node_id)
            if ranks_in_domain is None:
                continue  # drained node not in this WG
            affected_ranks.update(ranks_in_domain)
            affected_nodes.add(node_id)
            if deadline_ms > 0:
                deadlines_ms.append(deadline_ms)

        if not affected_ranks:
            logger.warning(
                "Drain detected on nodes %s but none belong to this worker "
                "group (domain map keys: %s). No mark_preempt will fire. "
                "If you're testing locally, check that the env var "
                "RAY_TRAIN_PREEMPTION_FAKE_NODE_MAP propagated to the "
                "watcher actor (restart Jupyter kernel if needed).",
                sorted(drained.keys()),
                sorted(self._node_to_domain_ranks.keys()),
            )
            return

        deadline = self._resolve_deadline(deadlines_ms)
        info = PreemptionInfo(
            deadline=deadline,
            reason="drain",
            preempted_ranks=sorted(affected_ranks),
            preempted_node_ids=sorted(affected_nodes),
        )

        # Notify controller first so it can transition to PreemptingState
        # on its next tick.
        try:
            self._controller.notify_preempting.remote(info)
        except Exception as e:
            logger.warning("Failed to notify controller of preemption: %s", e)

        # Fan out to every worker. Each worker checks
        # ``rank in info.preempted_ranks`` inside its UDF to decide its branch.
        # TorchFT mode is handled by skipping callback registration entirely
        # in the controller (see ``_build_preemption_callback``).
        for rank, worker_actor in self._workers_by_rank.items():
            try:
                worker_actor.mark_preempt.remote(info)
            except Exception as e:
                logger.warning(
                    "Failed to send mark_preempt to rank %d: %s", rank, e
                )

    @staticmethod
    def _resolve_deadline(deadlines_ms: List[int]) -> float:
        """Return the UNIX-seconds deadline to use for a drain event.

        If Ray Core reported any non-zero deadlines, use the earliest one
        (cross-rank-conservative — once the first preempted host is gone,
        cross-rank collectives can hang).

        If Ray Core reported only zeros (it doesn't know when eviction will
        happen), fall back to a bounded default rather than waiting forever.
        Configurable via ``RAY_TRAIN_PREEMPTION_DEFAULT_DEADLINE_S``; set to
        ``"inf"`` / ``"none"`` to explicitly opt into the unbounded behavior.
        """
        if deadlines_ms:
            return min(deadlines_ms) / 1000.0

        raw = os.environ.get(_DEFAULT_DEADLINE_ENV)
        if raw is None:
            return time.time() + _DEFAULT_DEADLINE_FALLBACK_S
        if raw.strip().lower() in ("inf", "none", "infinity"):
            return float("inf")
        try:
            return time.time() + float(raw)
        except ValueError:
            logger.warning(
                "Invalid %s=%r; falling back to %ds",
                _DEFAULT_DEADLINE_ENV,
                raw,
                int(_DEFAULT_DEADLINE_FALLBACK_S),
            )
            return time.time() + _DEFAULT_DEADLINE_FALLBACK_S


def make_preemption_watcher_actor():
    """Return a ``ray.remote``-wrapped PreemptionWatcher class.

    Factory rather than a module-level constant so that importing this
    module does not require Ray to be initialized (unit tests).
    """
    return ray.remote(num_cpus=0)(PreemptionWatcher)


# ─── PreemptionCallback (lifecycle shim) ────────────────────────────────────


class PreemptionCallback(WorkerGroupCallback):
    """Spawns / kills a :class:`PreemptionWatcher` actor for each worker
    group instance.

    The controller skips registering this callback entirely when running
    in TorchFT mode (``manages_replica_groups=True``) — TorchFT has its own
    per-step quorum that handles peer loss without a controller-level
    PreemptingState. See ``TrainController._build_preemption_callback``.

    Args:
        controller_actor: Handle to the TrainController actor.
        poll_interval_s: Seconds between drain-source polls. Passed through
            to the watcher.
    """

    def __init__(
        self,
        controller_actor: "ActorHandle",
        poll_interval_s: float = 5.0,
    ):
        self._controller_actor = controller_actor
        self._poll_interval_s = poll_interval_s
        self._watcher: Optional["ActorHandle"] = None
        self._watch_ref = None

    # ─── Lifecycle hooks ──────────────────────────────────────────────────

    def after_worker_group_start(self, worker_group: "WorkerGroup"):
        node_to_domain_ranks = self._build_failure_domain_map(worker_group)
        worker_actors_by_rank = {
            w.distributed_context.world_rank: w.actor
            for w in worker_group.get_workers()
        }

        watcher_cls = make_preemption_watcher_actor()
        self._watcher = watcher_cls.remote(
            controller_actor=self._controller_actor,
            worker_actors_by_rank=worker_actors_by_rank,
            node_to_domain_ranks=node_to_domain_ranks,
            poll_interval_s=self._poll_interval_s,
        )
        # Fire the watch loop; runs forever inside the watcher actor.
        self._watch_ref = self._watcher.watch.remote()
        logger.info(
            "PreemptionWatcher actor spawned (workers=%d, nodes=%d)",
            len(worker_actors_by_rank),
            len(set(node_to_domain_ranks.keys())),
        )

    def before_worker_group_shutdown(self, worker_group: "WorkerGroup"):
        if self._watcher is None:
            return
        # Try graceful stop first (lets the poll loop exit cleanly and emit
        # its shutdown log line), then hard-kill the actor so resources are
        # reclaimed even if stop() didn't complete.
        try:
            self._watcher.stop.remote()
        except Exception:
            logger.debug("PreemptionWatcher.stop() RPC failed", exc_info=True)
        try:
            ray.kill(self._watcher)
        except Exception:
            logger.debug("ray.kill(PreemptionWatcher) failed", exc_info=True)
        self._watcher = None
        self._watch_ref = None

    # ─── Failure-domain mapping ───────────────────────────────────────────

    def _build_failure_domain_map(
        self, wg: "WorkerGroup"
    ) -> Dict[str, List[int]]:
        """Return ``{node_id: ranks_in_same_failure_domain}``.

        Auto-detection — no user configuration required:

          * If the worker's node has a TPU slice label (Ray Core sets
            ``RAY_NODE_TPU_SLICE_NAME_KEY`` on every TPU node), all ranks
            sharing that slice label form one failure domain. This captures
            GKE's slice-atomic preemption semantics.
          * Otherwise, the failure domain is the node itself. Standard for
            GPU / CPU clusters.

        Test override: ``RAY_TRAIN_PREEMPTION_FAKE_NODE_MAP`` (test-only)
        replaces each rank's effective node_id so a single-node local
        cluster can simulate multi-node topology.
        """
        workers = wg.get_workers()
        if not workers:
            return {}

        fake_node_map = self._parse_fake_node_map(
            os.environ.get(_FAKE_NODE_MAP_ENV)
        )
        if fake_node_map:
            logger.warning(
                "PreemptionCallback using fake node map (test mode): %s",
                fake_node_map,
            )

        real_node_ids = {w.metadata.node_id for w in workers}
        node_id_to_slice = self._lookup_tpu_slice_labels(real_node_ids)

        # First pass: figure out each rank's failure-domain key.
        rank_to_domain: Dict[int, str] = {}
        rank_to_node: Dict[int, str] = {}
        for w in workers:
            rank = w.distributed_context.world_rank
            real_node_id = w.metadata.node_id
            node_id = fake_node_map.get(rank, real_node_id)
            rank_to_node[rank] = node_id
            slice_label = node_id_to_slice.get(real_node_id)
            rank_to_domain[rank] = (
                f"tpu_slice/{slice_label}" if slice_label else node_id
            )

        # Second pass: invert to {domain: [ranks]}.
        domain_to_ranks: Dict[str, List[int]] = defaultdict(list)
        for rank, domain in rank_to_domain.items():
            domain_to_ranks[domain].append(rank)

        return {
            node_id: sorted(domain_to_ranks[rank_to_domain[rank]])
            for rank, node_id in rank_to_node.items()
        }

    @staticmethod
    def _lookup_tpu_slice_labels(node_ids: Set[str]) -> Dict[str, Optional[str]]:
        """Return ``{node_id: tpu_slice_label_or_None}`` for given node IDs."""
        try:
            from ray.util.tpu import get_tpu_slice_name_from_node

            if not ray.is_initialized():
                return {}
            result: Dict[str, Optional[str]] = {}
            for node in ray.nodes():
                node_id = node.get("NodeID")
                if node_id in node_ids:
                    result[node_id] = get_tpu_slice_name_from_node(node)
            return result
        except Exception as e:
            logger.debug("TPU slice label lookup failed: %s", e)
            return {}

    @staticmethod
    def _parse_fake_node_map(env_val: Optional[str]) -> Dict[int, str]:
        """Parse 'rank:node_id,rank:node_id,...' into {rank: node_id}."""
        if not env_val:
            return {}
        result: Dict[int, str] = {}
        for entry in env_val.split(","):
            entry = entry.strip()
            if not entry:
                continue
            try:
                rank_str, node_id = entry.split(":", 1)
                result[int(rank_str)] = node_id.strip()
            except Exception as e:
                logger.warning(
                    "Skipping malformed fake-node-map entry %r: %s", entry, e
                )
        return result
