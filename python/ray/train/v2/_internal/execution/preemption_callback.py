"""PreemptionCallback: polls Ray Core's drain-node signal and fans
``PreemptionInfo`` out to affected workers.

Lifecycle is tied to the worker group: polling starts in
``after_worker_group_start`` and stops in ``before_worker_group_shutdown``.

The drain source is injectable so unit/integration tests can simulate drains
without going through real Ray Core APIs. The default source calls
``ray._private.state.state.get_draining_nodes()``.

Design rules implemented here (see preemption_controller_state_design.docx):

  * Rule 3: TorchFT-aware fan-out. When ``manages_replica_groups`` is True,
    ``mark_preempt`` is sent only to ranks in the affected replica group;
    survivor RG members are not signaled and keep training transparently.
"""

import asyncio
import json
import logging
import os
import time
from collections import defaultdict
from dataclasses import replace
from typing import TYPE_CHECKING, Callable, Dict, List, Optional, Set

from ray.train.v2._internal.execution.callback import WorkerGroupCallback
from ray.train.v2._internal.execution.preemption import (
    PreemptionInfo,
    make_preemption_info_for_worker,
)

# Default bounded-wait deadline (seconds) when Ray Core reports a drain
# but no deadline (deadline_ms=0). Set to "inf" / "none" to disable the
# bounded wait and revert to the unbounded behavior.
_DEFAULT_DEADLINE_ENV = "RAY_TRAIN_PREEMPTION_DEFAULT_DEADLINE_S"
_DEFAULT_DEADLINE_FALLBACK_S = 60.0

# Test-only env var: when set to a filesystem path, the default drain source
# reads from that JSON file ({node_id_hex: deadline_ms}) instead of calling
# Ray Core. The path must be readable by both the driver and the controller
# actor (same node /tmp works). NEVER set this in production.
_FAKE_DRAINS_FILE_ENV = "RAY_TRAIN_PREEMPTION_FAKE_DRAINS_FILE"

# Test-only env var: comma-separated "rank:fake_node_id" entries that override
# the per-worker node_id lookup when building the failure-domain map. Lets a
# single-node local cluster simulate a multi-node deployment so the survivor
# branch can be exercised end-to-end.
#
# Example:
#   RAY_TRAIN_PREEMPTION_FAKE_NODE_MAP="0:nodeA,1:nodeB"
#   FAKE_DRAINS file = {"nodeB": <deadline_ms>}
#   → rank 1 becomes preempted, rank 0 stays as survivor.
#
# NEVER set this in production.
_FAKE_NODE_MAP_ENV = "RAY_TRAIN_PREEMPTION_FAKE_NODE_MAP"

if TYPE_CHECKING:
    from ray.actor import ActorHandle
    from ray.train.v2._internal.execution.worker_group.worker_group import WorkerGroup


logger = logging.getLogger(__name__)


# Type alias: () -> {node_id_hex: deadline_ms}. deadline_ms=0 means
# "no deadline reported by Core."
DrainSource = Callable[[], Dict[str, int]]


def _default_drain_source() -> Dict[str, int]:
    """Default drain source.

    For tests: if ``RAY_TRAIN_PREEMPTION_FAKE_DRAINS_FILE`` is set and the
    file exists, read fake drains from it as JSON. This is the recommended
    way to inject drains in local Ray Train integration tests because the
    file path crosses the driver / controller-actor process boundary
    (monkey-patching the module attribute only affects the driver).

    For production: calls Ray Core's ``get_draining_nodes()``.
    """
    test_file = os.environ.get(_FAKE_DRAINS_FILE_ENV)
    if test_file and os.path.exists(test_file):
        try:
            with open(test_file) as f:
                data = json.load(f)
            # Coerce values to int in case JSON serializes them as strings.
            return {k: int(v) for k, v in data.items()}
        except Exception as e:
            logger.warning(
                "Failed to read fake drains file %s: %s", test_file, e
            )

    import ray._private.state as state

    try:
        return state.state.get_draining_nodes()
    except Exception as e:  # pragma: no cover -- defensive
        logger.warning("get_draining_nodes() failed: %s", e)
        return {}


class PreemptionCallback(WorkerGroupCallback):
    """Polls Ray Core for draining nodes and routes a PreemptionInfo to
    affected workers + the controller.

    Args:
        controller_actor: Handle to the TrainController actor (used to call
            ``notify_preempting`` when a drain is observed).
        manages_replica_groups: Whether the worker group is managing replica
            groups (TorchFT integration). If True, only signal affected
            ranks (Rule 3); otherwise signal everyone.
        poll_interval_s: Seconds between drain-source polls.
        drain_source: Callable returning ``{node_id_hex: deadline_ms}``.
            Defaults to Ray Core's get_draining_nodes(). Override for tests.
        failure_domain: How to group affected ranks. Options:
            - "node": one rank per node (default; correct for GPU/CPU clusters).
            - "tpu_slice": all ranks within the same TPU slice are fate-shared;
              a single host drain implies the whole slice.
    """

    def __init__(
        self,
        controller_actor: "ActorHandle",
        manages_replica_groups: bool = False,
        poll_interval_s: float = 5.0,
        drain_source: Optional[DrainSource] = None,
        failure_domain: str = "node",
    ):
        if failure_domain not in ("node", "tpu_slice"):
            raise ValueError(
                f"Invalid failure_domain={failure_domain!r}; "
                "must be 'node' or 'tpu_slice'."
            )
        self._controller_actor = controller_actor
        self._manages_replica_groups = manages_replica_groups
        self._poll_interval_s = poll_interval_s
        self._drain_source: DrainSource = drain_source or _default_drain_source
        self._failure_domain = failure_domain

        self._wg: Optional["WorkerGroup"] = None
        self._poll_task: Optional[asyncio.Task] = None
        self._last_drained: Dict[str, int] = {}
        # node_id -> list of ranks in the same failure domain as that node.
        # Built when the worker group starts.
        self._node_to_domain_ranks: Dict[str, List[int]] = {}

    # ─── Lifecycle hooks (WorkerGroupCallback) ────────────────────────────

    def after_worker_group_start(self, worker_group: "WorkerGroup"):
        self._wg = worker_group
        self._node_to_domain_ranks = self._build_failure_domain_map(worker_group)
        # Start polling. Use the asyncio loop running the controller.
        try:
            loop = asyncio.get_event_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
        self._poll_task = loop.create_task(self._poll_loop())
        n_workers = len(worker_group.get_workers())
        n_nodes = len(set(self._node_to_domain_ranks.keys()))
        test_file = os.environ.get(_FAKE_DRAINS_FILE_ENV)
        fake_map_env = os.environ.get(_FAKE_NODE_MAP_ENV)
        logger.info(
            "PreemptionCallback started: poll_interval=%.1fs, "
            "manages_replica_groups=%s, failure_domain=%s, "
            "nodes=%d, workers=%d, drain_source=%s, "
            "fake_node_map=%s",
            self._poll_interval_s,
            self._manages_replica_groups,
            self._failure_domain,
            n_nodes,
            n_workers,
            f"fake_file={test_file}" if test_file else "ray_core",
            fake_map_env if fake_map_env else "(none)",
        )
        # Make domain map visible at startup so misconfigured fake_node_map
        # is obvious in the logs.
        logger.info(
            "PreemptionCallback domain map: %s",
            dict(sorted(self._node_to_domain_ranks.items())),
        )

    def before_worker_group_shutdown(self, worker_group: "WorkerGroup"):
        if self._poll_task is not None:
            self._poll_task.cancel()
            self._poll_task = None
        self._wg = None
        self._last_drained = {}
        self._node_to_domain_ranks = {}

    # ─── Internals ────────────────────────────────────────────────────────

    def _build_failure_domain_map(
        self, wg: "WorkerGroup"
    ) -> Dict[str, List[int]]:
        """Return {node_id: ranks_in_same_failure_domain}.

        For ``failure_domain="node"``: each node maps to just the ranks
        physically on that node.
        For ``failure_domain="tpu_slice"``: ranks are grouped by TPU slice id
        (read from the worker's resources / metadata); a node maps to *all*
        ranks in its slice.

        Test override: if ``RAY_TRAIN_PREEMPTION_FAKE_NODE_MAP`` is set,
        per-worker ``node_id`` is replaced with the fake mapping. Lets a
        single-node local cluster simulate multi-node topology.
        """
        workers = wg.get_workers()
        if not workers:
            return {}

        fake_node_map = self._parse_fake_node_map(
            os.environ.get(_FAKE_NODE_MAP_ENV)
        )

        # First pass: figure out each rank's domain key.
        rank_to_domain: Dict[int, str] = {}
        rank_to_node: Dict[int, str] = {}
        for w in workers:
            rank = w.distributed_context.world_rank
            real_node_id = w.metadata.node_id
            node_id = fake_node_map.get(rank, real_node_id)
            rank_to_node[rank] = node_id
            if self._failure_domain == "tpu_slice":
                # TPU slice id is exposed as a custom resource label, e.g.
                # "TPU-v6e-8-head" or via metadata. Fall back to node_id if
                # not present (degenerates to per-node behavior).
                slice_id = self._extract_tpu_slice_id(w) or node_id
                rank_to_domain[rank] = f"tpu_slice/{slice_id}"
            else:
                rank_to_domain[rank] = node_id

        if fake_node_map:
            logger.warning(
                "PreemptionCallback using fake node map (test mode): %s",
                fake_node_map,
            )

        # Second pass: invert to {domain: [ranks]}.
        domain_to_ranks: Dict[str, List[int]] = defaultdict(list)
        for rank, domain in rank_to_domain.items():
            domain_to_ranks[domain].append(rank)

        # Result: {node_id: ranks_in_its_domain}.
        return {
            node_id: sorted(domain_to_ranks[rank_to_domain[rank]])
            for rank, node_id in rank_to_node.items()
        }

    @staticmethod
    def _resolve_deadline(deadlines_ms: List[int]) -> float:
        """Return the UNIX-seconds deadline to use for a drain event.

        If Ray Core reported any non-zero deadlines, use the earliest one
        (cross-rank-conservative — once the first preempted host is gone,
        cross-rank collectives can hang).

        If Ray Core reported only zeros (it doesn't know when eviction will
        happen), fall back to a bounded default rather than waiting forever.
        Without a bound, a UDF that acknowledges the signal but never exits
        would keep the controller stuck in PreemptingState indefinitely.
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
                _DEFAULT_DEADLINE_ENV, raw, int(_DEFAULT_DEADLINE_FALLBACK_S),
            )
            return time.time() + _DEFAULT_DEADLINE_FALLBACK_S

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

    @staticmethod
    def _extract_tpu_slice_id(worker) -> Optional[str]:
        """Pull TPU slice id from worker resources. Best-effort; returns
        None if no slice label is found.

        We treat any resource key starting with ``TPU-`` as a slice
        identifier. Workers on the same slice share the same key (this is
        how Ray TPU gang scheduling already groups them). If multiple TPU
        keys exist, the first sorted one wins (deterministic).
        """
        resources = worker.resources or {}
        tpu_keys = sorted(k for k in resources if k.startswith("TPU-"))
        return tpu_keys[0] if tpu_keys else None

    async def _poll_loop(self):
        """Periodically poll the drain source. On change, fan out via
        ``_on_drain_change``."""
        try:
            while True:
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
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    logger.warning("Preemption poll failed: %s", e)
                await asyncio.sleep(self._poll_interval_s)
        except asyncio.CancelledError:
            logger.info("PreemptionCallback poll loop cancelled.")

    def _on_drain_change(self, drained: Dict[str, int]) -> None:
        """Compute affected ranks and the deadline, then fan out
        ``mark_preempt`` and notify the controller."""
        if self._wg is None:
            return

        affected_ranks: Set[int] = set()
        affected_nodes: Set[str] = set()
        deadlines_ms: List[int] = []

        for node_id, deadline_ms in drained.items():
            ranks_in_domain = self._node_to_domain_ranks.get(node_id)
            if ranks_in_domain is None:
                # Drained node is not part of this worker group.
                continue
            affected_ranks.update(ranks_in_domain)
            affected_nodes.add(node_id)
            if deadline_ms > 0:
                deadlines_ms.append(deadline_ms)

        if not affected_ranks:
            # The drained nodes are not part of this worker group. Common
            # causes: stale Jupyter import of ENV_VARS_TO_PROPAGATE so
            # RAY_TRAIN_PREEMPTION_FAKE_NODE_MAP didn't reach the
            # controller; or fake_drains pointing at a node id that
            # isn't in our domain map.
            logger.warning(
                "Drain detected on nodes %s but none belong to this worker "
                "group (domain map keys: %s). No mark_preempt will fire. "
                "If you're testing locally, check that the env var "
                "RAY_TRAIN_PREEMPTION_FAKE_NODE_MAP propagated to the "
                "controller (restart Jupyter kernel if needed).",
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
            this_worker_preempted=False,  # set per-worker below
        )

        # Tell the controller first so it can transition to PreemptingState
        # on its next tick.
        try:
            self._controller_actor.notify_preempting.remote(info)
        except Exception as e:
            logger.warning("Failed to notify controller of preemption: %s", e)

        # Choose target workers per Rule 3 (TorchFT-aware fan-out).
        if self._manages_replica_groups:
            target_workers = [
                w for w in self._wg.get_workers()
                if w.distributed_context.world_rank in affected_ranks
            ]
            logger.info(
                "TorchFT mode: signaling only affected ranks %s (not survivors)",
                sorted(affected_ranks),
            )
        else:
            target_workers = self._wg.get_workers()

        # Fan out per-worker (each worker gets its own this_worker_preempted).
        for worker in target_workers:
            rank = worker.distributed_context.world_rank
            per_worker = make_preemption_info_for_worker(info, rank)
            try:
                worker.actor.mark_preempt.remote(per_worker)
            except Exception as e:
                logger.warning(
                    "Failed to send mark_preempt to rank %d: %s", rank, e
                )
