"""Preemption data structures and the watcher actor.

The :class:`PreemptionWatcher` polls Ray Core's ``get_draining_nodes`` on an
interval, maps drained node IDs to affected ranks via a failure-domain map
(TPU-slice-aware), and logs detected events. Its lifecycle is managed by
``PreemptionCallback`` (see ``callbacks/preemption_callback.py``).
"""
import logging
import threading
from dataclasses import dataclass
from typing import Callable, Dict, List, Optional, Set

import ray
from ray.train.v2._internal.constants import (
    DEFAULT_PREEMPTION_POLL_INTERVAL_S,
    PREEMPTION_WATCHER_STOP_TIMEOUT_S,
)
from ray.util.tpu import get_tpu_slice_name_from_node

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class PreemptionInfo:
    """Information about an imminent preemption event.

    Attributes:
        deadline: UNIX timestamp (seconds since epoch) by which the affected
            nodes are expected to be reclaimed, or ``0.0`` when the source
            reported no deadline.
        preempted_ranks: Worker ranks (``world_rank``) whose nodes are being
            reclaimed, expanded to all ranks in the same failure domain
            (e.g., a fate-shared TPU slice). Sorted ascending.
        preempted_node_ids: Node IDs being reclaimed, hex-encoded as returned
            by Ray Core. Sorted lexicographically.
    """

    deadline: float
    preempted_ranks: List[int]
    preempted_node_ids: List[str]


# Drain source contract: () -> {node_id_hex: deadline_ms}, where deadline_ms
# == 0 means "no deadline reported". The default reads Ray Core's GCS state;
# unit tests inject a synthetic source.
DrainSource = Callable[[], Dict[str, int]]


def _default_drain_source() -> Dict[str, int]:
    # ray._private.state.state is the module-level GlobalState singleton.
    return ray._private.state.state.get_draining_nodes()


class PreemptionWatcher:
    """Polls Ray Core for node drains and logs detected preemption events.

    One watcher per worker group, spawned as a ``num_cpus=0`` actor by
    ``PreemptionCallback``. The poll loop runs in a background thread. The
    failure-domain map is built once on construction and is immutable for the
    watcher's lifetime.

    Args:
        node_to_ranks: Map ``node_id_hex -> [ranks on that node]``. Used both
            as the set of nodes we care about (drains elsewhere are ignored)
            and as the seed for failure-domain expansion.
        poll_interval_s: Seconds between drain-state polls.
        drain_source: Override the default drain source (used by unit tests).
    """

    def __init__(
        self,
        node_to_ranks: Dict[str, List[int]],
        poll_interval_s: float = DEFAULT_PREEMPTION_POLL_INTERVAL_S,
        drain_source: Optional[DrainSource] = None,
    ):
        self._node_to_ranks: Dict[str, List[int]] = {
            nid: sorted(ranks) for nid, ranks in node_to_ranks.items()
        }
        self._poll_interval_s = poll_interval_s
        self._drain_source = drain_source or _default_drain_source
        self._failure_domain_map: Dict[str, List[int]] = self._build_failure_domain_map(
            self._node_to_ranks
        )

        self._stop_event = threading.Event()
        self._last_drained: Dict[str, int] = {}
        self._latest_info: Optional[PreemptionInfo] = None

        # Start the poll loop here (not via a separate RPC) so it resumes
        # automatically if Ray restarts the actor.
        self._monitor_thread = threading.Thread(
            target=self._watch_loop,
            name="PreemptionWatcher",
            daemon=True,
        )
        self._monitor_thread.start()

    # ------------------------------------------------------------------ #
    # Failure-domain auto-detect
    # ------------------------------------------------------------------ #

    @staticmethod
    def _build_failure_domain_map(
        node_to_ranks: Dict[str, List[int]],
    ) -> Dict[str, List[int]]:
        """Map each node we care about to all ranks in its failure domain.

        For non-TPU clusters the failure domain is just the node itself. For
        TPU multislice, every host in a slice is reclaimed atomically, so a
        drain on any host is fate-shared with the rest: we union ranks across
        all hosts sharing a slice label (Ray Core attaches one to every TPU
        node, exposed by :func:`ray.util.tpu.get_tpu_slice_name_from_node`).

        The result includes entries for slice-mates we don't host: GKE may
        surface a drain on a non-hosted slice-mate first, so we need to
        recognize those drains too.
        """
        per_node = {nid: sorted(set(ranks)) for nid, ranks in node_to_ranks.items()}

        try:
            all_nodes = ray.nodes()
        except Exception:
            logger.debug(
                "Could not read ray.nodes(); failure-domain map will be "
                "per-node (no TPU-slice expansion).",
                exc_info=True,
            )
            return per_node

        # node_id -> slice_label (or None). Includes nodes outside our worker
        # group so we can union slice-mates.
        node_to_slice: Dict[str, Optional[str]] = {
            node["NodeID"]: get_tpu_slice_name_from_node(node) for node in all_nodes
        }

        # Per slice we have at least one rank in: union of our ranks.
        slice_to_ranks: Dict[str, Set[int]] = {}
        for node_id, ranks in node_to_ranks.items():
            slice_label = node_to_slice.get(node_id)
            if slice_label:
                slice_to_ranks.setdefault(slice_label, set()).update(ranks)

        # Non-TPU cluster (or none of our nodes are on a slice): nothing to
        # expand, so the failure domain is each node on its own.
        if not slice_to_ranks:
            return per_node

        # All node IDs in slices we care about (from the full cluster view).
        slice_to_all_node_ids: Dict[str, Set[str]] = {}
        for node_id, slice_label in node_to_slice.items():
            if slice_label and slice_label in slice_to_ranks:
                slice_to_all_node_ids.setdefault(slice_label, set()).add(node_id)

        # WG nodes map to their per-slice union (or own ranks if non-TPU);
        # then add the slice-mates we don't host, pointing at the same union.
        result: Dict[str, List[int]] = {}
        for node_id, ranks in node_to_ranks.items():
            slice_label = node_to_slice.get(node_id)
            if slice_label and slice_label in slice_to_ranks:
                result[node_id] = sorted(slice_to_ranks[slice_label])
            else:
                result[node_id] = sorted(set(ranks))
        for slice_label, all_node_ids in slice_to_all_node_ids.items():
            ranks_union = sorted(slice_to_ranks[slice_label])
            for node_id in all_node_ids:
                result.setdefault(node_id, ranks_union)
        return result

    # ------------------------------------------------------------------ #
    # Public API (exposed via the actor handle)
    # ------------------------------------------------------------------ #

    def stop(self) -> None:
        """Signal the poll loop to exit and wait for the thread to join."""
        self._stop_event.set()
        if self._monitor_thread is not None:
            self._monitor_thread.join(timeout=PREEMPTION_WATCHER_STOP_TIMEOUT_S)
            self._monitor_thread = None

    def get_latest_info(self) -> Optional[PreemptionInfo]:
        """Most recent :class:`PreemptionInfo` observed, or ``None``."""
        return self._latest_info

    # ------------------------------------------------------------------ #
    # Internal
    # ------------------------------------------------------------------ #

    def _watch_loop(self) -> None:
        logger.debug(
            "PreemptionWatcher polling %d node(s) every %.1fs.",
            len(self._node_to_ranks),
            self._poll_interval_s,
        )
        while not self._stop_event.is_set():
            try:
                drained = self._drain_source()
                # Only consider nodes in our failure domains; cluster-wide
                # drains for other workloads are irrelevant.
                relevant = {
                    n: d for n, d in drained.items() if n in self._failure_domain_map
                }
                if relevant != self._last_drained:
                    self._on_drain_change(relevant)
                    self._last_drained = relevant
            except Exception:
                logger.warning("PreemptionWatcher poll failed", exc_info=True)
            self._stop_event.wait(timeout=self._poll_interval_s)
        logger.debug("PreemptionWatcher stopped.")

    def _on_drain_change(self, drained: Dict[str, int]) -> None:
        """Handle a change in the (already-filtered) drained-node set."""
        if not drained:
            return

        affected_node_ids = sorted(drained.keys())
        affected_ranks = sorted(
            {
                r
                for node_id in affected_node_ids
                for r in self._failure_domain_map[node_id]
            }
        )

        # Earliest non-zero deadline wins; 0.0 means "unknown" (Ray Core's
        # deadline_ms=0 convention) when no node reported a deadline.
        nonzero_deadlines = [drained[n] for n in affected_node_ids if drained[n] > 0]
        deadline = min(nonzero_deadlines) / 1000.0 if nonzero_deadlines else 0.0

        info = PreemptionInfo(
            deadline=deadline,
            preempted_ranks=affected_ranks,
            preempted_node_ids=affected_node_ids,
        )
        self._latest_info = info

        logger.warning(
            "PreemptionWatcher: drain detected — "
            "preempted_node_ids=%s, preempted_ranks=%s, deadline=%s",
            affected_node_ids,
            affected_ranks,
            deadline,
        )
        # TODO(stage 2): fan out mark_preempt(info) to the worker actors here
        # so the UDF can read it via ray.train.preemption_status().
